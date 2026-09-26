//! Línea WebSocket redundante.
//!
//! Cada línea es una conexión independiente que entrega eventos normalizados a
//! un `FrameSink`. Con dos o más líneas por stream, el motor arbitra por id
//! (primero que llega gana) y un corte en una línea no produce hueco.
//! Rotación programada antes del corte de 24 h de Binance, escalonada entre
//! líneas para que nunca caigan juntas; watchdog de silencio; backoff con jitter.

use futures_util::{SinkExt, StreamExt};
use lu_core::{wall_now_ns, MarketEvent, RxStamp};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio_tungstenite::{connect_async_tls_with_config, tungstenite::Message, Connector};

/// Destino de los eventos de una línea.
pub trait FrameSink: Send + Sync + 'static {
    /// Entrega un evento; `false` si fue descartado (cola llena ⇒ el motor detectará el hueco).
    fn deliver(&self, ev: MarketEvent) -> bool;
    /// La línea quedó conectada.
    fn line_up(&self, line: u8);
    /// La línea se desconectó.
    fn line_down(&self, line: u8, reason: &'static str);
    /// Frame imposible de parsear.
    fn parse_error(&self, line: u8, err: &str);
}

/// Especificación de una línea.
#[derive(Debug, Clone)]
pub struct LineSpec {
    /// Etiqueta para logs.
    pub label: String,
    /// Índice de línea (0 = A, 1 = B, ...).
    pub line: u8,
    /// URL completa del stream combinado.
    pub url: String,
    /// URLs alternativas (otro host, mismo stream). Ante un rechazo regional o de
    /// acceso (HTTP 451/403) o 3 fallos seguidos, la línea rota a la siguiente.
    pub fallback_urls: Vec<String>,
    /// Vida máxima de la conexión antes de rotarla.
    pub max_age: Duration,
    /// Silencio máximo tolerado antes de reconectar.
    pub idle_timeout: Duration,
    /// Configuración TLS compartida.
    pub tls: Arc<rustls::ClientConfig>,
}

impl LineSpec {
    /// Línea con parámetros institucionales por defecto: rotación a las 23 h menos
    /// un escalón de 3 h por índice (A: 23 h, B: 20 h, ...), watchdog de 10 s.
    pub fn new(
        label: impl Into<String>,
        line: u8,
        url: impl Into<String>,
        tls: Arc<rustls::ClientConfig>,
    ) -> Self {
        let hours = 23u64.saturating_sub(3 * u64::from(line)).max(6);
        Self {
            label: label.into(),
            line,
            url: url.into(),
            fallback_urls: Vec::new(),
            max_age: Duration::from_secs(hours * 3600),
            idle_timeout: Duration::from_secs(10),
            tls,
        }
    }
}

impl LineSpec {
    /// Agrega URLs alternativas.
    pub fn with_fallbacks(mut self, urls: Vec<String>) -> Self {
        self.fallback_urls = urls;
        self
    }
}

fn jitter(max_ms: u64) -> Duration {
    Duration::from_millis(wall_now_ns() % max_ms.max(1))
}

/// Ejecuta la línea hasta que `shutdown` pase a `true`.
pub async fn run_line(
    spec: LineSpec,
    sink: Arc<dyn FrameSink>,
    mut shutdown: watch::Receiver<bool>,
) {
    let base = Duration::from_millis(250);
    let cap = Duration::from_secs(30);
    let mut backoff = base;
    let urls: Vec<String> = std::iter::once(spec.url.clone())
        .chain(spec.fallback_urls.iter().cloned())
        .collect();
    let mut idx = 0usize;
    let mut failures = 0u32;
    loop {
        if *shutdown.borrow() {
            return;
        }
        let url = urls[idx].as_str();
        tracing::info!(line = spec.line, label = %spec.label, url = %url, "conectando");
        let connector = Connector::Rustls(spec.tls.clone());
        let connect = connect_async_tls_with_config(url, None, true, Some(connector));
        match tokio::time::timeout(Duration::from_secs(10), connect).await {
            Ok(Ok((ws, _))) => {
                sink.line_up(spec.line);
                backoff = base;
                failures = 0;
                let (mut tx, mut rx) = ws.split();
                let deadline = tokio::time::Instant::now() + spec.max_age;
                let reason: &'static str = loop {
                    tokio::select! {
                        biased;
                        _ = shutdown.changed() => {
                            let _ = tx.send(Message::Close(None)).await;
                            break "apagado";
                        }
                        _ = tokio::time::sleep_until(deadline) => {
                            let _ = tx.send(Message::Close(None)).await;
                            break "rotación programada";
                        }
                        r = tokio::time::timeout(spec.idle_timeout, rx.next()) => match r {
                            Err(_) => break "línea muda (watchdog)",
                            Ok(None) => break "cerrada por el servidor",
                            Ok(Some(Err(e))) => {
                                tracing::warn!(line = spec.line, label = %spec.label, error = %e, "error de socket");
                                break "error de socket";
                            }
                            Ok(Some(Ok(Message::Text(t)))) => {
                                let stamp = RxStamp::now(spec.line);
                                match crate::wire::parse_frame(t.as_str(), stamp) {
                                    Ok(MarketEvent::Ignored) => {}
                                    Ok(ev) => { let _ = sink.deliver(ev); }
                                    Err(e) => sink.parse_error(spec.line, &e.to_string()),
                                }
                            }
                            Ok(Some(Ok(Message::Ping(p)))) => {
                                // tungstenite ya encola el pong; lo enviamos explícito para no depender del flush.
                                let _ = tx.send(Message::Pong(p)).await;
                            }
                            Ok(Some(Ok(Message::Close(_)))) => break "close del servidor",
                            Ok(Some(Ok(_))) => {}
                        }
                    }
                };
                sink.line_down(spec.line, reason);
                if reason == "apagado" {
                    return;
                }
                if reason == "rotación programada" {
                    continue; // reconexión inmediata: la otra línea cubre el intervalo
                }
            }
            Ok(Err(e)) => {
                tracing::warn!(line = spec.line, label = %spec.label, error = %e, "fallo de conexión");
                sink.line_down(spec.line, "fallo de conexión");
                failures += 1;
                let msg = e.to_string();
                if urls.len() > 1 && (msg.contains("451") || msg.contains("403") || failures >= 3) {
                    idx = (idx + 1) % urls.len();
                    failures = 0;
                    backoff = base;
                    tracing::warn!(line = spec.line, label = %spec.label, url = %urls[idx], "rotando a host alternativo");
                }
            }
            Err(_) => {
                sink.line_down(spec.line, "timeout de conexión");
                failures += 1;
                if urls.len() > 1 && failures >= 3 {
                    idx = (idx + 1) % urls.len();
                    failures = 0;
                    tracing::warn!(line = spec.line, label = %spec.label, url = %urls[idx], "rotando a host alternativo");
                }
            }
        }
        let wait = backoff + jitter(250);
        tokio::select! {
            _ = shutdown.changed() => return,
            _ = tokio::time::sleep(wait) => {}
        }
        backoff = (backoff * 2).min(cap);
    }
}

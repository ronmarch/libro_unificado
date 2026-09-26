//! Línea WebSocket redundante, genérica por protocolo.
//!
//! Cada línea es una conexión independiente que entrega eventos normalizados a
//! un `FrameSink`. Con dos o más líneas por stream, el motor arbitra por id
//! (primero que llega gana) y un corte en una línea no produce hueco.
//! Rotación programada escalonada entre líneas para que nunca caigan juntas;
//! watchdog de silencio; backoff con jitter; hosts alternativos ante bloqueo
//! (HTTP 451/403) o fallos repetidos; latido de aplicación y re-suscripción
//! (snapshot nuevo sin reconectar) cuando el protocolo los define.

use futures_util::{SinkExt, StreamExt};
use lu_core::{wall_now_ns, MarketEvent, RxStamp};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
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

/// Particularidades de un venue sobre la conexión.
pub trait Protocol: Send + Sync + 'static {
    /// Mensajes de texto a enviar tras conectar (suscripción). Vacío si la URL ya suscribe.
    fn on_connect(&self) -> Vec<String> {
        Vec::new()
    }
    /// Mensajes para obtener un snapshot nuevo sin reconectar. `None` ⇒ se reconecta.
    fn resubscribe(&self) -> Option<Vec<String>> {
        None
    }
    /// Latido de aplicación: cada cuánto y qué texto enviar.
    fn keepalive(&self) -> Option<(Duration, &'static str)> {
        None
    }
    /// Parsea un frame de texto y agrega 0..n eventos a `out`.
    fn parse(&self, text: &str, rx: RxStamp, out: &mut Vec<MarketEvent>) -> Result<(), String>;
    /// El protocolo detectó que el estado de ESTA conexión es inválido (p. ej. hueco en
    /// la secuencia por conexión) y pide un snapshot nuevo. Se consulta tras cada frame.
    fn take_resync(&self) -> bool {
        false
    }
}

/// Órdenes a una línea en marcha.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineCmd {
    /// Obtener un snapshot nuevo (re-suscripción o reconexión según el protocolo).
    Resnapshot,
}

/// Especificación de una línea.
#[derive(Debug, Clone)]
pub struct LineSpec {
    /// Etiqueta para logs.
    pub label: String,
    /// Índice de línea (0 = A, 1 = B, ...).
    pub line: u8,
    /// URL del stream.
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
    proto: Arc<dyn Protocol>,
    sink: Arc<dyn FrameSink>,
    mut cmds: Option<mpsc::UnboundedReceiver<LineCmd>>,
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
    let keepalive = proto.keepalive();
    let mut out: Vec<MarketEvent> = Vec::with_capacity(8);
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
                let (mut tx, mut rx) = ws.split();
                let mut subscribed = true;
                for m in proto.on_connect() {
                    if tx.send(Message::text(m)).await.is_err() {
                        subscribed = false;
                        break;
                    }
                }
                if !subscribed {
                    sink.line_down(spec.line, "fallo al suscribir");
                } else {
                    sink.line_up(spec.line);
                    backoff = base;
                    failures = 0;
                    // Una conexión nueva ya trae snapshot: los pedidos previos sobran.
                    if let Some(c) = cmds.as_mut() {
                        while c.try_recv().is_ok() {}
                    }
                    let deadline = tokio::time::Instant::now() + spec.max_age;
                    let ka_every = keepalive.map_or(Duration::from_secs(3600), |k| k.0);
                    let mut ka = tokio::time::interval(ka_every);
                    ka.tick().await;
                    let reason: &'static str = loop {
                        let cmd = async {
                            match cmds.as_mut() {
                                Some(c) => c.recv().await,
                                None => std::future::pending().await,
                            }
                        };
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
                            c = cmd => match c {
                                Some(LineCmd::Resnapshot) => match proto.resubscribe() {
                                    Some(msgs) => {
                                        tracing::info!(line = spec.line, label = %spec.label, "re-suscripción para snapshot nuevo");
                                        let mut ok = true;
                                        for m in msgs {
                                            ok &= tx.send(Message::text(m)).await.is_ok();
                                        }
                                        if !ok {
                                            break "error al re-suscribir";
                                        }
                                    }
                                    None => {
                                        let _ = tx.send(Message::Close(None)).await;
                                        break "reconexión para snapshot";
                                    }
                                },
                                None => cmds = None,
                            },
                            _ = ka.tick(), if keepalive.is_some() => {
                                if let Some((_, txt)) = keepalive {
                                    if tx.send(Message::text(txt)).await.is_err() {
                                        break "error de latido";
                                    }
                                }
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
                                    out.clear();
                                    match proto.parse(t.as_str(), stamp, &mut out) {
                                        Ok(()) => {
                                            for ev in out.drain(..) {
                                                if !matches!(ev, MarketEvent::Ignored) {
                                                    let _ = sink.deliver(ev);
                                                }
                                            }
                                        }
                                        Err(e) => sink.parse_error(spec.line, &e),
                                    }
                                    if proto.take_resync() {
                                        match proto.resubscribe() {
                                            Some(msgs) => {
                                                tracing::warn!(line = spec.line, label = %spec.label, "estado de la conexión inválido (hueco de secuencia o checksum): re-suscripción");
                                                let mut ok = true;
                                                for m in msgs {
                                                    ok &= tx.send(Message::text(m)).await.is_ok();
                                                }
                                                if !ok {
                                                    break "error al re-suscribir";
                                                }
                                            }
                                            None => break "estado de la conexión inválido",
                                        }
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
                    if reason == "rotación programada" || reason == "reconexión para snapshot" {
                        continue; // reconexión inmediata: la otra línea cubre el intervalo
                    }
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

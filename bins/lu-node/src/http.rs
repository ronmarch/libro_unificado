//! Servidor HTTP mínimo de observabilidad (sin framework: superficie de ataque
//! mínima; solo GET, respuesta completa, conexión cerrada).
//!
//! * `/health`  — estado de cada mercado (200 siempre que el proceso viva).
//! * `/ready`   — 200 solo si TODOS los libros están `Live`; si no, 503.
//! * `/metrics` — Prometheus.
//! * `/book/<spot|perp>` — vista JSON completa del mercado.

use crate::view::{render_prometheus, BookView};
use arc_swap::ArcSwap;
use lu_book::Phase;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

/// Vistas registradas: (ruta corta, vista).
pub type Registry = Arc<Vec<(String, Arc<ArcSwap<BookView>>)>>;

/// Sirve hasta el apagado.
pub async fn serve(listener: TcpListener, reg: Registry, mut shutdown: watch::Receiver<bool>) {
    loop {
        tokio::select! {
            _ = shutdown.changed() => return,
            acc = listener.accept() => match acc {
                Ok((sock, _)) => {
                    let reg = reg.clone();
                    tokio::spawn(async move {
                        let _ = tokio::time::timeout(Duration::from_secs(5), handle(sock, reg)).await;
                    });
                }
                Err(e) => tracing::warn!(error = %e, "accept falló"),
            }
        }
    }
}

async fn handle(mut sock: TcpStream, reg: Registry) -> std::io::Result<()> {
    let mut buf = [0u8; 2048];
    let mut n = 0usize;
    while n < buf.len() {
        let r = sock.read(&mut buf[n..]).await?;
        if r == 0 {
            break;
        }
        n += r;
        if buf[..n].windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }
    let req = String::from_utf8_lossy(&buf[..n]);
    let mut parts = req.lines().next().unwrap_or("").split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("/");
    let (code, ctype, body) = if method != "GET" {
        (405, "text/plain", "solo GET\n".to_string())
    } else {
        route(path, &reg)
    };
    let reason = match code {
        200 => "OK",
        404 => "Not Found",
        405 => "Method Not Allowed",
        _ => "Service Unavailable",
    };
    let head = format!(
        "HTTP/1.1 {code} {reason}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
        body.len()
    );
    sock.write_all(head.as_bytes()).await?;
    sock.write_all(body.as_bytes()).await?;
    sock.shutdown().await
}

fn route(path: &str, reg: &Registry) -> (u16, &'static str, String) {
    let views: Vec<Arc<BookView>> = reg.iter().map(|(_, v)| v.load_full()).collect();
    match path {
        "/metrics" => (200, "text/plain; version=0.0.4", render_prometheus(&views)),
        "/health" | "/ready" => {
            let all_live = views.iter().all(|v| v.phase == Phase::Live);
            let body = serde_json::json!({
                "status": if all_live { "live" } else { "syncing" },
                "markets": views.iter().map(|v| serde_json::json!({
                    "market": v.market,
                    "phase": v.phase,
                    "book_age_ms": v.book_age_ms,
                    "epoch": v.epoch,
                    "resyncs": v.sync.resyncs(),
                })).collect::<Vec<_>>(),
            });
            let code = if path == "/ready" && !all_live {
                503
            } else {
                200
            };
            (code, "application/json", format!("{body}\n"))
        }
        p if p.starts_with("/book/") => {
            let key = &p["/book/".len()..];
            match reg.iter().find(|(k, _)| k == key) {
                Some((_, v)) => {
                    let body = serde_json::to_string_pretty(&*v.load_full()).unwrap_or_default();
                    (200, "application/json", body + "\n")
                }
                None => (404, "text/plain", "mercado no registrado\n".into()),
            }
        }
        _ => (
            404,
            "text/plain",
            "rutas: /health /ready /metrics /book/spot /book/perp\n".into(),
        ),
    }
}

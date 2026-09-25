//! Servidor HTTP mínimo de observabilidad y API en vivo (sin framework:
//! superficie de ataque mínima; solo GET; respuesta completa y conexión cerrada,
//! salvo la mejora a WebSocket).
//!
//! * `/health`  — estado de cada mercado (200 siempre que el proceso viva).
//! * `/ready`   — 200 solo si TODOS los libros están `Live`; si no, 503.
//! * `/metrics` — Prometheus.
//! * `/book/<spot|perp>` — vista JSON completa del mercado (incluye F2).
//! * `/footprint/<spot|perp>` — F3: velas footprint en curso y cerradas, muros retirados.
//! * `/cvd` — CVD del libro spot + perp (5 pares simétricos alrededor del precio).
//! * `/ws?markets=spot,perp&interval_ms=250` — F4: flujo en vivo (ver `ws.rs`).
//! * `/` y `/ui` — F4: interfaz web autocontenida.

use crate::view::{render_prometheus, render_sim, BookView, Views};
use crate::ws;
use lu_book::Phase;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

/// Vistas registradas: (ruta corta, vistas).
pub type Registry = Arc<Vec<(String, Views)>>;

/// Máximo de sesiones WebSocket simultáneas.
const MAX_WS: usize = 16;

const UI: &str = include_str!("ui.html");

/// Petición ya leída.
struct Request {
    method: String,
    path: String,
    query: String,
    ws_key: Option<String>,
}

/// Sirve hasta el apagado.
pub async fn serve(listener: TcpListener, reg: Registry, shutdown: watch::Receiver<bool>) {
    let ws_count = Arc::new(AtomicUsize::new(0));
    let mut sd = shutdown.clone();
    loop {
        tokio::select! {
            _ = sd.changed() => return,
            acc = listener.accept() => match acc {
                Ok((sock, _)) => {
                    let (reg, shutdown, ws_count) = (reg.clone(), shutdown.clone(), ws_count.clone());
                    tokio::spawn(async move {
                        let _ = handle(sock, reg, shutdown, ws_count).await;
                    });
                }
                Err(e) => tracing::warn!(error = %e, "accept falló"),
            }
        }
    }
}

async fn read_request(sock: &mut TcpStream) -> std::io::Result<Request> {
    let mut buf = [0u8; 4096];
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
    let mut lines = req.lines();
    let mut parts = lines.next().unwrap_or("").split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let target = parts.next().unwrap_or("/");
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let mut upgrade = false;
    let mut ws_key = None;
    for l in lines {
        let Some((k, v)) = l.split_once(':') else {
            continue;
        };
        let (k, v) = (k.trim().to_ascii_lowercase(), v.trim());
        if k == "upgrade" && v.eq_ignore_ascii_case("websocket") {
            upgrade = true;
        } else if k == "sec-websocket-key" {
            ws_key = Some(v.to_string());
        }
    }
    Ok(Request {
        method,
        path: path.to_string(),
        query: query.to_string(),
        ws_key: if upgrade { ws_key } else { None },
    })
}

async fn handle(
    mut sock: TcpStream,
    reg: Registry,
    shutdown: watch::Receiver<bool>,
    ws_count: Arc<AtomicUsize>,
) -> std::io::Result<()> {
    let req = match tokio::time::timeout(Duration::from_secs(5), read_request(&mut sock)).await {
        Ok(r) => r?,
        Err(_) => return Ok(()),
    };
    if req.method == "GET" && req.path == "/ws" {
        if let Some(key) = req.ws_key {
            if ws_count.fetch_add(1, Ordering::AcqRel) >= MAX_WS {
                ws_count.fetch_sub(1, Ordering::AcqRel);
                return respond(&mut sock, 503, "text/plain", "demasiadas sesiones\n").await;
            }
            let r = ws::session(sock, &key, &req.query, reg, shutdown).await;
            ws_count.fetch_sub(1, Ordering::AcqRel);
            return r;
        }
    }
    let (code, ctype, body) = if req.method != "GET" {
        (405, "text/plain", "solo GET\n".to_string())
    } else {
        route(&req.path, &reg)
    };
    tokio::time::timeout(
        Duration::from_secs(5),
        respond(&mut sock, code, ctype, &body),
    )
    .await
    .unwrap_or(Ok(()))
}

async fn respond(sock: &mut TcpStream, code: u16, ctype: &str, body: &str) -> std::io::Result<()> {
    let reason = match code {
        200 => "OK",
        404 => "Not Found",
        405 => "Method Not Allowed",
        _ => "Service Unavailable",
    };
    let head = format!(
        "HTTP/1.1 {code} {reason}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\n\r\n",
        body.len()
    );
    sock.write_all(head.as_bytes()).await?;
    sock.write_all(body.as_bytes()).await?;
    sock.shutdown().await
}

fn route(path: &str, reg: &Registry) -> (u16, &'static str, String) {
    let views: Vec<Arc<BookView>> = reg.iter().map(|(_, v)| v.book.load_full()).collect();
    let find = |prefix: &str| {
        let key = &path[prefix.len()..];
        reg.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone())
    };
    match path {
        "/" | "/ui" => (200, "text/html; charset=utf-8", UI.to_string()),
        "/metrics" => (
            200,
            "text/plain; version=0.0.4",
            render_prometheus(&views) + &render_sim(reg),
        ),
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
        p if p.starts_with("/book/") => match find("/book/") {
            Some(v) => {
                let body = serde_json::to_string_pretty(&*v.book.load_full()).unwrap_or_default();
                (200, "application/json", body + "\n")
            }
            None => (404, "text/plain", "mercado no registrado\n".into()),
        },
        "/cvd" => match crate::cvd::compute(reg) {
            Some(c) => (
                200,
                "application/json",
                serde_json::to_string_pretty(&c).unwrap_or_default() + "\n",
            ),
            None => (503, "text/plain", "requiere spot y perp en vivo\n".into()),
        },
        p if p.starts_with("/footprint/") => match find("/footprint/") {
            Some(v) => {
                let body = serde_json::to_string(&*v.metrics.load_full()).unwrap_or_default();
                (200, "application/json", body + "\n")
            }
            None => (404, "text/plain", "mercado no registrado\n".into()),
        },
        _ => (
            404,
            "text/plain",
            "rutas: / /ui /ws /health /ready /metrics /cvd /book/<m> /footprint/<m>\n".into(),
        ),
    }
}

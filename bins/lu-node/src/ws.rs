//! F4 — API WebSocket en vivo.
//!
//! `GET /ws?markets=spot,perp&interval_ms=250`
//!
//! Servidor → cliente (texto JSON):
//! * `{"type":"hello","markets":[...],"interval_ms":250}`
//! * `{"type":"book","key":"spot","data":<BookView>}` cada `interval_ms` (100..=5000)
//! * `{"type":"footprint","key":"spot","data":<MetricsView>}` cuando cambia (≤ 1 Hz)
//!
//! Cliente → servidor: se ignora (ping/pong los resuelve la biblioteca; `Close` cierra).
//! Un cliente lento (envío > 2 s) se desconecta: nunca frena al nodo.

use crate::http::Registry;
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_tungstenite::tungstenite::protocol::{Role, WebSocketConfig};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

type Tx = futures_util::stream::SplitSink<WebSocketStream<TcpStream>, Message>;

/// Envía con plazo: `false` ⇒ cliente lento o caído (se cierra la sesión).
async fn send(tx: &mut Tx, text: String) -> bool {
    matches!(
        tokio::time::timeout(Duration::from_secs(2), tx.send(Message::text(text))).await,
        Ok(Ok(()))
    )
}

fn param<'a>(query: &'a str, name: &str) -> Option<&'a str> {
    query
        .split('&')
        .filter_map(|kv| kv.split_once('='))
        .find(|(k, _)| *k == name)
        .map(|(_, v)| v)
}

/// Sesión WebSocket (tras leer la petición de mejora).
pub async fn session(
    mut sock: TcpStream,
    key: &str,
    query: &str,
    reg: Registry,
    mut shutdown: watch::Receiver<bool>,
) -> std::io::Result<()> {
    let accept = derive_accept_key(key.as_bytes());
    let head = format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
    );
    sock.write_all(head.as_bytes()).await?;
    let cfg = WebSocketConfig::default()
        .max_message_size(Some(64 << 10))
        .max_frame_size(Some(64 << 10));
    let ws = WebSocketStream::from_raw_socket(sock, Role::Server, Some(cfg)).await;
    let (mut tx, mut rx) = ws.split();

    let wanted: Vec<&str> = param(query, "markets")
        .map(|m| m.split(',').collect())
        .unwrap_or_default();
    let markets: Vec<_> = reg
        .iter()
        .filter(|(k, _)| wanted.is_empty() || wanted.contains(&k.as_str()))
        .cloned()
        .collect();
    let interval_ms = param(query, "interval_ms")
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(250)
        .clamp(100, 5_000);

    let hello = serde_json::json!({
        "type": "hello",
        "markets": markets.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(),
        "interval_ms": interval_ms,
    });
    if !send(&mut tx, hello.to_string()).await {
        return Ok(());
    }
    let mut last_fp: Vec<Option<Arc<lu_metrics::MetricsView>>> = vec![None; markets.len()];
    let mut tick = tokio::time::interval(Duration::from_millis(interval_ms));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            msg = rx.next() => match msg {
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => break,
                Some(Ok(_)) => {}
            },
            _ = tick.tick() => {
                for (i, (k, v)) in markets.iter().enumerate() {
                    let book = serde_json::to_string(&*v.book.load_full()).unwrap_or_default();
                    if !send(&mut tx, format!(r#"{{"type":"book","key":"{k}","data":{book}}}"#)).await {
                        return Ok(());
                    }
                    let fp = v.metrics.load_full();
                    if last_fp[i].as_ref().is_none_or(|p| !Arc::ptr_eq(p, &fp)) {
                        let body = serde_json::to_string(&*fp).unwrap_or_default();
                        if !send(&mut tx, format!(r#"{{"type":"footprint","key":"{k}","data":{body}}}"#)).await {
                            return Ok(());
                        }
                        last_fp[i] = Some(fp);
                    }
                }
            }
        }
    }
    let _ = tokio::time::timeout(Duration::from_secs(1), tx.close()).await;
    Ok(())
}

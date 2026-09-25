//! Servicio de snapshots REST: coalesce pedidos, respeta límites de peso
//! (429/418 con `Retry-After`) e intervalo mínimo entre descargas.

use crate::engine::EngineMsg;
use crossbeam_channel::{Sender, TrySendError};
use lu_binance::{Endpoints, RestClient, RestError};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

/// Ejecuta el servicio hasta el apagado.
pub async fn run(
    ep: Endpoints,
    rest: Arc<RestClient>,
    mut req: mpsc::UnboundedReceiver<()>,
    eng: Sender<EngineMsg>,
    mut shutdown: watch::Receiver<bool>,
) {
    let label = ep.market.to_string();
    let min_interval = Duration::from_millis(1_000);
    let mut last_fetch: Option<Instant> = None;
    loop {
        tokio::select! {
            _ = shutdown.changed() => return,
            r = req.recv() => if r.is_none() { return },
        }
        while req.try_recv().is_ok() {} // coalescer ráfagas de pedidos
        let mut backoff = Duration::from_millis(500);
        loop {
            if let Some(t) = last_fetch {
                let el = t.elapsed();
                if el < min_interval {
                    tokio::time::sleep(min_interval - el).await;
                }
            }
            last_fetch = Some(Instant::now());
            let (rest2, hosts, path, limit) = (
                rest.clone(),
                ep.rest_hosts.clone(),
                ep.snapshot_path.clone(),
                ep.snapshot_limit,
            );
            let res =
                tokio::task::spawn_blocking(move || rest2.depth_snapshot(&hosts, &path, limit))
                    .await;
            let wait = match res {
                Ok(Ok(snap)) => {
                    deliver(&eng, EngineMsg::Snapshot(snap)).await;
                    break;
                }
                Ok(Err(e)) => {
                    let w = match &e {
                        RestError::RateLimited { retry_after } => {
                            retry_after.unwrap_or(Duration::from_secs(60))
                        }
                        RestError::Banned { retry_after } => {
                            retry_after.unwrap_or(Duration::from_secs(300))
                        }
                        _ => backoff,
                    };
                    tracing::warn!(market = %label, error = %e, espera_ms = w.as_millis() as u64, "snapshot fallido");
                    w
                }
                Err(e) => {
                    tracing::error!(market = %label, error = %e, "tarea de snapshot abortada");
                    backoff
                }
            };
            tokio::select! {
                _ = shutdown.changed() => return,
                _ = tokio::time::sleep(wait) => {}
            }
            backoff = (backoff * 2).min(Duration::from_secs(30));
        }
    }
}

/// Entrega confiable sin bloquear el runtime: reintenta si la cola está llena.
pub async fn deliver(eng: &Sender<EngineMsg>, mut msg: EngineMsg) {
    loop {
        match eng.try_send(msg) {
            Ok(()) | Err(TrySendError::Disconnected(_)) => return,
            Err(TrySendError::Full(m)) => {
                msg = m;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
    }
}

//! # lu-okx
//!
//! Conector OKX v5 (público) que produce eventos normalizados de `lu-core`.
//!
//! Protocolo verificado en vivo (2026-09-26, `wss://ws.okx.com/ws/v5/public`):
//! * canal `books`: primer mensaje `action = "snapshot"` con 400 niveles por lado
//!   (`prevSeqId = -1`), luego `action = "update"` cada 100 ms con cantidades
//!   absolutas y `prevSeqId` = `seqId` anterior. Es un libro **top-400 rodante**:
//!   los niveles que salen del top llegan con tamaño 0 ⇒ cobertura dinámica.
//!   `checksum` llega en 0 (OKX dejó de calcularlo).
//! * canal `trades`: agregados por orden taker (`tradeId`, `px`, `sz`, `side` del
//!   taker, `ts`, `count` = fills).
//! * perpetuo `SOL-USDT-SWAP`: `sz` en contratos; `ctVal` = 1 SOL (se aplica el
//!   multiplicador leído de `/api/v5/public/instruments`).
//! * Snapshot nuevo: re-suscripción al canal `books` (el REST no trae `seqId`).
//! * Latido: `"ping"` de texto (el servidor corta tras 30 s sin tráfico) → `"pong"`.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod proto;
pub mod rest;

pub use proto::{OkxEndpoints, OkxProtocol};
pub use rest::{instrument, OkxRestError};

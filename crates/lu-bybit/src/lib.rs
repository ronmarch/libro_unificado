//! # lu-bybit
//!
//! Conector Bybit v5 público (`wss://stream.bybit.com/v5/public/{spot|linear}`).
//!
//! Protocolo verificado en vivo (2026-09-26):
//! * `orderbook.1000.<SYM>`: `snapshot` y luego `delta` con cantidades absolutas
//!   (0 = eliminar). `u` es **contiguo y global**: 152/152 deltas con el mismo `u` fueron
//!   idénticos en dos conexiones independientes ⇒ arbitraje A/B por id como Binance/OKX.
//!   `cts` = tiempo del motor de matching; `ts` = emisión. Libro top-1000 (cobertura dinámica).
//!   `u = 1` o un `u` que retrocede indica reinicio del servicio ⇒ nueva época de ids.
//! * `publicTrade.<SYM>`: `S` = lado del **agresor** (112/112 contra el libro), `T` tiempo,
//!   `RPI` = ejecutado contra órdenes RPI (no visibles en el libro) ⇒ se reporta aparte.
//!   `i` es numérico en spot y UUID en lineal ⇒ id de 64 bits por FNV-1a.
//! * El REST de Bybit responde 403 desde regiones restringidas; el snapshot llega por el
//!   WebSocket, así que el conector no depende del REST.
//! * Latido: `{"op":"ping"}` cada 20 s.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod proto;

pub use proto::{BybitProtocol, WS_LINEAR, WS_SPOT};

//! # lu-kraken
//!
//! Conector Kraken WebSocket v2 (`wss://ws.kraken.com/v2`), público, spot.
//!
//! Protocolo verificado en vivo (2026-09-26):
//! * canal `book` (`depth` 1000): `snapshot` y luego `update` con cantidades absolutas,
//!   **sin número de secuencia**. Cada mensaje trae `checksum` = CRC32 del top 10
//!   (asks ascendentes, luego bids descendentes; precio y cantidad formateados con la
//!   precisión del par, sin punto ni ceros a la izquierda). Libro top-N: tras cada update
//!   el cliente recorta a N niveles (Kraken no envía bajas de los que salen del rango).
//!   Algoritmo validado: 1 945/1 945 checksums correctos sobre datos reales.
//! * Números JSON (no cadenas) ⇒ se leen desde el texto crudo, nunca vía `f64`.
//! * canal `trade`: `side` es el lado del **agresor** (16/16 contra el libro).
//!
//! Diseño: la conexión mantiene un espejo del libro, aplica y recorta cada update y
//! solo emite si el CRC32 coincide (incluyendo como bajas los niveles recortados). Ante
//! una discrepancia deja de emitir profundidad y re-suscribe. La profundidad se numera
//! con un contador contiguo por línea (regla `OkxRule`). Solo la línea 0 alimenta el
//! libro; las demás aportan trades (deduplicados por `trade_id`).
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod crc;
pub mod proto;
pub mod rest;

pub use proto::KrakenProtocol;
pub use rest::{pair_precision, KrakenRestError};

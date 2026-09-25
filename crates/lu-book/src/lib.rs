//! # lu-book
//!
//! Libro L2 determinista y su máquina de sincronización snapshot + diff con
//! arbitraje de líneas redundantes. Sin E/S, sin relojes, sin locks: el
//! conductor (bins/lu-node) inyecta eventos y tiempo; el motor responde con
//! acciones (`Step`). Esto permite verificar con propiedades que el libro
//! nunca está en `Live` con un estado distinto al del exchange.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod book;
pub mod observer;
pub mod seq;
pub mod sync;

pub use book::{Coverage, L2Book};
pub use observer::BookObserver;
pub use seq::{BinanceFuturesRule, BinanceSpotRule, Class, SeqRule};
pub use sync::{
    CrossPolicy, Phase, ResyncReason, Step, SyncBook, SyncConfig, SyncStats, MAX_LINES,
};

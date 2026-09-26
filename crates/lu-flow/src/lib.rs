//! # lu-flow
//!
//! F2 del libro unificado: alineador determinista trades ↔ depth. Convierte el
//! libro sincronizado (`lu-book`) y los trades del venue en flujos por nivel y
//! lote con **cotas inferiores** verificables de liquidez no visible y
//! cancelada. Sin E/S, sin relojes: el tiempo es el del exchange.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod align;

pub use align::{
    Aligner, AlignerConfig, AlignerStats, BatchInfo, Contamination, FlowSink, LevelFlow, LevelKey,
};

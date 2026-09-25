//! # lu-metrics
//!
//! F3 del libro unificado: velas footprint por bucket de precio (15 m / 1 h / 4 h)
//! con TWA perezoso, ejecución bid/ask, fills, cotas de F2 agregadas, RPI aparte,
//! persistencia foto ÷ TWA y detector de muros retirados. Consume `LevelFlow`
//! vía `FlowSink`; determinista y en aritmética entera.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod footprint;

pub use footprint::{
    CandleView, CellView, Metrics, MetricsConfig, MetricsView, WallConfig, WallRetired, WallStats,
    TF_15M, TF_1H, TF_4H,
};

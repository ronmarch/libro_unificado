//! # lu-core
//!
//! Tipos fundamentales del sistema de libro unificado:
//! punto fijo exacto (escala universal 1e-8), identidad de mercados y
//! eventos normalizados que todo conector de venue debe producir.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod event;
pub mod fixed;
pub mod instrument;

pub use event::{
    init_clock, mono_now_ms, mono_now_ns, wall_now_ns, AggTrade, Aggressor, DepthDiff,
    DepthSnapshot, Level, MarketEvent, RxStamp, Side,
};
pub use fixed::{notional, parse_fixed8, ParseFixedError, Px, Qty, DECIMALS, SCALE};
pub use instrument::{InstrumentSpec, MarketId, MarketKind, Venue};

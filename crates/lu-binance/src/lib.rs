//! # lu-binance
//!
//! Conector Binance (spot y USDⓈ-M) que produce eventos normalizados de
//! `lu-core`. Es la plantilla para los demás venues: otro exchange = otro
//! crate con estas mismas piezas (endpoints, wire, rest, line) y su `SeqRule`.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod endpoints;
pub mod line;
pub mod rest;
pub mod tls;
pub mod wire;

pub use endpoints::Endpoints;
pub use line::{run_line, FrameSink, LineSpec};
pub use rest::{RestClient, RestError};
pub use tls::{client_config, TlsError};
pub use wire::{parse_frame, parse_snapshot, WireError};

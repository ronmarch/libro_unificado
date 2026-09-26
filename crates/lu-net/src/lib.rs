//! # lu-net
//!
//! Infraestructura de red compartida por todos los conectores de venue:
//! configuración TLS única y el ejecutor de líneas WebSocket redundantes,
//! genérico sobre un [`Protocol`] (suscripción, latido y parseo del venue).
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod line;
pub mod tls;

pub use line::{run_line, FrameSink, LineCmd, LineSpec, Protocol};
pub use tls::{client_config, TlsError};

//! Protocolo Binance sobre la línea genérica: la URL ya suscribe los streams
//! (stream combinado), el servidor manda el ping y cada frame es un evento.

use lu_core::{MarketEvent, RxStamp};
use lu_net::Protocol;

/// Protocolo Binance (spot y USDⓈ-M).
#[derive(Debug, Clone, Copy, Default)]
pub struct BinanceProtocol;

impl Protocol for BinanceProtocol {
    fn parse(&self, text: &str, rx: RxStamp, out: &mut Vec<MarketEvent>) -> Result<(), String> {
        out.push(crate::wire::parse_frame(text, rx).map_err(|e| e.to_string())?);
        Ok(())
    }
}

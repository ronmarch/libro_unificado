//! REST de Kraken: precisión del par (necesaria para el checksum).

use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Error REST.
#[derive(Debug, thiserror::Error)]
pub enum KrakenRestError {
    /// Red o HTTP.
    #[error("http: {0}")]
    Http(String),
    /// Respuesta inesperada.
    #[error("respuesta: {0}")]
    Body(String),
}

#[derive(Deserialize)]
struct Resp {
    error: Vec<String>,
    result: Option<HashMap<String, Pair>>,
}

#[derive(Deserialize)]
struct Pair {
    wsname: String,
    pair_decimals: u32,
    lot_decimals: u32,
}

/// (decimales de precio, decimales de cantidad) del par `wsname` (p. ej. `SOL/USD`).
pub fn pair_precision(
    tls: Arc<rustls::ClientConfig>,
    wsname: &str,
) -> Result<(u32, u32), KrakenRestError> {
    let agent = ureq::AgentBuilder::new()
        .tls_config(tls)
        .timeout(Duration::from_secs(10))
        .build();
    let pair = wsname.replace('/', "");
    let text = agent
        .get(&format!(
            "https://api.kraken.com/0/public/AssetPairs?pair={pair}"
        ))
        .call()
        .map_err(|e| KrakenRestError::Http(e.to_string()))?
        .into_string()
        .map_err(|e| KrakenRestError::Http(e.to_string()))?;
    let r: Resp = serde_json::from_str(&text).map_err(|e| KrakenRestError::Body(e.to_string()))?;
    if !r.error.is_empty() {
        return Err(KrakenRestError::Body(r.error.join("; ")));
    }
    r.result
        .unwrap_or_default()
        .into_values()
        .find(|p| p.wsname == wsname)
        .map(|p| (p.pair_decimals, p.lot_decimals))
        .ok_or_else(|| KrakenRestError::Body(format!("par {wsname} inexistente")))
}

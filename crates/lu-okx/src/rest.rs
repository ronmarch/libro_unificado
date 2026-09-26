//! REST de OKX: solo reglas del instrumento (el snapshot llega por WebSocket).

use lu_core::{InstrumentSpec, MarketId, Px, Qty};
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;

/// Error REST.
#[derive(Debug, thiserror::Error)]
pub enum OkxRestError {
    /// Red o HTTP.
    #[error("http: {0}")]
    Http(String),
    /// Respuesta inesperada.
    #[error("respuesta: {0}")]
    Body(String),
}

#[derive(Deserialize)]
struct Resp {
    code: String,
    #[serde(default)]
    msg: String,
    data: Vec<Inst>,
}

#[derive(Deserialize)]
struct Inst {
    #[serde(rename = "tickSz")]
    tick_sz: String,
    #[serde(rename = "lotSz")]
    lot_sz: String,
    #[serde(rename = "ctVal", default)]
    ct_val: String,
}

/// Reglas del instrumento y unidades de base por contrato (`ctVal`, 1 en spot).
pub fn instrument(
    tls: Arc<rustls::ClientConfig>,
    host: &str,
    inst_type: &str,
    inst_id: &str,
    market: &MarketId,
) -> Result<(InstrumentSpec, Qty), OkxRestError> {
    let agent = ureq::AgentBuilder::new()
        .tls_config(tls)
        .timeout(Duration::from_secs(10))
        .build();
    let url = format!("{host}/api/v5/public/instruments?instType={inst_type}&instId={inst_id}");
    let text = agent
        .get(&url)
        .call()
        .map_err(|e| OkxRestError::Http(e.to_string()))?
        .into_string()
        .map_err(|e| OkxRestError::Http(e.to_string()))?;
    let r: Resp = serde_json::from_str(&text).map_err(|e| OkxRestError::Body(e.to_string()))?;
    if r.code != "0" {
        return Err(OkxRestError::Body(format!("code {}: {}", r.code, r.msg)));
    }
    let i = r
        .data
        .first()
        .ok_or_else(|| OkxRestError::Body("instrumento inexistente".into()))?;
    let bad = |e| OkxRestError::Body(format!("{e}"));
    let contract = if i.ct_val.is_empty() {
        Qty::from_units(1)
    } else {
        Qty::parse(&i.ct_val).map_err(bad)?
    };
    let lot = Qty::parse(&i.lot_sz).map_err(bad)?;
    let step = Qty::from_raw(
        (i128::from(lot.raw()) * i128::from(contract.raw()) / i128::from(lu_core::SCALE)) as i64,
    );
    Ok((
        InstrumentSpec {
            market: market.clone(),
            tick: Px::parse(&i.tick_sz).map_err(bad)?,
            step,
            base_per_contract: contract,
        },
        contract,
    ))
}

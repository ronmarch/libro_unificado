//! Parseo zero-copy del protocolo de Binance hacia eventos normalizados.
//!
//! Los precios y cantidades se leen como `&str` prestados del frame y se
//! convierten directo a punto fijo: sin `String` intermedios ni `f64`.

use lu_core::{
    AggTrade, Aggressor, DepthDiff, DepthSnapshot, InstrumentSpec, Level, MarketEvent, MarketId,
    Px, Qty, RxStamp,
};
use serde::de::{self, Deserializer, SeqAccess, Visitor};
use serde::Deserialize;
use serde_json::value::RawValue;
use std::fmt;

/// Error de protocolo.
#[derive(Debug, thiserror::Error)]
pub enum WireError {
    /// JSON inválido o campo faltante.
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    /// Decimal fuera de la escala 1e-8 o inválido.
    #[error("decimal: {0}")]
    Fixed(#[from] lu_core::ParseFixedError),
    /// `exchangeInfo` sin el símbolo o sin filtros.
    #[error("exchangeInfo: {0}")]
    Spec(String),
}

#[derive(Deserialize)]
struct Envelope<'a> {
    #[serde(borrow)]
    stream: &'a str,
    #[serde(borrow)]
    data: &'a RawValue,
}

#[derive(Deserialize)]
struct DepthWire {
    #[serde(rename = "E")]
    event_time: u64,
    #[serde(rename = "T", default)]
    match_time: Option<u64>,
    #[serde(rename = "U")]
    first: u64,
    #[serde(rename = "u")]
    last: u64,
    #[serde(rename = "pu", default)]
    prev: Option<u64>,
    #[serde(rename = "b", deserialize_with = "de_levels")]
    bids: Vec<Level>,
    #[serde(rename = "a", deserialize_with = "de_levels")]
    asks: Vec<Level>,
}

#[derive(Deserialize)]
struct AggWire<'a> {
    #[serde(rename = "E")]
    event_time: u64,
    #[serde(rename = "a")]
    agg_id: u64,
    #[serde(rename = "p", borrow)]
    px: &'a str,
    #[serde(rename = "q", borrow)]
    qty: &'a str,
    #[serde(rename = "nq", borrow, default)]
    qty_normal: Option<&'a str>,
    #[serde(rename = "f")]
    first: u64,
    #[serde(rename = "l")]
    last: u64,
    #[serde(rename = "T")]
    trade_time: u64,
    #[serde(rename = "m")]
    buyer_is_maker: bool,
}

#[derive(Deserialize)]
struct SnapshotWire {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    #[serde(rename = "E", default)]
    event_time: Option<u64>,
    #[serde(deserialize_with = "de_levels")]
    bids: Vec<Level>,
    #[serde(deserialize_with = "de_levels")]
    asks: Vec<Level>,
}

/// Deserializa `[["precio","cantidad"], ...]` directo a `Vec<Level>` sin intermedios.
fn de_levels<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<Level>, D::Error> {
    struct V;
    impl<'de> Visitor<'de> for V {
        type Value = Vec<Level>;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("lista de [precio, cantidad]")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let mut out = Vec::with_capacity(seq.size_hint().unwrap_or(16));
            while let Some((p, q)) = seq.next_element::<(&'de str, &'de str)>()? {
                let px = Px::parse(p).map_err(de::Error::custom)?;
                let qty = Qty::parse(q).map_err(de::Error::custom)?;
                out.push(Level { px, qty });
            }
            Ok(out)
        }
    }
    d.deserialize_seq(V)
}

/// Parsea un frame de stream combinado (`{"stream": ..., "data": ...}`).
pub fn parse_frame(text: &str, rx: RxStamp) -> Result<MarketEvent, WireError> {
    let env: Envelope<'_> = serde_json::from_str(text)?;
    if env.stream.contains("@depth") {
        let w: DepthWire = serde_json::from_str(env.data.get())?;
        Ok(MarketEvent::Depth(DepthDiff {
            first_id: w.first,
            last_id: w.last,
            prev_last_id: w.prev,
            exch_ts_ms: w.event_time,
            match_ts_ms: w.match_time,
            bids: w.bids,
            asks: w.asks,
            rx,
        }))
    } else if env.stream.ends_with("@aggTrade") {
        let w: AggWire<'_> = serde_json::from_str(env.data.get())?;
        let px = Px::parse(w.px)?;
        let qty = Qty::parse(w.qty)?;
        let qty_normal = w.qty_normal.map(Qty::parse).transpose()?;
        Ok(MarketEvent::Trade(AggTrade {
            agg_id: w.agg_id,
            first_trade_id: w.first,
            last_trade_id: w.last,
            px,
            qty,
            qty_normal,
            aggressor: if w.buyer_is_maker {
                Aggressor::Sell
            } else {
                Aggressor::Buy
            },
            trade_ts_ms: w.trade_time,
            exch_ts_ms: w.event_time,
            rx,
        }))
    } else {
        Ok(MarketEvent::Ignored)
    }
}

/// Parsea la respuesta REST de profundidad.
pub fn parse_snapshot(text: &str, limit: usize, rx: RxStamp) -> Result<DepthSnapshot, WireError> {
    let w: SnapshotWire = serde_json::from_str(text)?;
    Ok(DepthSnapshot {
        last_update_id: w.last_update_id,
        limit,
        bids: w.bids,
        asks: w.asks,
        exch_ts_ms: w.event_time,
        rx,
    })
}

/// Extrae tick y step de `exchangeInfo` (spot o USDⓈ-M; misma estructura de filtros).
pub fn parse_instrument(text: &str, market: &MarketId) -> Result<InstrumentSpec, WireError> {
    #[derive(Deserialize)]
    struct Info {
        symbols: Vec<Sym>,
    }
    #[derive(Deserialize)]
    struct Sym {
        symbol: String,
        filters: Vec<serde_json::Value>,
    }
    let info: Info = serde_json::from_str(text)?;
    let sym = info
        .symbols
        .into_iter()
        .find(|s| s.symbol == market.symbol)
        .ok_or_else(|| WireError::Spec(format!("símbolo {} ausente", market.symbol)))?;
    let find = |ftype: &str, field: &str| -> Result<i64, WireError> {
        let v = sym
            .filters
            .iter()
            .find(|f| f.get("filterType").and_then(|t| t.as_str()) == Some(ftype))
            .and_then(|f| f.get(field))
            .and_then(|v| v.as_str())
            .ok_or_else(|| WireError::Spec(format!("{ftype}.{field} ausente")))?;
        lu_core::parse_fixed8(v).map_err(|e| WireError::Spec(format!("{ftype}.{field}: {e}")))
    };
    Ok(InstrumentSpec {
        market: market.clone(),
        tick: Px::from_raw(find("PRICE_FILTER", "tickSize")?),
        step: Qty::from_raw(find("LOT_SIZE", "stepSize")?),
        base_per_contract: Qty::from_units(1),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use lu_core::{MarketKind, Venue};

    #[test]
    fn depth_spot() {
        let f = r#"{"stream":"solusdt@depth@100ms","data":{"e":"depthUpdate","E":1758800000123,"s":"SOLUSDT","U":100,"u":105,"b":[["150.10","12.500"],["150.00","0.00000000"]],"a":[["150.20","3"]]}}"#;
        let MarketEvent::Depth(d) = parse_frame(f, RxStamp::default()).unwrap() else {
            panic!()
        };
        assert_eq!((d.first_id, d.last_id, d.prev_last_id), (100, 105, None));
        assert_eq!(d.exch_ts_ms, 1758800000123);
        assert_eq!(d.bids.len(), 2);
        assert_eq!(d.bids[0].px, Px::parse("150.1").unwrap());
        assert_eq!(d.bids[0].qty, Qty::parse("12.5").unwrap());
        assert!(d.bids[1].qty.is_zero());
        assert_eq!(d.asks[0].qty, Qty::from_units(3));
    }

    #[test]
    fn depth_futuros_con_pu_y_t() {
        let f = r#"{"stream":"solusdt@depth@100ms","data":{"e":"depthUpdate","E":10,"T":9,"s":"SOLUSDT","U":157,"u":160,"pu":149,"b":[["0.0024","10"]],"a":[["0.0026","100"]]}}"#;
        let MarketEvent::Depth(d) = parse_frame(f, RxStamp::default()).unwrap() else {
            panic!()
        };
        assert_eq!(d.prev_last_id, Some(149));
        assert_eq!(d.match_ts_ms, Some(9));
    }

    #[test]
    fn aggtrade_futuros_con_nq_rpi() {
        let f = r#"{"stream":"solusdt@aggTrade","data":{"e":"aggTrade","E":123456789,"s":"SOLUSDT","a":5933014,"p":"150.25","q":"10","nq":"7.5","f":100,"l":105,"T":123456785,"m":true}}"#;
        let MarketEvent::Trade(t) = parse_frame(f, RxStamp::default()).unwrap() else {
            panic!()
        };
        assert_eq!(t.aggressor, Aggressor::Sell);
        assert_eq!(t.n_fills(), 6);
        assert_eq!(t.qty_rpi(), Some(Qty::parse("2.5").unwrap()));
    }

    #[test]
    fn aggtrade_spot_sin_nq() {
        let f = r#"{"stream":"solusdt@aggTrade","data":{"e":"aggTrade","E":1,"s":"SOLUSDT","a":1,"p":"1","q":"2","f":7,"l":7,"T":1,"m":false,"M":true}}"#;
        let MarketEvent::Trade(t) = parse_frame(f, RxStamp::default()).unwrap() else {
            panic!()
        };
        assert_eq!(t.aggressor, Aggressor::Buy);
        assert_eq!(t.qty_normal, None);
        assert_eq!(t.n_fills(), 1);
    }

    #[test]
    fn otros_streams_se_ignoran_y_precision_invalida_falla() {
        let f = r#"{"stream":"solusdt@markPrice@1s","data":{"e":"markPriceUpdate","E":1}}"#;
        assert_eq!(
            parse_frame(f, RxStamp::default()).unwrap(),
            MarketEvent::Ignored
        );
        let bad = r#"{"stream":"solusdt@depth@100ms","data":{"E":1,"U":1,"u":1,"b":[["1.000000001","1"]],"a":[]}}"#;
        assert!(parse_frame(bad, RxStamp::default()).is_err());
    }

    #[test]
    fn snapshot_y_exchange_info() {
        let s = r#"{"lastUpdateId":1027024,"E":5,"T":4,"bids":[["4.00000000","431.00000000"]],"asks":[["4.00000200","12.00000000"]]}"#;
        let snap = parse_snapshot(s, 1000, RxStamp::default()).unwrap();
        assert_eq!(snap.last_update_id, 1027024);
        assert_eq!(snap.asks[0].px, Px::parse("4.000002").unwrap());
        let info = r#"{"symbols":[{"symbol":"SOLUSDT","filters":[{"filterType":"PRICE_FILTER","tickSize":"0.01000000"},{"filterType":"LOT_SIZE","stepSize":"0.00100000"}]}]}"#;
        let m = MarketId::new(Venue::Binance, MarketKind::Spot, "SOLUSDT");
        let spec = parse_instrument(info, &m).unwrap();
        assert_eq!(spec.tick, Px::parse("0.01").unwrap());
        assert_eq!(spec.step, Qty::parse("0.001").unwrap());
        assert!(spec.on_tick(Px::parse("150.25").unwrap()));
        assert!(!spec.on_tick(Px::parse("150.255").unwrap()));
    }
}

//! Protocolo y parseo de Bybit v5.

use lu_core::{
    AggTrade, Aggressor, DepthDiff, DepthSnapshot, Level, MarketEvent, Px, Qty, RxStamp,
};
use lu_net::Protocol;
use serde::Deserialize;
use std::sync::Mutex;
use std::time::Duration;

/// WebSocket spot.
pub const WS_SPOT: &str = "wss://stream.bybit.com/v5/public/spot";
/// WebSocket perpetuo lineal (USDT).
pub const WS_LINEAR: &str = "wss://stream.bybit.com/v5/public/linear";
/// Profundidad suscrita.
pub const DEPTH: usize = 1000;

/// Bits para `u` dentro del id; los superiores numeran épocas (reinicios del servicio).
const EPOCH_SHIFT: u32 = 44;

#[derive(Debug, Default)]
struct State {
    epoch: u64,
    last_u: u64,
}

/// Protocolo Bybit para un símbolo en una línea.
#[derive(Debug)]
pub struct BybitProtocol {
    symbol: String,
    st: Mutex<State>,
}

/// FNV-1a de 64 bits (ids de trade no numéricos).
pub fn fnv1a64(s: &str) -> u64 {
    s.bytes().fold(0xcbf2_9ce4_8422_2325u64, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

impl BybitProtocol {
    /// Símbolo nativo (`SOLUSDT`).
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            symbol: symbol.into(),
            st: Mutex::new(State::default()),
        }
    }

    fn book_topic(&self) -> String {
        format!("orderbook.{DEPTH}.{}", self.symbol)
    }

    fn op(&self, op: &str, topics: &[String]) -> String {
        serde_json::json!({"op": op, "args": topics}).to_string()
    }

    /// Id global con época: un `u` menor al último visto ⇒ reinicio ⇒ época nueva.
    fn id(&self, u: u64) -> Result<u64, String> {
        let mut st = self
            .st
            .lock()
            .map_err(|_| "estado envenenado".to_string())?;
        if u < st.last_u {
            st.epoch += 1;
        }
        st.last_u = u;
        Ok((st.epoch << EPOCH_SHIFT) | (u & ((1 << EPOCH_SHIFT) - 1)))
    }
}

#[derive(Deserialize)]
struct Env<'a> {
    #[serde(borrow, default)]
    topic: Option<&'a str>,
    #[serde(borrow, default, rename = "type")]
    kind: Option<&'a str>,
    #[serde(default)]
    ts: Option<u64>,
    #[serde(default)]
    cts: Option<u64>,
    #[serde(borrow, default)]
    data: Option<&'a serde_json::value::RawValue>,
    #[serde(borrow, default)]
    op: Option<&'a str>,
    #[serde(default)]
    success: Option<bool>,
    #[serde(borrow, default)]
    ret_msg: Option<&'a str>,
}

#[derive(Deserialize)]
struct BookWire<'a> {
    #[serde(borrow)]
    b: Vec<(&'a str, &'a str)>,
    #[serde(borrow)]
    a: Vec<(&'a str, &'a str)>,
    u: u64,
}

#[derive(Deserialize)]
struct TradeWire<'a> {
    #[serde(rename = "T")]
    t: u64,
    #[serde(rename = "S", borrow)]
    side: &'a str,
    #[serde(borrow)]
    v: &'a str,
    #[serde(borrow)]
    p: &'a str,
    #[serde(borrow)]
    i: &'a str,
    #[serde(rename = "RPI", default)]
    rpi: bool,
}

fn levels(v: &[(&str, &str)]) -> Result<Vec<Level>, String> {
    v.iter()
        .map(|(p, q)| {
            Ok(Level {
                px: Px::parse(p).map_err(|e| format!("px {p}: {e}"))?,
                qty: Qty::parse(q).map_err(|e| format!("qty {q}: {e}"))?,
            })
        })
        .collect()
}

impl Protocol for BybitProtocol {
    fn on_connect(&self) -> Vec<String> {
        vec![self.op(
            "subscribe",
            &[self.book_topic(), format!("publicTrade.{}", self.symbol)],
        )]
    }

    fn resubscribe(&self) -> Option<Vec<String>> {
        let t = [self.book_topic()];
        Some(vec![self.op("unsubscribe", &t), self.op("subscribe", &t)])
    }

    fn keepalive(&self) -> Option<(Duration, &'static str)> {
        Some((Duration::from_secs(20), r#"{"op":"ping"}"#))
    }

    fn parse(&self, text: &str, rx: RxStamp, out: &mut Vec<MarketEvent>) -> Result<(), String> {
        let env: Env<'_> = serde_json::from_str(text).map_err(|e| e.to_string())?;
        if env.op.is_some() {
            return match env.success {
                Some(false) => Err(format!(
                    "Bybit rechazó {}: {}",
                    env.op.unwrap_or(""),
                    env.ret_msg.unwrap_or("")
                )),
                _ => Ok(()),
            };
        }
        let (Some(topic), Some(data)) = (env.topic, env.data) else {
            return Ok(());
        };
        if topic.starts_with("orderbook.") {
            let w: BookWire<'_> = serde_json::from_str(data.get()).map_err(|e| e.to_string())?;
            let ts = env.ts.unwrap_or(0);
            let bids = levels(&w.b)?;
            let asks = levels(&w.a)?;
            match env.kind {
                Some("snapshot") => {
                    let id = self.id(w.u)?;
                    out.push(MarketEvent::Snapshot(DepthSnapshot {
                        last_update_id: id,
                        limit: DEPTH,
                        rolling: true,
                        bids,
                        asks,
                        exch_ts_ms: Some(env.cts.unwrap_or(ts)),
                        rx,
                    }));
                }
                Some("delta") => {
                    let id = self.id(w.u)?;
                    out.push(MarketEvent::Depth(DepthDiff {
                        first_id: id,
                        last_id: id,
                        prev_last_id: Some(id.saturating_sub(1)),
                        exch_ts_ms: ts,
                        match_ts_ms: env.cts,
                        bids,
                        asks,
                        rx,
                    }));
                }
                o => return Err(format!("tipo de libro desconocido: {o:?}")),
            }
        } else if topic.starts_with("publicTrade.") {
            let trades: Vec<TradeWire<'_>> =
                serde_json::from_str(data.get()).map_err(|e| e.to_string())?;
            for t in trades {
                let id = t.i.parse::<u64>().unwrap_or_else(|_| fnv1a64(t.i));
                let qty = Qty::parse(t.v).map_err(|e| format!("v {}: {e}", t.v))?;
                out.push(MarketEvent::Trade(AggTrade {
                    agg_id: id,
                    first_trade_id: id,
                    last_trade_id: id,
                    px: Px::parse(t.p).map_err(|e| format!("p {}: {e}", t.p))?,
                    qty,
                    // RPI: ejecutado contra órdenes que no están en el libro.
                    qty_normal: Some(if t.rpi { Qty::ZERO } else { qty }),
                    aggressor: match t.side {
                        "Buy" => Aggressor::Buy,
                        "Sell" => Aggressor::Sell,
                        o => return Err(format!("S desconocido: {o}")),
                    },
                    trade_ts_ms: t.t,
                    exch_ts_ms: env.ts.unwrap_or(t.t).max(t.t),
                    rx,
                }));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Mensajes reales (2026-09-26), recortados.
    const SNAP: &str = r#"{"topic":"orderbook.1000.SOLUSDT","type":"snapshot","ts":1790398529604,"data":{"s":"SOLUSDT","b":[["120.640","1374.5"],["120.630","913.5"]],"a":[["120.650","85.2"]],"u":27618816,"seq":282179960106},"cts":1790398529590}"#;
    const DELTA: &str = r#"{"topic":"orderbook.1000.SOLUSDT","type":"delta","ts":1790398529803,"data":{"s":"SOLUSDT","b":[["120.610","2176.3"],["118.010","443.6"]],"a":[["120.650","62.1"]],"u":27618817,"seq":282179960268},"cts":1790398529803}"#;
    const TRADES: &str = r#"{"topic":"publicTrade.SOLUSDT","type":"snapshot","ts":1790398534647,"data":[{"T":1790398534646,"s":"SOLUSDT","S":"Buy","v":"34.0","p":"120.650","L":"PlusTick","i":"a11350c6-eb47-5471-9f6f-9698f08fd040","BT":false,"RPI":false,"seq":282179964761},{"i":"2210000001608762408","T":1790398535414,"p":"120.69","v":"2.0193","S":"Sell","seq":162270086950,"s":"SOLUSDT","BT":false,"RPI":true}]}"#;

    fn run(p: &BybitProtocol, t: &str) -> Vec<MarketEvent> {
        let mut out = Vec::new();
        p.parse(t, RxStamp::default(), &mut out).unwrap();
        out
    }

    #[test]
    fn snapshot_y_delta_encadenados_por_u() {
        let p = BybitProtocol::new("SOLUSDT");
        let MarketEvent::Snapshot(s) = &run(&p, SNAP)[0] else {
            panic!()
        };
        assert_eq!(s.last_update_id, 27618816);
        assert!(s.rolling);
        assert_eq!(s.exch_ts_ms, Some(1790398529590));
        let MarketEvent::Depth(d) = &run(&p, DELTA)[0] else {
            panic!()
        };
        assert_eq!(
            (d.first_id, d.last_id, d.prev_last_id),
            (27618817, 27618817, Some(27618816))
        );
        assert_eq!(d.match_ts_ms, Some(1790398529803));
        assert_eq!(d.bids[0].qty, Qty::parse("2176.3").unwrap());
    }

    #[test]
    fn reinicio_del_servicio_abre_epoca_nueva() {
        let p = BybitProtocol::new("SOLUSDT");
        run(&p, SNAP);
        let restart = SNAP.replace("\"u\":27618816", "\"u\":1");
        let MarketEvent::Snapshot(s) = &run(&p, &restart)[0] else {
            panic!()
        };
        assert_eq!(s.last_update_id, (1 << 44) | 1, "los ids nunca retroceden");
    }

    #[test]
    fn trades_agresor_rpi_e_ids() {
        let p = BybitProtocol::new("SOLUSDT");
        let out = run(&p, TRADES);
        let (MarketEvent::Trade(a), MarketEvent::Trade(b)) = (&out[0], &out[1]) else {
            panic!()
        };
        assert_eq!(a.aggressor, Aggressor::Buy);
        assert_eq!(a.agg_id, fnv1a64("a11350c6-eb47-5471-9f6f-9698f08fd040"));
        assert_eq!(a.qty_rpi(), Some(Qty::ZERO));
        assert_eq!(b.agg_id, 2210000001608762408);
        assert_eq!(b.aggressor, Aggressor::Sell);
        assert_eq!(
            b.qty_rpi(),
            Some(Qty::parse("2.0193").unwrap()),
            "RPI se reporta aparte"
        );
    }
}

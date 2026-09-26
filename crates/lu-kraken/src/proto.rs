//! Protocolo, espejo verificado por CRC32 y parseo de Kraken v2.

use crate::crc::crc32;
use lu_core::{
    parse_json_number8, rfc3339_ms, AggTrade, Aggressor, DepthDiff, DepthSnapshot, Level,
    MarketEvent, Px, Qty, RxStamp, DECIMALS,
};
use lu_net::Protocol;
use serde::Deserialize;
use serde_json::value::RawValue;
use std::collections::BTreeMap;
use std::sync::Mutex;

/// URL pública.
pub const WS_URL: &str = "wss://ws.kraken.com/v2";

#[derive(Debug, Default)]
struct State {
    counter: u64,
    awaiting_snapshot: bool,
    resync: bool,
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
    checksum_ok: u64,
    checksum_bad: u64,
}

/// Protocolo Kraken para un par y una línea (estado por conexión).
#[derive(Debug)]
pub struct KrakenProtocol {
    symbol: String,
    depth: bool,
    n: usize,
    price_dec: u32,
    qty_dec: u32,
    st: Mutex<State>,
}

impl KrakenProtocol {
    /// `symbol` estilo `SOL/USD`; `depth_levels` ∈ {10, 25, 100, 500, 1000};
    /// precisión del par (de `AssetPairs`) para el checksum.
    pub fn new(
        symbol: impl Into<String>,
        depth: bool,
        depth_levels: usize,
        price_dec: u32,
        qty_dec: u32,
    ) -> Self {
        Self {
            symbol: symbol.into(),
            depth,
            n: depth_levels,
            price_dec,
            qty_dec,
            st: Mutex::new(State {
                awaiting_snapshot: true,
                ..State::default()
            }),
        }
    }

    /// (checksums correctos, incorrectos) de esta conexión (diagnóstico).
    pub fn checksum_stats(&self) -> (u64, u64) {
        self.st
            .lock()
            .map(|s| (s.checksum_ok, s.checksum_bad))
            .unwrap_or((0, 0))
    }

    fn msg(&self, method: &str, channel: &str) -> String {
        let mut params = serde_json::json!({"channel": channel, "symbol": [self.symbol]});
        if channel == "book" && method == "subscribe" {
            params["depth"] = serde_json::json!(self.n);
        }
        serde_json::json!({"method": method, "params": params}).to_string()
    }

    /// Dígitos del valor con `dec` decimales, sin punto ni ceros a la izquierda.
    fn digits(raw: i64, dec: u32) -> Option<String> {
        let div = 10i64.pow(DECIMALS - dec);
        (raw % div == 0).then(|| {
            let s = (raw / div).to_string();
            s.trim_start_matches('0').to_string()
        })
    }

    fn checksum(&self, st: &State) -> Option<u32> {
        let mut s = String::with_capacity(400);
        for (p, q) in st.asks.iter().take(10).chain(st.bids.iter().rev().take(10)) {
            s.push_str(&Self::digits(*p, self.price_dec)?);
            s.push_str(&Self::digits(*q, self.qty_dec)?);
        }
        Some(crc32(s.as_bytes()))
    }
}

#[derive(Deserialize)]
struct Env<'a> {
    #[serde(borrow, default)]
    channel: Option<&'a str>,
    #[serde(borrow, default, rename = "type")]
    kind: Option<&'a str>,
    #[serde(borrow, default)]
    data: Option<&'a RawValue>,
    #[serde(borrow, default)]
    method: Option<&'a str>,
    #[serde(default)]
    success: Option<bool>,
    #[serde(borrow, default)]
    error: Option<&'a str>,
}

#[derive(Deserialize)]
struct Lvl<'a> {
    #[serde(borrow)]
    price: &'a RawValue,
    #[serde(borrow)]
    qty: &'a RawValue,
}

#[derive(Deserialize)]
struct Book<'a> {
    #[serde(borrow, default)]
    bids: Vec<Lvl<'a>>,
    #[serde(borrow, default)]
    asks: Vec<Lvl<'a>>,
    checksum: u32,
    #[serde(borrow, default)]
    timestamp: Option<&'a str>,
}

#[derive(Deserialize)]
struct Trade<'a> {
    #[serde(borrow)]
    side: &'a str,
    #[serde(borrow)]
    price: &'a RawValue,
    #[serde(borrow)]
    qty: &'a RawValue,
    trade_id: u64,
    #[serde(borrow)]
    timestamp: &'a str,
}

fn num(v: &RawValue) -> Result<i64, String> {
    parse_json_number8(v.get()).map_err(|e| format!("número {}: {e}", v.get()))
}

fn levels(v: &[Lvl<'_>]) -> Result<Vec<(i64, i64)>, String> {
    v.iter().map(|l| Ok((num(l.price)?, num(l.qty)?))).collect()
}

fn to_levels(v: &[(i64, i64)]) -> Vec<Level> {
    v.iter()
        .map(|&(p, q)| Level {
            px: Px::from_raw(p),
            qty: Qty::from_raw(q),
        })
        .collect()
}

impl Protocol for KrakenProtocol {
    fn on_connect(&self) -> Vec<String> {
        if let Ok(mut s) = self.st.lock() {
            s.awaiting_snapshot = true;
            s.resync = false;
            s.bids.clear();
            s.asks.clear();
        }
        let mut v = vec![self.msg("subscribe", "trade")];
        if self.depth {
            v.push(self.msg("subscribe", "book"));
        }
        v
    }

    fn resubscribe(&self) -> Option<Vec<String>> {
        if !self.depth {
            return Some(Vec::new());
        }
        if let Ok(mut s) = self.st.lock() {
            s.awaiting_snapshot = true;
        }
        Some(vec![
            self.msg("unsubscribe", "book"),
            self.msg("subscribe", "book"),
        ])
    }

    fn take_resync(&self) -> bool {
        self.st
            .lock()
            .map(|mut s| std::mem::take(&mut s.resync))
            .unwrap_or(false)
    }

    fn parse(&self, text: &str, rx: RxStamp, out: &mut Vec<MarketEvent>) -> Result<(), String> {
        let env: Env<'_> = serde_json::from_str(text).map_err(|e| e.to_string())?;
        if env.method.is_some() {
            return match env.success {
                Some(false) => Err(format!(
                    "Kraken rechazó {}: {}",
                    env.method.unwrap_or(""),
                    env.error.unwrap_or("")
                )),
                _ => Ok(()),
            };
        }
        let Some(data) = env.data else { return Ok(()) };
        match env.channel {
            Some("book") if self.depth => {
                let books: Vec<Book<'_>> =
                    serde_json::from_str(data.get()).map_err(|e| e.to_string())?;
                let mut st = self
                    .st
                    .lock()
                    .map_err(|_| "estado envenenado".to_string())?;
                for b in books {
                    let ts = b.timestamp.map(rfc3339_ms).transpose()?.unwrap_or(0);
                    let bids = levels(&b.bids)?;
                    let asks = levels(&b.asks)?;
                    let snapshot = env.kind == Some("snapshot");
                    if snapshot {
                        st.bids = bids.iter().copied().filter(|l| l.1 > 0).collect();
                        st.asks = asks.iter().copied().filter(|l| l.1 > 0).collect();
                    } else if st.awaiting_snapshot {
                        continue;
                    }
                    let mut removed_b = Vec::new();
                    let mut removed_a = Vec::new();
                    if !snapshot {
                        for &(p, q) in &bids {
                            if q == 0 {
                                st.bids.remove(&p);
                            } else {
                                st.bids.insert(p, q);
                            }
                        }
                        for &(p, q) in &asks {
                            if q == 0 {
                                st.asks.remove(&p);
                            } else {
                                st.asks.insert(p, q);
                            }
                        }
                    }
                    // Recorte al top N: los que salen se emiten como bajas explícitas.
                    while st.bids.len() > self.n {
                        if let Some((p, _)) = st.bids.pop_first() {
                            removed_b.push((p, 0));
                        }
                    }
                    while st.asks.len() > self.n {
                        if let Some((p, _)) = st.asks.pop_last() {
                            removed_a.push((p, 0));
                        }
                    }
                    if self.checksum(&st) != Some(b.checksum) {
                        st.checksum_bad += 1;
                        st.awaiting_snapshot = true;
                        st.resync = true;
                        return Ok(());
                    }
                    st.checksum_ok += 1;
                    st.counter += 1;
                    if snapshot {
                        st.awaiting_snapshot = false;
                        let mirror_b: Vec<(i64, i64)> =
                            st.bids.iter().rev().map(|(p, q)| (*p, *q)).collect();
                        let mirror_a: Vec<(i64, i64)> =
                            st.asks.iter().map(|(p, q)| (*p, *q)).collect();
                        out.push(MarketEvent::Snapshot(DepthSnapshot {
                            last_update_id: st.counter,
                            limit: self.n,
                            rolling: true,
                            bids: to_levels(&mirror_b),
                            asks: to_levels(&mirror_a),
                            exch_ts_ms: Some(ts),
                            rx,
                        }));
                    } else {
                        let mut bl = bids;
                        bl.extend(removed_b);
                        let mut al = asks;
                        al.extend(removed_a);
                        out.push(MarketEvent::Depth(DepthDiff {
                            first_id: st.counter,
                            last_id: st.counter,
                            prev_last_id: Some(st.counter - 1),
                            exch_ts_ms: ts,
                            match_ts_ms: None,
                            bids: to_levels(&bl),
                            asks: to_levels(&al),
                            rx,
                        }));
                    }
                }
            }
            Some("trade") if env.kind == Some("update") => {
                let trades: Vec<Trade<'_>> =
                    serde_json::from_str(data.get()).map_err(|e| e.to_string())?;
                for t in trades {
                    let ts = rfc3339_ms(t.timestamp)?;
                    out.push(MarketEvent::Trade(AggTrade {
                        agg_id: t.trade_id,
                        first_trade_id: t.trade_id,
                        last_trade_id: t.trade_id,
                        px: Px::from_raw(num(t.price)?),
                        qty: Qty::from_raw(num(t.qty)?),
                        qty_normal: None,
                        // `side` = lado del agresor (verificado 16/16).
                        aggressor: match t.side {
                            "buy" => Aggressor::Buy,
                            "sell" => Aggressor::Sell,
                            o => return Err(format!("side desconocido: {o}")),
                        },
                        trade_ts_ms: ts,
                        exch_ts_ms: ts,
                        rx,
                    }));
                }
            }
            _ => {}
        }
        Ok(())
    }
}

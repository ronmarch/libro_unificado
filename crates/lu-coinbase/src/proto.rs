//! Protocolo y parseo de Coinbase Advanced Trade.

use lu_core::{
    AggTrade, Aggressor, DepthDiff, DepthSnapshot, Level, MarketEvent, Px, Qty, RxStamp,
};
use lu_net::{DepthLeader, Protocol};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// URL pública.
pub const WS_URL: &str = "wss://advanced-trade-ws.coinbase.com";

#[derive(Debug, Default)]
struct State {
    last_seq: Option<u64>,
    awaiting_snapshot: bool,
    resync: bool,
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
}

/// Protocolo Coinbase para un producto y una línea (estado por conexión).
#[derive(Debug)]
pub struct CoinbaseProtocol {
    product: String,
    line: u8,
    leader: Arc<DepthLeader>,
    st: Mutex<State>,
}

impl CoinbaseProtocol {
    /// Todas las líneas de un mercado comparten `leader`: cada una mantiene y valida su
    /// propio libro (secuencia por conexión); solo la líder publica.
    pub fn new(product: impl Into<String>, line: u8, leader: Arc<DepthLeader>) -> Self {
        Self {
            product: product.into(),
            line,
            leader,
            st: Mutex::new(State {
                awaiting_snapshot: true,
                ..State::default()
            }),
        }
    }

    fn msg(&self, kind: &str, channel: &str) -> String {
        serde_json::json!({"type": kind, "product_ids": [self.product], "channel": channel})
            .to_string()
    }
}

pub use lu_core::rfc3339_ms;

#[derive(Deserialize)]
struct Env<'a> {
    #[serde(borrow, default)]
    channel: Option<&'a str>,
    #[serde(default)]
    sequence_num: Option<u64>,
    #[serde(borrow, default)]
    events: Vec<Event<'a>>,
    #[serde(borrow, default, rename = "type")]
    kind: Option<&'a str>,
    #[serde(borrow, default)]
    message: Option<&'a str>,
}

#[derive(Deserialize)]
struct Event<'a> {
    #[serde(borrow, default, rename = "type")]
    kind: Option<&'a str>,
    #[serde(borrow, default)]
    updates: Vec<Upd<'a>>,
    #[serde(borrow, default)]
    trades: Vec<Trade<'a>>,
}

#[derive(Deserialize)]
struct Upd<'a> {
    #[serde(borrow)]
    side: &'a str,
    #[serde(borrow)]
    event_time: &'a str,
    #[serde(borrow)]
    price_level: &'a str,
    #[serde(borrow)]
    new_quantity: &'a str,
}

#[derive(Deserialize)]
struct Trade<'a> {
    #[serde(borrow)]
    trade_id: &'a str,
    #[serde(borrow)]
    price: &'a str,
    #[serde(borrow)]
    size: &'a str,
    #[serde(borrow)]
    time: &'a str,
    #[serde(borrow)]
    side: &'a str,
}

fn apply(m: &mut BTreeMap<i64, i64>, l: &Level) {
    if l.qty.is_zero() {
        m.remove(&l.px.raw());
    } else {
        m.insert(l.px.raw(), l.qty.raw());
    }
}

fn levels(updates: &[Upd<'_>]) -> Result<(Vec<Level>, Vec<Level>, u64), String> {
    let (mut bids, mut asks, mut ts) = (Vec::new(), Vec::new(), 0u64);
    for u in updates {
        let l = Level {
            px: Px::parse(u.price_level).map_err(|e| format!("px {}: {e}", u.price_level))?,
            qty: Qty::parse(u.new_quantity).map_err(|e| format!("qty {}: {e}", u.new_quantity))?,
        };
        ts = ts.max(rfc3339_ms(u.event_time)?);
        match u.side {
            "bid" => bids.push(l),
            "offer" | "ask" => asks.push(l),
            o => return Err(format!("lado desconocido: {o}")),
        }
    }
    Ok((bids, asks, ts))
}

impl Protocol for CoinbaseProtocol {
    fn on_connect(&self) -> Vec<String> {
        if let Ok(mut s) = self.st.lock() {
            s.last_seq = None;
            s.awaiting_snapshot = true;
            s.resync = false;
            s.bids.clear();
            s.asks.clear();
        }
        self.leader.release(self.line);
        vec![
            self.msg("subscribe", "heartbeats"),
            self.msg("subscribe", "market_trades"),
            self.msg("subscribe", "level2"),
        ]
    }

    fn on_disconnect(&self) {
        self.leader.release(self.line);
        if let Ok(mut s) = self.st.lock() {
            s.awaiting_snapshot = true;
        }
    }

    fn resubscribe(&self) -> Option<Vec<String>> {
        // Pedido de snapshot del motor: el liderazgo queda vacante y lo asume la primera
        // línea con estado válido.
        self.leader.vacate();
        if let Ok(mut s) = self.st.lock() {
            s.awaiting_snapshot = true;
        }
        Some(vec![
            self.msg("unsubscribe", "level2"),
            self.msg("subscribe", "level2"),
        ])
    }

    fn keepalive(&self) -> Option<(Duration, &'static str)> {
        None // el canal `heartbeats` mantiene viva la conexión
    }

    fn take_resync(&self) -> bool {
        self.st
            .lock()
            .map(|mut s| std::mem::take(&mut s.resync))
            .unwrap_or(false)
    }

    fn parse(&self, text: &str, rx: RxStamp, out: &mut Vec<MarketEvent>) -> Result<(), String> {
        let env: Env<'_> = serde_json::from_str(text).map_err(|e| e.to_string())?;
        if env.kind == Some("error") {
            return Err(format!("Coinbase error: {}", env.message.unwrap_or("")));
        }
        let mut st = self
            .st
            .lock()
            .map_err(|_| "estado envenenado".to_string())?;
        if let Some(seq) = env.sequence_num {
            if let Some(last) = st.last_seq {
                if seq != last + 1 {
                    // Hueco en ESTA conexión: su libro ya no es confiable.
                    st.awaiting_snapshot = true;
                    st.resync = true;
                    self.leader.release(self.line);
                }
            }
            st.last_seq = Some(seq);
        }
        match env.channel {
            Some("l2_data") => {
                for e in &env.events {
                    let (bids, asks, ts) = levels(&e.updates)?;
                    let snapshot = match e.kind {
                        Some("snapshot") => {
                            st.bids.clear();
                            st.asks.clear();
                            st.awaiting_snapshot = false;
                            true
                        }
                        Some("update") if !st.awaiting_snapshot => false,
                        _ => continue,
                    };
                    for l in &bids {
                        apply(&mut st.bids, l);
                    }
                    for l in &asks {
                        apply(&mut st.asks, l);
                    }
                    // Libro válido. La líder publica; si el liderazgo está vacante esta
                    // línea lo asume y publica su libro completo como estado autoritativo.
                    let acquired = self.leader.try_acquire(self.line);
                    if !acquired && !self.leader.is_leader(self.line) {
                        continue; // seguidora: mantiene su libro en silencio (standby)
                    }
                    let id = self.leader.next_id();
                    if acquired || snapshot {
                        let lv = |m: &BTreeMap<i64, i64>| -> Vec<Level> {
                            m.iter()
                                .map(|(p, q)| Level {
                                    px: Px::from_raw(*p),
                                    qty: Qty::from_raw(*q),
                                })
                                .collect()
                        };
                        let mut b = lv(&st.bids);
                        b.reverse();
                        out.push(MarketEvent::Reset(DepthSnapshot {
                            last_update_id: id,
                            limit: 0, // libro completo: sin truncamiento
                            rolling: false,
                            bids: b,
                            asks: lv(&st.asks),
                            exch_ts_ms: Some(ts),
                            rx,
                        }));
                    } else {
                        out.push(MarketEvent::Depth(DepthDiff {
                            first_id: id,
                            last_id: id,
                            prev_last_id: Some(id - 1),
                            exch_ts_ms: ts,
                            match_ts_ms: None,
                            bids,
                            asks,
                            rx,
                        }));
                    }
                }
            }
            Some("market_trades") => {
                for e in &env.events {
                    if e.kind != Some("update") {
                        continue; // el `snapshot` inicial son trades históricos
                    }
                    for t in &e.trades {
                        let id: u64 = t
                            .trade_id
                            .parse()
                            .map_err(|_| format!("trade_id {}", t.trade_id))?;
                        let ts = rfc3339_ms(t.time)?;
                        out.push(MarketEvent::Trade(AggTrade {
                            agg_id: id,
                            first_trade_id: id,
                            last_trade_id: id,
                            px: Px::parse(t.price).map_err(|e| format!("px: {e}"))?,
                            qty: Qty::parse(t.size).map_err(|e| format!("size: {e}"))?,
                            qty_normal: None,
                            // `side` = lado del maker ⇒ el agresor es el opuesto.
                            aggressor: match t.side {
                                "SELL" => Aggressor::Buy,
                                "BUY" => Aggressor::Sell,
                                o => return Err(format!("side desconocido: {o}")),
                            },
                            trade_ts_ms: ts,
                            exch_ts_ms: ts,
                            rx,
                        }));
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SNAP: &str = r#"{"channel":"l2_data","timestamp":"2026-09-26T04:14:54.777523902Z","sequence_num":0,"events":[{"type":"snapshot","product_id":"SOL-USD","updates":[{"side":"bid","event_time":"2026-09-26T04:14:54.693295Z","price_level":"120.7","new_quantity":"110.06502693"},{"side":"offer","event_time":"2026-09-26T04:14:54.693295Z","price_level":"120.72","new_quantity":"31.20476988"}]}]}"#;
    const UPD: &str = r#"{"channel":"l2_data","timestamp":"2026-09-26T04:15:38.40993531Z","sequence_num":1,"events":[{"type":"update","product_id":"SOL-USD","updates":[{"side":"bid","event_time":"2026-09-26T04:15:38.391474Z","price_level":"120.73","new_quantity":"42.55393162"},{"side":"offer","event_time":"2026-09-26T04:15:38.391474Z","price_level":"120.72","new_quantity":"0"}]}]}"#;
    const TRADE: &str = r#"{"channel":"market_trades","timestamp":"2026-09-26T04:15:00.2Z","sequence_num":2,"events":[{"type":"update","trades":[{"product_id":"SOL-USD","trade_id":"355495415","price":"120.71","size":"1.29080517","time":"2026-09-26T04:15:00.114692Z","side":"SELL"}]}]}"#;

    fn run(p: &CoinbaseProtocol, t: &str) -> Vec<MarketEvent> {
        let mut out = Vec::new();
        p.parse(t, RxStamp::default(), &mut out).unwrap();
        out
    }

    fn upd(seq: u64, px: &str, qty: &str) -> String {
        UPD.replace("\"sequence_num\":1", &format!("\"sequence_num\":{seq}"))
            .replace("\"120.73\"", &format!("\"{px}\""))
            .replace("\"42.55393162\"", &format!("\"{qty}\""))
    }

    #[test]
    fn snapshot_update_y_contador_contiguo() {
        let p = CoinbaseProtocol::new("SOL-USD", 0, DepthLeader::new());
        p.on_connect();
        let MarketEvent::Reset(s) = &run(&p, SNAP)[0] else {
            panic!()
        };
        assert_eq!((s.last_update_id, s.limit), (1, 0));
        assert_eq!(s.asks[0].px, Px::parse("120.72").unwrap());
        let MarketEvent::Depth(d) = &run(&p, UPD)[0] else {
            panic!()
        };
        assert_eq!((d.first_id, d.last_id, d.prev_last_id), (2, 2, Some(1)));
        assert_eq!(d.asks[0].qty, Qty::ZERO);
        assert!(!p.take_resync());
    }

    #[test]
    fn trade_side_es_maker_y_agresor_opuesto() {
        let p = CoinbaseProtocol::new("SOL-USD", 0, DepthLeader::new());
        p.on_connect();
        let MarketEvent::Trade(t) = &run(&p, TRADE)[0] else {
            panic!()
        };
        assert_eq!(t.aggressor, Aggressor::Buy);
        assert_eq!(t.agg_id, 355495415);
    }

    #[test]
    fn hueco_de_secuencia_detiene_profundidad_y_pide_resync() {
        let p = CoinbaseProtocol::new("SOL-USD", 0, DepthLeader::new());
        p.on_connect();
        run(&p, SNAP);
        let gap = UPD.replace("\"sequence_num\":1", "\"sequence_num\":5");
        assert!(
            run(&p, &gap).is_empty(),
            "no se emite profundidad tras un hueco"
        );
        assert!(p.take_resync());
        assert!(!p.take_resync(), "el pedido se consume una vez");
        let upd2 = UPD.replace("\"sequence_num\":1", "\"sequence_num\":6");
        assert!(run(&p, &upd2).is_empty(), "sigue esperando snapshot");
        let snap2 = SNAP.replace("\"sequence_num\":0", "\"sequence_num\":7");
        let MarketEvent::Reset(s) = &run(&p, &snap2)[0] else {
            panic!()
        };
        assert_eq!(s.last_update_id, 2, "el contador nunca retrocede");
    }

    #[test]
    fn relevo_de_linea_publica_el_libro_de_la_seguidora() {
        let leader = DepthLeader::new();
        let a = CoinbaseProtocol::new("SOL-USD", 0, leader.clone());
        let b = CoinbaseProtocol::new("SOL-USD", 1, leader.clone());
        a.on_connect();
        b.on_connect();
        assert!(matches!(run(&a, SNAP)[0], MarketEvent::Reset(_)), "A asume");
        assert!(run(&b, SNAP).is_empty(), "B es seguidora");
        assert!(matches!(
            run(&a, &upd(1, "120.73", "5")).as_slice(),
            [MarketEvent::Depth(_)]
        ));
        assert!(run(&b, &upd(1, "120.73", "5")).is_empty());
        // A pierde un mensaje (hueco en su conexión): deja el liderazgo.
        assert!(run(&a, &upd(3, "120.74", "6")).is_empty());
        assert!(!leader.is_leader(0));
        // B, válida, recibe el mismo update y asume publicando SU libro completo.
        let ev = run(&b, &upd(2, "120.74", "6"));
        let [MarketEvent::Reset(s)] = ev.as_slice() else {
            panic!("se esperaba el relevo: {ev:?}")
        };
        assert!(leader.is_leader(1));
        assert_eq!(s.last_update_id, 3, "ids contiguos tras el relevo");
        let bids: Vec<String> = s
            .bids
            .iter()
            .map(|l| format!("{}={}", l.px, l.qty))
            .collect();
        assert_eq!(bids, ["120.74=6", "120.73=5", "120.7=110.06502693"]);
        assert!(
            s.asks.is_empty(),
            "el ask 120.72 fue eliminado por el update"
        );
        let MarketEvent::Depth(d) = &run(&b, &upd(3, "120.75", "7"))[0] else {
            panic!()
        };
        assert_eq!((d.last_id, d.prev_last_id), (4, Some(3)));
    }
}

//! Endpoints, protocolo y parseo de OKX.

use lu_core::{
    AggTrade, Aggressor, DepthDiff, DepthSnapshot, Level, MarketEvent, MarketId, MarketKind, Px,
    Qty, RxStamp, Venue, SCALE,
};
use lu_net::Protocol;
use serde::Deserialize;
use serde_json::value::RawValue;
use std::time::Duration;

/// Niveles que mantiene el canal `books`.
pub const BOOK_DEPTH: usize = 400;

/// Topología de un mercado OKX.
#[derive(Debug, Clone)]
pub struct OkxEndpoints {
    /// Mercado canónico (`okx.spot.SOLUSDT`).
    pub market: MarketId,
    /// `instId` nativo (`SOL-USDT`, `SOL-USDT-SWAP`).
    pub inst_id: String,
    /// `instType` para REST (`SPOT` / `SWAP`).
    pub inst_type: &'static str,
    /// URL WebSocket por línea.
    pub lines: Vec<String>,
    /// Alternativas por línea.
    pub fallbacks: Vec<Vec<String>>,
    /// Host REST.
    pub rest_host: String,
}

/// `SOLUSDT` → (`SOL`, `USDT`).
pub fn split_symbol(symbol: &str) -> Option<(&str, &str)> {
    ["USDT", "USDC", "USD"]
        .iter()
        .find_map(|q| symbol.strip_suffix(q).map(|b| (b, *q)))
        .filter(|(b, _)| !b.is_empty())
}

impl OkxEndpoints {
    /// Endpoints de spot o perpetuo lineal para un símbolo estilo `SOLUSDT`.
    pub fn new(symbol: &str, kind: MarketKind, lines: usize) -> Option<Self> {
        let (base, quote) = split_symbol(symbol)?;
        let (inst_id, inst_type) = match kind {
            MarketKind::Spot => (format!("{base}-{quote}"), "SPOT"),
            MarketKind::Perp => (format!("{base}-{quote}-SWAP"), "SWAP"),
        };
        // Puerto 443 (8443 suele estar bloqueado por proxies); host AWS como alternativa.
        let hosts = [
            "wss://ws.okx.com/ws/v5/public",
            "wss://wsaws.okx.com:8443/ws/v5/public",
        ];
        let n = lines.max(1);
        Some(Self {
            market: MarketId::new(Venue::Okx, kind, symbol),
            inst_id,
            inst_type,
            lines: (0..n).map(|_| hosts[0].to_string()).collect(),
            fallbacks: (0..n).map(|_| vec![hosts[1].to_string()]).collect(),
            rest_host: "https://www.okx.com".into(),
        })
    }
}

/// Protocolo OKX para un instrumento (books + trades en la misma conexión).
#[derive(Debug, Clone)]
pub struct OkxProtocol {
    inst_id: String,
    /// Unidades de base por contrato (1 en spot).
    contract: Qty,
}

impl OkxProtocol {
    /// `contract` = `ctVal` (perpetuo) o 1 (spot).
    pub fn new(inst_id: impl Into<String>, contract: Qty) -> Self {
        Self {
            inst_id: inst_id.into(),
            contract,
        }
    }

    fn sub(&self, op: &str, channels: &[&str]) -> String {
        let args: Vec<serde_json::Value> = channels
            .iter()
            .map(|c| serde_json::json!({"channel": c, "instId": self.inst_id}))
            .collect();
        serde_json::json!({"op": op, "args": args}).to_string()
    }

    /// Tamaño del venue → cantidad en unidades de base, exacta (sin redondeo).
    fn qty(&self, sz: &str) -> Result<Qty, String> {
        let q = Qty::parse(sz).map_err(|e| format!("sz {sz}: {e}"))?;
        if self.contract == Qty::from_units(1) {
            return Ok(q);
        }
        let prod = i128::from(q.raw()) * i128::from(self.contract.raw());
        if prod % i128::from(SCALE) != 0 {
            return Err(format!("sz {sz} × ctVal no es exacto en 1e-8"));
        }
        i64::try_from(prod / i128::from(SCALE))
            .map(Qty::from_raw)
            .map_err(|_| "desbordamiento de cantidad".to_string())
    }

    fn levels(&self, raw: &[(&str, &str, &str, &str)]) -> Result<Vec<Level>, String> {
        raw.iter()
            .map(|(p, s, _, _)| {
                Ok(Level {
                    px: Px::parse(p).map_err(|e| format!("px {p}: {e}"))?,
                    qty: self.qty(s)?,
                })
            })
            .collect()
    }
}

#[derive(Deserialize)]
struct Envelope<'a> {
    #[serde(borrow, default)]
    event: Option<&'a str>,
    #[serde(borrow, default)]
    msg: Option<&'a str>,
    #[serde(borrow, default)]
    arg: Option<Arg<'a>>,
    #[serde(borrow, default)]
    action: Option<&'a str>,
    #[serde(borrow, default)]
    data: Option<&'a RawValue>,
}

#[derive(Deserialize)]
struct Arg<'a> {
    #[serde(borrow)]
    channel: &'a str,
}

#[derive(Deserialize)]
struct BookWire<'a> {
    #[serde(borrow)]
    asks: Vec<(&'a str, &'a str, &'a str, &'a str)>,
    #[serde(borrow)]
    bids: Vec<(&'a str, &'a str, &'a str, &'a str)>,
    #[serde(borrow)]
    ts: &'a str,
    #[serde(rename = "seqId")]
    seq_id: i64,
    #[serde(rename = "prevSeqId")]
    prev_seq_id: i64,
}

#[derive(Deserialize)]
struct TradeWire<'a> {
    #[serde(rename = "tradeId", borrow)]
    trade_id: &'a str,
    #[serde(borrow)]
    px: &'a str,
    #[serde(borrow)]
    sz: &'a str,
    #[serde(borrow)]
    side: &'a str,
    #[serde(borrow)]
    ts: &'a str,
    #[serde(borrow, default)]
    count: Option<&'a str>,
}

fn num(s: &str, what: &str) -> Result<u64, String> {
    s.parse::<u64>()
        .map_err(|_| format!("{what} inválido: {s}"))
}

impl Protocol for OkxProtocol {
    fn on_connect(&self) -> Vec<String> {
        vec![self.sub("subscribe", &["books", "trades"])]
    }

    fn resubscribe(&self) -> Option<Vec<String>> {
        Some(vec![
            self.sub("unsubscribe", &["books"]),
            self.sub("subscribe", &["books"]),
        ])
    }

    fn keepalive(&self) -> Option<(Duration, &'static str)> {
        Some((Duration::from_secs(20), "ping"))
    }

    fn parse(&self, text: &str, rx: RxStamp, out: &mut Vec<MarketEvent>) -> Result<(), String> {
        if text == "pong" {
            return Ok(());
        }
        let env: Envelope<'_> = serde_json::from_str(text).map_err(|e| e.to_string())?;
        if let Some(ev) = env.event {
            return match ev {
                "error" => Err(format!("OKX error: {}", env.msg.unwrap_or(""))),
                _ => Ok(()), // subscribe / unsubscribe / notice
            };
        }
        let (Some(arg), Some(data)) = (env.arg, env.data) else {
            return Ok(());
        };
        match arg.channel {
            "books" => {
                let books: Vec<BookWire<'_>> =
                    serde_json::from_str(data.get()).map_err(|e| e.to_string())?;
                for b in books {
                    let ts = num(b.ts, "ts")?;
                    let seq = u64::try_from(b.seq_id).map_err(|_| "seqId negativo".to_string())?;
                    let bids = self.levels(&b.bids)?;
                    let asks = self.levels(&b.asks)?;
                    if env.action == Some("snapshot") {
                        out.push(MarketEvent::Snapshot(DepthSnapshot {
                            last_update_id: seq,
                            limit: BOOK_DEPTH,
                            rolling: true,
                            bids,
                            asks,
                            exch_ts_ms: Some(ts),
                            rx,
                        }));
                    } else {
                        let prev = u64::try_from(b.prev_seq_id)
                            .map_err(|_| "update con prevSeqId negativo".to_string())?;
                        out.push(MarketEvent::Depth(DepthDiff {
                            first_id: prev + 1,
                            last_id: seq,
                            prev_last_id: Some(prev),
                            exch_ts_ms: ts,
                            match_ts_ms: None,
                            bids,
                            asks,
                            rx,
                        }));
                    }
                }
            }
            "trades" => {
                let trades: Vec<TradeWire<'_>> =
                    serde_json::from_str(data.get()).map_err(|e| e.to_string())?;
                for t in trades {
                    let id = num(t.trade_id, "tradeId")?;
                    let ts = num(t.ts, "ts")?;
                    let count = t.count.map_or(Ok(1), |c| num(c, "count"))?.max(1);
                    out.push(MarketEvent::Trade(AggTrade {
                        agg_id: id,
                        first_trade_id: id,
                        last_trade_id: id + count - 1,
                        px: Px::parse(t.px).map_err(|e| format!("px: {e}"))?,
                        qty: self.qty(t.sz)?,
                        qty_normal: None,
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

#[cfg(test)]
mod tests {
    use super::*;

    // Mensajes reales capturados de OKX el 2026-09-26 (recortados a 2 niveles).
    const SNAP: &str = r#"{"arg":{"channel":"books","instId":"SOL-USDT"},"action":"snapshot","data":[{"asks":[["120.67","90.475897","0","6"],["120.68","83.806786","0","7"]],"bids":[["120.66","28.448458","0","3"],["120.65","46.716665","0","3"]],"ts":"1790394877200","checksum":0,"seqId":32032594283,"prevSeqId":-1}]}"#;
    const UPD: &str = r#"{"arg":{"channel":"books","instId":"SOL-USDT"},"action":"update","data":[{"asks":[["120.67","60.475897","0","5"]],"bids":[["120.66","0","0","0"]],"ts":"1790394877300","checksum":0,"seqId":32032594315,"prevSeqId":32032594283}]}"#;
    const TRADE: &str = r#"{"arg":{"channel":"trades","instId":"SOL-USDT-SWAP"},"data":[{"instId":"SOL-USDT-SWAP","tradeId":"1632918528","px":"120.6","sz":"391.07","side":"sell","ts":"1790394876983","count":"26","source":"0","seqId":92286018766}]}"#;

    fn parse(p: &OkxProtocol, t: &str) -> Vec<MarketEvent> {
        let mut out = Vec::new();
        p.parse(t, RxStamp::default(), &mut out).unwrap();
        out
    }

    #[test]
    fn snapshot_rodante_y_update_encadenado() {
        let p = OkxProtocol::new("SOL-USDT", Qty::from_units(1));
        let MarketEvent::Snapshot(s) = &parse(&p, SNAP)[0] else {
            panic!("se esperaba snapshot")
        };
        assert_eq!(s.last_update_id, 32032594283);
        assert!(s.rolling);
        assert_eq!(s.bids[0].px, Px::parse("120.66").unwrap());
        assert_eq!(s.asks[1].qty, Qty::parse("83.806786").unwrap());
        let MarketEvent::Depth(d) = &parse(&p, UPD)[0] else {
            panic!("se esperaba update")
        };
        assert_eq!(d.prev_last_id, Some(32032594283));
        assert_eq!((d.first_id, d.last_id), (32032594284, 32032594315));
        assert_eq!(d.bids[0].qty, Qty::ZERO);
        assert_eq!(d.exch_ts_ms, 1790394877300);
    }

    #[test]
    fn trade_agresor_fills_y_contratos() {
        let p = OkxProtocol::new("SOL-USDT-SWAP", Qty::parse("0.1").unwrap());
        let MarketEvent::Trade(t) = &parse(&p, TRADE)[0] else {
            panic!("se esperaba trade")
        };
        assert_eq!(t.aggressor, Aggressor::Sell);
        assert_eq!(t.n_fills(), 26);
        assert_eq!(t.qty, Qty::parse("39.107").unwrap()); // 391.07 contratos × 0.1
        assert_eq!(t.trade_ts_ms, 1790394876983);
    }

    #[test]
    fn eventos_de_control() {
        let p = OkxProtocol::new("SOL-USDT", Qty::from_units(1));
        assert!(parse(&p, "pong").is_empty());
        assert!(parse(
            &p,
            r#"{"event":"subscribe","arg":{"channel":"books","instId":"SOL-USDT"},"connId":"x"}"#
        )
        .is_empty());
        let mut out = Vec::new();
        assert!(p
            .parse(
                r#"{"event":"error","msg":"bad","code":"60012"}"#,
                RxStamp::default(),
                &mut out
            )
            .is_err());
        assert_eq!(split_symbol("SOLUSDT"), Some(("SOL", "USDT")));
        let e = OkxEndpoints::new("SOLUSDT", MarketKind::Perp, 2).unwrap();
        assert_eq!(e.inst_id, "SOL-USDT-SWAP");
        assert_eq!(e.market.to_string(), "okx.perp.SOLUSDT");
    }
}

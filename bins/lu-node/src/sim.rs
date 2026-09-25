//! Exchange sintético (`--sim`) y caos (`--chaos-*`): base de F5.
//!
//! Genera un mercado con reglas de secuencia de Binance (spot: ids contiguos;
//! perp: ids con saltos y `pu`), lotes de 100 ms, órdenes límite, cancelaciones,
//! órdenes a mercado que barren niveles, icebergs que recargan y RPI en perp.
//! Lo entrega al motor REAL por líneas redundantes con latencia, pérdidas y
//! cortes configurables, y responde snapshots.
//!
//! Un verificador compara continuamente el libro publicado por el motor con la
//! verdad del simulador en el mismo `last_update_id` (propiedad P2 en vivo):
//! `lu_sim_checks_total{result="mismatch"}` debe ser siempre 0.

use crate::engine::{ChannelSink, EngineMsg, IngestCounters, StreamKind};
use crate::view::Views;
use crossbeam_channel::Sender;
use lu_binance::FrameSink;
use lu_book::Phase;
use lu_core::{
    wall_now_ns, AggTrade, Aggressor, DepthDiff, DepthSnapshot, InstrumentSpec, Level, MarketEvent,
    MarketId, MarketKind, Px, Qty, RxStamp,
};
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, watch};

const TICK: i64 = 1_000_000; // 0.01
const STEP: i64 = 100_000; // 0.001
const TOP_CHECK: usize = 10;

/// Parámetros del simulador y del caos.
#[derive(Debug, Clone)]
pub struct SimConfig {
    /// Semilla del generador.
    pub seed: u64,
    /// Probabilidad de perder cada evento en cada línea (independiente por línea).
    pub drop: f64,
    /// Latencia adicional máxima por línea (ms, uniforme).
    pub jitter_ms: u64,
    /// Segundos medios entre cortes de cada línea (0 = sin cortes).
    pub outage_every_s: u64,
    /// Probabilidad de que un snapshot solicitado no llegue.
    pub snapshot_fail: f64,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            seed: 0x5eed,
            drop: 0.0,
            jitter_ms: 20,
            outage_every_s: 0,
            snapshot_fail: 0.0,
        }
    }
}

/// Contadores del simulador (se exportan a Prometheus).
#[derive(Debug, Default)]
pub struct SimStats {
    /// Comparaciones libro publicado vs verdad.
    pub checks_ok: AtomicU64,
    /// Discrepancias (defecto: debe ser 0).
    pub checks_mismatch: AtomicU64,
    /// Diffs generados.
    pub diffs: AtomicU64,
    /// Trades generados.
    pub trades: AtomicU64,
    /// Eventos perdidos a propósito por el caos (suma de líneas).
    pub chaos_dropped: AtomicU64,
    /// Cortes de línea provocados.
    pub chaos_outages: AtomicU64,
    /// Snapshots no entregados a propósito.
    pub chaos_snapshot_fail: AtomicU64,
}

/// xorshift64* determinista.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
    fn chance(&mut self, p: f64) -> bool {
        p > 0.0 && (self.next() >> 11) as f64 / (1u64 << 53) as f64 <= p
    }
}

type Top = (Vec<(Px, Qty)>, Vec<(Px, Qty)>);

struct Truth {
    kind: MarketKind,
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
    iceberg: BTreeMap<(bool, i64), i64>,
    last_id: u64,
    last_end_ms: u64,
    agg_id: u64,
    rng: Rng,
    history: BTreeMap<u64, Top>,
}

impl Truth {
    fn new(kind: MarketKind, seed: u64) -> Self {
        let mut t = Self {
            kind,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            iceberg: BTreeMap::new(),
            last_id: 1_000_000,
            last_end_ms: wall_now_ns() / 1_000_000,
            agg_id: 1,
            rng: Rng(seed | 1),
            history: BTreeMap::new(),
        };
        let mid = 150 * 100_000_000i64;
        for i in 1..=400 {
            let q = t.qty();
            t.bids.insert(mid - i * TICK, q);
            let q = t.qty();
            t.asks.insert(mid + i * TICK, q);
        }
        t
    }

    fn qty(&mut self) -> i64 {
        // Mayoría pequeña, cola larga (paretiana aproximada).
        let r = self.rng.below(1000);
        let units = if r < 700 {
            1 + self.rng.below(2_000)
        } else if r < 970 {
            2_000 + self.rng.below(20_000)
        } else {
            20_000 + self.rng.below(200_000)
        };
        units as i64 * STEP
    }

    fn top(&self) -> Top {
        (
            self.bids
                .iter()
                .rev()
                .take(TOP_CHECK)
                .map(|(p, q)| (Px::from_raw(*p), Qty::from_raw(*q)))
                .collect(),
            self.asks
                .iter()
                .take(TOP_CHECK)
                .map(|(p, q)| (Px::from_raw(*p), Qty::from_raw(*q)))
                .collect(),
        )
    }

    fn snapshot(&self) -> DepthSnapshot {
        let lv = |(p, q): (&i64, &i64)| Level {
            px: Px::from_raw(*p),
            qty: Qty::from_raw(*q),
        };
        DepthSnapshot {
            last_update_id: self.last_id,
            limit: 5000,
            bids: self.bids.iter().rev().map(lv).collect(),
            asks: self.asks.iter().map(lv).collect(),
            exch_ts_ms: Some(self.last_end_ms),
            rx: RxStamp::now(0),
        }
    }

    /// Un lote de 100 ms: devuelve el diff y los trades.
    fn step(&mut self, end_ms: u64) -> (DepthDiff, Vec<AggTrade>) {
        let start = self.last_end_ms;
        let end = end_ms.max(start + 1);
        let mut changed: BTreeMap<(bool, i64), ()> = BTreeMap::new();
        let mut trades = Vec::new();
        let n = 5 + self.rng.below(30);
        for _ in 0..n {
            let bid = self.rng.below(2) == 0;
            let r = self.rng.below(100);
            let (bb, ba) = (
                *self.bids.keys().next_back().unwrap_or(&(149 * 100_000_000)),
                *self.asks.keys().next().unwrap_or(&(151 * 100_000_000)),
            );
            if r < 55 {
                let dist = if self.rng.below(100) < 70 {
                    self.rng.below(6)
                } else {
                    self.rng.below(80)
                } as i64;
                // Creadores de mercado: si el spread supera 1 tick, se cotiza dentro.
                let inside = ba - bb > TICK && self.rng.below(100) < 60;
                let px = if inside {
                    if bid {
                        bb + TICK
                    } else {
                        ba - TICK
                    }
                } else if bid {
                    (bb - dist * TICK).min(ba - TICK)
                } else {
                    (ba + dist * TICK).max(bb + TICK)
                };
                let q = self.qty();
                let book = if bid { &mut self.bids } else { &mut self.asks };
                *book.entry(px).or_insert(0) += q;
                changed.insert((bid, px), ());
                if self.rng.below(100) < 3 {
                    let reserve = self.qty() * 5;
                    *self.iceberg.entry((bid, px)).or_insert(0) += reserve;
                }
            } else if r < 85 {
                let book = if bid { &mut self.bids } else { &mut self.asks };
                let depth = book.len().min(30);
                if depth == 0 {
                    continue;
                }
                let idx = self.rng.below(depth as u64) as usize;
                let px = if bid {
                    *book.keys().rev().nth(idx).unwrap_or(&0)
                } else {
                    *book.keys().nth(idx).unwrap_or(&0)
                };
                let cur = book.get(&px).copied().unwrap_or(0);
                let cut = if self.rng.below(100) < 25 {
                    cur
                } else {
                    (cur / STEP * (1 + self.rng.below(90) as i64) / 100) * STEP
                };
                let new = cur - cut;
                if new <= 0 {
                    book.remove(&px);
                    self.iceberg.remove(&(bid, px));
                } else {
                    book.insert(px, new);
                }
                changed.insert((bid, px), ());
            } else {
                // Orden a mercado: agresor compra (consume asks) si !bid.
                let maker_bid = bid;
                let mut rem = (self.qty() / 3 / STEP).max(1) * STEP;
                let t = start + 1 + self.rng.below(end - start);
                while rem > 0 {
                    let book = if maker_bid {
                        &mut self.bids
                    } else {
                        &mut self.asks
                    };
                    let best = if maker_bid {
                        book.keys().next_back().copied()
                    } else {
                        book.keys().next().copied()
                    };
                    let Some(px) = best else { break };
                    let avail = book[&px];
                    let take = rem.min(avail);
                    rem -= take;
                    let left = avail - take;
                    changed.insert((maker_bid, px), ());
                    if left == 0 {
                        book.remove(&px);
                        if let Some(res) = self.iceberg.remove(&(maker_bid, px)) {
                            let refill = res.min(20_000 * STEP);
                            book.insert(px, refill);
                            if res > refill {
                                self.iceberg.insert((maker_bid, px), res - refill);
                            }
                        }
                    } else {
                        book.insert(px, left);
                    }
                    let rpi = if self.kind == MarketKind::Perp && self.rng.below(100) < 5 {
                        (1 + self.rng.below(200) as i64) * STEP
                    } else {
                        0
                    };
                    self.agg_id += 1;
                    let tid = self.agg_id * 10;
                    trades.push(AggTrade {
                        agg_id: self.agg_id,
                        first_trade_id: tid,
                        last_trade_id: tid + self.rng.below(4),
                        px: Px::from_raw(px),
                        qty: Qty::from_raw(take + rpi),
                        qty_normal: (self.kind == MarketKind::Perp).then_some(Qty::from_raw(take)),
                        aggressor: if maker_bid {
                            Aggressor::Sell
                        } else {
                            Aggressor::Buy
                        },
                        trade_ts_ms: t,
                        exch_ts_ms: t + self.rng.below(3),
                        rx: RxStamp::default(),
                    });
                }
            }
        }
        // Reponer profundidad lejana (libro de ~400 niveles por lado).
        for bid in [true, false] {
            let book = if bid { &mut self.bids } else { &mut self.asks };
            while book.len() < 300 {
                let far = if bid {
                    book.keys().next().copied().unwrap_or(150 * 100_000_000) - TICK
                } else {
                    book.keys()
                        .next_back()
                        .copied()
                        .unwrap_or(150 * 100_000_000)
                        + TICK
                };
                let q = (1 + (far.unsigned_abs() / TICK as u64 * 7_919 % 20_000) as i64) * STEP;
                book.insert(far, q);
                changed.insert((bid, far), ());
            }
        }
        let lv = |book: &BTreeMap<i64, i64>, px: i64| Level {
            px: Px::from_raw(px),
            qty: Qty::from_raw(book.get(&px).copied().unwrap_or(0)),
        };
        let bids = changed
            .keys()
            .filter(|(b, _)| *b)
            .map(|(_, p)| lv(&self.bids, *p))
            .collect();
        let asks = changed
            .keys()
            .filter(|(b, _)| !*b)
            .map(|(_, p)| lv(&self.asks, *p))
            .collect();
        let prev = self.last_id;
        let (first, pu) = match self.kind {
            MarketKind::Spot => (prev + 1, None),
            MarketKind::Perp => (prev + 1 + self.rng.below(5), Some(prev)),
        };
        let last = first + self.rng.below(4);
        self.last_id = last;
        self.last_end_ms = end;
        let top = self.top();
        self.history.insert(last, top);
        while self.history.len() > 4_000 {
            self.history.pop_first();
        }
        let diff = DepthDiff {
            first_id: first,
            last_id: last,
            prev_last_id: pu,
            exch_ts_ms: end + u64::from(self.kind == MarketKind::Perp),
            match_ts_ms: (self.kind == MarketKind::Perp).then_some(end),
            bids,
            asks,
            rx: RxStamp::default(),
        };
        (diff, trades)
    }
}

/// Línea simulada: latencia, pérdidas y cortes antes de entregar al motor.
async fn line_task(
    line: u8,
    mut rx: mpsc::UnboundedReceiver<(MarketEvent, bool)>,
    depth: Arc<dyn FrameSink>,
    trade: Arc<dyn FrameSink>,
    cfg: SimConfig,
    stats: Arc<SimStats>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut rng = Rng(cfg.seed ^ (0x9e37_79b9 * (u64::from(line) + 1)));
    let mut down_until: Option<tokio::time::Instant> = None;
    let mut next_outage = outage_at(&mut rng, &cfg);
    let mut deliver_at = tokio::time::Instant::now();
    depth.line_up(line);
    trade.line_up(line);
    loop {
        let (ev, is_trade) = tokio::select! {
            _ = shutdown.changed() => return,
            m = rx.recv() => match m { Some(m) => m, None => return },
        };
        let now = tokio::time::Instant::now();
        if let Some(t) = next_outage {
            if now >= t {
                let secs = 5 + rng.below(25);
                down_until = Some(now + Duration::from_secs(secs));
                next_outage = None;
                stats.chaos_outages.fetch_add(1, Ordering::Relaxed);
                depth.line_down(line, "caos: corte de línea");
                trade.line_down(line, "caos: corte de línea");
            }
        }
        if let Some(u) = down_until {
            if now < u {
                continue;
            }
            down_until = None;
            next_outage = outage_at(&mut rng, &cfg);
            depth.line_up(line);
            trade.line_up(line);
        }
        if rng.chance(cfg.drop) {
            stats.chaos_dropped.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        // Orden FIFO dentro de la línea: la latencia nunca reordena un mismo socket.
        // Un retardo por lote (al llegar el diff); los trades viajan con el mismo retardo.
        let extra = if is_trade {
            Duration::ZERO
        } else {
            Duration::from_millis(rng.below(cfg.jitter_ms + 1))
        };
        deliver_at = deliver_at.max(now + extra);
        if deliver_at > tokio::time::Instant::now() {
            tokio::time::sleep_until(deliver_at).await;
        }
        let ev = match ev {
            MarketEvent::Depth(mut d) => {
                d.rx = RxStamp::now(line);
                MarketEvent::Depth(d)
            }
            MarketEvent::Trade(mut t) => {
                t.rx = RxStamp::now(line);
                MarketEvent::Trade(t)
            }
            MarketEvent::Ignored => MarketEvent::Ignored,
        };
        if is_trade {
            trade.deliver(ev);
        } else {
            depth.deliver(ev);
        }
    }
}

fn outage_at(rng: &mut Rng, cfg: &SimConfig) -> Option<tokio::time::Instant> {
    (cfg.outage_every_s > 0).then(|| {
        let s = cfg.outage_every_s / 2 + rng.below(cfg.outage_every_s + 1);
        tokio::time::Instant::now() + Duration::from_secs(s)
    })
}

/// Ejecuta el mercado simulado hasta el apagado.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    market: MarketId,
    lines: usize,
    cfg: SimConfig,
    eng: Sender<EngineMsg>,
    counters: Arc<IngestCounters>,
    mut snap_req: mpsc::UnboundedReceiver<()>,
    views: Views,
    stats: Arc<SimStats>,
    shutdown: watch::Receiver<bool>,
) {
    let label = market.to_string();
    let kind = market.kind;
    let seed = cfg.seed ^ if kind == MarketKind::Perp { 0xfeed } else { 0 };
    let truth = Arc::new(Mutex::new(Truth::new(kind, seed)));
    crate::snapshot::deliver(
        &eng,
        EngineMsg::Spec(InstrumentSpec {
            market: market.clone(),
            tick: Px::from_raw(TICK),
            step: Qty::from_raw(STEP),
            base_per_contract: Qty::from_units(1),
        }),
    )
    .await;

    // Líneas.
    let mut line_tx = Vec::new();
    for li in 0..lines {
        let (tx, rx) = mpsc::unbounded_channel();
        line_tx.push(tx);
        let mk = |k| -> Arc<dyn FrameSink> {
            Arc::new(ChannelSink::new(
                eng.clone(),
                counters.clone(),
                k,
                label.clone(),
            ))
        };
        let (d, t) = match kind {
            MarketKind::Spot => (mk(StreamKind::Depth), mk(StreamKind::Depth)),
            MarketKind::Perp => (mk(StreamKind::Depth), mk(StreamKind::Trade)),
        };
        tokio::spawn(line_task(
            li as u8,
            rx,
            d,
            t,
            cfg.clone(),
            stats.clone(),
            shutdown.clone(),
        ));
    }

    // Snapshots.
    {
        let (truth, eng, stats, mut sd, cfg) = (
            truth.clone(),
            eng.clone(),
            stats.clone(),
            shutdown.clone(),
            cfg.clone(),
        );
        tokio::spawn(async move {
            let mut rng = Rng(cfg.seed ^ 0xabcd);
            loop {
                tokio::select! {
                    _ = sd.changed() => return,
                    r = snap_req.recv() => if r.is_none() { return },
                }
                while snap_req.try_recv().is_ok() {}
                tokio::time::sleep(Duration::from_millis(50 + rng.below(250))).await;
                if rng.chance(cfg.snapshot_fail) {
                    stats.chaos_snapshot_fail.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                let s = truth.lock().ok().map(|t| t.snapshot());
                if let Some(s) = s {
                    crate::snapshot::deliver(&eng, EngineMsg::Snapshot(s)).await;
                }
            }
        });
    }

    // Verificador P2 en vivo.
    {
        let (truth, stats, mut sd) = (truth.clone(), stats.clone(), shutdown.clone());
        let views = views.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(250));
            let mut recent: VecDeque<u64> = VecDeque::new();
            loop {
                tokio::select! {
                    _ = sd.changed() => return,
                    _ = tick.tick() => {}
                }
                let v = views.book.load_full();
                if v.phase != Phase::Live || recent.contains(&v.last_update_id) {
                    continue;
                }
                let expected = truth
                    .lock()
                    .ok()
                    .and_then(|t| t.history.get(&v.last_update_id).cloned());
                let Some((eb, ea)) = expected else { continue };
                let got_b: Vec<_> = v.top_bids.iter().map(|l| (l.px, l.qty)).collect();
                let got_a: Vec<_> = v.top_asks.iter().map(|l| (l.px, l.qty)).collect();
                let n = got_b.len().min(eb.len());
                let m = got_a.len().min(ea.len());
                if got_b[..n] == eb[..n] && got_a[..m] == ea[..m] {
                    stats.checks_ok.fetch_add(1, Ordering::Relaxed);
                } else {
                    stats.checks_mismatch.fetch_add(1, Ordering::Relaxed);
                    tracing::error!(market = %v.market, id = v.last_update_id, "SIM: libro publicado distinto de la verdad");
                }
                recent.push_back(v.last_update_id);
                if recent.len() > 64 {
                    recent.pop_front();
                }
            }
        });
    }

    tracing::info!(market = %label, lines, ?cfg, "exchange simulado iniciado");
    let mut sd = shutdown.clone();
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = sd.changed() => return,
            _ = tick.tick() => {}
        }
        let now_ms = wall_now_ns() / 1_000_000;
        let Ok((diff, trades)) = truth.lock().map(|mut t| t.step(now_ms)) else {
            return;
        };
        let mut trades = trades;
        // Un socket entrega en orden de `E`.
        trades.sort_by_key(|t| (t.exch_ts_ms, t.agg_id));
        stats.diffs.fetch_add(1, Ordering::Relaxed);
        stats
            .trades
            .fetch_add(trades.len() as u64, Ordering::Relaxed);
        for tx in &line_tx {
            for t in &trades {
                let _ = tx.send((MarketEvent::Trade(t.clone()), true));
            }
            let _ = tx.send((MarketEvent::Depth(diff.clone()), false));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spread_mediano_realista() {
        let mut t = Truth::new(MarketKind::Spot, 7);
        let mut ts = t.last_end_ms;
        let mut spreads = Vec::new();
        for _ in 0..20_000 {
            ts += 100;
            t.step(ts);
            let bb = *t.bids.keys().next_back().unwrap();
            let ba = *t.asks.keys().next().unwrap();
            spreads.push((ba - bb) / TICK);
        }
        spreads.sort_unstable();
        let med = spreads[spreads.len() / 2];
        let p99 = spreads[spreads.len() * 99 / 100];
        assert!(
            med <= 3 && p99 <= 30,
            "mediana {med} ticks, p99 {p99} ticks"
        );
    }

    #[test]
    fn verdad_nunca_cruzada_y_secuencia_valida() {
        for kind in [MarketKind::Spot, MarketKind::Perp] {
            let mut t = Truth::new(kind, 42);
            let mut prev = t.last_id;
            let mut ts = t.last_end_ms;
            for _ in 0..5_000 {
                ts += 100;
                let (d, trades) = t.step(ts);
                let bb = t.bids.keys().next_back().copied().unwrap();
                let ba = t.asks.keys().next().copied().unwrap();
                assert!(bb < ba, "libro cruzado");
                match kind {
                    MarketKind::Spot => assert_eq!(d.first_id, prev + 1),
                    MarketKind::Perp => assert_eq!(d.prev_last_id, Some(prev)),
                }
                for tr in &trades {
                    assert!(tr.trade_ts_ms > ts - 100 && tr.trade_ts_ms <= ts);
                }
                prev = d.last_id;
            }
        }
    }
}

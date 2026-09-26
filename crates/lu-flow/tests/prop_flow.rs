//! Pruebas de propiedades del alineador (F2) contra un motor de matching sintético.
//!
//! El simulador mantiene por nivel una cola FIFO de órdenes visibles (cada
//! trozo marcado con el lote en que nació), reserva de icebergs (se recarga
//! como liquidez visible nueva) y liquidez oscura (ejecutable, nunca visible).
//! Genera diffs absolutos al cierre de cada lote de 100 ms y aggTrades con
//! RPI, entregados por un socket con latencia variable.
//!
//! * **F1 — Cotas válidas**: para todo flujo limpio, `no_visible_min` ≤
//!   ejecutado no proveniente de la liquidez visible al inicio, y
//!   `cancelado_min` ≤ cancelado real. `q0`, `q1` y ejecutado son exactos (δ = 0).
//! * **F2 — Cotas válidas con incertidumbre temporal**: con `T` desplazado
//!   hasta ±δ, las cotas siguen siendo válidas usando `e_lo`/`e_hi`.
//! * **F3 — Completitud**: todo nivel con cambio o ejecución en un lote
//!   limpio cerrado se emite.
//! * **F4 — Conservación**: `unaccounted() == 0` en cada paso, incluso con
//!   pérdidas de sincronización y trades tardíos.

use lu_book::{BookObserver, L2Book, ResyncReason};
use lu_core::{AggTrade, Aggressor, DepthDiff, DepthSnapshot, Level, Px, Qty, RxStamp, Side};
use lu_flow::{Aligner, AlignerConfig, BatchInfo, FlowSink, LevelFlow};
use proptest::prelude::*;
use std::collections::BTreeMap;

const BATCH: u64 = 100;
const NLVL: usize = 10;
const REFILL: i64 = 5;

#[derive(Clone, Debug)]
enum Op {
    Add {
        lvl: u8,
        q: u8,
    },
    Cancel {
        lvl: u8,
        pick: u8,
        q: u8,
    },
    Exec {
        lvl: u8,
        q: u8,
        rpi: u8,
        delay: u8,
        jitter: i8,
    },
    Hidden {
        lvl: u8,
        q: u8,
        iceberg: bool,
    },
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => (0u8..10, 1u8..20).prop_map(|(lvl, q)| Op::Add { lvl, q }),
        2 => (0u8..10, any::<u8>(), 1u8..20).prop_map(|(lvl, pick, q)| Op::Cancel { lvl, pick, q }),
        3 => (0u8..10, 1u8..30, prop_oneof![3 => Just(0u8), 1 => 1u8..5], 0u8..150, any::<i8>())
            .prop_map(|(lvl, q, rpi, delay, jitter)| Op::Exec { lvl, q, rpi, delay, jitter }),
        1 => (0u8..10, 1u8..15, any::<bool>()).prop_map(|(lvl, q, iceberg)| Op::Hidden { lvl, q, iceberg }),
    ]
}

#[derive(Clone, Debug)]
struct BatchSpec {
    ops: Vec<(u8, Op)>,
    depth_delay: u8,
    list_untouched: bool,
}

fn batch_spec() -> impl Strategy<Value = BatchSpec> {
    (
        prop::collection::vec((1u8..=100, op()), 0..8),
        0u8..100,
        any::<bool>(),
    )
        .prop_map(|(ops, depth_delay, list_untouched)| BatchSpec {
            ops,
            depth_delay,
            list_untouched,
        })
}

fn side_px(l: usize) -> (Side, Px) {
    if l < 5 {
        (Side::Bid, Px::from_units(95 + l as i64))
    } else {
        (Side::Ask, Px::from_units(101 + (l as i64 - 5)))
    }
}

#[derive(Clone, Default)]
struct Lvl {
    chunks: Vec<(i64, usize)>,
    iceberg: i64,
    dark: i64,
}

impl Lvl {
    fn vis(&self) -> i64 {
        self.chunks.iter().map(|c| c.0).sum()
    }
}

#[derive(Clone, Copy, Default, Debug)]
struct Truth {
    q0: i64,
    q1: i64,
    consumed_q0: i64,
    exec: i64,
    rpi: i64,
    cancelled: i64,
}

enum Ev {
    Depth {
        diff: DepthDiff,
        prevs: Vec<(Side, Px, Qty)>,
    },
    Trade(AggTrade),
}

struct World {
    init: Vec<Lvl>,
    truth: Vec<[Truth; NLVL]>,
    events: Vec<(u64, u8, Ev)>,
}

/// Construye la verdad y la secuencia de llegada. `slack` > 0 desplaza `T` hasta ±slack.
fn build(init_q: &[u8], specs: &[BatchSpec], futures_ts: bool, slack: u64) -> World {
    let mut lv: Vec<Lvl> = (0..NLVL)
        .map(|i| Lvl {
            chunks: if init_q[i] > 0 {
                vec![(i64::from(init_q[i]), 0)]
            } else {
                vec![]
            },
            ..Lvl::default()
        })
        .collect();
    let init = lv.clone();
    let mut reported: Vec<i64> = lv.iter().map(Lvl::vis).collect();
    let mut truth = Vec::new();
    let mut events = Vec::new();
    let (mut agg_id, mut upd_id) = (1u64, 1000u64);
    let (mut trade_arr, mut depth_arr) = (0u64, 0u64);
    for (k, spec) in specs.iter().enumerate() {
        let born = k + 1; // lote k nace con marca k+1; el estado inicial tiene marca 0
        let start = k as u64 * BATCH;
        let mut tr = [Truth::default(); NLVL];
        for (i, t) in tr.iter_mut().enumerate() {
            t.q0 = lv[i].vis();
        }
        let mut ops = spec.ops.clone();
        ops.sort_by_key(|o| o.0);
        for (frac, o) in ops {
            let t_true = start + u64::from(frac);
            match o {
                Op::Add { lvl, q } => lv[lvl as usize].chunks.push((i64::from(q), born)),
                Op::Cancel { lvl, pick, q } => {
                    let l = &mut lv[lvl as usize];
                    if !l.chunks.is_empty() {
                        let idx = pick as usize % l.chunks.len();
                        let c = i64::from(q).min(l.chunks[idx].0);
                        l.chunks[idx].0 -= c;
                        if l.chunks[idx].0 == 0 {
                            l.chunks.remove(idx);
                        }
                        tr[lvl as usize].cancelled += c;
                    }
                }
                Op::Hidden { lvl, q, iceberg } => {
                    let l = &mut lv[lvl as usize];
                    if iceberg {
                        l.iceberg += i64::from(q);
                    } else {
                        l.dark += i64::from(q);
                    }
                }
                Op::Exec {
                    lvl,
                    q,
                    rpi,
                    delay,
                    jitter,
                } => {
                    let li = lvl as usize;
                    let l = &mut lv[li];
                    let mut rem = i64::from(q);
                    let mut from_q0 = 0;
                    while rem > 0 {
                        if let Some(front) = l.chunks.first_mut() {
                            let t = rem.min(front.0);
                            front.0 -= t;
                            rem -= t;
                            if front.1 < born {
                                from_q0 += t;
                            }
                            if front.0 == 0 {
                                l.chunks.remove(0);
                            }
                        } else if l.iceberg > 0 {
                            let r = l.iceberg.min(REFILL);
                            l.iceberg -= r;
                            l.chunks.push((r, born));
                        } else if l.dark > 0 {
                            let t = rem.min(l.dark);
                            l.dark -= t;
                            rem -= t;
                        } else {
                            break;
                        }
                    }
                    let exec = i64::from(q) - rem;
                    let rpi = i64::from(rpi);
                    if exec == 0 && rpi == 0 {
                        continue;
                    }
                    tr[li].consumed_q0 += from_q0;
                    tr[li].exec += exec;
                    tr[li].rpi += rpi;
                    let shift = if slack == 0 {
                        0
                    } else {
                        i64::from(jitter) % (slack as i64 + 1)
                    };
                    let t_rep = (t_true as i64 + shift).max(1) as u64;
                    let (side, px) = side_px(li);
                    let trade = AggTrade {
                        agg_id,
                        first_trade_id: agg_id * 10,
                        last_trade_id: agg_id * 10 + 1,
                        px,
                        qty: Qty::from_units(exec + rpi),
                        qty_normal: Some(Qty::from_units(exec)),
                        aggressor: if side == Side::Bid {
                            Aggressor::Sell
                        } else {
                            Aggressor::Buy
                        },
                        trade_ts_ms: t_rep,
                        exch_ts_ms: t_true,
                        rx: RxStamp::default(),
                    };
                    agg_id += 1;
                    trade_arr = trade_arr.max(t_true + u64::from(delay));
                    events.push((trade_arr, 1, Ev::Trade(trade)));
                }
            }
        }
        let end = start + BATCH;
        let mut bids = Vec::new();
        let mut asks = Vec::new();
        let mut prevs = Vec::new();
        for (i, t) in tr.iter_mut().enumerate() {
            t.q1 = lv[i].vis();
            let touched = t.exec > 0 || t.cancelled > 0 || t.q1 != t.q0;
            if t.q1 != reported[i] || (spec.list_untouched && touched) {
                let (side, px) = side_px(i);
                let l = Level {
                    px,
                    qty: Qty::from_units(t.q1),
                };
                prevs.push((side, px, Qty::from_units(reported[i])));
                if side == Side::Bid {
                    bids.push(l);
                } else {
                    asks.push(l);
                }
                reported[i] = t.q1;
            }
        }
        truth.push(tr);
        let diff = DepthDiff {
            first_id: upd_id,
            last_id: upd_id,
            prev_last_id: Some(upd_id - 1),
            exch_ts_ms: if futures_ts { end + 3 } else { end },
            match_ts_ms: futures_ts.then_some(end),
            bids,
            asks,
            rx: RxStamp::default(),
        };
        upd_id += 1;
        depth_arr = depth_arr.max(end + u64::from(spec.depth_delay));
        events.push((depth_arr, 0, Ev::Depth { diff, prevs }));
    }
    events.sort_by_key(|e| (e.0, e.1));
    World {
        init,
        truth,
        events,
    }
}

#[derive(Default)]
struct Collect {
    flows: Vec<LevelFlow>,
    batches: Vec<BatchInfo>,
}

impl FlowSink for Collect {
    fn on_level_flow(&mut self, f: &LevelFlow) {
        self.flows.push(*f);
    }
    fn on_batch(&mut self, b: &BatchInfo) {
        self.batches.push(*b);
    }
}

fn init_book(init: &[Lvl]) -> L2Book {
    let mut bids = Vec::new();
    let mut asks = Vec::new();
    for (i, l) in init.iter().enumerate() {
        let (side, px) = side_px(i);
        if l.vis() > 0 {
            let lv = Level {
                px,
                qty: Qty::from_units(l.vis()),
            };
            if side == Side::Bid {
                bids.push(lv)
            } else {
                asks.push(lv)
            }
        }
    }
    let mut b = L2Book::new();
    b.load_snapshot(&DepthSnapshot {
        last_update_id: 999,
        limit: 5000,
        rolling: false,
        bids,
        asks,
        exch_ts_ms: None,
        rx: RxStamp::default(),
    });
    b
}

fn feed(a: &mut Aligner<Collect>, ev: &Ev) {
    match ev {
        Ev::Depth { diff, prevs } => {
            for (i, l) in diff.bids.iter().chain(diff.asks.iter()).enumerate() {
                let (side, px, prev) = prevs[i];
                debug_assert_eq!(px, l.px);
                a.on_level(side, px, prev, l.qty, diff.exch_ts_ms);
            }
            a.on_diff_applied(diff);
        }
        Ev::Trade(t) => a.on_trade(t),
    }
}

fn lvl_index(side: Side, px: Px) -> usize {
    let u = px.raw() / Px::from_units(1).raw();
    match side {
        Side::Bid => (u - 95) as usize,
        Side::Ask => (u - 101) as usize + 5,
    }
}

fn run_and_check(init_q: Vec<u8>, specs: Vec<BatchSpec>, futures_ts: bool, slack: u64) {
    let w = build(&init_q, &specs, futures_ts, slack);
    let cfg = AlignerConfig {
        boundary_slack_ms: slack,
        ..AlignerConfig::default()
    };
    let mut a = Aligner::new(cfg, Collect::default());
    a.on_rebuild(1, &init_book(&w.init));
    for (_, _, ev) in &w.events {
        feed(&mut a, ev);
        assert_eq!(a.unaccounted(), 0, "conservación");
    }
    let s = a.stats().clone();
    assert_eq!(s.late, 0, "la topología de latencias impide tardíos");
    assert_eq!(s.mirror_mismatch, 0);
    let sink = a.sink();
    let mut emitted: BTreeMap<(u64, usize), LevelFlow> = BTreeMap::new();
    for f in &sink.flows {
        let k = (f.end_ts_ms / BATCH - 1) as usize;
        let li = lvl_index(f.side, f.px);
        let t = w.truth[k][li];
        // q0/q1 exactos siempre (también en lotes contaminados)
        assert_eq!(f.q0, Qty::from_units(t.q0), "q0 lote {k} nivel {li}");
        assert_eq!(f.q1, Qty::from_units(t.q1), "q1 lote {k} nivel {li}");
        if !f.clean {
            assert_eq!(f.no_visible_min, Qty::ZERO);
            assert_eq!(f.cancelado_min, Qty::ZERO);
            continue;
        }
        assert!(
            f.no_visible_min <= Qty::from_units(t.exec - t.consumed_q0),
            "no_visible_min excede la verdad: {f:?} {t:?}"
        );
        assert!(
            f.cancelado_min <= Qty::from_units(t.cancelled),
            "cancelado_min excede la verdad: {f:?} {t:?}"
        );
        assert!(f.exec_lo <= f.exec && f.exec <= f.exec_hi);
        if slack == 0 {
            assert_eq!(f.exec, Qty::from_units(t.exec), "ejecutado exacto");
            assert_eq!(f.exec_rpi, Qty::from_units(t.rpi), "RPI aparte exacto");
        }
        emitted.insert((f.end_ts_ms, li), *f);
    }
    // Completitud (δ = 0): todo nivel con cambio o ejecución en lotes limpios cerrados.
    if slack == 0 {
        for b in sink.batches.iter().filter(|b| b.contaminated.is_none()) {
            let k = (b.end_ts_ms / BATCH - 1) as usize;
            for (li, t) in w.truth[k].iter().enumerate() {
                if t.exec > 0 || t.rpi > 0 || t.q1 != t.q0 {
                    assert!(
                        emitted.contains_key(&(b.end_ts_ms, li)),
                        "falta flujo lote {k} nivel {li}: {t:?}"
                    );
                }
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, ..ProptestConfig::default() })]

    #[test]
    fn f1_f3_cotas_exactas_y_completitud(
        init in prop::collection::vec(0u8..20, NLVL),
        specs in prop::collection::vec(batch_spec(), 1..40),
        fut in any::<bool>(),
    ) {
        run_and_check(init, specs, fut, 0);
    }

    #[test]
    fn f2_cotas_validas_con_incertidumbre(
        init in prop::collection::vec(0u8..20, NLVL),
        specs in prop::collection::vec(batch_spec(), 1..40),
        fut in any::<bool>(),
        slack in 1u64..30,
    ) {
        run_and_check(init, specs, fut, slack);
    }

    /// F4: conservación bajo pérdidas de sincronización, trades durante resync y tardíos.
    #[test]
    fn f4_conservacion_con_resync_y_tardios(
        init in prop::collection::vec(0u8..20, NLVL),
        specs in prop::collection::vec(batch_spec(), 1..40),
        cuts in prop::collection::vec(any::<u16>(), 0..4),
        extra_late in prop::collection::vec((0u64..4000, any::<u8>()), 0..20),
    ) {
        let w = build(&init, &specs, false, 0);
        let mut a = Aligner::new(AlignerConfig::default(), Collect::default());
        let book = init_book(&w.init);
        a.on_rebuild(1, &book);
        let n = w.events.len().max(1);
        let cut_at: Vec<usize> = cuts.iter().map(|c| *c as usize % n).collect();
        let mut epoch = 1;
        let mut live = true;
        for (i, (_, _, ev)) in w.events.iter().enumerate() {
            if cut_at.contains(&i) {
                if live {
                    a.on_invalidate(epoch, ResyncReason::GapTimeout);
                    live = false;
                } else {
                    // Reconstrucción: el espejo se inicializa con el libro verdadero de ese momento.
                    epoch += 1;
                    a.on_rebuild(epoch, &book);
                    live = true;
                }
            }
            match ev {
                Ev::Depth { .. } if !live => {}
                // Tras una reconstrucción simulada el espejo no coincide con la verdad:
                // solo se verifica conservación, no contenido.
                _ => feed(&mut a, ev),
            }
            prop_assert_eq!(a.unaccounted(), 0);
        }
        for (ts, id) in extra_late {
            let t = AggTrade {
                agg_id: 1_000_000 + u64::from(id),
                first_trade_id: 1,
                last_trade_id: 1,
                px: Px::from_units(99),
                qty: Qty::from_units(1),
                qty_normal: None,
                aggressor: Aggressor::Sell,
                trade_ts_ms: ts,
                exch_ts_ms: ts,
                rx: RxStamp::default(),
            };
            a.on_trade(&t);
            prop_assert_eq!(a.unaccounted(), 0);
        }
    }
}

#[test]
fn contaminado_primer_lote_y_tardio_contado() {
    let mut a = Aligner::new(AlignerConfig::default(), Collect::default());
    let mut book = L2Book::new();
    book.load_snapshot(&DepthSnapshot {
        last_update_id: 1,
        limit: 5000,
        rolling: false,
        bids: vec![Level {
            px: Px::from_units(99),
            qty: Qty::from_units(10),
        }],
        asks: vec![],
        exch_ts_ms: None,
        rx: RxStamp::default(),
    });
    a.on_rebuild(1, &book);
    let mk = |id: u64, ts: u64| DepthDiff {
        first_id: id,
        last_id: id,
        prev_last_id: None,
        exch_ts_ms: ts,
        match_ts_ms: None,
        bids: vec![],
        asks: vec![],
        rx: RxStamp::default(),
    };
    let trade = |ts: u64, q: i64| AggTrade {
        agg_id: ts,
        first_trade_id: 1,
        last_trade_id: 3,
        px: Px::from_units(99),
        qty: Qty::from_units(q),
        qty_normal: None,
        aggressor: Aggressor::Sell,
        trade_ts_ms: ts,
        exch_ts_ms: ts,
        rx: RxStamp::default(),
    };
    a.on_diff_applied(&mk(2, 100)); // lote contaminado (0,100]
                                    // lote (100,200]: nivel 99 pasa de 10 a 4 con 2 ejecutados ⇒ cancelado ≥ 4
    a.on_level(
        Side::Bid,
        Px::from_units(99),
        Qty::from_units(10),
        Qty::from_units(4),
        200,
    );
    a.on_trade(&trade(150, 2));
    a.on_diff_applied(&mk(3, 200));
    a.on_diff_applied(&mk(4, 300));
    a.on_trade(&trade(301, 1)); // marca de agua de trades cierra (100,200]
    let s = a.sink();
    assert_eq!(
        s.batches[0].contaminated,
        Some(lu_flow::Contamination::EpochStart)
    );
    let f = s.flows.iter().find(|f| f.end_ts_ms == 200).unwrap();
    assert!(f.clean);
    assert_eq!(f.exec, Qty::from_units(2));
    assert_eq!(f.n_fills, 3);
    assert_eq!(f.cancelado_min, Qty::from_units(4));
    assert_eq!(f.no_visible_min, Qty::ZERO);
    a.on_trade(&trade(120, 1)); // su lote ya cerró
    assert_eq!(a.stats().late, 1);
    assert_eq!(a.unaccounted(), 0);
}

#[test]
fn diffs_con_mismo_tiempo_se_fusionan_conservando_q0() {
    let mut a = Aligner::new(AlignerConfig::default(), Collect::default());
    let mut book = L2Book::new();
    book.load_snapshot(&DepthSnapshot {
        last_update_id: 1,
        limit: 5000,
        rolling: false,
        bids: vec![Level {
            px: Px::from_units(99),
            qty: Qty::from_units(10),
        }],
        asks: vec![],
        exch_ts_ms: None,
        rx: RxStamp::default(),
    });
    a.on_rebuild(1, &book);
    let mk = |id: u64, ts: u64| DepthDiff {
        first_id: id,
        last_id: id,
        prev_last_id: None,
        exch_ts_ms: ts,
        match_ts_ms: Some(ts),
        bids: vec![],
        asks: vec![],
        rx: RxStamp::default(),
    };
    let p = Px::from_units(99);
    a.on_diff_applied(&mk(2, 100));
    a.on_level(Side::Bid, p, Qty::from_units(10), Qty::from_units(7), 200);
    a.on_diff_applied(&mk(3, 200));
    a.on_level(Side::Bid, p, Qty::from_units(7), Qty::from_units(5), 200);
    a.on_diff_applied(&mk(4, 200)); // mismo T: se fusiona en el lote (100,200]
    a.on_diff_applied(&mk(5, 300));
    a.on_trade(&AggTrade {
        agg_id: 1,
        first_trade_id: 1,
        last_trade_id: 1,
        px: Px::from_units(50),
        qty: Qty::from_units(1),
        qty_normal: None,
        aggressor: Aggressor::Sell,
        trade_ts_ms: 301,
        exch_ts_ms: 301,
        rx: RxStamp::default(),
    });
    let f = a
        .sink()
        .flows
        .iter()
        .find(|f| f.end_ts_ms == 200)
        .copied()
        .unwrap();
    assert_eq!((f.q0, f.q1), (Qty::from_units(10), Qty::from_units(5)));
    assert_eq!(f.cancelado_min, Qty::from_units(5));
    assert_eq!(a.sink().batches.len(), 2);
}

#[test]
fn marca_de_agua_espera_a_todas_las_lineas_vivas() {
    let mut a = Aligner::new(AlignerConfig::default(), Collect::default());
    let mut b = L2Book::new();
    b.load_snapshot(&DepthSnapshot {
        last_update_id: 1,
        limit: 5000,
        rolling: false,
        bids: vec![Level {
            px: Px::from_units(99),
            qty: Qty::from_units(10),
        }],
        asks: vec![],
        exch_ts_ms: None,
        rx: RxStamp::default(),
    });
    a.on_rebuild(1, &b);
    let d = |id: u64, ts: u64| DepthDiff {
        first_id: id,
        last_id: id,
        prev_last_id: None,
        exch_ts_ms: ts,
        match_ts_ms: None,
        bids: vec![],
        asks: vec![],
        rx: RxStamp::default(),
    };
    let tr = |id: u64, ts: u64, line: u8| AggTrade {
        agg_id: id,
        first_trade_id: id,
        last_trade_id: id,
        px: Px::from_units(99),
        qty: Qty::from_units(1),
        qty_normal: None,
        aggressor: Aggressor::Sell,
        trade_ts_ms: ts,
        exch_ts_ms: ts,
        rx: RxStamp {
            line,
            ..RxStamp::default()
        },
    };
    // Ambas líneas vivas.
    a.on_trade(&tr(1, 90, 0));
    a.observe_line(1, 90);
    a.on_diff_applied(&d(2, 100));
    a.on_diff_applied(&d(3, 200));
    a.on_diff_applied(&d(4, 300));
    // La línea A perdió el trade 2 (T=150) y ya entrega el 3 (T=260).
    a.on_trade(&tr(3, 260, 0));
    assert_eq!(
        a.finalized_until(),
        0,
        "nada cierra: la línea B sigue en 90"
    );
    // La línea B entrega el 2: se alinea, no llega tarde.
    a.on_trade(&tr(2, 150, 1));
    a.observe_line(1, 260);
    assert_eq!(a.stats().late, 0);
    assert_eq!(a.finalized_until(), 200);
    let f = a.sink().flows.iter().find(|f| f.end_ts_ms == 200).unwrap();
    assert_eq!(f.exec, Qty::from_units(1));
    assert_eq!(a.unaccounted(), 0);
}

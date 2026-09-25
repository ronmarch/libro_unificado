//! Pruebas de propiedades de la máquina de sincronización.
//!
//! Se genera un exchange simulado (verdad) y dos líneas redundantes con
//! pérdidas y latencias aleatorias. Propiedades:
//!
//! * **P1 — Arbitraje correcto**: si cada evento llega por al menos una línea,
//!   el libro termina `Live`, idéntico a la verdad, y con CERO resyncs.
//! * **P2 — Nunca silenciosamente incorrecto**: aunque un evento se pierda en
//!   ambas líneas, en todo instante en que la fase es `Live` el libro es
//!   idéntico a la verdad en `last_update_id`.
//! * **P3 — Conservación** (en cada paso de P1 y P2): todo diff recibido queda
//!   contabilizado exactamente una vez (`unaccounted() == 0`).

use lu_book::{BinanceFuturesRule, BinanceSpotRule, Phase, SeqRule, SyncBook, SyncConfig};
use lu_core::{DepthDiff, DepthSnapshot, Level, Px, Qty, RxStamp};
use proptest::prelude::*;
use std::collections::{BTreeMap, HashMap};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Spot,
    Futures,
}

#[derive(Clone, Debug)]
struct EvSpec {
    changes: Vec<(bool, u8, u8)>,
    extra: u8,
    skip: u8,
    on_a: bool,
    on_b: bool,
    delay_a: u16,
    delay_b: u16,
}

fn ev_spec() -> impl Strategy<Value = EvSpec> {
    (
        prop::collection::vec((any::<bool>(), 0u8..40, 0u8..6), 0..5),
        0u8..3,
        0u8..4,
        any::<bool>(),
        any::<bool>(),
        0u16..300,
        0u16..300,
    )
        .prop_map(
            |(changes, extra, skip, on_a, on_b, delay_a, delay_b)| EvSpec {
                changes,
                extra,
                skip,
                on_a,
                on_b,
                delay_a,
                delay_b,
            },
        )
}

type Side2 = (BTreeMap<Px, Qty>, BTreeMap<Px, Qty>);

fn level(bid: bool, off: u8, q: u8) -> (bool, Level) {
    // bids en 900..940, asks en 1000..1040: la verdad nunca se cruza
    let base = if bid { 900 } else { 1000 };
    (
        bid,
        Level {
            px: Px::from_units(base + i64::from(off)),
            qty: Qty::from_units(i64::from(q)),
        },
    )
}

fn apply_truth(state: &mut Side2, bid: bool, l: Level) {
    let m = if bid { &mut state.0 } else { &mut state.1 };
    if l.qty.is_zero() {
        m.remove(&l.px);
    } else {
        m.insert(l.px, l.qty);
    }
}

struct World {
    states: Vec<Side2>,
    diffs: Vec<DepthDiff>, // diffs[k-1] = evento k
    ids: Vec<u64>,         // ids[k] = u_k (ids[0] = estado inicial)
    id_to_k: HashMap<u64, usize>,
}

fn build(kind: Kind, init: &[(bool, u8, u8)], evs: &[EvSpec]) -> World {
    let mut s0: Side2 = (BTreeMap::new(), BTreeMap::new());
    for &(b, o, q) in init {
        let (b, l) = level(b, o, q);
        apply_truth(&mut s0, b, l);
    }
    let mut states = vec![s0];
    let mut ids = vec![1000u64];
    let mut diffs = Vec::with_capacity(evs.len());
    let mut id_to_k = HashMap::new();
    id_to_k.insert(1000u64, 0usize);
    for (i, e) in evs.iter().enumerate() {
        let prev_u = ids[i];
        let (first, pu) = match kind {
            Kind::Spot => (prev_u + 1, None),
            Kind::Futures => (prev_u + 1 + u64::from(e.skip), Some(prev_u)),
        };
        let last = first + u64::from(e.extra);
        let mut st = states[i].clone();
        let mut bids = Vec::new();
        let mut asks = Vec::new();
        for &(b, o, q) in &e.changes {
            let (b, l) = level(b, o, q);
            apply_truth(&mut st, b, l);
            if b {
                bids.push(l);
            } else {
                asks.push(l);
            }
        }
        diffs.push(DepthDiff {
            first_id: first,
            last_id: last,
            prev_last_id: pu,
            exch_ts_ms: last,
            match_ts_ms: None,
            bids,
            asks,
            rx: RxStamp::default(),
        });
        states.push(st);
        ids.push(last);
        id_to_k.insert(last, i + 1);
    }
    World {
        states,
        diffs,
        ids,
        id_to_k,
    }
}

fn snapshot_of(w: &World, j: usize) -> DepthSnapshot {
    let (b, a) = &w.states[j];
    DepthSnapshot {
        last_update_id: w.ids[j],
        limit: 10_000,
        bids: b.iter().map(|(p, q)| Level { px: *p, qty: *q }).collect(),
        asks: a.iter().map(|(p, q)| Level { px: *p, qty: *q }).collect(),
        exch_ts_ms: None,
        rx: RxStamp::default(),
    }
}

/// Índice de snapshot puenteable: spot necesita un evento posterior; futuros puentea con el propio.
fn clamp_snap(kind: Kind, j: usize, n: usize) -> usize {
    match kind {
        Kind::Spot => j.min(n - 1),
        Kind::Futures => j.clamp(1, n),
    }
}

#[derive(Debug)]
enum Arr {
    Diff(usize, u8),
    Snap(usize),
}

struct Outcome {
    phase: Phase,
    last: u64,
    resyncs: u64,
    final_ok: bool,
}

fn simulate<R: SeqRule>(
    rule: R,
    kind: Kind,
    w: &World,
    specs: &[EvSpec],
    snap_j: usize,
    snap_t: u64,
    gap: Option<usize>,
) -> Result<Outcome, TestCaseError> {
    let n = w.diffs.len();
    let mut arrivals: Vec<(u64, u64, Arr)> = Vec::new();
    let mut seq = 0u64;
    let mut first_seen = vec![u64::MAX; n + 1];
    for k in 1..=n {
        let s = &specs[k - 1];
        let (mut a, mut b) = (s.on_a, s.on_b);
        if gap == Some(k) {
            a = false;
            b = false;
        } else if !a && !b {
            a = true;
        }
        let base = k as u64 * 100;
        if a {
            let t = base + u64::from(s.delay_a);
            arrivals.push((t, seq, Arr::Diff(k, 0)));
            first_seen[k] = first_seen[k].min(t);
            seq += 1;
        }
        if b {
            let t = base + u64::from(s.delay_b);
            arrivals.push((t, seq, Arr::Diff(k, 1)));
            first_seen[k] = first_seen[k].min(t);
            seq += 1;
        }
    }
    arrivals.push((snap_t, seq, Arr::Snap(clamp_snap(kind, snap_j, n))));
    seq += 1;

    let cfg = SyncConfig::default();
    let mut book: SyncBook<R> = SyncBook::new(rule, cfg.clone(), ());
    let horizon = arrivals.iter().map(|a| a.0).max().unwrap_or(0) + 4 * cfg.gap_timeout_ms;
    let mut planned_in_flight = true; // el snapshot planificado cubre el primer pedido
    let mut t = 0u64;
    let mut iters = 0u32;

    // Un snapshot nuevo refleja el último evento que el exchange ya publicó (el mayor índice
    // visto por cualquier línea), no la cantidad de eventos recibidos: así se comporta Binance.
    let published_at = |t: u64| (1..=n).filter(|&k| first_seen[k] <= t).max().unwrap_or(0);

    loop {
        iters += 1;
        prop_assert!(iters < 500_000, "la simulación no termina");
        arrivals.sort_by_key(|a| (a.0, a.1));
        let step = match arrivals.first().map(|a| a.0) {
            Some(at) if at <= t => {
                let (_, _, arr) = arrivals.remove(0);
                match arr {
                    Arr::Diff(k, line) => {
                        let mut d = w.diffs[k - 1].clone();
                        d.rx.line = line;
                        book.on_diff(d, t)
                    }
                    Arr::Snap(j) => {
                        planned_in_flight = false;
                        book.on_snapshot(snapshot_of(w, j), t)
                    }
                }
            }
            Some(at) => {
                let st = book.on_tick(t);
                t = (t + 50).min(at);
                st
            }
            None => {
                if t > horizon {
                    break;
                }
                let st = book.on_tick(t);
                t += 50;
                st
            }
        };
        if step.need_snapshot && !planned_in_flight {
            arrivals.push((t + 50, seq, Arr::Snap(clamp_snap(kind, published_at(t), n))));
            seq += 1;
        }
        check_invariant(&book, w)?;
    }

    let final_ok = book.phase() == Phase::Live
        && book.last_update_id() == w.ids[n]
        && book.book().bids() == &w.states[n].0
        && book.book().asks() == &w.states[n].1;
    Ok(Outcome {
        phase: book.phase(),
        last: book.last_update_id(),
        resyncs: book.stats().resyncs(),
        final_ok,
    })
}

fn check_invariant<R: SeqRule>(book: &SyncBook<R>, w: &World) -> Result<(), TestCaseError> {
    prop_assert_eq!(book.unaccounted(), 0, "ley de conservación violada");
    if book.phase() == Phase::Live {
        let last = book.last_update_id();
        let k = *w.id_to_k.get(&last).ok_or_else(|| {
            TestCaseError::fail(format!(
                "last_update_id {last} no corresponde a ningún evento"
            ))
        })?;
        prop_assert_eq!(
            book.book().bids(),
            &w.states[k].0,
            "bids divergen en id {}",
            last
        );
        prop_assert_eq!(
            book.book().asks(),
            &w.states[k].1,
            "asks divergen en id {}",
            last
        );
    }
    Ok(())
}

fn run_case(
    kind: Kind,
    init: &[(bool, u8, u8)],
    specs: &[EvSpec],
    snap_j: usize,
    snap_t: u64,
    gap: Option<usize>,
) -> Result<Outcome, TestCaseError> {
    let w = build(kind, init, specs);
    match kind {
        Kind::Spot => simulate(BinanceSpotRule, kind, &w, specs, snap_j, snap_t, gap),
        Kind::Futures => simulate(BinanceFuturesRule, kind, &w, specs, snap_j, snap_t, gap),
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 400, .. ProptestConfig::default() })]

    /// P1: pérdidas en una sola línea nunca provocan resync y el libro final es exacto.
    #[test]
    fn p1_arbitraje_ab_exacto(
        futures in any::<bool>(),
        init in prop::collection::vec((any::<bool>(), 0u8..40, 1u8..6), 0..30),
        specs in prop::collection::vec(ev_spec(), 2..60),
        snap_j in 0usize..60,
        snap_t in 50u64..800,
    ) {
        let kind = if futures { Kind::Futures } else { Kind::Spot };
        let n = specs.len();
        let out = run_case(kind, &init, &specs, snap_j % n, snap_t, None)?;
        prop_assert!(out.final_ok, "final incorrecto: fase {:?}, last {}", out.phase, out.last);
        prop_assert_eq!(out.resyncs, 0);
    }

    /// P2: con un evento perdido en ambas líneas, jamás se expone un libro incorrecto como `Live`.
    #[test]
    fn p2_nunca_silenciosamente_incorrecto(
        futures in any::<bool>(),
        init in prop::collection::vec((any::<bool>(), 0u8..40, 1u8..6), 0..30),
        specs in prop::collection::vec(ev_spec(), 3..60),
        snap_j in 0usize..60,
        snap_t in 50u64..800,
        gap_sel in 0usize..60,
    ) {
        let kind = if futures { Kind::Futures } else { Kind::Spot };
        let n = specs.len();
        let gap = 1 + gap_sel % n;
        // `run_case` verifica el invariante en cada paso; aquí solo exigimos que termine.
        let _ = run_case(kind, &init, &specs, snap_j % n, snap_t, Some(gap))?;
    }
}

/// Canario de vivacidad: un hueco real en ambas líneas provoca exactamente un resync
/// y el libro se recupera exacto (garantiza que P2 ejercita la ruta de resync).
#[test]
fn canario_hueco_real_resync_y_recuperacion() {
    for kind in [Kind::Spot, Kind::Futures] {
        let specs: Vec<EvSpec> = (0..10)
            .map(|i| EvSpec {
                changes: vec![(i % 2 == 0, (i * 3) as u8, (i % 5 + 1) as u8)],
                extra: 1,
                skip: 2,
                on_a: true,
                on_b: false,
                delay_a: 0,
                delay_b: 0,
            })
            .collect();
        let out = run_case(kind, &[(true, 1, 3), (false, 2, 4)], &specs, 0, 50, Some(5)).unwrap();
        assert!(
            out.final_ok,
            "{kind:?}: no se recuperó (fase {:?}, last {})",
            out.phase, out.last
        );
        assert_eq!(
            out.resyncs, 1,
            "{kind:?}: se esperaba exactamente un resync"
        );
    }
}

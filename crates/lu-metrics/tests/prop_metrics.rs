//! Pruebas de F3.
//!
//! * **M1 — TWA exacto**: el TWA de cada celda de toda vela cerrada coincide
//!   con la integral por fuerza bruta de la cantidad del bucket sobre el tiempo
//!   sincronizado, con cortes y reconstrucciones aleatorias. También el tiempo
//!   observado y la suma de ejecuciones por vela.
//! * **M2 — Muros retirados**: escenarios deterministas de cada condición
//!   (dispara; rechazos por ejecución, distancia, percentil, contaminación; sin
//!   cruce del umbral X no se evalúa).

use lu_book::{L2Book, ResyncReason};
use lu_core::{DepthSnapshot, Level, Px, Qty, RxStamp, Side};
use lu_flow::{BatchInfo, Contamination, FlowSink, LevelFlow};
use lu_metrics::{Metrics, MetricsConfig, TF_15M, TF_1H, TF_4H};
use proptest::prelude::*;
use std::collections::BTreeMap;

fn px(cents: i64) -> Px {
    Px::from_raw(cents * 1_000_000)
}

fn book(levels: &[(Side, i64, i64)]) -> L2Book {
    let mut bids = Vec::new();
    let mut asks = Vec::new();
    for &(s, c, q) in levels {
        let l = Level {
            px: px(c),
            qty: Qty::from_units(q),
        };
        match s {
            Side::Bid => bids.push(l),
            Side::Ask => asks.push(l),
        }
    }
    let mut b = L2Book::new();
    b.load_snapshot(&DepthSnapshot {
        last_update_id: 1,
        limit: 5000,
        bids,
        asks,
        exch_ts_ms: None,
        rx: RxStamp::default(),
    });
    b
}

#[allow(clippy::too_many_arguments)]
fn flow(side: Side, cents: i64, q0: i64, q1: i64, exec: i64, end: u64, clean: bool) -> LevelFlow {
    LevelFlow {
        epoch: 1,
        side,
        px: px(cents),
        start_ts_ms: end.saturating_sub(100),
        end_ts_ms: end,
        q0: Qty::from_units(q0),
        q1: Qty::from_units(q1),
        exec: Qty::from_units(exec),
        exec_lo: Qty::from_units(exec),
        exec_hi: Qty::from_units(exec),
        exec_rpi: Qty::ZERO,
        n_fills: u64::from(exec > 0),
        n_trades: u64::from(exec > 0),
        no_visible_min: Qty::ZERO,
        cancelado_min: Qty::ZERO,
        agregado_min: Qty::ZERO,
        clean,
    }
}

fn batch(m: &mut Metrics, end: u64, flows: &[LevelFlow], clean: bool) {
    for f in flows {
        m.on_level_flow(f);
    }
    m.on_batch(&BatchInfo {
        epoch: 1,
        start_ts_ms: end.saturating_sub(100),
        end_ts_ms: end,
        levels_changed: flows.len() as u32,
        trades: 0,
        contaminated: (!clean).then_some(Contamination::EpochStart),
    });
}

// ------------------------------------------------------------------- M1

#[derive(Clone, Debug)]
enum Step {
    Batch {
        dt: u32,
        changes: Vec<(bool, u16, u8, u8)>,
    },
    Cut {
        dt: u32,
        rebuild: Vec<(bool, u16, u8)>,
    },
}

fn step() -> impl Strategy<Value = Step> {
    let dt = prop_oneof![8 => 1u32..5_000, 2 => 5_000u32..2_000_000, 1 => 2_000_000u32..20_000_000];
    prop_oneof![
        10 => (dt.clone(), prop::collection::vec((any::<bool>(), 0u16..600, 0u8..50, 0u8..5), 0..6))
            .prop_map(|(dt, changes)| Step::Batch { dt, changes }),
        1 => (dt, prop::collection::vec((any::<bool>(), 0u16..600, 1u8..50), 0..8))
            .prop_map(|(dt, rebuild)| Step::Cut { dt, rebuild }),
    ]
}

/// Precio en centavos: bids 94.00..99.99, asks 100.01..106.00 (buckets 94..105).
fn cents(bid: bool, off: u16) -> i64 {
    if bid {
        9400 + i64::from(off)
    } else {
        10001 + i64::from(off)
    }
}

/// Segmento de la verdad: desde `t`, con `live`, cantidades por (lado, bucket).
struct Seg {
    t: u64,
    live: bool,
    buckets: BTreeMap<(Side, i64), i64>,
}

fn buckets_of(levels: &BTreeMap<(Side, i64), i64>) -> BTreeMap<(Side, i64), i64> {
    let mut b: BTreeMap<(Side, i64), i64> = BTreeMap::new();
    for (&(s, c), &q) in levels {
        *b.entry((s, px(c).bucket(Px::from_units(1)))).or_default() += q;
    }
    b
}

fn integral(segs: &[Seg], key: (Side, i64), start: u64, end: u64) -> (i128, u64) {
    let mut area = 0i128;
    let mut obs = 0u64;
    for (i, s) in segs.iter().enumerate() {
        let t1 = segs.get(i + 1).map_or(u64::MAX, |n| n.t);
        let (a, b) = (s.t.max(start), t1.min(end));
        if s.live && b > a {
            obs += b - a;
            area += i128::from(s.buckets.get(&key).copied().unwrap_or(0)) * i128::from(b - a);
        }
    }
    (area, obs)
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, ..ProptestConfig::default() })]

    #[test]
    fn m1_twa_exacto_con_cortes(
        init in prop::collection::vec((any::<bool>(), 0u16..600, 1u8..50), 1..10),
        steps in prop::collection::vec(step(), 1..60),
    ) {
        let mut m = Metrics::new(MetricsConfig { keep_closed: 10_000, ..MetricsConfig::default() });
        let mut levels: BTreeMap<(Side, i64), i64> = BTreeMap::new();
        let side = |b: bool| if b { Side::Bid } else { Side::Ask };
        for &(b, o, q) in &init {
            levels.insert((side(b), cents(b, o)), i64::from(q));
        }
        let to_book = |lv: &BTreeMap<(Side, i64), i64>| {
            book(&lv.iter().map(|(&(s, c), &q)| (s, c, q)).collect::<Vec<_>>())
        };
        m.on_rebuild(1, &to_book(&levels));
        let mut segs: Vec<Seg> = Vec::new();
        let mut exec_by_ts: Vec<(u64, Side, i64, i64)> = Vec::new();
        let mut t: u64 = TF_4H * 1000 + 7; // no alineado a propósito
        let mut live = true;
        for st in &steps {
            match st {
                Step::Batch { dt, changes } => {
                    t += u64::from(*dt);
                    let mut fl = Vec::new();
                    let mut seen = BTreeMap::new();
                    for &(b, o, q, e) in changes {
                        let key = (side(b), cents(b, o));
                        if seen.insert(key, ()).is_some() {
                            continue;
                        }
                        let q0 = levels.get(&key).copied().unwrap_or(0);
                        let q1 = i64::from(q);
                        if q1 == 0 { levels.remove(&key); } else { levels.insert(key, q1); }
                        fl.push(flow(key.0, key.1, q0, q1, i64::from(e), t, true));
                        if live {
                            exec_by_ts.push((t, key.0, px(key.1).bucket(Px::from_units(1)), i64::from(e)));
                        }
                    }
                    batch(&mut m, t, &fl, true);
                    if live {
                        segs.push(Seg { t, live: true, buckets: buckets_of(&levels) });
                    }
                }
                Step::Cut { dt, rebuild } => {
                    if live {
                        m.on_invalidate(1, ResyncReason::GapTimeout);
                        segs.push(Seg { t, live: false, buckets: BTreeMap::new() });
                    }
                    t += u64::from(*dt);
                    levels.clear();
                    for &(b, o, q) in rebuild {
                        levels.insert((side(b), cents(b, o)), i64::from(q));
                    }
                    m.on_rebuild(2, &to_book(&levels));
                    // Se reanuda con el siguiente lote (su propio instante).
                    live = true;
                    t += 1;
                    batch(&mut m, t, &[], false);
                    segs.push(Seg { t, live: true, buckets: buckets_of(&levels) });
                }
            }
        }
        let v = m.view(10_000);
        for (ti, tf) in [TF_15M, TF_1H, TF_4H].into_iter().enumerate() {
            for c in &v.closed[ti] {
                prop_assert_eq!(c.tf_ms, tf);
                let mut obs_any = None;
                for cell in &c.cells {
                    let (area, obs) = integral(&segs, (cell.side, cell.bucket), c.start_ms, c.end_ms);
                    prop_assert_eq!(c.observed_ms, obs, "observado vela {}", c.start_ms);
                    obs_any = Some(obs);
                    let exp = if obs > 0 { (area * i128::from(lu_core::SCALE) / i128::from(obs)) as i64 } else { 0 };
                    prop_assert_eq!(cell.twa.raw(), exp, "TWA {:?} vela {}", (cell.side, cell.bucket), c.start_ms);
                }
                let _ = obs_any;
                let ex: i64 = exec_by_ts
                    .iter()
                    .filter(|e| e.0 >= c.start_ms && e.0 < c.end_ms)
                    .map(|e| e.3)
                    .sum();
                let got: i64 = c.cells.iter().map(|x| x.exec.raw()).sum();
                prop_assert_eq!(got, Qty::from_units(ex).raw(), "Σ ejecutado vela {}", c.start_ms);
            }
        }
    }
}

// ------------------------------------------------------------------- M2

const T0: u64 = TF_4H * 5000; // inicio exacto de una vela 4 h

/// Libro: bids en 95.50..99.50 (buckets 95..99), asks 101.50..105.50; muro en `wall_bucket`.
fn wall_setup(wall_bucket: i64, wall_qty: i64, second: Option<(i64, i64)>) -> Metrics {
    let mut lv = Vec::new();
    for b in 95..=99 {
        let mut q = 10;
        if b == wall_bucket {
            q = wall_qty;
        }
        if let Some((sb, sq)) = second {
            if b == sb {
                q = sq;
            }
        }
        lv.push((Side::Bid, b * 100 + 50, q));
    }
    for a in 101..=105 {
        lv.push((Side::Ask, a * 100 + 50, 10));
    }
    let mut m = Metrics::new(MetricsConfig::default());
    m.on_rebuild(1, &book(&lv));
    batch(&mut m, T0, &[], false); // arranque (contaminado)
                                   // Una hora estable con lotes limpios vacíos.
    for k in 1..=36 {
        batch(&mut m, T0 + k * 100_000, &[], true);
    }
    m
}

fn drop_wall(m: &mut Metrics, bucket: i64, from: i64, to: i64, exec: i64, clean: bool) {
    let t = T0 + 3_700_000;
    batch(
        m,
        t,
        &[flow(Side::Bid, bucket * 100 + 50, from, to, exec, t, clean)],
        clean,
    );
}

#[test]
fn m2_dispara_muro_retirado() {
    let mut m = wall_setup(99, 1000, None);
    drop_wall(&mut m, 99, 1000, 100, 50, true); // caída 900, ejecutado 50 (5,6 %)
    let s = m.wall_stats().clone();
    assert_eq!((s.evaluated, s.fired), (1, 1), "{s:?}");
    let w = *m.walls().next().unwrap();
    assert_eq!(w.side, Side::Bid);
    assert_eq!(w.bucket, 99);
    assert_eq!(w.start_qty, Qty::from_units(1000));
    assert_eq!(w.qty, Qty::from_units(100));
    assert_eq!(w.exec, Qty::from_units(50));
    assert!(w.distance <= 2);
}

#[test]
fn m2_rechaza_si_ejecuciones_explican_la_caida() {
    let mut m = wall_setup(99, 1000, None);
    drop_wall(&mut m, 99, 1000, 100, 180, true); // 180/900 = 20 % ⇒ no es < 20 %
    let s = m.wall_stats();
    assert_eq!((s.fired, s.rejected_exec), (0, 1));
}

#[test]
fn m2_rechaza_por_distancia() {
    let mut m = wall_setup(95, 1000, None);
    drop_wall(&mut m, 95, 1000, 100, 0, true); // bucket 95, medio en 100
    let s = m.wall_stats();
    assert_eq!((s.fired, s.rejected_distance), (0, 1));
}

#[test]
fn m2_rechaza_por_percentil() {
    let mut m = wall_setup(99, 500, Some((98, 1000)));
    drop_wall(&mut m, 99, 500, 50, 0, true); // 98 tiene mayor TWA: 99 no alcanza P90 de 5 buckets
    let s = m.wall_stats();
    assert_eq!((s.fired, s.rejected_percentile), (0, 1));
}

#[test]
fn m2_rechaza_ventana_contaminada() {
    let mut m = wall_setup(99, 1000, None);
    drop_wall(&mut m, 99, 1000, 100, 0, false);
    let s = m.wall_stats();
    assert_eq!((s.fired, s.rejected_contaminated), (0, 1));
}

#[test]
fn m2_sin_cruce_de_x_no_evalua() {
    let mut m = wall_setup(99, 1000, None);
    drop_wall(&mut m, 99, 1000, 600, 0, true); // cae 40 % < X = 50 %
    assert_eq!(m.wall_stats().evaluated, 0);
}

#[test]
fn m2_caida_gradual_acumula_ejecuciones_de_la_ventana() {
    let mut m = wall_setup(99, 1000, None);
    let p = 99 * 100 + 50;
    let t1 = T0 + 3_700_000;
    // 1000 → 700 con 100 ejecutado (aún ≥ 50 % del TWA)
    batch(
        &mut m,
        t1,
        &[flow(Side::Bid, p, 1000, 700, 100, t1, true)],
        true,
    );
    // 700 → 300 con 100 ejecutado: cruza; ventana acumula 200 sobre caída 700 (28 %) ⇒ rechazo
    let t2 = t1 + 100;
    batch(
        &mut m,
        t2,
        &[flow(Side::Bid, p, 700, 300, 100, t2, true)],
        true,
    );
    let s = m.wall_stats();
    assert_eq!((s.evaluated, s.fired, s.rejected_exec), (1, 0, 1), "{s:?}");
}

// ------------------------------------------------------------------- M3: táctica vs estructural

/// Vela 4 h completa con el bucket 120 (bid) en `ref_qty` y 115..119 en `others`;
/// en la vela 4 h siguiente, el bucket 120 pasa a `now_qty` al inicio del primer bloque de 15 m.
fn tactical(ref_qty: i64, others: i64, now_qty: i64) -> lu_metrics::CellView {
    let mut lv: Vec<(Side, i64, i64)> = (115..=119)
        .map(|b| (Side::Bid, b * 100 + 50, others))
        .collect();
    lv.push((Side::Bid, 12050, ref_qty));
    lv.push((Side::Ask, 12150, 10));
    let mut m = Metrics::new(MetricsConfig::default());
    m.on_rebuild(1, &book(&lv));
    batch(&mut m, T0, &[], false);
    // Antes de la vela de referencia completa: sin lectura.
    batch(&mut m, T0 + 1_000, &[], true);
    let v = m.view(0);
    assert_eq!(v.current[0].ref_start_ms, None);
    assert!(v.current[0].cells.iter().all(|c| c.liquidity.is_none()));
    for k in 1..=144 {
        batch(&mut m, T0 + k * 100_000, &[], true);
    }
    let t = T0 + TF_4H;
    batch(
        &mut m,
        t,
        &[flow(Side::Bid, 12050, ref_qty, now_qty, 0, t, true)],
        true,
    );
    batch(&mut m, t + 300_000, &[], true);
    let v = m.view(0);
    let c = &v.current[0];
    assert_eq!(c.tf_ms, TF_15M);
    assert_eq!(
        c.ref_start_ms,
        Some(T0),
        "referencia = vela 4 h anterior cerrada (opción B)"
    );
    c.cells
        .iter()
        .find(|x| x.side == Side::Bid && x.bucket == 120)
        .cloned()
        .unwrap()
}

#[test]
fn m3_estructural_2000_a_2100() {
    let c = tactical(2000, 10, 2100);
    assert_eq!(c.twa_ref, Some(Qty::from_units(2000)));
    assert!((c.ratio.unwrap() - 1.05).abs() < 1e-9);
    assert_eq!(c.liquidity, Some(lu_metrics::Liquidity::Estructural));
}

#[test]
fn m3_tactica_300_a_1500() {
    let c = tactical(300, 10, 1500);
    assert!((c.ratio.unwrap() - 5.0).abs() < 1e-9);
    assert_eq!(c.liquidity, Some(lu_metrics::Liquidity::Tactica));
}

#[test]
fn m3_desarmandose_2000_a_600() {
    let c = tactical(2000, 10, 600);
    assert!((c.ratio.unwrap() - 0.3).abs() < 1e-9);
    assert_eq!(c.liquidity, Some(lu_metrics::Liquidity::Desarmandose));
}

#[test]
fn m3_r_cercano_a_1_sin_tamano_relevante_es_estable() {
    let c = tactical(2000, 5000, 2100); // otros buckets más grandes: 2000 no alcanza P90
    assert_eq!(c.liquidity, Some(lu_metrics::Liquidity::Estable));
}

#[test]
fn m3_sin_liquidez_en_la_referencia_es_tactica() {
    let c = tactical(0, 10, 1500);
    assert_eq!(c.ratio, None);
    assert_eq!(c.liquidity, Some(lu_metrics::Liquidity::Tactica));
}

// ------------------------------------------------------------------- M4: CVD del libro

fn view_of(levels: &[(Side, i64, i64)], limit: usize) -> lu_metrics::MetricsView {
    let mut bids = Vec::new();
    let mut asks = Vec::new();
    for &(s, c, q) in levels {
        let l = Level {
            px: px(c),
            qty: Qty::from_units(q),
        };
        match s {
            Side::Bid => bids.push(l),
            Side::Ask => asks.push(l),
        }
    }
    let mut b = L2Book::new();
    b.load_snapshot(&DepthSnapshot {
        last_update_id: 1,
        limit,
        bids,
        asks,
        exch_ts_ms: None,
        rx: RxStamp::default(),
    });
    let mut m = Metrics::new(MetricsConfig::default());
    m.on_rebuild(1, &b);
    batch(&mut m, T0 + 5, &[], false);
    m.view(0)
}

#[test]
fn m4_cvd_del_libro_pares_simetricos() {
    // precio 101,30 ⇒ m = 101: k=1 bids 100 vs asks 102; k=2 bids 99 vs asks 103.
    let spot = view_of(
        &[
            (Side::Bid, 10050, 10),
            (Side::Bid, 9950, 20),
            (Side::Bid, 10120, 999), // bucket m: no participa
            (Side::Ask, 10250, 5),
            (Side::Ask, 10350, 7),
        ],
        5000,
    );
    let perp = view_of(&[(Side::Bid, 10020, 3), (Side::Ask, 10270, 4)], 5000);
    let c = lu_metrics::book_cvd(&spot, &perp, px(10130), Px::from_units(1), 5);
    assert_eq!(c.ref_bucket, 101);
    assert_eq!(c.levels.len(), 5);
    let l1 = c.levels[0];
    assert_eq!((l1.bid_bucket, l1.ask_bucket), (100, 102));
    assert_eq!(l1.delta, Qty::from_units(10 + 3 - 5 - 4));
    assert_eq!(c.levels[1].delta, Qty::from_units(20 - 7));
    assert_eq!(c.levels[1].cum, Qty::from_units(17));
    assert_eq!(c.levels[4].bid_bucket, 96);
    assert_eq!(c.levels[4].ask_bucket, 106);
    assert_eq!(c.total, Qty::from_units(17));
    assert!(c.complete);
}

#[test]
fn m4_cvd_marca_incompleto_fuera_de_cobertura() {
    // Snapshot spot truncado (limit = 2 bids) ⇒ bids bajo 99 desconocidos.
    let spot = view_of(
        &[
            (Side::Bid, 10050, 10),
            (Side::Bid, 9950, 20),
            (Side::Ask, 10250, 5),
        ],
        2,
    );
    let perp = view_of(&[(Side::Bid, 10020, 3), (Side::Ask, 10270, 4)], 5000);
    let c = lu_metrics::book_cvd(&spot, &perp, px(10130), Px::from_units(1), 5);
    assert!(c.levels[0].complete, "100 está sobre el piso");
    assert!(!c.levels[1].complete, "99 contiene el piso de cobertura");
    assert!(!c.complete);
}

//! CVD del libro (definición del usuario, 2026-09-25).
//!
//! Con `m` = bucket de 1 USDT del precio de referencia, para cada distancia
//! `k = 1..=niveles` (5):
//!
//! `delta_k = (bid_spot + bid_perp)(m − k) − (ask_spot + ask_perp)(m + k)`
//!
//! Ejemplo: precio 101 ⇒ k=1 compara bids en 100 con asks en 102; k=2, 99 con 103…
//! El bucket `m` (mezcla bids y asks alrededor del precio) no participa.
//! `cum_k = Σ_{j ≤ k} delta_j`; `total = cum_niveles`.
//!
//! Multi-venue: `bid_spot` es la suma de los bids spot de todos los venues (igual
//! para perp y asks). El libro unificado suma la liquidez de todos los exchanges.
//!
//! Cantidades: foto actual del libro (cantidad visible en el bucket). Futuros
//! Binance USDⓈ-M: 1 contrato = 1 unidad de base, sin conversión.
//! Un par con algún bucket fuera de la cobertura del snapshot de cualquiera de los
//! dos mercados se marca `complete = false` (la cantidad real puede ser mayor).

use crate::footprint::{CellView, MetricsView};
use lu_core::{Px, Qty, Side};
use serde::Serialize;

/// Un par de niveles simétricos.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct CvdLevel {
    /// Distancia en buckets.
    pub k: i64,
    /// Bucket bid (`m − k`).
    pub bid_bucket: i64,
    /// Bucket ask (`m + k`).
    pub ask_bucket: i64,
    /// Bids spot en `m − k`.
    pub bid_spot: Qty,
    /// Bids perp en `m − k`.
    pub bid_perp: Qty,
    /// Asks spot en `m + k`.
    pub ask_spot: Qty,
    /// Asks perp en `m + k`.
    pub ask_perp: Qty,
    /// `(bid_spot + bid_perp) − (ask_spot + ask_perp)`.
    pub delta: Qty,
    /// Acumulado hasta `k`.
    pub cum: Qty,
    /// Ambos buckets dentro de la cobertura de ambos mercados.
    pub complete: bool,
}

/// CVD del libro spot + perp.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct BookCvd {
    /// Precio de referencia.
    pub ref_px: Px,
    /// Bucket del precio de referencia (`m`).
    pub ref_bucket: i64,
    /// Pares k = 1..=niveles.
    pub levels: Vec<CvdLevel>,
    /// Σ deltas.
    pub total: Qty,
    /// Todos los pares completos.
    pub complete: bool,
    /// Último instante de exchange de la vista spot (ms).
    pub spot_ts_ms: u64,
    /// Último instante de exchange de la vista perp (ms).
    pub perp_ts_ms: u64,
    /// Desfase entre ambas vistas (ms).
    pub skew_ms: u64,
    /// Mercados sumados.
    pub markets: usize,
}

fn qty(v: &MetricsView, side: Side, bucket: i64) -> (i64, bool) {
    let covered = match side {
        Side::Bid => v.bid_floor_bucket.is_none_or(|f| bucket > f),
        Side::Ask => v.ask_ceiling_bucket.is_none_or(|c| bucket < c),
    };
    let q = v
        .current
        .first()
        .and_then(|c| {
            c.cells
                .iter()
                .find(|x: &&CellView| x.side == side && x.bucket == bucket)
        })
        .map_or(0, |x| x.qty.raw());
    (q, covered)
}

/// Calcula el CVD del libro con las velas en curso de un mercado spot y uno perp.
pub fn book_cvd(
    spot: &MetricsView,
    perp: &MetricsView,
    ref_px: Px,
    bucket: Px,
    levels: usize,
) -> BookCvd {
    book_cvd_multi(&[spot], &[perp], ref_px, bucket, levels)
}

fn sum(views: &[&MetricsView], side: Side, bucket: i64) -> (i64, bool) {
    views.iter().fold((0, true), |(q, c), v| {
        let (q2, c2) = qty(v, side, bucket);
        (q + q2, c && c2)
    })
}

/// CVD del libro sumando varios mercados spot y perp (libro unificado multi-venue).
pub fn book_cvd_multi(
    spots: &[&MetricsView],
    perps: &[&MetricsView],
    ref_px: Px,
    bucket: Px,
    levels: usize,
) -> BookCvd {
    let m = ref_px.bucket(bucket);
    let mut cum = 0i64;
    let mut all = true;
    let lv = (1..=levels as i64)
        .map(|k| {
            let (bs, c1) = sum(spots, Side::Bid, m - k);
            let (bp, c2) = sum(perps, Side::Bid, m - k);
            let (as_, c3) = sum(spots, Side::Ask, m + k);
            let (ap, c4) = sum(perps, Side::Ask, m + k);
            let delta = bs + bp - as_ - ap;
            cum += delta;
            let complete = c1 && c2 && c3 && c4;
            all &= complete;
            CvdLevel {
                k,
                bid_bucket: m - k,
                ask_bucket: m + k,
                bid_spot: Qty::from_raw(bs),
                bid_perp: Qty::from_raw(bp),
                ask_spot: Qty::from_raw(as_),
                ask_perp: Qty::from_raw(ap),
                delta: Qty::from_raw(delta),
                cum: Qty::from_raw(cum),
                complete,
            }
        })
        .collect();
    BookCvd {
        ref_px,
        ref_bucket: m,
        levels: lv,
        total: Qty::from_raw(cum),
        complete: all,
        spot_ts_ms: spots.iter().map(|v| v.now_ms).min().unwrap_or(0),
        perp_ts_ms: perps.iter().map(|v| v.now_ms).min().unwrap_or(0),
        skew_ms: {
            let ts = spots.iter().chain(perps).map(|v| v.now_ms);
            ts.clone().max().unwrap_or(0) - ts.min().unwrap_or(0)
        },
        markets: spots.len() + perps.len(),
    }
}

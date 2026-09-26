//! CVD del libro unificado: suma spot y perp de todos los venues (ver `lu_metrics::cvd`).
//!
//! Supuesto declarado: el precio de referencia es el precio medio del primer
//! mercado spot registrado (Binance si está activo). Solo participan mercados
//! `Live`; se requiere al menos un spot y un perp.

use crate::http::Registry;
use lu_book::Phase;
use lu_core::Px;
use lu_metrics::{book_cvd_multi, BookCvd, MetricsView};
use std::sync::Arc;

/// Niveles (pares simétricos) del CVD del libro.
pub const LEVELS: usize = 5;

/// Calcula el CVD del libro con las vistas publicadas más recientes.
pub fn compute(reg: &Registry) -> Option<BookCvd> {
    let mut spots: Vec<Arc<MetricsView>> = Vec::new();
    let mut perps: Vec<Arc<MetricsView>> = Vec::new();
    let mut ref_px: Option<Px> = None;
    for (key, v) in reg.iter() {
        let b = v.book.load();
        if b.phase != Phase::Live {
            continue;
        }
        if key.ends_with("spot") {
            ref_px = ref_px.or(b.mid);
            spots.push(v.metrics.load_full());
        } else if key.ends_with("perp") {
            perps.push(v.metrics.load_full());
        }
    }
    if spots.is_empty() || perps.is_empty() {
        return None;
    }
    let s: Vec<&MetricsView> = spots.iter().map(|x| x.as_ref()).collect();
    let p: Vec<&MetricsView> = perps.iter().map(|x| x.as_ref()).collect();
    Some(book_cvd_multi(&s, &p, ref_px?, Px::from_units(1), LEVELS))
}

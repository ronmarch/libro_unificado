//! CVD del libro unificado: suma spot y perp de todos los venues (ver `lu_metrics::cvd`).
//!
//! Fuente: la profundidad exacta por bucket que cada motor publica junto a su libro
//! (cada 250 ms), así el desfase entre venues queda acotado por la publicación y no
//! por las métricas (1 s). `skew_ms` = diferencia entre los instantes en que cada
//! estado estaba vigente (≤ intervalo de publicación; más la latencia de red de cada
//! venue, visible en `lu_line_latency_ms`).
//!
//! Supuesto declarado: el precio de referencia es el precio medio del primer
//! mercado spot registrado (Binance si está activo). Solo participan mercados
//! `Live`; se requiere al menos un spot y un perp.

use crate::http::Registry;
use crate::view::BookView;
use lu_book::Phase;
use lu_core::Px;
use lu_metrics::{book_cvd_depths, BookCvd};
use std::sync::Arc;

/// Niveles (pares simétricos) del CVD del libro.
pub const LEVELS: usize = 5;

/// Calcula el CVD del libro con las vistas publicadas más recientes.
pub fn compute(reg: &Registry) -> Option<BookCvd> {
    let mut spots: Vec<Arc<BookView>> = Vec::new();
    let mut perps: Vec<Arc<BookView>> = Vec::new();
    for (key, v) in reg.iter() {
        let b = v.book.load_full();
        if b.phase != Phase::Live {
            continue;
        }
        if key.ends_with("spot") {
            spots.push(b);
        } else if key.ends_with("perp") {
            perps.push(b);
        }
    }
    let ref_px = spots.first()?.mid?;
    if perps.is_empty() {
        return None;
    }
    let s: Vec<_> = spots.iter().map(|b| &b.depth).collect();
    let p: Vec<_> = perps.iter().map(|b| &b.depth).collect();
    Some(book_cvd_depths(&s, &p, ref_px, Px::from_units(1), LEVELS))
}

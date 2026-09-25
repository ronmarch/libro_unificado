//! CVD del libro combinado spot + perp (ver `lu_metrics::cvd`).
//!
//! Supuesto declarado: el precio de referencia es el precio medio del libro
//! spot. Solo se calcula con ambos libros `Live`.

use crate::http::Registry;
use lu_book::Phase;
use lu_core::Px;
use lu_metrics::{book_cvd, BookCvd};

/// Niveles (pares simétricos) del CVD del libro.
pub const LEVELS: usize = 5;

/// Calcula el CVD del libro con las vistas publicadas más recientes.
pub fn compute(reg: &Registry) -> Option<BookCvd> {
    let get = |k: &str| reg.iter().find(|(key, _)| key == k).map(|(_, v)| v);
    let (spot, perp) = (get("spot")?, get("perp")?);
    let (sb, pb) = (spot.book.load(), perp.book.load());
    if sb.phase != Phase::Live || pb.phase != Phase::Live {
        return None;
    }
    let ref_px = sb.mid?;
    Some(book_cvd(
        &spot.metrics.load(),
        &perp.metrics.load(),
        ref_px,
        Px::from_units(1),
        LEVELS,
    ))
}

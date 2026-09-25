//! Punto de extensión del libro. Las métricas (F3: TWA por nivel, consumido vs.
//! cancelado, footprint) se enchufan aquí con despacho estático: cero costo si
//! no se usan y sin tocar la máquina de sincronización.

use crate::book::L2Book;
use crate::sync::ResyncReason;
use lu_core::{Px, Qty, Side};

/// Observador de cambios del libro sincronizado. Todos los métodos son opcionales.
pub trait BookObserver {
    /// Un nivel cambió por un diff en vivo: `prev` → `new` (cantidades absolutas).
    #[inline]
    fn on_level(&mut self, _side: Side, _px: Px, _prev: Qty, _new: Qty, _exch_ts_ms: u64) {}

    /// Un diff completo quedó aplicado (cierra lotes para el alineador trades↔depth).
    #[inline]
    fn on_diff_applied(&mut self, _first_id: u64, _last_id: u64, _exch_ts_ms: u64) {}

    /// Libro reconstruido desde snapshot: nueva época, todo estado derivado previo es inválido.
    #[inline]
    fn on_rebuild(&mut self, _epoch: u64, _book: &L2Book) {}

    /// Se perdió la sincronización: las métricas en curso deben marcarse contaminadas.
    #[inline]
    fn on_invalidate(&mut self, _epoch: u64, _reason: ResyncReason) {}
}

/// Observador nulo.
impl BookObserver for () {}

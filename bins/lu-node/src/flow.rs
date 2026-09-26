//! Resumen publicable de F2 (flujos por nivel alineados con trades).

use lu_core::{Qty, Side};
use lu_flow::{AlignerStats, BatchInfo, FlowSink, LevelFlow};
use lu_metrics::{WallRetired, WallStats};
use serde::Serialize;
use std::collections::VecDeque;

/// Flujos notables recientes que se conservan para la vista.
const RECENT: usize = 50;

/// Totales acumulados sobre lotes limpios (cotas inferiores: su suma también lo es).
#[derive(Debug, Clone, Default, Serialize)]
pub struct FlowTotals {
    /// Ejecutado contra bids (ventas agresivas), sin RPI.
    pub exec_bid: Qty,
    /// Ejecutado contra asks (compras agresivas), sin RPI.
    pub exec_ask: Qty,
    /// Σ no_visible_min en bids.
    pub no_visible_min_bid: Qty,
    /// Σ no_visible_min en asks.
    pub no_visible_min_ask: Qty,
    /// Σ cancelado_min en bids.
    pub cancelado_min_bid: Qty,
    /// Σ cancelado_min en asks.
    pub cancelado_min_ask: Qty,
    /// Ejecutado contra órdenes RPI.
    pub exec_rpi: Qty,
    /// Flujos limpios.
    pub flows_clean: u64,
}

/// Consumidor de F2 para la vista del nodo.
#[derive(Debug, Default)]
pub struct FlowAgg {
    /// Totales.
    pub totals: FlowTotals,
    /// Flujos notables recientes (con alguna cota > 0).
    pub recent: VecDeque<LevelFlow>,
    /// Último lote cerrado.
    pub last_batch: Option<BatchInfo>,
}

impl FlowSink for FlowAgg {
    fn on_level_flow(&mut self, f: &LevelFlow) {
        if !f.clean {
            return;
        }
        let t = &mut self.totals;
        t.flows_clean += 1;
        t.exec_rpi = t.exec_rpi + f.exec_rpi;
        match f.side {
            Side::Bid => {
                t.exec_bid = t.exec_bid + f.exec;
                t.no_visible_min_bid = t.no_visible_min_bid + f.no_visible_min;
                t.cancelado_min_bid = t.cancelado_min_bid + f.cancelado_min;
            }
            Side::Ask => {
                t.exec_ask = t.exec_ask + f.exec;
                t.no_visible_min_ask = t.no_visible_min_ask + f.no_visible_min;
                t.cancelado_min_ask = t.cancelado_min_ask + f.cancelado_min;
            }
        }
        if f.no_visible_min.is_positive() || f.cancelado_min.is_positive() {
            if self.recent.len() == RECENT {
                self.recent.pop_front();
            }
            self.recent.push_back(*f);
        }
    }
    fn on_batch(&mut self, b: &BatchInfo) {
        self.last_batch = Some(*b);
    }
}

/// Vista de F2 publicada con el libro.
#[derive(Debug, Clone, Default, Serialize)]
pub struct FlowView {
    /// Contadores del alineador.
    pub stats: AlignerStats,
    /// Conservación de trades (debe ser 0).
    pub unaccounted: i64,
    /// Lotes abiertos.
    pub open_batches: usize,
    /// Trades retenidos.
    pub buffered_trades: usize,
    /// Fin del último lote cerrado (ms de exchange).
    pub finalized_until_ms: u64,
    /// Totales.
    pub totals: FlowTotals,
    /// Último lote cerrado.
    pub last_batch: Option<BatchInfo>,
    /// Flujos notables recientes (más nuevo al final).
    pub recent: Vec<LevelFlow>,
    /// F3: contadores del detector de muros retirados.
    pub walls: WallStats,
    /// F3: últimos muros retirados (más nuevo primero).
    pub recent_walls: Vec<WallRetired>,
}

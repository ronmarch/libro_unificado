//! Alineador trades ↔ depth (F2).
//!
//! # Modelo
//!
//! Cada diff aplicado cierra un **lote**: el intervalo de tiempo de exchange
//! `(inicio, fin]` entre el diff anterior y este. Para cada nivel `(lado, precio)`
//! el libro sincronizado da exactamente `q0` (cantidad al inicio del lote) y
//! `q1` (al final). Los trades se asignan al lote por su tiempo `T`; un trade con
//! agresor comprador consume el lado ask (y viceversa).
//!
//! Con `e` = cantidad ejecutada en el nivel dentro del lote (sin RPI), se publican
//! dos **cotas inferiores** (nunca estimaciones puntuales):
//!
//! * `no_visible_min = max(0, e − q0)` ≤ ejecutado que NO provenía de la
//!   liquidez visible al inicio del lote (icebergs, órdenes ocultas y recargas
//!   dentro del lote). Prueba: de `q0` se consumió `c0 ≤ q0`, luego
//!   `e − c0 ≥ e − q0`.
//! * `cancelado_min = max(0, q0 − e − q1)` ≤ cantidad cancelada en el lote.
//!   Prueba: `q0 = c0 + k0 + s0` (consumido, cancelado, sobreviviente), con
//!   `c0 ≤ e` y `s0 ≤ q1`, luego `k0 ≥ q0 − e − q1`.
//!
//! Además `agregado_min = max(0, q1 − q0)`.
//!
//! # Incertidumbre temporal (`boundary_slack_ms` = δ)
//!
//! Si el tiempo `T` de un trade puede diferir hasta δ de su lote real, un trade
//! a menos de δ de una frontera es **ambiguo**. Se usan dos sumas:
//! `e_lo` (solo trades ciertamente dentro) para `no_visible_min` y `e_hi`
//! (incluye ambiguos de ambos lados) para `cancelado_min`. Ambas cotas siguen
//! siendo válidas bajo esa incertidumbre (aritmética de intervalos). δ = 0
//! reproduce exactamente la especificación.
//!
//! # Marca de agua
//!
//! Un lote se cierra cuando (a) ya existe un lote posterior y (b) llegó algún
//! trade con `E` > `fin + 2δ` (en un mismo socket los mensajes llegan en orden
//! de `E`, así que todos los trades con `T ≤ fin + δ` ya llegaron), o bien el tiempo de
//! depth avanzó `watermark_ms` más allá (flujo de trades quieto). Un trade que
//! llega después de cerrado su lote se cuenta como `late` (métrica de calidad).
//!
//! # Épocas
//!
//! Al perder la sincronización todo lote abierto se descarta como contaminado.
//! El primer lote tras reconstruir desde snapshot también es contaminado (su
//! `q0` proviene del snapshot, no del inicio del intervalo). Las épocas nunca
//! se mezclan.
//!
//! # Conservación
//!
//! Todo trade recibido queda contabilizado exactamente una vez
//! (`unaccounted() == 0`), igual que los diffs en `lu-book`.

use lu_book::{BookObserver, L2Book, ResyncReason};
use lu_core::{AggTrade, Aggressor, DepthDiff, Px, Qty, Side};
use serde::Serialize;
use std::collections::{BTreeMap, VecDeque};
use std::ops::Bound::{Excluded, Included};

/// Clave de nivel.
pub type LevelKey = (Side, Px);

/// Parámetros del alineador.
#[derive(Debug, Clone)]
pub struct AlignerConfig {
    /// Espera máxima (tiempo de depth) por trades de un lote cuando el flujo de trades está quieto (ms).
    pub watermark_ms: u64,
    /// Incertidumbre de asignación de trades a lotes, δ (ms). 0 = especificación literal.
    pub boundary_slack_ms: u64,
    /// Máximo de trades retenidos.
    pub max_trades: usize,
    /// Máximo de lotes abiertos (salvaguarda: se fuerzan cierres).
    pub max_open_batches: usize,
}

impl Default for AlignerConfig {
    fn default() -> Self {
        Self {
            watermark_ms: 250,
            boundary_slack_ms: 0,
            max_trades: 200_000,
            max_open_batches: 4_096,
        }
    }
}

/// Por qué un lote no admite afirmaciones sobre ejecución.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Contamination {
    /// Primer lote de una época: `q0` viene del snapshot.
    EpochStart,
    /// Se desbordó el buffer de trades mientras el lote estaba abierto.
    TradeOverflow,
}

/// Flujo de un nivel en un lote cerrado.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct LevelFlow {
    /// Época del libro.
    pub epoch: u64,
    /// Lado del libro (maker).
    pub side: Side,
    /// Precio.
    pub px: Px,
    /// Inicio del lote (exclusivo, ms de exchange).
    pub start_ts_ms: u64,
    /// Fin del lote (inclusivo, ms de exchange).
    pub end_ts_ms: u64,
    /// Cantidad visible al inicio.
    pub q0: Qty,
    /// Cantidad visible al final.
    pub q1: Qty,
    /// Ejecutado (asignación nominal por `T`, sin RPI).
    pub exec: Qty,
    /// Ejecutado con certeza dentro del lote (≤ `exec`).
    pub exec_lo: Qty,
    /// Ejecutado posiblemente dentro del lote (≥ `exec`).
    pub exec_hi: Qty,
    /// Ejecutado contra órdenes RPI (`q − nq`), aparte.
    pub exec_rpi: Qty,
    /// Fills reales.
    pub n_fills: u64,
    /// Trades agregados.
    pub n_trades: u64,
    /// Cota inferior de ejecutado no visible al inicio del lote.
    pub no_visible_min: Qty,
    /// Cota inferior de cancelado.
    pub cancelado_min: Qty,
    /// Cota inferior de agregado.
    pub agregado_min: Qty,
    /// `false` ⇒ lote contaminado: `q0`/`q1` exactos pero sin afirmaciones de ejecución (cotas en 0).
    pub clean: bool,
}

/// Resumen de un lote cerrado.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct BatchInfo {
    /// Época.
    pub epoch: u64,
    /// Inicio (exclusivo).
    pub start_ts_ms: u64,
    /// Fin (inclusivo).
    pub end_ts_ms: u64,
    /// Niveles que cambiaron.
    pub levels_changed: u32,
    /// Trades asignados nominalmente.
    pub trades: u64,
    /// Contaminación, si la hubo.
    pub contaminated: Option<Contamination>,
}

/// Consumidor de flujos (F3 implementa las métricas aquí). Despacho estático.
pub trait FlowSink {
    /// Flujo de un nivel (se entregan en orden determinista: lado, precio).
    #[inline]
    fn on_level_flow(&mut self, _f: &LevelFlow) {}
    /// Lote cerrado (después de sus `on_level_flow`).
    #[inline]
    fn on_batch(&mut self, _b: &BatchInfo) {}
    /// Nueva época: libro reconstruido.
    #[inline]
    fn on_rebuild(&mut self, _epoch: u64, _book: &L2Book) {}
    /// Pérdida de sincronización: todo estado en curso queda contaminado.
    #[inline]
    fn on_invalidate(&mut self, _epoch: u64, _reason: ResyncReason) {}
}

/// Sumidero nulo.
impl FlowSink for () {}

/// Contadores del alineador.
#[derive(Debug, Clone, Default, Serialize)]
pub struct AlignerStats {
    /// Trades recibidos.
    pub trades_rx: u64,
    /// Asignados a un lote limpio.
    pub aligned: u64,
    /// Asignados a un lote contaminado.
    pub contaminated: u64,
    /// Llegaron con su lote ya cerrado.
    pub late: u64,
    /// Llegaron con el libro sin sincronizar.
    pub syncing: u64,
    /// Expulsados por desborde del buffer.
    pub overflow: u64,
    /// Descartados por pérdida de sincronización.
    pub invalidated: u64,
    /// Repetidos (mismo `T` y id).
    pub duplicate: u64,
    /// Trades a menos de δ de una frontera (informativo).
    pub ambiguous: u64,
    /// Llegaron tras cerrar el lote vecino que podía contenerlos (informativo).
    pub late_ambiguous: u64,
    /// Lotes limpios cerrados.
    pub batches_clean: u64,
    /// Lotes contaminados.
    pub batches_contaminated: u64,
    /// Flujos de nivel emitidos.
    pub flows_emitted: u64,
    /// Cierres por marca de agua de trades.
    pub closed_by_trades: u64,
    /// Cierres por tiempo de depth.
    pub closed_by_timeout: u64,
    /// Cierres forzados por límite de lotes abiertos.
    pub closed_forced: u64,
    /// Diffs con tiempo menor al anterior (se fijan al anterior).
    pub ts_regressions: u64,
    /// `prev` del libro distinto del espejo (defecto: debe ser 0).
    pub mirror_mismatch: u64,
    /// Máximo de lotes abiertos observado.
    pub max_open_batches_seen: u64,
}

#[derive(Debug, Clone, Copy)]
struct TradeRec {
    key: LevelKey,
    qty_n: Qty,
    rpi: Qty,
    fills: u64,
    counted: bool,
}

#[derive(Debug)]
struct Batch {
    start: u64,
    end: u64,
    contaminated: Option<Contamination>,
    changes: BTreeMap<LevelKey, (Qty, Qty)>,
}

#[derive(Debug, Default, Clone, Copy)]
struct Acc {
    exec: Qty,
    lo: Qty,
    hi: Qty,
    rpi: Qty,
    fills: u64,
    trades: u64,
}

#[inline]
fn pos(a: Qty, b: Qty) -> Qty {
    Qty::from_raw((a.raw() - b.raw()).max(0))
}

/// Alineador. Se instala como `BookObserver` del `SyncBook`; los trades entran por [`Aligner::on_trade`].
pub struct Aligner<S: FlowSink = ()> {
    cfg: AlignerConfig,
    sink: S,
    live: bool,
    epoch: u64,
    fresh: bool,
    bids: BTreeMap<Px, Qty>,
    asks: BTreeMap<Px, Qty>,
    staging: BTreeMap<LevelKey, (Qty, Qty)>,
    batches: VecDeque<Batch>,
    last_end: u64,
    finalized_until: u64,
    trades: BTreeMap<(u64, u64), TradeRec>,
    uncounted: u64,
    trade_wm: u64,
    depth_wm: u64,
    stats: AlignerStats,
}

impl<S: FlowSink> Aligner<S> {
    /// Nuevo alineador (sin sincronizar hasta el primer `on_rebuild`).
    pub fn new(cfg: AlignerConfig, sink: S) -> Self {
        Self {
            cfg,
            sink,
            live: false,
            epoch: 0,
            fresh: true,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            staging: BTreeMap::new(),
            batches: VecDeque::new(),
            last_end: 0,
            finalized_until: 0,
            trades: BTreeMap::new(),
            uncounted: 0,
            trade_wm: 0,
            depth_wm: 0,
            stats: AlignerStats::default(),
        }
    }

    /// Contadores.
    pub fn stats(&self) -> &AlignerStats {
        &self.stats
    }
    /// Consumidor.
    pub fn sink(&self) -> &S {
        &self.sink
    }
    /// Consumidor (mutable).
    pub fn sink_mut(&mut self) -> &mut S {
        &mut self.sink
    }
    /// Lotes abiertos.
    pub fn open_batches(&self) -> usize {
        self.batches.len()
    }
    /// Trades retenidos (incluye los ya contados que aún sirven a un lote vecino).
    pub fn buffered_trades(&self) -> usize {
        self.trades.len()
    }
    /// Fin del último lote cerrado (ms de exchange).
    pub fn finalized_until(&self) -> u64 {
        self.finalized_until
    }

    /// Ley de conservación de trades: debe ser SIEMPRE 0.
    pub fn unaccounted(&self) -> i64 {
        let s = &self.stats;
        let terminal = s.aligned
            + s.contaminated
            + s.late
            + s.syncing
            + s.overflow
            + s.invalidated
            + s.duplicate;
        s.trades_rx as i64 - terminal as i64 - self.uncounted as i64
    }

    /// Entrada: trade único (ya deduplicado entre líneas).
    pub fn on_trade(&mut self, t: &AggTrade) {
        self.stats.trades_rx += 1;
        if !self.live {
            self.stats.syncing += 1;
            return;
        }
        self.trade_wm = self.trade_wm.max(t.exch_ts_ms);
        let ts = t.trade_ts_ms;
        if self.finalized_until > 0 && ts <= self.finalized_until {
            self.stats.late += 1;
            return;
        }
        let d = self.cfg.boundary_slack_ms;
        if d > 0 && self.finalized_until > 0 && ts <= self.finalized_until + d {
            self.stats.late_ambiguous += 1;
        }
        let side = match t.aggressor {
            Aggressor::Buy => Side::Ask,
            Aggressor::Sell => Side::Bid,
        };
        let rec = TradeRec {
            key: (side, t.px),
            qty_n: t.qty_normal.unwrap_or(t.qty),
            rpi: t.qty_rpi().unwrap_or(Qty::ZERO),
            fills: t.n_fills(),
            counted: false,
        };
        match self.trades.entry((ts, t.agg_id)) {
            std::collections::btree_map::Entry::Occupied(_) => {
                self.stats.duplicate += 1;
                return;
            }
            std::collections::btree_map::Entry::Vacant(v) => {
                v.insert(rec);
            }
        }
        self.uncounted += 1;
        while self.trades.len() > self.cfg.max_trades {
            if let Some((_, r)) = self.trades.pop_first() {
                if !r.counted {
                    self.uncounted -= 1;
                    self.stats.overflow += 1;
                    for b in &mut self.batches {
                        b.contaminated.get_or_insert(Contamination::TradeOverflow);
                    }
                }
            }
        }
        self.close_ready();
    }

    // ---------------------------------------------------------------- interno

    fn close_ready(&mut self) {
        let d = self.cfg.boundary_slack_ms;
        while self.batches.len() >= 2 {
            let end = self.batches[0].end;
            // `E` es monotónico por socket y ≥ tiempo real de ejecución; `T` difiere
            // del real hasta δ ⇒ con E > fin + 2δ ya llegaron todos los T ≤ fin + δ.
            if self.trade_wm > end + 2 * d {
                self.stats.closed_by_trades += 1;
            } else if self.depth_wm >= end + d + self.cfg.watermark_ms {
                self.stats.closed_by_timeout += 1;
            } else {
                break;
            }
            self.close_front();
        }
    }

    /// Cantidad de un nivel al final del lote que se está cerrando (ya fuera de la cola).
    fn resolve(&self, key: LevelKey) -> Qty {
        for b in &self.batches {
            if let Some(&(q0, _)) = b.changes.get(&key) {
                return q0;
            }
        }
        let m = match key.0 {
            Side::Bid => &self.bids,
            Side::Ask => &self.asks,
        };
        m.get(&key.1).copied().unwrap_or(Qty::ZERO)
    }

    fn close_front(&mut self) {
        let Some(b) = self.batches.pop_front() else {
            return;
        };
        let d = self.cfg.boundary_slack_ms;
        let clean = b.contaminated.is_none();
        let (lo_s, lo_e) = (b.start + d, b.end.saturating_sub(d));
        let (hi_s, hi_e) = (b.start.saturating_sub(d), b.end + d);
        let mut acc: BTreeMap<LevelKey, Acc> = BTreeMap::new();
        let mut n_nominal = 0u64;
        for (&(ts, _), tr) in self
            .trades
            .range_mut((Excluded((hi_s, u64::MAX)), Included((hi_e, u64::MAX))))
        {
            let a = acc.entry(tr.key).or_default();
            a.hi = a.hi + tr.qty_n;
            let certain = ts > lo_s && ts <= lo_e;
            if certain {
                a.lo = a.lo + tr.qty_n;
            }
            if ts > b.start && ts <= b.end && !tr.counted {
                tr.counted = true;
                n_nominal += 1;
                a.exec = a.exec + tr.qty_n;
                a.rpi = a.rpi + tr.rpi;
                a.fills += tr.fills;
                a.trades += 1;
                if !certain {
                    self.stats.ambiguous += 1;
                }
            }
        }
        self.uncounted -= n_nominal;
        if clean {
            self.stats.aligned += n_nominal;
            self.stats.batches_clean += 1;
        } else {
            self.stats.contaminated += n_nominal;
            self.stats.batches_contaminated += 1;
        }

        // Unión ordenada de niveles con cambio y niveles con trades.
        let mut keys: BTreeMap<LevelKey, (Option<(Qty, Qty)>, Acc)> = BTreeMap::new();
        for (k, qq) in &b.changes {
            keys.insert(*k, (Some(*qq), Acc::default()));
        }
        for (k, a) in acc {
            keys.entry(k).or_insert((None, Acc::default())).1 = a;
        }
        for (key, (qq, a)) in keys {
            let (q0, q1) = qq.unwrap_or_else(|| {
                let q = self.resolve(key);
                (q, q)
            });
            let f = LevelFlow {
                epoch: self.epoch,
                side: key.0,
                px: key.1,
                start_ts_ms: b.start,
                end_ts_ms: b.end,
                q0,
                q1,
                exec: a.exec,
                exec_lo: a.lo,
                exec_hi: a.hi,
                exec_rpi: a.rpi,
                n_fills: a.fills,
                n_trades: a.trades,
                no_visible_min: if clean { pos(a.lo, q0) } else { Qty::ZERO },
                cancelado_min: if clean { pos(q0, a.hi + q1) } else { Qty::ZERO },
                agregado_min: pos(q1, q0),
                clean,
            };
            self.stats.flows_emitted += 1;
            self.sink.on_level_flow(&f);
        }
        self.sink.on_batch(&BatchInfo {
            epoch: self.epoch,
            start_ts_ms: b.start,
            end_ts_ms: b.end,
            levels_changed: b.changes.len() as u32,
            trades: n_nominal,
            contaminated: b.contaminated,
        });
        self.finalized_until = b.end;

        // Podar trades que ningún lote abierto puede usar ya.
        while let Some((&(ts, _), r)) = self.trades.first_key_value() {
            if ts + d > self.finalized_until {
                break;
            }
            if !r.counted {
                // Imposible con lotes contiguos; defensa: se contabiliza como tardío.
                self.uncounted -= 1;
                self.stats.late += 1;
            }
            self.trades.pop_first();
        }
    }
}

impl<S: FlowSink> BookObserver for Aligner<S> {
    fn on_level(&mut self, side: Side, px: Px, prev: Qty, new: Qty, _exch_ts_ms: u64) {
        let m = match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };
        let cur = if new.is_zero() {
            m.remove(&px)
        } else {
            m.insert(px, new)
        }
        .unwrap_or(Qty::ZERO);
        if cur != prev {
            self.stats.mirror_mismatch += 1;
        }
        self.staging
            .entry((side, px))
            .and_modify(|e| e.1 = new)
            .or_insert((prev, new));
    }

    fn on_diff_applied(&mut self, diff: &DepthDiff) {
        if !self.live {
            self.staging.clear();
            return;
        }
        let raw = diff.match_ts_ms.unwrap_or(diff.exch_ts_ms);
        let ts = if raw < self.last_end {
            self.stats.ts_regressions += 1;
            self.last_end
        } else {
            raw
        };
        self.depth_wm = self.depth_wm.max(ts);
        let changes = std::mem::take(&mut self.staging);
        match self.batches.back_mut() {
            Some(b) if b.end == ts => {
                for (k, (p, n)) in changes {
                    b.changes.entry(k).and_modify(|e| e.1 = n).or_insert((p, n));
                }
            }
            _ => {
                self.batches.push_back(Batch {
                    start: self.last_end,
                    end: ts,
                    contaminated: self.fresh.then_some(Contamination::EpochStart),
                    changes,
                });
                self.fresh = false;
                self.last_end = ts;
            }
        }
        self.stats.max_open_batches_seen = self
            .stats
            .max_open_batches_seen
            .max(self.batches.len() as u64);
        while self.batches.len() > self.cfg.max_open_batches {
            self.stats.closed_forced += 1;
            self.close_front();
        }
        self.close_ready();
    }

    fn on_rebuild(&mut self, epoch: u64, book: &L2Book) {
        self.live = true;
        self.epoch = epoch;
        self.fresh = true;
        self.bids = book.bids().clone();
        self.asks = book.asks().clone();
        self.staging.clear();
        self.batches.clear();
        self.last_end = 0;
        self.finalized_until = 0;
        self.sink.on_rebuild(epoch, book);
    }

    fn on_invalidate(&mut self, epoch: u64, reason: ResyncReason) {
        self.stats.batches_contaminated += self.batches.len() as u64;
        self.stats.invalidated += self.uncounted;
        self.uncounted = 0;
        self.trades.clear();
        self.batches.clear();
        self.staging.clear();
        self.bids.clear();
        self.asks.clear();
        self.live = false;
        self.sink.on_invalidate(epoch, reason);
    }
}

/// Composición: dos consumidores en paralelo (despacho estático).
impl<A: FlowSink, B: FlowSink> FlowSink for (A, B) {
    #[inline]
    fn on_level_flow(&mut self, f: &LevelFlow) {
        self.0.on_level_flow(f);
        self.1.on_level_flow(f);
    }
    #[inline]
    fn on_batch(&mut self, b: &BatchInfo) {
        self.0.on_batch(b);
        self.1.on_batch(b);
    }
    #[inline]
    fn on_rebuild(&mut self, epoch: u64, book: &L2Book) {
        self.0.on_rebuild(epoch, book);
        self.1.on_rebuild(epoch, book);
    }
    #[inline]
    fn on_invalidate(&mut self, epoch: u64, reason: ResyncReason) {
        self.0.on_invalidate(epoch, reason);
        self.1.on_invalidate(epoch, reason);
    }
}

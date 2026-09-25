//! Motor por mercado: hilo de SO dedicado, dueño exclusivo de su libro.
//! Sin locks en el camino crítico: entra por una cola acotada, sale por una
//! vista inmutable (`ArcSwap`) que el servidor HTTP lee sin bloquear.

use crate::flow::{FlowAgg, FlowView};
use crate::view::{BookView, LevelView, LineView, TradeView};
use arc_swap::ArcSwap;
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, TrySendError};
use lu_binance::{FrameSink, RestClient};
use lu_book::{Phase, SeqRule, Step, SyncBook, MAX_LINES};
use lu_core::{
    mono_now_ms, wall_now_ns, AggTrade, DepthDiff, DepthSnapshot, InstrumentSpec, MarketEvent,
    MarketId, Px,
};
use lu_flow::Aligner;
use lu_metrics::{Metrics, MetricsView};
use lu_telemetry::LatencyHist;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Mensajes hacia el motor.
pub enum EngineMsg {
    /// Evento de mercado de una línea.
    Event(MarketEvent),
    /// Snapshot REST.
    Snapshot(DepthSnapshot),
    /// Reglas del instrumento (llegan de forma asíncrona al arrancar).
    Spec(InstrumentSpec),
    /// Apagado ordenado.
    Shutdown,
}

/// Qué transporta una línea.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum StreamKind {
    /// Profundidad (en spot también trades, mismo socket).
    Depth,
    /// Solo trades (futuros: endpoint `/market`).
    Trade,
}

/// Contadores compartidos entre líneas (escriben) y motor (lee al publicar).
#[derive(Default)]
pub struct IngestCounters {
    /// Eventos descartados por cola llena.
    pub dropped: AtomicU64,
    /// Frames no parseables.
    pub parse_errors: AtomicU64,
    /// Estado de conexión de profundidad por línea.
    pub depth_up: [AtomicBool; MAX_LINES],
    /// Estado de conexión de trades por línea.
    pub trade_up: [AtomicBool; MAX_LINES],
    /// Desconexiones por línea.
    pub disconnects: [AtomicU64; MAX_LINES],
}

/// Sumidero de una línea hacia la cola del motor.
pub struct ChannelSink {
    tx: Sender<EngineMsg>,
    counters: Arc<IngestCounters>,
    kind: StreamKind,
    label: String,
}

impl ChannelSink {
    /// Nuevo sumidero.
    pub fn new(
        tx: Sender<EngineMsg>,
        counters: Arc<IngestCounters>,
        kind: StreamKind,
        label: String,
    ) -> Self {
        Self {
            tx,
            counters,
            kind,
            label,
        }
    }
    fn flag(&self, line: u8) -> Option<&AtomicBool> {
        let arr = match self.kind {
            StreamKind::Depth => &self.counters.depth_up,
            StreamKind::Trade => &self.counters.trade_up,
        };
        arr.get(usize::from(line))
    }
}

impl FrameSink for ChannelSink {
    fn deliver(&self, ev: MarketEvent) -> bool {
        match self.tx.try_send(EngineMsg::Event(ev)) {
            Ok(()) => true,
            // Fail-safe: nunca bloquear el hilo de E/S. Un diff perdido aquí se
            // convierte en hueco detectado ⇒ resync. Jamás un libro silenciosamente malo.
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                self.counters.dropped.fetch_add(1, Ordering::Relaxed);
                false
            }
        }
    }
    fn line_up(&self, line: u8) {
        if let Some(f) = self.flag(line) {
            f.store(true, Ordering::Relaxed);
        }
        tracing::info!(market = %self.label, line, "línea conectada");
    }
    fn line_down(&self, line: u8, reason: &'static str) {
        if let Some(f) = self.flag(line) {
            f.store(false, Ordering::Relaxed);
        }
        if let Some(c) = self.counters.disconnects.get(usize::from(line)) {
            c.fetch_add(1, Ordering::Relaxed);
        }
        tracing::warn!(market = %self.label, line, reason, "línea caída");
    }
    fn parse_error(&self, line: u8, err: &str) {
        self.counters.parse_errors.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(market = %self.label, line, error = err, "frame no parseable");
    }
}

/// Deduplicador de trades A/B por id con ventana acotada.
struct TradeDedup {
    seen: BTreeSet<u64>,
    floor: u64,
    cap: usize,
}

impl TradeDedup {
    fn new(cap: usize) -> Self {
        Self {
            seen: BTreeSet::new(),
            floor: 0,
            cap,
        }
    }
    /// `true` si es nuevo.
    fn accept(&mut self, id: u64) -> bool {
        if id <= self.floor && self.floor != 0 {
            return false;
        }
        if !self.seen.insert(id) {
            return false;
        }
        while self.seen.len() > self.cap {
            if let Some(min) = self.seen.pop_first() {
                self.floor = min;
            }
        }
        true
    }
}

/// Parámetros del motor.
pub struct EngineConfig {
    /// Pulso de reloj (ms).
    pub tick_ms: u64,
    /// Período de publicación de la vista (ms).
    pub publish_ms: u64,
    /// Niveles en el top publicado.
    pub top_levels: usize,
    /// Período de publicación de las métricas F3 (ms).
    pub metrics_ms: u64,
    /// Velas cerradas publicadas por temporalidad.
    pub closed_candles: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            tick_ms: 50,
            publish_ms: 250,
            top_levels: 10,
            metrics_ms: 1_000,
            closed_candles: 12,
        }
    }
}

/// Observador del libro en el nodo: alineador F2 con su resumen.
pub type Obs = Aligner<(FlowAgg, Metrics)>;

/// Motor de un mercado.
pub struct Engine<R: SeqRule> {
    label: String,
    sync: SyncBook<R, Obs>,
    rx: Receiver<EngineMsg>,
    snap_req: tokio::sync::mpsc::UnboundedSender<()>,
    view: Arc<ArcSwap<BookView>>,
    metrics_view: Arc<ArcSwap<MetricsView>>,
    counters: Arc<IngestCounters>,
    rest: Arc<RestClient>,
    cfg: EngineConfig,
    spec: Option<InstrumentSpec>,
    off_tick: u64,
    lat_depth: Vec<LatencyHist>,
    lat_trade: Vec<LatencyHist>,
    trades_first: [u64; MAX_LINES],
    trades: TradeView,
    dedup: TradeDedup,
}

impl<R: SeqRule> Engine<R> {
    /// Construye el motor.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        market: MarketId,
        sync: SyncBook<R, Obs>,
        rx: Receiver<EngineMsg>,
        snap_req: tokio::sync::mpsc::UnboundedSender<()>,
        view: Arc<ArcSwap<BookView>>,
        metrics_view: Arc<ArcSwap<MetricsView>>,
        counters: Arc<IngestCounters>,
        rest: Arc<RestClient>,
        cfg: EngineConfig,
    ) -> Self {
        let label = market.to_string();
        Self {
            label,
            sync,
            rx,
            snap_req,
            view,
            metrics_view,
            counters,
            rest,
            cfg,
            spec: None,
            off_tick: 0,
            lat_depth: (0..MAX_LINES).map(|_| LatencyHist::new()).collect(),
            lat_trade: (0..MAX_LINES).map(|_| LatencyHist::new()).collect(),
            trades_first: [0; MAX_LINES],
            trades: TradeView::default(),
            dedup: TradeDedup::new(100_000),
        }
    }

    /// Bucle principal (hilo dedicado).
    pub fn run(mut self) {
        tracing::info!(market = %self.label, "motor iniciado");
        let tick = Duration::from_millis(self.cfg.tick_ms);
        let mut next_publish = 0u64;
        let mut next_metrics = 0u64;
        loop {
            match self.rx.recv_timeout(tick) {
                Ok(EngineMsg::Shutdown) | Err(RecvTimeoutError::Disconnected) => break,
                Ok(msg) => self.handle(msg),
                Err(RecvTimeoutError::Timeout) => {}
            }
            let now = mono_now_ms();
            let st = self.sync.on_tick(now);
            self.after(st);
            if now >= next_publish {
                self.publish();
                next_publish = now + self.cfg.publish_ms;
            }
            if now >= next_metrics {
                self.publish_metrics();
                next_metrics = now + self.cfg.metrics_ms;
            }
        }
        self.publish();
        self.publish_metrics();
        tracing::info!(market = %self.label, "motor detenido");
    }

    fn handle(&mut self, msg: EngineMsg) {
        let now = mono_now_ms();
        match msg {
            EngineMsg::Event(MarketEvent::Depth(d)) => {
                self.record_depth(&d);
                let st = self.sync.on_diff(d, now);
                self.after(st);
            }
            EngineMsg::Event(MarketEvent::Trade(t)) => self.on_trade(t),
            EngineMsg::Event(MarketEvent::Ignored) | EngineMsg::Shutdown => {}
            EngineMsg::Snapshot(s) => {
                tracing::info!(
                    market = %self.label,
                    last_update_id = s.last_update_id,
                    bids = s.bids.len(),
                    asks = s.asks.len(),
                    "snapshot recibido"
                );
                let st = self.sync.on_snapshot(s, now);
                self.after(st);
            }
            EngineMsg::Spec(s) => {
                tracing::info!(market = %self.label, tick = %s.tick, step = %s.step, "reglas del instrumento");
                self.spec = Some(s);
            }
        }
    }

    fn record_depth(&mut self, d: &DepthDiff) {
        let lat_us = (d.rx.wall_ns / 1_000) as i64 - (d.exch_ts_ms as i64) * 1_000;
        if let Some(h) = self.lat_depth.get_mut(usize::from(d.rx.line)) {
            h.record_us(lat_us);
        }
        if let Some(spec) = &self.spec {
            self.off_tick += d
                .bids
                .iter()
                .chain(d.asks.iter())
                .filter(|l| !spec.on_tick(l.px))
                .count() as u64;
        }
    }

    fn on_trade(&mut self, t: AggTrade) {
        self.trades.rx += 1;
        let lat_us = (t.rx.wall_ns / 1_000) as i64 - (t.exch_ts_ms as i64) * 1_000;
        if let Some(h) = self.lat_trade.get_mut(usize::from(t.rx.line)) {
            h.record_us(lat_us);
        }
        if !self.dedup.accept(t.agg_id) {
            self.trades.dup += 1;
            // Marca de agua por línea (F2): el duplicado prueba que esta línea ya pasó `E`.
            self.sync
                .observer_mut()
                .observe_line(t.rx.line, t.exch_ts_ms);
            return;
        }
        if let Some(c) = self.trades_first.get_mut(usize::from(t.rx.line)) {
            *c += 1;
        }
        self.trades.unique += 1;
        self.trades.fills += t.n_fills();
        match t.aggressor {
            lu_core::Aggressor::Buy => self.trades.buy_qty = self.trades.buy_qty + t.qty,
            lu_core::Aggressor::Sell => self.trades.sell_qty = self.trades.sell_qty + t.qty,
        }
        if let Some(r) = t.qty_rpi() {
            self.trades.with_nq += 1;
            self.trades.rpi_qty = self.trades.rpi_qty + r;
        }
        self.trades.last_px = Some(t.px);
        self.sync.observer_mut().on_trade(&t);
    }

    fn after(&mut self, st: Step) {
        if st.need_snapshot {
            let _ = self.snap_req.send(());
        }
        if st.went_live {
            tracing::info!(
                market = %self.label,
                epoch = self.sync.epoch(),
                last_update_id = self.sync.last_update_id(),
                "libro EN VIVO"
            );
        }
        if let Some(r) = st.resync {
            tracing::warn!(market = %self.label, reason = r.as_str(), "RESYNC");
        }
    }

    fn publish_metrics(&self) {
        let m = &self.sync.observer().sink().1;
        self.metrics_view
            .store(Arc::new(m.view(self.cfg.closed_candles)));
    }

    fn publish(&mut self) {
        let book = self.sync.book();
        let live = self.sync.phase() == Phase::Live;
        let bb = if live { book.best_bid() } else { None };
        let ba = if live { book.best_ask() } else { None };
        let (mid, spread_bps) = match (bb, ba) {
            (Some((b, _)), Some((a, _))) => {
                let mid = Px::from_raw((b.raw() + a.raw()) / 2);
                let bps = (a.raw() - b.raw()) as f64 / mid.raw() as f64 * 10_000.0;
                (Some(mid), Some(bps))
            }
            _ => (None, None),
        };
        let top = self.cfg.top_levels;
        let exch_ts = self.sync.last_exch_ts_ms();
        let wall_ms = wall_now_ns() / 1_000_000;
        let stats = self.sync.stats().clone();
        let lines = (0..MAX_LINES)
            .map(|i| LineView {
                line: i as u8,
                depth_up: self.counters.depth_up[i].load(Ordering::Relaxed),
                trade_up: self.counters.trade_up[i].load(Ordering::Relaxed),
                depth_latency: self.lat_depth[i].summary(),
                trade_latency: self.lat_trade[i].summary(),
                depth_first: stats.applied_by_line[i],
                depth_dup: stats.stale_by_line[i],
                trades_first: self.trades_first[i],
                disconnects: self.counters.disconnects[i].load(Ordering::Relaxed),
            })
            .collect();
        let view = BookView {
            market: self.label.clone(),
            phase: self.sync.phase(),
            epoch: self.sync.epoch(),
            last_update_id: self.sync.last_update_id(),
            exch_ts_ms: exch_ts,
            book_age_ms: if exch_ts > 0 {
                wall_ms as i64 - exch_ts as i64
            } else {
                0
            },
            best_bid: bb.map(|(px, qty)| LevelView { px, qty }),
            best_ask: ba.map(|(px, qty)| LevelView { px, qty }),
            mid,
            spread_bps,
            top_bids: if live {
                book.bids_desc()
                    .take(top)
                    .map(|(px, qty)| LevelView { px, qty })
                    .collect()
            } else {
                Vec::new()
            },
            top_asks: if live {
                book.asks_asc()
                    .take(top)
                    .map(|(px, qty)| LevelView { px, qty })
                    .collect()
            } else {
                Vec::new()
            },
            levels: {
                let (b, a) = book.depth_len();
                [b, a]
            },
            coverage: book.coverage(),
            tick: self.spec.as_ref().map(|s| s.tick),
            step: self.spec.as_ref().map(|s| s.step),
            off_tick_levels: self.off_tick,
            pending: self.sync.pending_len(),
            buffer: self.sync.buffer_len(),
            unaccounted: self.sync.unaccounted(),
            sync: stats,
            lines,
            trades: self.trades.clone(),
            flow: {
                let al = self.sync.observer();
                FlowView {
                    stats: al.stats().clone(),
                    unaccounted: al.unaccounted(),
                    open_batches: al.open_batches(),
                    buffered_trades: al.buffered_trades(),
                    finalized_until_ms: al.finalized_until(),
                    totals: al.sink().0.totals.clone(),
                    last_batch: al.sink().0.last_batch,
                    recent: al.sink().0.recent.iter().copied().collect(),
                    walls: al.sink().1.wall_stats().clone(),
                    recent_walls: al.sink().1.walls().rev().take(10).copied().collect(),
                }
            },
            ingest_dropped: self.counters.dropped.load(Ordering::Relaxed),
            parse_errors: self.counters.parse_errors.load(Ordering::Relaxed),
            rest_weight_1m: self.rest.used_weight_1m(),
            published_wall_ms: wall_ms,
        };
        self.view.store(Arc::new(view));
    }
}

#[cfg(test)]
mod tests {
    use super::TradeDedup;

    #[test]
    fn dedup_ab_con_ventana() {
        let mut d = TradeDedup::new(3);
        assert!(d.accept(10));
        assert!(!d.accept(10));
        assert!(d.accept(12));
        assert!(d.accept(11)); // desordenado entre líneas: válido
        assert!(d.accept(13)); // expulsa 10 → floor = 10
        assert!(!d.accept(10));
        assert!(!d.accept(12));
    }
}

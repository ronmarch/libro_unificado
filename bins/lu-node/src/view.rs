//! Vistas inmutables publicadas por cada motor (lectura sin locks vía `ArcSwap`)
//! y su exposición en Prometheus.

use lu_book::{Coverage, Phase, SyncStats, MAX_LINES};
use lu_core::{Px, Qty};
use lu_telemetry::{LatencySummary, MetricType, PromWriter};
use serde::Serialize;
use std::sync::Arc;

/// Nivel para presentación.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct LevelView {
    /// Precio.
    pub px: Px,
    /// Cantidad.
    pub qty: Qty,
}

/// Estado de una línea redundante.
#[derive(Debug, Clone, Default, Serialize)]
pub struct LineView {
    /// Índice (0 = A).
    pub line: u8,
    /// Conexión de profundidad activa.
    pub depth_up: bool,
    /// Conexión de trades activa (futuros: endpoint separado).
    pub trade_up: bool,
    /// Latencia exchange → recepción de los diffs.
    pub depth_latency: LatencySummary,
    /// Latencia exchange → recepción de los trades.
    pub trade_latency: LatencySummary,
    /// Diffs aplicados que llegaron primero por esta línea.
    pub depth_first: u64,
    /// Diffs duplicados descartados de esta línea.
    pub depth_dup: u64,
    /// Trades que llegaron primero por esta línea.
    pub trades_first: u64,
    /// Desconexiones.
    pub disconnects: u64,
}

/// Resumen de trades (F1: validación de líneas; F2 los alineará con el libro).
#[derive(Debug, Clone, Default, Serialize)]
pub struct TradeView {
    /// Trades recibidos (todas las líneas).
    pub rx: u64,
    /// Duplicados A/B descartados.
    pub dup: u64,
    /// Trades únicos.
    pub unique: u64,
    /// Fills reales (Σ l − f + 1).
    pub fills: u64,
    /// Volumen comprador agresivo.
    pub buy_qty: Qty,
    /// Volumen vendedor agresivo.
    pub sell_qty: Qty,
    /// Volumen ejecutado contra órdenes RPI (q − nq; solo futuros).
    pub rpi_qty: Qty,
    /// Trades únicos que traen el campo `nq` (0 ⇒ el venue no informa RPI en este stream).
    pub with_nq: u64,
    /// Último precio.
    pub last_px: Option<Px>,
}

/// Vista completa de un mercado.
#[derive(Debug, Clone, Serialize)]
pub struct BookView {
    /// `binance.spot.SOLUSDT`.
    pub market: String,
    /// Fase.
    pub phase: Phase,
    /// Época (reconstrucciones desde snapshot).
    pub epoch: u64,
    /// Último update id aplicado.
    pub last_update_id: u64,
    /// Tiempo de exchange del último diff aplicado (ms).
    pub exch_ts_ms: u64,
    /// Antigüedad del libro respecto del reloj local (ms; requiere chrony).
    pub book_age_ms: i64,
    /// Mejor bid.
    pub best_bid: Option<LevelView>,
    /// Mejor ask.
    pub best_ask: Option<LevelView>,
    /// Precio medio.
    pub mid: Option<Px>,
    /// Spread en bps (solo presentación).
    pub spread_bps: Option<f64>,
    /// Top de bids.
    pub top_bids: Vec<LevelView>,
    /// Top de asks.
    pub top_asks: Vec<LevelView>,
    /// Niveles por lado.
    pub levels: [usize; 2],
    /// Cobertura garantizada por el snapshot.
    pub coverage: Coverage,
    /// Tick/step del instrumento si se obtuvo `exchangeInfo`.
    pub tick: Option<Px>,
    /// Paso de cantidad.
    pub step: Option<Qty>,
    /// Precios fuera de tick observados (control de calidad del feed).
    pub off_tick_levels: u64,
    /// Diffs retenidos esperando un faltante.
    pub pending: usize,
    /// Diffs en buffer de sincronización.
    pub buffer: usize,
    /// Ley de conservación de mensajes (debe ser 0).
    pub unaccounted: i64,
    /// Contadores de sincronización.
    pub sync: SyncStats,
    /// Líneas.
    pub lines: Vec<LineView>,
    /// Trades.
    pub trades: TradeView,
    /// Eventos descartados por cola llena (fail-safe: fuerzan hueco ⇒ resync).
    pub ingest_dropped: u64,
    /// Frames no parseables.
    pub parse_errors: u64,
    /// Peso REST usado en el último minuto (según Binance).
    pub rest_weight_1m: u32,
    /// Momento de publicación (ms UNIX).
    pub published_wall_ms: u64,
}

impl BookView {
    /// Vista inicial vacía.
    pub fn empty(market: String) -> Self {
        Self {
            market,
            phase: Phase::Syncing,
            epoch: 0,
            last_update_id: 0,
            exch_ts_ms: 0,
            book_age_ms: 0,
            best_bid: None,
            best_ask: None,
            mid: None,
            spread_bps: None,
            top_bids: Vec::new(),
            top_asks: Vec::new(),
            levels: [0, 0],
            coverage: Coverage::default(),
            tick: None,
            step: None,
            off_tick_levels: 0,
            pending: 0,
            buffer: 0,
            unaccounted: 0,
            sync: SyncStats::default(),
            lines: (0..MAX_LINES as u8)
                .map(|line| LineView {
                    line,
                    ..LineView::default()
                })
                .collect(),
            trades: TradeView::default(),
            ingest_dropped: 0,
            parse_errors: 0,
            rest_weight_1m: 0,
            published_wall_ms: 0,
        }
    }
}

/// Renderiza todas las vistas en formato Prometheus.
pub fn render_prometheus(views: &[Arc<BookView>]) -> String {
    use MetricType::{Counter, Gauge};
    let mut w = PromWriter::new();
    for v in views {
        let m = v.market.as_str();
        let l = [("market", m)];
        w.sample(
            "lu_book_live",
            "1 si el libro está sincronizado",
            Gauge,
            &l,
            f64::from(u8::from(v.phase == Phase::Live)),
        );
        w.sample(
            "lu_book_epoch",
            "reconstrucciones desde snapshot",
            Gauge,
            &l,
            v.epoch as f64,
        );
        w.sample(
            "lu_book_age_ms",
            "antigüedad del último diff aplicado",
            Gauge,
            &l,
            v.book_age_ms as f64,
        );
        w.sample(
            "lu_book_levels",
            "niveles por lado",
            Gauge,
            &[("market", m), ("side", "bid")],
            v.levels[0] as f64,
        );
        w.sample(
            "lu_book_levels",
            "niveles por lado",
            Gauge,
            &[("market", m), ("side", "ask")],
            v.levels[1] as f64,
        );
        if let Some(s) = v.spread_bps {
            w.sample("lu_book_spread_bps", "spread en bps", Gauge, &l, s);
        }
        w.sample(
            "lu_book_pending",
            "diffs retenidos esperando faltante",
            Gauge,
            &l,
            v.pending as f64,
        );
        w.sample(
            "lu_book_off_tick_total",
            "precios fuera de tick",
            Counter,
            &l,
            v.off_tick_levels as f64,
        );
        let s = &v.sync;
        w.sample(
            "lu_sync_diffs_rx_total",
            "diffs recibidos",
            Counter,
            &l,
            s.diffs_rx as f64,
        );
        w.sample(
            "lu_sync_applied_total",
            "diffs aplicados",
            Counter,
            &l,
            s.applied as f64,
        );
        w.sample(
            "lu_sync_stale_total",
            "diffs duplicados/obsoletos",
            Counter,
            &l,
            s.stale as f64,
        );
        w.sample(
            "lu_sync_ahead_total",
            "diffs adelantados retenidos",
            Counter,
            &l,
            s.ahead as f64,
        );
        w.sample(
            "lu_sync_reordered_total",
            "reordenamientos recuperados",
            Counter,
            &l,
            s.reordered_applied as f64,
        );
        w.sample(
            "lu_sync_snapshots_requested_total",
            "snapshots pedidos",
            Counter,
            &l,
            s.snapshots_requested as f64,
        );
        w.sample(
            "lu_sync_snapshots_unbridgeable_total",
            "snapshots no puenteables",
            Counter,
            &l,
            s.snapshots_unbridgeable as f64,
        );
        w.sample(
            "lu_sync_crossed_total",
            "libro cruzado observado",
            Counter,
            &l,
            s.crossed_seen as f64,
        );
        w.sample(
            "lu_sync_pre_snapshot_total",
            "diffs previos al snapshot descartados (regla oficial)",
            Counter,
            &l,
            s.pre_snapshot as f64,
        );
        w.sample(
            "lu_sync_invalid_dropped_total",
            "diffs con secuencia imposible",
            Counter,
            &l,
            s.invalid_dropped as f64,
        );
        w.sample(
            "lu_sync_buffer_dropped_total",
            "diffs perdidos por desborde del buffer",
            Counter,
            &l,
            s.buffer_dropped as f64,
        );
        w.sample(
            "lu_sync_unaccounted",
            "ley de conservación de mensajes (debe ser 0)",
            Gauge,
            &l,
            v.unaccounted as f64,
        );
        for (reason, n) in [
            ("gap_timeout", s.resync_gap_timeout),
            ("pending_overflow", s.resync_pending_overflow),
            ("invalid_sequence", s.resync_invalid_sequence),
            ("crossed", s.resync_crossed),
            ("manual", s.resync_manual),
        ] {
            w.sample(
                "lu_sync_resync_total",
                "resincronizaciones",
                Counter,
                &[("market", m), ("reason", reason)],
                n as f64,
            );
        }
        for lv in v.lines.iter().filter(|x| {
            x.depth_first + x.depth_dup + x.trades_first > 0 || x.depth_up || x.trade_up
        }) {
            let li = lv.line.to_string();
            let ll = [("market", m), ("line", li.as_str())];
            w.sample(
                "lu_line_up",
                "línea conectada",
                Gauge,
                &[("market", m), ("line", li.as_str()), ("stream", "depth")],
                f64::from(u8::from(lv.depth_up)),
            );
            w.sample(
                "lu_line_up",
                "línea conectada",
                Gauge,
                &[("market", m), ("line", li.as_str()), ("stream", "trade")],
                f64::from(u8::from(lv.trade_up)),
            );
            w.sample(
                "lu_line_depth_first_total",
                "diffs ganados por la línea",
                Counter,
                &ll,
                lv.depth_first as f64,
            );
            w.sample(
                "lu_line_depth_dup_total",
                "diffs duplicados de la línea",
                Counter,
                &ll,
                lv.depth_dup as f64,
            );
            w.sample(
                "lu_line_trades_first_total",
                "trades ganados por la línea",
                Counter,
                &ll,
                lv.trades_first as f64,
            );
            w.sample(
                "lu_line_disconnects_total",
                "desconexiones",
                Counter,
                &ll,
                lv.disconnects as f64,
            );
            for (stream, lat) in [("depth", &lv.depth_latency), ("trade", &lv.trade_latency)] {
                for (q, val) in [
                    ("0.5", lat.p50_ms),
                    ("0.99", lat.p99_ms),
                    ("0.999", lat.p999_ms),
                ] {
                    w.sample(
                        "lu_line_latency_ms",
                        "latencia exchange→recepción",
                        Gauge,
                        &[
                            ("market", m),
                            ("line", li.as_str()),
                            ("stream", stream),
                            ("quantile", q),
                        ],
                        val,
                    );
                }
            }
        }
        w.sample(
            "lu_trades_unique_total",
            "trades únicos",
            Counter,
            &l,
            v.trades.unique as f64,
        );
        w.sample(
            "lu_trades_with_nq_total",
            "trades que informan nq (RPI)",
            Counter,
            &l,
            v.trades.with_nq as f64,
        );
        w.sample(
            "lu_trades_dup_total",
            "trades duplicados A/B",
            Counter,
            &l,
            v.trades.dup as f64,
        );
        w.sample(
            "lu_ingest_dropped_total",
            "eventos descartados por cola llena",
            Counter,
            &l,
            v.ingest_dropped as f64,
        );
        w.sample(
            "lu_parse_errors_total",
            "frames no parseables",
            Counter,
            &l,
            v.parse_errors as f64,
        );
        w.sample(
            "lu_rest_weight_1m",
            "peso REST usado (Binance)",
            Gauge,
            &l,
            f64::from(v.rest_weight_1m),
        );
    }
    w.finish()
}

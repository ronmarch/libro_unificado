//! Velas footprint por bucket de precio y detector de muros retirados (F3).
//!
//! Consume `LevelFlow` de F2 (`FlowSink`). Todo en tiempo de exchange y en
//! aritmética entera: las cantidades son `i64` escala 1e-8 y las áreas
//! `qty·ms` son `i128`. `f64` solo en las vistas.
//!
//! # TWA perezoso
//!
//! Por vela y bucket se mantiene `acc` (área `qty·ms`) y el instante del último
//! cambio. Solo al cambiar la cantidad: `acc += qty_prev · Δt`. El TWA en `t` es
//! `(acc + qty·(t − último)) / tiempo_observado(t)`. El tiempo observado excluye
//! los intervalos sin sincronización (entre `on_invalidate` y la primera
//! reconstrucción), así que el TWA nunca promedia datos inexistentes.
//!
//! Resolución temporal: un cambio se fecha al fin de su lote (100 ms en Binance).
//!
//! # Muros retirados (parámetros confirmados; `ARCHITECTURE.md` §9)
//!
//! Un bucket de un lado es muro retirado si, en el instante en que su cantidad
//! cruza bajo `(1 − X)·TWA₄ₕ` (TWA de la vela 4 h que lo contiene):
//! 1. su TWA₄ₕ ≥ percentil P de los TWA₄ₕ del mismo lado (buckets dentro de la
//!    cobertura del snapshot; percentil de rango más cercano),
//! 2. está a ≤ N buckets del bucket del precio medio en ese instante,
//! 3. las ejecuciones en ese bucket y lado dentro de la ventana explican menos
//!    de Y de la caída visible. La ventana empieza la última vez que el bucket
//!    estuvo ≥ su TWA₄ₕ; la caída visible es `cantidad_inicio − cantidad_actual`.
//!
//! Cada ventana se evalúa una vez (en el cruce). Una ventana que contiene un
//! lote contaminado no puede afirmar nada sobre ejecuciones: se rechaza.

use lu_book::{L2Book, ResyncReason};
use lu_core::{Px, Qty, Side};
use lu_flow::{BatchInfo, FlowSink, LevelFlow};
use serde::Serialize;
use std::collections::{BTreeMap, VecDeque};

/// 15 minutos en ms.
pub const TF_15M: u64 = 15 * 60 * 1000;
/// 1 hora en ms.
pub const TF_1H: u64 = 60 * 60 * 1000;
/// 4 horas en ms.
pub const TF_4H: u64 = 4 * 60 * 60 * 1000;

/// Parámetros del detector de muros retirados (valores confirmados por el usuario).
#[derive(Debug, Clone)]
pub struct WallConfig {
    /// P: percentil del TWA₄ₕ (90).
    pub percentile: u32,
    /// X: caída mínima en % del TWA₄ₕ (50).
    pub drop_pct: u32,
    /// N: distancia máxima al precio medio en buckets (2).
    pub max_distance: i64,
    /// Y: fracción máxima de la caída explicada por ejecuciones, en % (20).
    pub max_exec_pct: u32,
}

impl Default for WallConfig {
    fn default() -> Self {
        Self {
            percentile: 90,
            drop_pct: 50,
            max_distance: 2,
            max_exec_pct: 20,
        }
    }
}

/// Parámetros de táctica vs estructural (R = TWA corto ÷ TWA largo de referencia).
///
/// Definición del usuario (2026-09-25): R = TWA 15 m ÷ TWA 4 h del mismo bucket y lado;
/// referencia = vela de 4 h ANTERIOR ya cerrada (opción B), no la que contiene a la de
/// 15 m (que acota R a ≤ 16 y vale 1 en el primer bloque).
///
/// Supuestos declarados (no especificados; confirmar):
/// * "R ≈ 1" = `approx_low_pct/100 ≤ R ≤ approx_high_pct/100` (80 % – 125 %).
/// * "TWA 4 h alto" = ≥ percentil `high_percentile` (90, como el detector de muros) de
///   los TWA de la vela de referencia, mismo lado, dentro de la cobertura.
#[derive(Debug, Clone)]
pub struct TacticalConfig {
    /// Temporalidad corta (15 m).
    pub short_tf_ms: u64,
    /// Temporalidad larga de referencia (4 h).
    pub long_tf_ms: u64,
    /// Límite inferior de "R ≈ 1", en %.
    pub approx_low_pct: u32,
    /// Límite superior de "R ≈ 1", en %.
    pub approx_high_pct: u32,
    /// Percentil que define "TWA 4 h alto".
    pub high_percentile: u32,
}

impl Default for TacticalConfig {
    fn default() -> Self {
        Self {
            short_tf_ms: TF_15M,
            long_tf_ms: TF_4H,
            approx_low_pct: 80,
            approx_high_pct: 125,
            high_percentile: 90,
        }
    }
}

/// Lectura de táctica vs estructural.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Liquidity {
    /// R ≈ 1 con TWA 4 h alto: el tamaño lleva horas ahí.
    Estructural,
    /// R ≈ 1 sin TWA 4 h alto: estable pero no relevante.
    Estable,
    /// R > 1 (o sin liquidez en la referencia): alguien lo puso hace poco; candidata a spoof si se retira.
    Tactica,
    /// R < 1: había tamaño y se está yendo.
    Desarmandose,
}

/// Parámetros de F3.
#[derive(Debug, Clone)]
pub struct MetricsConfig {
    /// Ancho del bucket (1 USDT).
    pub bucket: Px,
    /// Temporalidades de las velas (ms).
    pub timeframes_ms: Vec<u64>,
    /// Temporalidad del TWA del detector de muros (4 h).
    pub wall_tf_ms: u64,
    /// Velas cerradas conservadas por temporalidad.
    pub keep_closed: usize,
    /// Eventos de muro conservados.
    pub keep_walls: usize,
    /// Detector.
    pub wall: WallConfig,
    /// Táctica vs estructural.
    pub tactical: TacticalConfig,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            bucket: Px::from_units(1),
            timeframes_ms: vec![TF_15M, TF_1H, TF_4H],
            wall_tf_ms: TF_4H,
            keep_closed: 96,
            keep_walls: 500,
            wall: WallConfig::default(),
            tactical: TacticalConfig::default(),
        }
    }
}

type Key = (Side, i64);

#[derive(Debug, Clone, Copy, Default)]
struct CellState {
    acc: i128,
    last_ts: u64,
    qty: i64,
    exec: i64,
    rpi: i64,
    fills: u64,
    trades: u64,
    nv: i64,
    cm: i64,
    am: i64,
}

#[derive(Debug, Clone)]
struct Candle {
    tf: u64,
    start: u64,
    end: u64,
    observed: u64,
    live_since: Option<u64>,
    clean: bool,
    cells: BTreeMap<Key, CellState>,
}

impl Candle {
    fn observed_at(&self, ts: u64) -> u64 {
        self.observed + self.live_since.map_or(0, |s| ts.saturating_sub(s))
    }
    /// Área acumulada del bucket hasta `ts`.
    fn acc_at(c: &CellState, ts: u64, live: bool) -> i128 {
        if live {
            c.acc + i128::from(c.qty) * i128::from(ts.saturating_sub(c.last_ts))
        } else {
            c.acc
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Window {
    start_ts: u64,
    start_qty: i64,
    exec: i64,
    evaluated: bool,
    poisoned: bool,
}

/// Muro retirado detectado.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct WallRetired {
    /// Lado.
    pub side: Side,
    /// Bucket (índice).
    pub bucket: i64,
    /// Precio inferior del bucket.
    pub px_low: Px,
    /// Instante del cruce (ms de exchange).
    pub ts_ms: u64,
    /// Inicio de la ventana de caída.
    pub window_start_ms: u64,
    /// TWA₄ₕ del bucket en el cruce.
    pub twa: Qty,
    /// Umbral del percentil (TWA₄ₕ).
    pub percentile_twa: Qty,
    /// Cantidad al inicio de la ventana.
    pub start_qty: Qty,
    /// Cantidad en el cruce.
    pub qty: Qty,
    /// Ejecutado en la ventana (sin RPI).
    pub exec: Qty,
    /// Ejecutado ÷ caída visible (presentación).
    pub exec_ratio: f64,
    /// Distancia al bucket del precio medio.
    pub distance: i64,
}

/// Contadores del detector.
#[derive(Debug, Clone, Default, Serialize)]
pub struct WallStats {
    /// Ventanas evaluadas (cruces bajo el umbral X).
    pub evaluated: u64,
    /// Muros retirados emitidos.
    pub fired: u64,
    /// Rechazos por percentil.
    pub rejected_percentile: u64,
    /// Rechazos por distancia.
    pub rejected_distance: u64,
    /// Rechazos por ejecuciones ≥ Y.
    pub rejected_exec: u64,
    /// Rechazos por ventana contaminada.
    pub rejected_contaminated: u64,
    /// Rechazos por bucket fuera de la cobertura.
    pub rejected_coverage: u64,
    /// Rechazos por no haber precio medio.
    pub rejected_no_mid: u64,
}

/// Celda de una vela para presentación.
#[derive(Debug, Clone, Serialize)]
pub struct CellView {
    /// Lado.
    pub side: Side,
    /// Bucket.
    pub bucket: i64,
    /// Precio inferior del bucket.
    pub px_low: Px,
    /// TWA en la vela.
    pub twa: Qty,
    /// Foto: cantidad al cierre (o actual si la vela está abierta).
    pub qty: Qty,
    /// Persistencia = foto ÷ TWA (presentación).
    pub persistence: Option<f64>,
    /// Ejecutado contra este lado (sin RPI).
    pub exec: Qty,
    /// Ejecutado RPI.
    pub exec_rpi: Qty,
    /// Fills.
    pub n_fills: u64,
    /// Trades agregados.
    pub n_trades: u64,
    /// Σ cota inferior no visible.
    pub no_visible_min: Qty,
    /// Σ cota inferior cancelado.
    pub cancelado_min: Qty,
    /// Σ cota inferior agregado.
    pub agregado_min: Qty,
    /// Bucket parcialmente fuera de la cobertura del snapshot.
    pub partial: bool,
    /// Solo velas de 15 m: TWA del mismo bucket en la vela de 4 h anterior cerrada.
    pub twa_ref: Option<Qty>,
    /// Solo velas de 15 m: R = TWA ÷ `twa_ref` (presentación; `None` si la referencia es 0).
    pub ratio: Option<f64>,
    /// Solo velas de 15 m: lectura táctica vs estructural.
    pub liquidity: Option<Liquidity>,
}

/// Vela para presentación.
#[derive(Debug, Clone, Serialize)]
pub struct CandleView {
    /// Temporalidad (ms).
    pub tf_ms: u64,
    /// Inicio (ms UNIX de exchange, alineado a la temporalidad).
    pub start_ms: u64,
    /// Fin exclusivo.
    pub end_ms: u64,
    /// Tiempo con libro sincronizado (ms).
    pub observed_ms: u64,
    /// `false` si hubo pérdida de sincronización o lotes contaminados.
    pub clean: bool,
    /// Ejecutado comprador (contra asks) total.
    pub buy_exec: Qty,
    /// Ejecutado vendedor (contra bids) total.
    pub sell_exec: Qty,
    /// Delta de trades = compra − venta agresiva.
    pub delta: Qty,
    /// Solo velas de 15 m: inicio de la vela de 4 h de referencia (`None` = aún no existe).
    pub ref_start_ms: Option<u64>,
    /// Celdas ordenadas por lado y bucket.
    pub cells: Vec<CellView>,
}

/// Vista completa de F3.
#[derive(Debug, Clone, Default, Serialize)]
pub struct MetricsView {
    /// Último instante procesado (ms de exchange).
    pub now_ms: u64,
    /// Velas en curso por temporalidad.
    pub current: Vec<CandleView>,
    /// Velas cerradas recientes por temporalidad (más nueva al final).
    pub closed: Vec<Vec<CandleView>>,
    /// Muros retirados recientes (más nuevo al final).
    pub walls: Vec<WallRetired>,
    /// Contadores del detector.
    pub wall_stats: WallStats,
    /// Bids por debajo de este bucket (inclusive) son desconocidos (snapshot truncado).
    pub bid_floor_bucket: Option<i64>,
    /// Asks por encima de este bucket (inclusive) son desconocidos.
    pub ask_ceiling_bucket: Option<i64>,
}

/// Motor de métricas F3.
pub struct Metrics {
    cfg: MetricsConfig,
    levels: [BTreeMap<Px, i64>; 2],
    buckets: [BTreeMap<i64, i64>; 2],
    floor_bucket: Option<i64>,
    ceiling_bucket: Option<i64>,
    candles: Vec<Candle>,
    closed: Vec<VecDeque<Candle>>,
    wall_idx: Option<usize>,
    windows: BTreeMap<Key, Window>,
    batch: Vec<LevelFlow>,
    live: bool,
    resume_pending: bool,
    now: u64,
    walls: VecDeque<WallRetired>,
    wall_stats: WallStats,
}

#[inline]
fn si(s: Side) -> usize {
    match s {
        Side::Bid => 0,
        Side::Ask => 1,
    }
}

impl Metrics {
    /// Nuevo motor (inactivo hasta el primer `on_rebuild`).
    pub fn new(cfg: MetricsConfig) -> Self {
        let wall_idx = cfg.timeframes_ms.iter().position(|&t| t == cfg.wall_tf_ms);
        let n = cfg.timeframes_ms.len();
        Self {
            cfg,
            levels: [BTreeMap::new(), BTreeMap::new()],
            buckets: [BTreeMap::new(), BTreeMap::new()],
            floor_bucket: None,
            ceiling_bucket: None,
            candles: Vec::new(),
            closed: (0..n).map(|_| VecDeque::new()).collect(),
            wall_idx,
            windows: BTreeMap::new(),
            batch: Vec::new(),
            live: false,
            resume_pending: false,
            now: 0,
            walls: VecDeque::new(),
            wall_stats: WallStats::default(),
        }
    }

    /// Contadores del detector.
    pub fn wall_stats(&self) -> &WallStats {
        &self.wall_stats
    }
    /// Muros retirados recientes.
    pub fn walls(&self) -> impl DoubleEndedIterator<Item = &WallRetired> {
        self.walls.iter()
    }
    /// Último instante procesado.
    pub fn now_ms(&self) -> u64 {
        self.now
    }

    /// TWA (escala 1e-8) de un bucket en la vela en curso de la temporalidad `tf`.
    pub fn twa(&self, tf: u64, side: Side, bucket: i64) -> Option<Qty> {
        let c = self.candles.iter().find(|c| c.tf == tf)?;
        let cell = c.cells.get(&(side, bucket))?;
        let obs = c.observed_at(self.now);
        (obs > 0).then(|| {
            Qty::from_raw(
                (Candle::acc_at(cell, self.now, c.live_since.is_some()) / i128::from(obs)) as i64,
            )
        })
    }

    fn is_partial(&self, side: Side, b: i64) -> bool {
        match side {
            Side::Bid => self.floor_bucket.is_some_and(|f| b <= f),
            Side::Ask => self.ceiling_bucket.is_some_and(|c| b >= c),
        }
    }

    fn open_candle(&self, tf: u64, start: u64, live_at: Option<u64>) -> Candle {
        let mut cells = BTreeMap::new();
        for (i, side) in [Side::Bid, Side::Ask].into_iter().enumerate() {
            for (&b, &q) in &self.buckets[i] {
                cells.insert(
                    (side, b),
                    CellState {
                        last_ts: live_at.unwrap_or(start),
                        qty: q,
                        ..CellState::default()
                    },
                );
            }
        }
        Candle {
            tf,
            start,
            end: start + tf,
            observed: 0,
            live_since: live_at,
            clean: live_at == Some(start),
            cells,
        }
    }

    fn close_candle(c: &mut Candle, at: u64) {
        let live = c.live_since.is_some();
        if let Some(s) = c.live_since.take() {
            c.observed += at.saturating_sub(s);
        }
        for cell in c.cells.values_mut() {
            if live {
                cell.acc += i128::from(cell.qty) * i128::from(at.saturating_sub(cell.last_ts));
            }
            cell.last_ts = at;
        }
    }

    /// Avanza el reloj de exchange hasta `ts`, cerrando y abriendo velas.
    fn advance_to(&mut self, ts: u64) {
        if self.candles.is_empty() {
            let live_at = self.live.then_some(ts);
            self.candles = self
                .cfg
                .timeframes_ms
                .clone()
                .into_iter()
                .map(|tf| {
                    let start = ts / tf * tf;
                    let mut c = self.open_candle(tf, start, live_at);
                    // Vela iniciada a mitad: nunca es limpia (no cubre su comienzo).
                    c.clean = false;
                    c
                })
                .collect();
            return;
        }
        for i in 0..self.candles.len() {
            while ts >= self.candles[i].end {
                let end = self.candles[i].end;
                let was_live = self.candles[i].live_since.is_some();
                let mut old = std::mem::replace(
                    &mut self.candles[i],
                    Candle {
                        tf: 0,
                        start: 0,
                        end: 0,
                        observed: 0,
                        live_since: None,
                        clean: false,
                        cells: BTreeMap::new(),
                    },
                );
                Self::close_candle(&mut old, end);
                // Velas contiguas: un libro quieto también produce velas (con su TWA).
                let live_at = was_live.then_some(end);
                self.candles[i] = self.open_candle(old.tf, end, live_at);
                let q = &mut self.closed[i];
                q.push_back(old);
                while q.len() > self.cfg.keep_closed {
                    q.pop_front();
                }
            }
        }
    }

    fn pause(&mut self, at: u64) {
        for c in &mut self.candles {
            if let Some(s) = c.live_since.take() {
                c.observed += at.saturating_sub(s);
                for cell in c.cells.values_mut() {
                    cell.acc += i128::from(cell.qty) * i128::from(at.saturating_sub(cell.last_ts));
                    cell.last_ts = at;
                }
            }
            c.clean = false;
        }
        self.windows.clear();
    }

    fn resume(&mut self, at: u64) {
        self.buckets = [BTreeMap::new(), BTreeMap::new()];
        for i in 0..2 {
            for (px, &q) in &self.levels[i] {
                *self.buckets[i]
                    .entry(px.bucket(self.cfg.bucket))
                    .or_default() += q;
            }
        }
        for c in &mut self.candles {
            c.live_since = Some(at);
            c.clean = false;
            for ((side, b), cell) in c.cells.iter_mut() {
                cell.qty = self.buckets[si(*side)].get(b).copied().unwrap_or(0);
                cell.last_ts = at;
            }
            for (i, side) in [Side::Bid, Side::Ask].into_iter().enumerate() {
                for (&b, &q) in &self.buckets[i] {
                    c.cells.entry((side, b)).or_insert(CellState {
                        last_ts: at,
                        qty: q,
                        ..CellState::default()
                    });
                }
            }
        }
    }

    fn process_batch(&mut self, b: &BatchInfo) {
        let ts = b.end_ts_ms;
        let flows = std::mem::take(&mut self.batch);
        if !self.live {
            return;
        }
        self.advance_to(ts);
        if self.resume_pending {
            self.resume(ts);
            self.resume_pending = false;
        }
        self.now = ts;
        let clean = b.contaminated.is_none();
        if !clean {
            for c in &mut self.candles {
                c.clean = false;
            }
        }

        // 1. Cantidades por nivel → cantidades por bucket; ejecución por bucket.
        #[derive(Default, Clone, Copy)]
        struct Touch {
            prev: i64,
            new: i64,
            exec: i64,
            rpi: i64,
            fills: u64,
            trades: u64,
            nv: i64,
            cm: i64,
            am: i64,
            seen: bool,
        }
        let mut touched: BTreeMap<Key, Touch> = BTreeMap::new();
        for f in &flows {
            let i = si(f.side);
            let bk = f.px.bucket(self.cfg.bucket);
            let cur_bucket = self.buckets[i].get(&bk).copied().unwrap_or(0);
            let t = touched.entry((f.side, bk)).or_insert(Touch {
                prev: cur_bucket,
                new: cur_bucket,
                ..Touch::default()
            });
            t.seen = true;
            let old_level = self.levels[i].get(&f.px).copied().unwrap_or(0);
            let q1 = f.q1.raw();
            if q1 != old_level {
                if q1 == 0 {
                    self.levels[i].remove(&f.px);
                } else {
                    self.levels[i].insert(f.px, q1);
                }
                t.new += q1 - old_level;
                if t.new == 0 {
                    self.buckets[i].remove(&bk);
                } else {
                    self.buckets[i].insert(bk, t.new);
                }
            }
            t.exec += f.exec.raw();
            t.rpi += f.exec_rpi.raw();
            t.fills += f.n_fills;
            t.trades += f.n_trades;
            t.nv += f.no_visible_min.raw();
            t.cm += f.cancelado_min.raw();
            t.am += f.agregado_min.raw();
        }

        // 2. Muros (antes de aplicar el cambio se evalúa si el bucket estaba ≥ TWA).
        let mid_bucket = match (
            self.levels[0].last_key_value(),
            self.levels[1].first_key_value(),
        ) {
            (Some((bb, _)), Some((ba, _))) => {
                Some(Px::from_raw((bb.raw() + ba.raw()) / 2).bucket(self.cfg.bucket))
            }
            _ => None,
        };
        if let Some(wi) = self.wall_idx {
            for (&key, t) in &touched {
                self.wall_step(wi, key, t.prev, t.new, t.exec, clean, ts, mid_bucket);
            }
        }

        // 3. Velas: área con la cantidad previa hasta `ts`, luego la nueva cantidad.
        for c in &mut self.candles {
            let live = c.live_since.is_some();
            for (&key, t) in &touched {
                let cell = c.cells.entry(key).or_insert(CellState {
                    last_ts: ts,
                    qty: t.prev,
                    ..CellState::default()
                });
                if live {
                    cell.acc += i128::from(cell.qty) * i128::from(ts.saturating_sub(cell.last_ts));
                }
                cell.last_ts = ts;
                cell.qty = t.new;
                cell.exec += t.exec;
                cell.rpi += t.rpi;
                cell.fills += t.fills;
                cell.trades += t.trades;
                cell.nv += t.nv;
                cell.cm += t.cm;
                cell.am += t.am;
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn wall_step(
        &mut self,
        wi: usize,
        key: Key,
        prev: i64,
        new: i64,
        exec: i64,
        clean: bool,
        ts: u64,
        mid_bucket: Option<i64>,
    ) {
        let c = &self.candles[wi];
        let obs = i128::from(c.observed_at(ts));
        if obs == 0 {
            self.windows.remove(&key);
            return;
        }
        let live = c.live_since.is_some();
        let acc = c
            .cells
            .get(&key)
            .map_or(0, |cell| Candle::acc_at(cell, ts, live));
        // ¿estaba ≥ TWA justo antes del cambio? ⇒ la ventana empieza aquí con la cantidad previa.
        if i128::from(prev) * obs >= acc {
            self.windows.insert(
                key,
                Window {
                    start_ts: ts,
                    start_qty: prev,
                    exec: 0,
                    evaluated: false,
                    poisoned: false,
                },
            );
        }
        if i128::from(new) * obs >= acc {
            self.windows.insert(
                key,
                Window {
                    start_ts: ts,
                    start_qty: new,
                    exec: 0,
                    evaluated: false,
                    poisoned: false,
                },
            );
            return;
        }
        let Some(w) = self.windows.get_mut(&key) else {
            return;
        };
        w.exec += exec;
        w.poisoned |= !clean;
        let x = i128::from(self.cfg.wall.drop_pct);
        let dropped = i128::from(new) * 100 * obs < (100 - x) * acc;
        if !dropped || w.evaluated {
            return;
        }
        w.evaluated = true;
        let w = *w;
        self.wall_stats.evaluated += 1;
        let (side, b) = key;
        if w.poisoned {
            self.wall_stats.rejected_contaminated += 1;
            return;
        }
        if self.is_partial(side, b) {
            self.wall_stats.rejected_coverage += 1;
            return;
        }
        let Some(mb) = mid_bucket else {
            self.wall_stats.rejected_no_mid += 1;
            return;
        };
        let distance = (b - mb).abs();
        // Percentil (rango más cercano) de las áreas del mismo lado dentro de la cobertura.
        let mut vals: Vec<i128> = c
            .cells
            .iter()
            .filter(|((s, bb), _)| *s == side && !self.is_partial(*s, *bb))
            .map(|(_, cell)| Candle::acc_at(cell, ts, live))
            .collect();
        let n = vals.len();
        let rank = (n * self.cfg.wall.percentile as usize).div_ceil(100).max(1);
        vals.sort_unstable();
        let thr = vals.get(rank - 1).copied().unwrap_or(0);
        let drop_vis = w.start_qty - new;
        let twa = Qty::from_raw((acc / obs) as i64);
        if acc < thr {
            self.wall_stats.rejected_percentile += 1;
            return;
        }
        if distance > self.cfg.wall.max_distance {
            self.wall_stats.rejected_distance += 1;
            return;
        }
        if drop_vis <= 0
            || i128::from(w.exec) * 100
                >= i128::from(self.cfg.wall.max_exec_pct) * i128::from(drop_vis)
        {
            self.wall_stats.rejected_exec += 1;
            return;
        }
        self.wall_stats.fired += 1;
        let ev = WallRetired {
            side,
            bucket: b,
            px_low: Px::from_raw(b * self.cfg.bucket.raw()),
            ts_ms: ts,
            window_start_ms: w.start_ts,
            twa,
            percentile_twa: Qty::from_raw((thr / obs) as i64),
            start_qty: Qty::from_raw(w.start_qty),
            qty: Qty::from_raw(new),
            exec: Qty::from_raw(w.exec),
            exec_ratio: w.exec as f64 / drop_vis as f64,
            distance,
        };
        if self.walls.len() == self.cfg.keep_walls {
            self.walls.pop_front();
        }
        self.walls.push_back(ev);
    }

    /// TWA por celda de una vela cerrada, y umbral de percentil por lado.
    fn reference(&self, start: u64) -> Option<(BTreeMap<Key, i64>, [i64; 2])> {
        let tc = &self.cfg.tactical;
        let li = self
            .cfg
            .timeframes_ms
            .iter()
            .position(|&t| t == tc.long_tf_ms)?;
        let c = self.closed[li].iter().find(|c| c.start == start)?;
        if c.observed == 0 {
            return None;
        }
        let obs = i128::from(c.observed);
        let twa: BTreeMap<Key, i64> = c
            .cells
            .iter()
            .map(|(k, cell)| (*k, (cell.acc / obs) as i64))
            .collect();
        let mut thr = [i64::MAX; 2];
        for (i, side) in [Side::Bid, Side::Ask].into_iter().enumerate() {
            let mut v: Vec<i64> = twa
                .iter()
                .filter(|((s, b), _)| *s == side && !self.is_partial(*s, *b))
                .map(|(_, t)| *t)
                .collect();
            if v.is_empty() {
                continue;
            }
            v.sort_unstable();
            let rank = (v.len() * tc.high_percentile as usize).div_ceil(100).max(1);
            thr[i] = v[rank - 1];
        }
        Some((twa, thr))
    }

    fn classify(&self, side: Side, twa: i64, ref_twa: i64, thr: &[i64; 2]) -> Option<Liquidity> {
        let tc = &self.cfg.tactical;
        if ref_twa <= 0 {
            return (twa > 0).then_some(Liquidity::Tactica);
        }
        let (t, r) = (i128::from(twa) * 100, i128::from(ref_twa));
        Some(if t > i128::from(tc.approx_high_pct) * r {
            Liquidity::Tactica
        } else if t < i128::from(tc.approx_low_pct) * r {
            Liquidity::Desarmandose
        } else if ref_twa >= thr[si(side)] {
            Liquidity::Estructural
        } else {
            Liquidity::Estable
        })
    }

    fn candle_view(&self, c: &Candle, at: u64) -> CandleView {
        let live = c.live_since.is_some();
        let obs = c.observed_at(at);
        let tc = &self.cfg.tactical;
        let ref_start = (c.tf == tc.short_tf_ms)
            .then(|| (c.start / tc.long_tf_ms * tc.long_tf_ms).checked_sub(tc.long_tf_ms))
            .flatten();
        let reference = ref_start.and_then(|s| self.reference(s));
        let (mut buy, mut sell) = (0i64, 0i64);
        let cells = c
            .cells
            .iter()
            .map(|(&(side, b), cell)| {
                let acc = Candle::acc_at(cell, at, live);
                let twa = if obs > 0 {
                    (acc / i128::from(obs)) as i64
                } else {
                    0
                };
                match side {
                    Side::Bid => sell += cell.exec,
                    Side::Ask => buy += cell.exec,
                }
                CellView {
                    side,
                    bucket: b,
                    px_low: Px::from_raw(b * self.cfg.bucket.raw()),
                    twa: Qty::from_raw(twa),
                    qty: Qty::from_raw(cell.qty),
                    persistence: (twa > 0).then(|| cell.qty as f64 / twa as f64),
                    exec: Qty::from_raw(cell.exec),
                    exec_rpi: Qty::from_raw(cell.rpi),
                    n_fills: cell.fills,
                    n_trades: cell.trades,
                    no_visible_min: Qty::from_raw(cell.nv),
                    cancelado_min: Qty::from_raw(cell.cm),
                    agregado_min: Qty::from_raw(cell.am),
                    partial: self.is_partial(side, b),
                    twa_ref: reference
                        .as_ref()
                        .map(|(m, _)| Qty::from_raw(m.get(&(side, b)).copied().unwrap_or(0))),
                    ratio: reference.as_ref().and_then(|(m, _)| {
                        let r = m.get(&(side, b)).copied().unwrap_or(0);
                        (r > 0).then(|| twa as f64 / r as f64)
                    }),
                    liquidity: reference.as_ref().and_then(|(m, thr)| {
                        self.classify(side, twa, m.get(&(side, b)).copied().unwrap_or(0), thr)
                    }),
                }
            })
            .collect();
        CandleView {
            tf_ms: c.tf,
            start_ms: c.start,
            end_ms: c.end,
            observed_ms: obs,
            clean: c.clean,
            buy_exec: Qty::from_raw(buy),
            sell_exec: Qty::from_raw(sell),
            delta: Qty::from_raw(buy - sell),
            ref_start_ms: reference.as_ref().and(ref_start),
            cells,
        }
    }

    /// Vista completa (velas en curso evaluadas en el último instante procesado).
    pub fn view(&self, closed_per_tf: usize) -> MetricsView {
        MetricsView {
            now_ms: self.now,
            current: self
                .candles
                .iter()
                .map(|c| self.candle_view(c, self.now))
                .collect(),
            closed: self
                .closed
                .iter()
                .map(|q| {
                    q.iter()
                        .rev()
                        .take(closed_per_tf)
                        .rev()
                        .map(|c| self.candle_view(c, c.end))
                        .collect()
                })
                .collect(),
            walls: self.walls.iter().copied().collect(),
            wall_stats: self.wall_stats.clone(),
            bid_floor_bucket: self.floor_bucket,
            ask_ceiling_bucket: self.ceiling_bucket,
        }
    }
}

impl FlowSink for Metrics {
    fn on_level_flow(&mut self, f: &LevelFlow) {
        self.batch.push(*f);
    }

    fn on_batch(&mut self, b: &BatchInfo) {
        self.process_batch(b);
    }

    fn on_rebuild(&mut self, _epoch: u64, book: &L2Book) {
        self.levels = [
            book.bids().iter().map(|(p, q)| (*p, q.raw())).collect(),
            book.asks().iter().map(|(p, q)| (*p, q.raw())).collect(),
        ];
        let cov = book.coverage();
        self.floor_bucket = cov.bid_floor.map(|p| p.bucket(self.cfg.bucket));
        self.ceiling_bucket = cov.ask_ceiling.map(|p| p.bucket(self.cfg.bucket));
        self.live = true;
        self.resume_pending = true;
        self.batch.clear();
    }

    fn on_invalidate(&mut self, _epoch: u64, _reason: ResyncReason) {
        if self.live {
            let now = self.now;
            self.pause(now);
        }
        self.live = false;
        self.resume_pending = false;
        self.batch.clear();
    }
}

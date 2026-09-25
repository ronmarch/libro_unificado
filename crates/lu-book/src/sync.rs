//! Máquina de sincronización snapshot + diff con arbitraje de líneas A/B.
//!
//! Propiedades garantizadas (verificadas con proptest en `tests/`):
//! 1. **Nunca silenciosamente incorrecto**: si la fase es `Live`, el libro es
//!    idéntico al estado real del exchange en `last_update_id`.
//! 2. **Arbitraje de líneas**: con dos o más conexiones redundantes, un evento
//!    perdido en una línea se recupera de la otra sin resync (el primero que
//!    llega gana; los duplicados se descartan por id).
//! 3. **Determinismo**: sin relojes internos ni E/S; el tiempo se inyecta.
//!    Mismo input ⇒ mismo output (replay exacto en tests).

use crate::book::L2Book;
use crate::observer::BookObserver;
use crate::seq::{Class, SeqRule};
use lu_core::{DepthDiff, DepthSnapshot, Side};
use serde::Serialize;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;

/// Fase del libro.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Phase {
    /// Sin estado válido: acumulando diffs y esperando un snapshot que los puentee.
    Syncing,
    /// Libro válido y encadenado.
    Live,
}

/// Motivo de resincronización.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResyncReason {
    /// Un diff faltante no llegó por ninguna línea dentro de la ventana.
    GapTimeout,
    /// Demasiados diffs adelantados retenidos.
    PendingOverflow,
    /// Secuencia imposible (p. ej. `pu` inconsistente).
    InvalidSequence,
    /// Libro cruzado tras aplicar un diff.
    Crossed,
    /// Solicitud externa (operación manual).
    Manual,
}

impl ResyncReason {
    /// Etiqueta estable.
    pub const fn as_str(self) -> &'static str {
        match self {
            ResyncReason::GapTimeout => "gap_timeout",
            ResyncReason::PendingOverflow => "pending_overflow",
            ResyncReason::InvalidSequence => "invalid_sequence",
            ResyncReason::Crossed => "crossed",
            ResyncReason::Manual => "manual",
        }
    }
}

/// Política ante libro cruzado.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrossPolicy {
    /// Resincronizar (por defecto: un libro cruzado en un venue es un libro corrupto).
    Resync,
    /// Solo contar (diagnóstico).
    Tolerate,
}

/// Configuración.
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// Espera máxima por un diff faltante antes de declarar hueco (ms).
    pub gap_timeout_ms: u64,
    /// Máximo de diffs adelantados retenidos.
    pub max_pending: usize,
    /// Máximo de diffs en buffer mientras se sincroniza.
    pub max_buffer: usize,
    /// Si un snapshot solicitado no llega en este plazo, se vuelve a solicitar (ms).
    pub snapshot_retry_ms: u64,
    /// Política ante libro cruzado.
    pub cross_policy: CrossPolicy,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            gap_timeout_ms: 750,
            max_pending: 256,
            max_buffer: 20_000,
            snapshot_retry_ms: 10_000,
            cross_policy: CrossPolicy::Resync,
        }
    }
}

/// Número máximo de líneas redundantes con estadísticas propias.
pub const MAX_LINES: usize = 4;

/// Contadores de la máquina (monótonos; se exportan a Prometheus).
#[derive(Debug, Clone, Default, Serialize)]
pub struct SyncStats {
    /// Diffs recibidos (todas las líneas).
    pub diffs_rx: u64,
    /// Diffs aplicados.
    pub applied: u64,
    /// Diffs descartados por ya contenidos (duplicados A/B o previos al snapshot).
    pub stale: u64,
    /// Diffs adelantados retenidos.
    pub ahead: u64,
    /// Diffs adelantados que luego se aplicaron (reordenamiento recuperado).
    pub reordered_applied: u64,
    /// Snapshots solicitados.
    pub snapshots_requested: u64,
    /// Snapshots recibidos.
    pub snapshots_rx: u64,
    /// Snapshots descartados por no poder puentear el buffer.
    pub snapshots_unbridgeable: u64,
    /// Snapshots ignorados por llegar con el libro ya en vivo.
    pub snapshots_ignored: u64,
    /// Resyncs por hueco.
    pub resync_gap_timeout: u64,
    /// Resyncs por desborde de pendientes.
    pub resync_pending_overflow: u64,
    /// Resyncs por secuencia inválida.
    pub resync_invalid_sequence: u64,
    /// Resyncs por libro cruzado.
    pub resync_crossed: u64,
    /// Resyncs manuales.
    pub resync_manual: u64,
    /// Veces que se observó libro cruzado.
    pub crossed_seen: u64,
    /// Diffs descartados por desborde del buffer de sincronización.
    pub buffer_dropped: u64,
    /// Diffs anteriores al snapshot descartados al puentear (regla oficial, no es pérdida).
    pub pre_snapshot: u64,
    /// Diffs con secuencia imposible descartados (disparan resync).
    pub invalid_dropped: u64,
    /// Máximo de pendientes observado.
    pub max_pending_seen: u64,
    /// Diffs aplicados por línea (cuál llegó primero: calidad relativa de cada línea).
    pub applied_by_line: [u64; MAX_LINES],
    /// Duplicados por línea.
    pub stale_by_line: [u64; MAX_LINES],
}

impl SyncStats {
    /// Total de resyncs.
    pub fn resyncs(&self) -> u64 {
        self.resync_gap_timeout
            + self.resync_pending_overflow
            + self.resync_invalid_sequence
            + self.resync_crossed
            + self.resync_manual
    }
}

/// Resultado de un paso de la máquina (acciones que el conductor de E/S debe ejecutar).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Step {
    /// Diffs aplicados en este paso.
    pub applied: u32,
    /// El conductor debe pedir un snapshot REST.
    pub need_snapshot: bool,
    /// El libro pasó a `Live` en este paso.
    pub went_live: bool,
    /// Hubo resync en este paso.
    pub resync: Option<ResyncReason>,
}

type Key = (u64, u64);

/// Libro sincronizado genérico sobre la regla de secuencia `R` y el observador `O`.
pub struct SyncBook<R: SeqRule, O: BookObserver = ()> {
    rule: R,
    cfg: SyncConfig,
    obs: O,
    phase: Phase,
    book: L2Book,
    last: u64,
    epoch: u64,
    buffer: BTreeMap<Key, DepthDiff>,
    pending: BTreeMap<Key, (DepthDiff, u64)>,
    held: Option<DepthSnapshot>,
    snapshot_req_at: Option<u64>,
    last_exch_ts_ms: u64,
    stats: SyncStats,
}

impl<R: SeqRule, O: BookObserver> SyncBook<R, O> {
    /// Nuevo libro en fase `Syncing`.
    pub fn new(rule: R, cfg: SyncConfig, obs: O) -> Self {
        Self {
            rule,
            cfg,
            obs,
            phase: Phase::Syncing,
            book: L2Book::new(),
            last: 0,
            epoch: 0,
            buffer: BTreeMap::new(),
            pending: BTreeMap::new(),
            held: None,
            snapshot_req_at: None,
            last_exch_ts_ms: 0,
            stats: SyncStats::default(),
        }
    }

    /// Fase actual.
    pub fn phase(&self) -> Phase {
        self.phase
    }
    /// Libro (solo es confiable si `phase() == Live`).
    pub fn book(&self) -> &L2Book {
        &self.book
    }
    /// Último update id aplicado.
    pub fn last_update_id(&self) -> u64 {
        self.last
    }
    /// Época: se incrementa en cada reconstrucción desde snapshot.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
    /// Tiempo de exchange del último diff aplicado.
    pub fn last_exch_ts_ms(&self) -> u64 {
        self.last_exch_ts_ms
    }
    /// Contadores.
    pub fn stats(&self) -> &SyncStats {
        &self.stats
    }
    /// Ley de conservación de mensajes: todo diff recibido queda contabilizado
    /// exactamente una vez (aplicado, duplicado, previo al snapshot, desbordado,
    /// inválido, o aún retenido). Debe ser SIEMPRE 0; distinto de 0 = defecto.
    pub fn unaccounted(&self) -> i64 {
        let s = &self.stats;
        let terminal = s.applied + s.stale + s.pre_snapshot + s.buffer_dropped + s.invalid_dropped;
        s.diffs_rx as i64 - terminal as i64 - self.buffer.len() as i64 - self.pending.len() as i64
    }

    /// Diffs retenidos esperando un faltante.
    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }
    /// Diffs en buffer de sincronización.
    pub fn buffer_len(&self) -> usize {
        self.buffer.len()
    }
    /// Observador.
    pub fn observer(&self) -> &O {
        &self.obs
    }
    /// Observador (mutable).
    pub fn observer_mut(&mut self) -> &mut O {
        &mut self.obs
    }

    /// Entrada: diff recibido por cualquier línea.
    pub fn on_diff(&mut self, d: DepthDiff, now_ms: u64) -> Step {
        let mut st = Step::default();
        self.stats.diffs_rx += 1;
        match self.phase {
            Phase::Syncing => {
                let key = (d.first_id, d.last_id);
                let line = d.rx.line;
                let inserted = match self.buffer.entry(key) {
                    Entry::Vacant(v) => {
                        v.insert(d);
                        true
                    }
                    Entry::Occupied(_) => false,
                };
                if inserted {
                    while self.buffer.len() > self.cfg.max_buffer {
                        self.buffer.pop_first();
                        self.stats.buffer_dropped += 1;
                    }
                } else {
                    self.count_stale(line);
                }
                if self.held.is_some() {
                    self.try_install(now_ms, &mut st);
                } else if self.snapshot_req_at.is_none() {
                    self.request_snapshot(now_ms, &mut st);
                }
            }
            Phase::Live => self.on_live_diff(d, now_ms, &mut st),
        }
        st
    }

    /// Entrada: snapshot REST.
    pub fn on_snapshot(&mut self, s: DepthSnapshot, now_ms: u64) -> Step {
        let mut st = Step::default();
        self.stats.snapshots_rx += 1;
        self.snapshot_req_at = None;
        if self.phase == Phase::Live {
            self.stats.snapshots_ignored += 1;
            return st;
        }
        self.held = Some(s);
        self.try_install(now_ms, &mut st);
        st
    }

    /// Entrada: pulso de reloj (timeouts de hueco y de snapshot).
    pub fn on_tick(&mut self, now_ms: u64) -> Step {
        let mut st = Step::default();
        match self.phase {
            Phase::Live => {
                if let Some(oldest) = self.pending.values().map(|(_, t)| *t).min() {
                    if now_ms.saturating_sub(oldest) >= self.cfg.gap_timeout_ms {
                        self.resync(ResyncReason::GapTimeout, now_ms, &mut st);
                    }
                }
            }
            Phase::Syncing => {
                if let Some(t) = self.snapshot_req_at {
                    if now_ms.saturating_sub(t) >= self.cfg.snapshot_retry_ms {
                        self.request_snapshot(now_ms, &mut st);
                    }
                }
            }
        }
        st
    }

    /// Fuerza resync (operación manual).
    pub fn force_resync(&mut self, now_ms: u64) -> Step {
        let mut st = Step::default();
        self.resync(ResyncReason::Manual, now_ms, &mut st);
        st
    }

    // ---------------------------------------------------------------- interno

    fn on_live_diff(&mut self, d: DepthDiff, now_ms: u64, st: &mut Step) {
        match self
            .rule
            .chain(d.first_id, d.last_id, d.prev_last_id, self.last)
        {
            Class::Stale => self.count_stale(d.rx.line),
            Class::Next => {
                if self.apply(&d, now_ms, st) {
                    self.drain_pending(now_ms, st);
                }
            }
            Class::Ahead => {
                let key = (d.first_id, d.last_id);
                let line = d.rx.line;
                match self.pending.entry(key) {
                    Entry::Occupied(_) => {
                        self.count_stale(line);
                        return;
                    }
                    Entry::Vacant(v) => {
                        v.insert((d, now_ms));
                    }
                }
                self.stats.ahead += 1;
                self.stats.max_pending_seen =
                    self.stats.max_pending_seen.max(self.pending.len() as u64);
                if self.pending.len() > self.cfg.max_pending {
                    self.resync(ResyncReason::PendingOverflow, now_ms, st);
                }
            }
            Class::Invalid => {
                self.stats.invalid_dropped += 1;
                self.resync(ResyncReason::InvalidSequence, now_ms, st);
            }
        }
    }

    fn drain_pending(&mut self, now_ms: u64, st: &mut Step) {
        while let Some((&key, (d, _))) = self.pending.first_key_value() {
            let line = d.rx.line;
            match self.rule.chain(key.0, key.1, d.prev_last_id, self.last) {
                Class::Stale => {
                    self.pending.remove(&key);
                    self.count_stale(line);
                }
                Class::Next => {
                    let Some((d, _)) = self.pending.remove(&key) else {
                        break;
                    };
                    self.stats.reordered_applied += 1;
                    if !self.apply(&d, now_ms, st) {
                        return;
                    }
                }
                Class::Ahead => break,
                Class::Invalid => {
                    self.resync(ResyncReason::InvalidSequence, now_ms, st);
                    return;
                }
            }
        }
    }

    /// Aplica un diff. Devuelve `false` si provocó resync.
    fn apply(&mut self, d: &DepthDiff, now_ms: u64, st: &mut Step) -> bool {
        let ts = d.exch_ts_ms;
        for l in &d.bids {
            let prev = self.book.set(Side::Bid, l.px, l.qty);
            self.obs.on_level(Side::Bid, l.px, prev, l.qty, ts);
        }
        for l in &d.asks {
            let prev = self.book.set(Side::Ask, l.px, l.qty);
            self.obs.on_level(Side::Ask, l.px, prev, l.qty, ts);
        }
        self.last = d.last_id;
        self.last_exch_ts_ms = ts;
        self.stats.applied += 1;
        if let Some(c) = self.stats.applied_by_line.get_mut(usize::from(d.rx.line)) {
            *c += 1;
        }
        st.applied += 1;
        self.obs.on_diff_applied(d.first_id, d.last_id, ts);
        if self.book.is_crossed() {
            self.stats.crossed_seen += 1;
            if self.cfg.cross_policy == CrossPolicy::Resync {
                self.resync(ResyncReason::Crossed, now_ms, st);
                return false;
            }
        }
        true
    }

    fn try_install(&mut self, now_ms: u64, st: &mut Step) {
        let Some(snap_id) = self.held.as_ref().map(|s| s.last_update_id) else {
            return;
        };
        let Some((&(first_buffered, _), _)) = self.buffer.first_key_value() else {
            return; // sin diffs aún: conservar el snapshot y esperar
        };
        if self.rule.snapshot_too_old(first_buffered, snap_id) {
            self.held = None;
            self.stats.snapshots_unbridgeable += 1;
            self.request_snapshot(now_ms, st);
            return;
        }
        // Buscar el primer diff que puentea el snapshot; descartar los anteriores.
        let bridge_key = loop {
            let Some((&key, d)) = self.buffer.first_key_value() else {
                return; // snapshot más nuevo que todo el buffer: esperar diffs posteriores
            };
            match self.rule.bridge(key.0, key.1, d.prev_last_id, snap_id) {
                Class::Stale => {
                    self.buffer.remove(&key);
                    self.stats.pre_snapshot += 1;
                }
                Class::Next => break key,
                Class::Ahead | Class::Invalid => {
                    // Hueco entre snapshot y buffer: un snapshot más nuevo lo resuelve.
                    self.held = None;
                    self.stats.snapshots_unbridgeable += 1;
                    self.request_snapshot(now_ms, st);
                    return;
                }
            }
        };
        let Some(snap) = self.held.take() else { return };
        self.book.load_snapshot(&snap);
        self.last = snap_id;
        self.epoch += 1;
        self.phase = Phase::Live;
        self.pending.clear();
        self.obs.on_rebuild(self.epoch, &self.book);
        st.went_live = true;
        let Some(first) = self.buffer.remove(&bridge_key) else {
            return;
        };
        if !self.apply(&first, now_ms, st) {
            return;
        }
        // Resto del buffer por la cadena normal (un hueco aquí aún puede llenarlo otra línea).
        let rest = std::mem::take(&mut self.buffer);
        for (key, d) in rest {
            if self.phase == Phase::Live {
                self.on_live_diff(d, now_ms, st);
            } else {
                self.buffer.insert(key, d);
            }
        }
    }

    fn resync(&mut self, reason: ResyncReason, now_ms: u64, st: &mut Step) {
        match reason {
            ResyncReason::GapTimeout => self.stats.resync_gap_timeout += 1,
            ResyncReason::PendingOverflow => self.stats.resync_pending_overflow += 1,
            ResyncReason::InvalidSequence => self.stats.resync_invalid_sequence += 1,
            ResyncReason::Crossed => self.stats.resync_crossed += 1,
            ResyncReason::Manual => self.stats.resync_manual += 1,
        }
        st.resync = Some(reason);
        self.obs.on_invalidate(self.epoch, reason);
        self.phase = Phase::Syncing;
        self.book.clear();
        self.last = 0;
        self.held = None;
        // Los pendientes son posteriores al hueco: sirven para puentear el próximo snapshot.
        for (key, (d, _)) in std::mem::take(&mut self.pending) {
            self.buffer.insert(key, d);
        }
        self.request_snapshot(now_ms, st);
    }

    fn request_snapshot(&mut self, now_ms: u64, st: &mut Step) {
        self.snapshot_req_at = Some(now_ms);
        self.stats.snapshots_requested += 1;
        st.need_snapshot = true;
    }

    fn count_stale(&mut self, line: u8) {
        self.stats.stale += 1;
        if let Some(c) = self.stats.stale_by_line.get_mut(usize::from(line)) {
            *c += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::seq::{BinanceFuturesRule, BinanceSpotRule};
    use lu_core::{Level, Px, Qty, RxStamp};

    fn lv(p: i64, q: i64) -> Level {
        Level {
            px: Px::from_units(p),
            qty: Qty::from_units(q),
        }
    }

    fn diff(
        first: u64,
        last: u64,
        pu: Option<u64>,
        bids: &[(i64, i64)],
        asks: &[(i64, i64)],
        line: u8,
    ) -> DepthDiff {
        DepthDiff {
            first_id: first,
            last_id: last,
            prev_last_id: pu,
            exch_ts_ms: last,
            match_ts_ms: None,
            bids: bids.iter().map(|&(p, q)| lv(p, q)).collect(),
            asks: asks.iter().map(|&(p, q)| lv(p, q)).collect(),
            rx: RxStamp {
                line,
                ..RxStamp::default()
            },
        }
    }

    fn snap(id: u64, bids: &[(i64, i64)], asks: &[(i64, i64)]) -> DepthSnapshot {
        DepthSnapshot {
            last_update_id: id,
            limit: 1000,
            bids: bids.iter().map(|&(p, q)| lv(p, q)).collect(),
            asks: asks.iter().map(|&(p, q)| lv(p, q)).collect(),
            exch_ts_ms: None,
            rx: RxStamp::default(),
        }
    }

    fn spot() -> SyncBook<BinanceSpotRule> {
        SyncBook::new(BinanceSpotRule, SyncConfig::default(), ())
    }

    #[test]
    fn spot_flujo_nominal() {
        let mut s = spot();
        let st = s.on_diff(diff(101, 103, None, &[(99, 5)], &[], 0), 0);
        assert!(st.need_snapshot);
        s.on_diff(diff(104, 106, None, &[], &[(101, 7)], 0), 10);
        let st = s.on_snapshot(snap(102, &[(99, 1), (98, 1)], &[(101, 1)]), 20);
        assert!(st.went_live);
        assert_eq!(st.applied, 2);
        assert_eq!(s.phase(), Phase::Live);
        assert_eq!(s.last_update_id(), 106);
        assert_eq!(
            s.book().qty_at(Side::Bid, Px::from_units(99)),
            Qty::from_units(5)
        );
        assert_eq!(
            s.book().qty_at(Side::Ask, Px::from_units(101)),
            Qty::from_units(7)
        );
    }

    #[test]
    fn futuros_puente_con_u_igual_a_snapshot() {
        let mut s: SyncBook<BinanceFuturesRule> =
            SyncBook::new(BinanceFuturesRule, SyncConfig::default(), ());
        s.on_diff(diff(90, 100, Some(80), &[(99, 2)], &[], 0), 0);
        s.on_diff(diff(107, 115, Some(100), &[(99, 3)], &[], 0), 5);
        let st = s.on_snapshot(snap(100, &[(99, 2)], &[(101, 1)]), 10);
        assert!(st.went_live);
        assert_eq!(s.last_update_id(), 115);
        assert_eq!(
            s.book().qty_at(Side::Bid, Px::from_units(99)),
            Qty::from_units(3)
        );
    }

    #[test]
    fn arbitraje_ab_descarta_duplicados_sin_resync() {
        let mut s = spot();
        s.on_diff(diff(101, 101, None, &[], &[], 0), 0);
        s.on_snapshot(snap(100, &[(99, 1)], &[(101, 1)]), 1);
        assert_eq!(s.phase(), Phase::Live);
        s.on_diff(diff(101, 101, None, &[], &[], 1), 2); // línea B tardía
        s.on_diff(diff(102, 102, None, &[(99, 4)], &[], 1), 3); // B gana
        s.on_diff(diff(102, 102, None, &[(99, 4)], &[], 0), 4); // A duplicado
        assert_eq!(s.stats().stale, 2);
        assert_eq!(s.stats().applied_by_line[1], 1);
        assert_eq!(s.stats().resyncs(), 0);
    }

    #[test]
    fn hueco_llenado_por_la_otra_linea_dentro_de_ventana() {
        let mut s = spot();
        s.on_diff(diff(101, 101, None, &[], &[], 0), 0);
        s.on_snapshot(snap(100, &[(99, 1)], &[(101, 1)]), 1);
        s.on_diff(diff(103, 103, None, &[(99, 9)], &[], 0), 10); // A perdió 102
        assert_eq!(s.pending_len(), 1);
        s.on_tick(500); // dentro de la ventana
        assert_eq!(s.phase(), Phase::Live);
        s.on_diff(diff(102, 102, None, &[(98, 2)], &[], 1), 600); // B lo trae
        assert_eq!(s.pending_len(), 0);
        assert_eq!(s.last_update_id(), 103);
        assert_eq!(s.stats().reordered_applied, 1);
        assert_eq!(s.stats().resyncs(), 0);
    }

    #[test]
    fn hueco_real_resync_y_recuperacion() {
        let mut s = spot();
        s.on_diff(diff(101, 101, None, &[], &[], 0), 0);
        s.on_snapshot(snap(100, &[(99, 1)], &[(101, 1)]), 1);
        s.on_diff(diff(103, 103, None, &[(99, 9)], &[], 0), 10);
        let st = s.on_tick(10 + SyncConfig::default().gap_timeout_ms);
        assert_eq!(st.resync, Some(ResyncReason::GapTimeout));
        assert!(st.need_snapshot);
        assert_eq!(s.phase(), Phase::Syncing);
        // el pendiente (103) se conserva en buffer y puentea el nuevo snapshot
        let st = s.on_snapshot(snap(102, &[(99, 1)], &[(101, 1)]), 900);
        assert!(st.went_live);
        assert_eq!(s.last_update_id(), 103);
        assert_eq!(s.epoch(), 2);
    }

    #[test]
    fn snapshot_viejo_se_vuelve_a_pedir() {
        let mut s = spot();
        s.on_diff(diff(200, 201, None, &[], &[], 0), 0);
        let st = s.on_snapshot(snap(150, &[(99, 1)], &[(101, 1)]), 5);
        assert!(st.need_snapshot);
        assert_eq!(s.stats().snapshots_unbridgeable, 1);
        assert_eq!(s.phase(), Phase::Syncing);
    }

    #[test]
    fn snapshot_antes_que_cualquier_diff_se_retiene() {
        let mut s = spot();
        s.on_snapshot(snap(100, &[(99, 1)], &[(101, 1)]), 0);
        assert_eq!(s.phase(), Phase::Syncing);
        let st = s.on_diff(diff(101, 102, None, &[], &[], 0), 1);
        assert!(st.went_live);
    }

    #[test]
    fn libro_cruzado_provoca_resync() {
        let mut s = spot();
        s.on_diff(diff(101, 101, None, &[], &[], 0), 0);
        s.on_snapshot(snap(100, &[(99, 1)], &[(101, 1)]), 1);
        let st = s.on_diff(diff(102, 102, None, &[(101, 1)], &[], 0), 2);
        assert_eq!(st.resync, Some(ResyncReason::Crossed));
        assert_eq!(s.phase(), Phase::Syncing);
    }

    #[test]
    fn futuros_pu_inconsistente_es_invalido() {
        let mut s: SyncBook<BinanceFuturesRule> =
            SyncBook::new(BinanceFuturesRule, SyncConfig::default(), ());
        s.on_diff(diff(95, 105, Some(90), &[], &[], 0), 0);
        s.on_snapshot(snap(100, &[(99, 1)], &[(101, 1)]), 1);
        assert_eq!(s.phase(), Phase::Live);
        let st = s.on_diff(diff(100, 120, Some(100), &[], &[], 0), 2);
        assert_eq!(st.resync, Some(ResyncReason::InvalidSequence));
    }

    #[test]
    fn retry_de_snapshot_por_timeout() {
        let mut s = spot();
        let st = s.on_diff(diff(1, 1, None, &[], &[], 0), 0);
        assert!(st.need_snapshot);
        assert!(!s.on_tick(5_000).need_snapshot);
        assert!(s.on_tick(10_000).need_snapshot);
    }
}

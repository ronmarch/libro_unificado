//! Eventos normalizados. Todo conector de venue produce exactamente estos tipos;
//! el motor nunca ve JSON ni particularidades del exchange.

use crate::fixed::{Px, Qty};
use serde::Serialize;
use std::sync::OnceLock;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Lado del libro.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    /// Compra (bid).
    Bid,
    /// Venta (ask).
    Ask,
}

/// Lado del agresor de un trade.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Aggressor {
    /// Compra agresiva: consume asks.
    Buy,
    /// Venta agresiva: consume bids.
    Sell,
}

/// Nivel de precio con cantidad ABSOLUTA (0 = eliminar nivel).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Level {
    /// Precio.
    pub px: Px,
    /// Cantidad absoluta en el nivel.
    pub qty: Qty,
}

/// Sello de recepción: qué línea (A/B) lo entregó y cuándo (reloj de pared y monotónico).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RxStamp {
    /// Línea redundante de origen (0 = A, 1 = B, ...).
    pub line: u8,
    /// Reloj de pared en ns (para latencia vs. tiempo del exchange; requiere chrony).
    pub wall_ns: u64,
    /// Reloj monotónico en ns desde el arranque del proceso (para timeouts deterministas).
    pub mono_ns: u64,
}

impl RxStamp {
    /// Sella ahora para la línea indicada.
    #[inline]
    pub fn now(line: u8) -> Self {
        Self {
            line,
            wall_ns: wall_now_ns(),
            mono_ns: mono_now_ns(),
        }
    }
}

/// Diff incremental de profundidad (cantidades absolutas por nivel).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DepthDiff {
    /// Primer update id del evento (`U`).
    pub first_id: u64,
    /// Último update id del evento (`u`).
    pub last_id: u64,
    /// Último update id del evento previo (`pu`), si el venue lo provee (futuros Binance).
    pub prev_last_id: Option<u64>,
    /// Tiempo del evento en el exchange, ms (`E`).
    pub exch_ts_ms: u64,
    /// Tiempo del motor de matching, ms (`T`), si existe.
    pub match_ts_ms: Option<u64>,
    /// Bids modificados.
    pub bids: Vec<Level>,
    /// Asks modificados.
    pub asks: Vec<Level>,
    /// Recepción.
    pub rx: RxStamp,
}

/// Snapshot REST del libro.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DepthSnapshot {
    /// `lastUpdateId`.
    pub last_update_id: u64,
    /// Límite de niveles solicitado (define la cobertura: si un lado trae `limit` niveles, está truncado).
    pub limit: usize,
    /// Bids.
    pub bids: Vec<Level>,
    /// Asks.
    pub asks: Vec<Level>,
    /// Tiempo del exchange si viene (`E` en futuros).
    pub exch_ts_ms: Option<u64>,
    /// Recepción.
    pub rx: RxStamp,
}

/// Trade agregado (un taker contra uno o más makers al mismo precio).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggTrade {
    /// Id del trade agregado (`a`).
    pub agg_id: u64,
    /// Primer trade id (`f`).
    pub first_trade_id: u64,
    /// Último trade id (`l`).
    pub last_trade_id: u64,
    /// Precio.
    pub px: Px,
    /// Cantidad total (`q`).
    pub qty: Qty,
    /// Cantidad solo de trades normales, sin órdenes RPI (`nq`, futuros Binance desde 2025-12-31).
    pub qty_normal: Option<Qty>,
    /// Lado agresor (`m == true` ⇒ el comprador era maker ⇒ venta agresiva).
    pub aggressor: Aggressor,
    /// Tiempo del trade, ms (`T`).
    pub trade_ts_ms: u64,
    /// Tiempo del evento, ms (`E`).
    pub exch_ts_ms: u64,
    /// Recepción.
    pub rx: RxStamp,
}

impl AggTrade {
    /// Número de fills reales contenidos (`l − f + 1`).
    #[inline]
    pub fn n_fills(&self) -> u64 {
        self.last_trade_id.saturating_sub(self.first_trade_id) + 1
    }
    /// Cantidad ejecutada contra órdenes RPI (`q − nq`), si el venue la informa.
    #[inline]
    pub fn qty_rpi(&self) -> Option<Qty> {
        self.qty_normal.map(|n| self.qty - n)
    }
}

/// Evento de mercado normalizado que produce cualquier conector.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarketEvent {
    /// Diff de profundidad.
    Depth(DepthDiff),
    /// Trade agregado.
    Trade(AggTrade),
    /// Mensaje válido que este sistema no consume (p. ej. otros streams).
    Ignored,
}

fn process_start() -> &'static Instant {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now)
}

/// Reloj monotónico en ns desde el arranque del proceso.
#[inline]
pub fn mono_now_ns() -> u64 {
    u64::try_from(process_start().elapsed().as_nanos()).unwrap_or(u64::MAX)
}

/// Reloj monotónico en ms desde el arranque del proceso.
#[inline]
pub fn mono_now_ms() -> u64 {
    mono_now_ns() / 1_000_000
}

/// Reloj de pared en ns (UNIX).
#[inline]
pub fn wall_now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// Fuerza la inicialización del reloj monotónico (llamar al inicio de `main`).
pub fn init_clock() {
    let _ = process_start();
}

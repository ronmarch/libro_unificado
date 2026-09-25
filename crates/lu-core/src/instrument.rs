//! Identidad de mercados e instrumentos (multi-venue desde el día uno).

use crate::fixed::{Px, Qty};
use core::fmt;
use serde::Serialize;

/// Exchange de origen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Venue {
    /// Binance
    Binance,
    /// Bybit
    Bybit,
    /// OKX
    Okx,
    /// Coinbase
    Coinbase,
    /// Kraken
    Kraken,
}

impl Venue {
    /// Etiqueta estable para métricas y logs.
    pub const fn as_str(self) -> &'static str {
        match self {
            Venue::Binance => "binance",
            Venue::Bybit => "bybit",
            Venue::Okx => "okx",
            Venue::Coinbase => "coinbase",
            Venue::Kraken => "kraken",
        }
    }
}

/// Tipo de mercado.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum MarketKind {
    /// Contado.
    Spot,
    /// Perpetuo lineal.
    Perp,
}

impl MarketKind {
    /// Etiqueta estable.
    pub const fn as_str(self) -> &'static str {
        match self {
            MarketKind::Spot => "spot",
            MarketKind::Perp => "perp",
        }
    }
}

/// Identificador canónico de un mercado: `binance.spot.SOLUSDT`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
pub struct MarketId {
    /// Exchange.
    pub venue: Venue,
    /// Spot o perp.
    pub kind: MarketKind,
    /// Símbolo nativo del venue.
    pub symbol: String,
}

impl MarketId {
    /// Constructor.
    pub fn new(venue: Venue, kind: MarketKind, symbol: impl Into<String>) -> Self {
        Self {
            venue,
            kind,
            symbol: symbol.into(),
        }
    }
}

impl fmt::Display for MarketId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}.{}.{}",
            self.venue.as_str(),
            self.kind.as_str(),
            self.symbol
        )
    }
}

/// Reglas de negociación del instrumento (desde `exchangeInfo` o equivalente).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InstrumentSpec {
    /// Mercado.
    pub market: MarketId,
    /// Tick de precio.
    pub tick: Px,
    /// Paso de cantidad.
    pub step: Qty,
    /// Unidades de base por contrato (1.0 en Binance USDⓈ-M y spot).
    pub base_per_contract: Qty,
}

impl InstrumentSpec {
    /// ¿El precio respeta el tick? (control de calidad del feed).
    #[inline]
    pub fn on_tick(&self, px: Px) -> bool {
        self.tick.raw() > 0 && px.raw() % self.tick.raw() == 0
    }
}

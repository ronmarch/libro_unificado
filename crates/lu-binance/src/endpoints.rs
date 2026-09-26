//! Topología de conexión por mercado.
//!
//! Futuros USDⓈ-M: desde 2026-03 Binance enruta los streams en `/public`
//! (depth, bookTicker) y `/market` (aggTrade, markPrice, ...); las URLs sin
//! ruta quedaron fuera de servicio el 2026-04-23. Por eso depth y trades van
//! en conexiones distintas y el alineador (F2) no puede asumir orden entre ellas.

use lu_core::{MarketId, MarketKind, Venue};

/// Endpoints de un mercado de Binance.
#[derive(Debug, Clone)]
pub struct Endpoints {
    /// Mercado.
    pub market: MarketId,
    /// Líneas WebSocket redundantes de profundidad (y trades en spot, mismo socket).
    pub depth_lines: Vec<String>,
    /// Líneas WebSocket redundantes de trades (solo futuros: otro endpoint).
    pub trade_lines: Vec<String>,
    /// Por línea de profundidad: URLs alternativas (otros hosts) ante bloqueo o caída.
    pub depth_fallbacks: Vec<Vec<String>>,
    /// Hosts REST en orden de preferencia (fallback ante bloqueo regional o caída).
    pub rest_hosts: Vec<String>,
    /// Ruta REST del snapshot de profundidad.
    pub snapshot_path: String,
    /// Niveles pedidos en el snapshot (define la cobertura).
    pub snapshot_limit: usize,
    /// Ruta REST de `exchangeInfo`.
    pub exchange_info_path: String,
}

impl Endpoints {
    /// Binance Spot. Las líneas alternan hosts distintos (diversidad de ruta de red).
    pub fn spot(symbol: &str, lines: usize) -> Self {
        let s = symbol.to_lowercase();
        let hosts = [
            "wss://stream.binance.com:443",
            "wss://data-stream.binance.vision",
        ];
        let streams = format!("{s}@depth@100ms/{s}@aggTrade");
        let url = |h: &str| format!("{h}/stream?streams={streams}");
        let n = lines.max(1);
        Self {
            market: MarketId::new(Venue::Binance, MarketKind::Spot, symbol),
            depth_lines: (0..n).map(|i| url(hosts[i % hosts.len()])).collect(),
            trade_lines: Vec::new(),
            depth_fallbacks: (0..n)
                .map(|i| {
                    (1..hosts.len())
                        .map(|j| url(hosts[(i + j) % hosts.len()]))
                        .collect()
                })
                .collect(),
            rest_hosts: vec![
                "https://api.binance.com".into(),
                "https://data-api.binance.vision".into(),
            ],
            snapshot_path: format!("/api/v3/depth?symbol={symbol}&limit=5000"),
            snapshot_limit: 5000,
            exchange_info_path: format!("/api/v3/exchangeInfo?symbol={symbol}"),
        }
    }

    /// Binance USDⓈ-M perpetuo.
    pub fn usdm_perp(symbol: &str, lines: usize) -> Self {
        let s = symbol.to_lowercase();
        let n = lines.max(1);
        Self {
            market: MarketId::new(Venue::Binance, MarketKind::Perp, symbol),
            depth_lines: (0..n)
                .map(|_| format!("wss://fstream.binance.com/public/stream?streams={s}@depth@100ms"))
                .collect(),
            trade_lines: (0..n)
                .map(|_| format!("wss://fstream.binance.com/market/stream?streams={s}@aggTrade"))
                .collect(),
            depth_fallbacks: vec![Vec::new(); n],
            rest_hosts: vec!["https://fapi.binance.com".into()],
            snapshot_path: format!("/fapi/v1/depth?symbol={symbol}&limit=1000"),
            snapshot_limit: 1000,
            exchange_info_path: "/fapi/v1/exchangeInfo".into(),
        }
    }
}

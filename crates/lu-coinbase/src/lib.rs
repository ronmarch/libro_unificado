//! # lu-coinbase
//!
//! Conector Coinbase Advanced Trade (`wss://advanced-trade-ws.coinbase.com`), público.
//!
//! Protocolo verificado en vivo (2026-09-26):
//! * `level2` → canal `l2_data`: primer evento `snapshot` con el libro **completo**
//!   (bids hasta 0,01; ~13 000 niveles), luego `update` con cantidades absolutas
//!   (`new_quantity`, 0 = eliminar) y `event_time` por nivel. Lados `bid` / `offer`.
//! * `sequence_num` es **por conexión** y cuenta todos los canales (contiguo 889/889).
//!   No es comparable entre conexiones ⇒ no hay arbitraje A/B de profundidad por id.
//! * `market_trades`: `side` es el lado del **maker** (43/43 `trade_id` idénticos al
//!   feed Exchange, cuyo `side` está documentado como lado maker) ⇒ agresor = opuesto.
//! * El feed Exchange sin autenticar degrada `level2` a top 50 y sus `l2update` no traen
//!   secuencia: descartado.
//!
//! Diseño (standby caliente): cada conexión mantiene su espejo del libro y valida su propia
//! continuidad; ante un hueco suelta el liderazgo y pide re-suscripción. Solo la línea
//! líder (`lu_net::DepthLeader`) emite; la que asume un liderazgo vacante emite
//! `MarketEvent::Reset` con su libro completo. Ids del contador del líder, contiguos
//! (`prev = id − 1`, regla `OkxRule`). Trades de todas las líneas (deduplicados por `trade_id`).
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod proto;

pub use proto::CoinbaseProtocol;

//! Reglas de secuencia por venue/mercado.
//!
//! Cada venue define cómo un diff se encadena con el estado local. El motor de
//! sincronización es genérico sobre estas reglas: agregar un venue = agregar
//! un `SeqRule`, sin tocar la máquina de estados.

/// Clasificación de un diff respecto del estado local.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    /// Ya contenido en el estado (duplicado de otra línea o previo al snapshot): descartar.
    Stale,
    /// Se encadena exactamente: aplicar.
    Next,
    /// Llegó antes de tiempo (falta uno intermedio): retener a la espera de la otra línea.
    Ahead,
    /// Inconsistente con cualquier estado válido: resincronizar.
    Invalid,
}

/// Regla de encadenamiento de un feed de profundidad.
pub trait SeqRule: Send + 'static {
    /// Nombre para métricas.
    const NAME: &'static str;
    /// Clasifica un diff contra el `lastUpdateId` de un snapshot (primer evento a aplicar).
    fn bridge(&self, first: u64, last: u64, prev: Option<u64>, snap_id: u64) -> Class;
    /// Clasifica un diff contra el último id aplicado en vivo.
    fn chain(&self, first: u64, last: u64, prev: Option<u64>, local: u64) -> Class;
    /// ¿El snapshot es anterior al primer evento capturado (hueco imposible de puentear)?
    fn snapshot_too_old(&self, first_buffered: u64, snap_id: u64) -> bool;
}

/// Binance Spot.
///
/// Documentación oficial: descartar `u <= lastUpdateId`; primer evento con
/// `U <= lastUpdateId+1 AND u >= lastUpdateId+1`; luego `U == u_prev + 1`
/// (se acepta solapamiento `U <= local+1 < u`, idempotente con cantidades absolutas).
#[derive(Debug, Clone, Copy, Default)]
pub struct BinanceSpotRule;

impl SeqRule for BinanceSpotRule {
    const NAME: &'static str = "binance_spot";

    #[inline]
    fn bridge(&self, first: u64, last: u64, _prev: Option<u64>, snap_id: u64) -> Class {
        if last <= snap_id {
            Class::Stale
        } else if first <= snap_id + 1 {
            Class::Next
        } else {
            Class::Ahead
        }
    }

    #[inline]
    fn chain(&self, first: u64, last: u64, _prev: Option<u64>, local: u64) -> Class {
        if last <= local {
            Class::Stale
        } else if first <= local + 1 {
            Class::Next
        } else {
            Class::Ahead
        }
    }

    #[inline]
    fn snapshot_too_old(&self, first_buffered: u64, snap_id: u64) -> bool {
        snap_id + 1 < first_buffered
    }
}

/// Binance USDⓈ-M Futures.
///
/// Documentación oficial: descartar `u < lastUpdateId`; primer evento con
/// `U <= lastUpdateId AND u >= lastUpdateId`; luego `pu == u_prev`.
/// Los ids NO son contiguos por símbolo: por eso la cadena se valida con `pu`.
#[derive(Debug, Clone, Copy, Default)]
pub struct BinanceFuturesRule;

impl SeqRule for BinanceFuturesRule {
    const NAME: &'static str = "binance_futures";

    #[inline]
    fn bridge(&self, first: u64, last: u64, _prev: Option<u64>, snap_id: u64) -> Class {
        if last < snap_id {
            Class::Stale
        } else if first <= snap_id {
            Class::Next
        } else {
            Class::Ahead
        }
    }

    #[inline]
    fn chain(&self, _first: u64, last: u64, prev: Option<u64>, local: u64) -> Class {
        if last <= local {
            return Class::Stale;
        }
        match prev {
            Some(pu) if pu == local => Class::Next,
            Some(pu) if pu > local => Class::Ahead,
            // pu < local < u: solapamiento imposible en un feed sano; o falta `pu`.
            _ => Class::Invalid,
        }
    }

    #[inline]
    fn snapshot_too_old(&self, first_buffered: u64, snap_id: u64) -> bool {
        snap_id < first_buffered
    }
}

/// OKX v5 canal `books` (snapshot por WebSocket + updates).
///
/// Verificado en vivo (2026-09-26): el snapshot trae `prevSeqId = -1` y su `seqId`;
/// cada update trae `prevSeqId` = `seqId` del mensaje anterior (ids no contiguos).
/// El conector mapea `first = prevSeqId + 1`, `last = seqId`, `prev = prevSeqId`.
/// Un update sin cambios (latido) trae `prevSeqId == seqId` ⇒ `Stale`.
/// Un `seqId` que retrocede (reinicio por mantenimiento) es `Invalid` ⇒ resync.
/// El campo `checksum` llega en 0 (OKX dejó de calcularlo): la integridad descansa
/// solo en la cadena de secuencia.
#[derive(Debug, Clone, Copy, Default)]
pub struct OkxRule;

impl SeqRule for OkxRule {
    const NAME: &'static str = "okx";

    #[inline]
    fn bridge(&self, _first: u64, last: u64, prev: Option<u64>, snap_id: u64) -> Class {
        if last <= snap_id {
            return Class::Stale;
        }
        match prev {
            Some(pu) if pu == snap_id => Class::Next,
            Some(pu) if pu > snap_id => Class::Ahead,
            _ => Class::Invalid,
        }
    }

    #[inline]
    fn chain(&self, _first: u64, last: u64, prev: Option<u64>, local: u64) -> Class {
        match prev {
            Some(pu) if last < pu => Class::Invalid,
            _ if last <= local => Class::Stale,
            Some(pu) if pu == local => Class::Next,
            Some(pu) if pu > local => Class::Ahead,
            _ => Class::Invalid,
        }
    }

    #[inline]
    fn snapshot_too_old(&self, first_buffered: u64, snap_id: u64) -> bool {
        // first_buffered = prev + 1 del diff más antiguo retenido.
        snap_id + 1 < first_buffered
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spot_puente_y_cadena() {
        let r = BinanceSpotRule;
        // snapshot 100
        assert_eq!(r.bridge(90, 100, None, 100), Class::Stale);
        assert_eq!(r.bridge(95, 105, None, 100), Class::Next);
        assert_eq!(r.bridge(101, 105, None, 100), Class::Next);
        assert_eq!(r.bridge(102, 105, None, 100), Class::Ahead);
        // en vivo, local 105
        assert_eq!(r.chain(101, 105, None, 105), Class::Stale);
        assert_eq!(r.chain(106, 110, None, 105), Class::Next);
        assert_eq!(r.chain(107, 110, None, 105), Class::Ahead);
        assert!(r.snapshot_too_old(102, 100));
        assert!(!r.snapshot_too_old(101, 100));
    }

    #[test]
    fn futuros_puente_y_cadena() {
        let r = BinanceFuturesRule;
        // snapshot 100
        assert_eq!(r.bridge(80, 99, Some(70), 100), Class::Stale);
        assert_eq!(r.bridge(95, 100, Some(90), 100), Class::Next); // u == lastUpdateId
        assert_eq!(r.bridge(95, 120, Some(90), 100), Class::Next);
        assert_eq!(r.bridge(101, 120, Some(99), 100), Class::Ahead);
        // en vivo, local 120
        assert_eq!(r.chain(95, 120, Some(90), 120), Class::Stale);
        assert_eq!(r.chain(125, 130, Some(120), 120), Class::Next);
        assert_eq!(r.chain(135, 140, Some(130), 120), Class::Ahead);
        assert_eq!(r.chain(110, 140, Some(110), 120), Class::Invalid);
        assert_eq!(r.chain(125, 130, None, 120), Class::Invalid);
        assert!(r.snapshot_too_old(101, 100));
        assert!(!r.snapshot_too_old(100, 100));
    }

    #[test]
    fn okx_puente_y_cadena() {
        let r = OkxRule;
        // snapshot seqId 283; updates (prev, seq)
        assert_eq!(r.bridge(250, 283, Some(249), 283), Class::Stale);
        assert_eq!(r.bridge(284, 315, Some(283), 283), Class::Next);
        assert_eq!(r.bridge(316, 328, Some(315), 283), Class::Ahead);
        assert_eq!(r.bridge(270, 300, Some(269), 283), Class::Invalid);
        // en vivo, local 315
        assert_eq!(r.chain(316, 328, Some(315), 315), Class::Next);
        assert_eq!(r.chain(316, 315, Some(315), 315), Class::Stale); // latido
        assert_eq!(r.chain(284, 315, Some(283), 315), Class::Stale); // duplicado de otra línea
        assert_eq!(r.chain(329, 340, Some(328), 315), Class::Ahead);
        assert_eq!(r.chain(301, 320, Some(300), 315), Class::Invalid);
        assert_eq!(r.chain(11, 10, Some(500), 315), Class::Invalid); // seqId retrocede
        assert!(r.snapshot_too_old(316, 283 + 20));
        assert!(!r.snapshot_too_old(284, 283));
        assert!(r.snapshot_too_old(316, 283));
    }
}

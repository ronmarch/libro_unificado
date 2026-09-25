//! Libro L2 de un venue/mercado. Estructura pura: no sabe de secuencias ni de red.

use lu_core::{DepthSnapshot, Px, Qty, Side};
use serde::Serialize;
use std::collections::BTreeMap;

/// Región del libro que el snapshot garantiza como completa.
///
/// Un snapshot REST trae como máximo `limit` niveles por lado. Si un lado llegó
/// completo hasta el límite, los niveles más allá del último son DESCONOCIDOS:
/// el diff stream solo informa cambios, así que un nivel lejano que no cambió
/// jamás aparece. `None` = lado completo (no truncado) o sin datos.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct Coverage {
    /// Bids por debajo de este precio: desconocidos.
    pub bid_floor: Option<Px>,
    /// Asks por encima de este precio: desconocidos.
    pub ask_ceiling: Option<Px>,
}

/// Libro L2 con precios ordenados (`BTreeMap`: mejor bid = último, mejor ask = primero).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct L2Book {
    bids: BTreeMap<Px, Qty>,
    asks: BTreeMap<Px, Qty>,
    coverage: Coverage,
}

impl L2Book {
    /// Libro vacío.
    pub fn new() -> Self {
        Self::default()
    }

    /// Vacía el libro y su cobertura.
    pub fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.coverage = Coverage::default();
    }

    /// Reemplaza el contenido por un snapshot y calcula la cobertura.
    pub fn load_snapshot(&mut self, s: &DepthSnapshot) {
        self.clear();
        for l in &s.bids {
            if l.qty.is_positive() {
                self.bids.insert(l.px, l.qty);
            }
        }
        for l in &s.asks {
            if l.qty.is_positive() {
                self.asks.insert(l.px, l.qty);
            }
        }
        self.coverage = Coverage {
            bid_floor: (s.limit > 0 && s.bids.len() >= s.limit)
                .then(|| s.bids.iter().map(|l| l.px).min())
                .flatten(),
            ask_ceiling: (s.limit > 0 && s.asks.len() >= s.limit)
                .then(|| s.asks.iter().map(|l| l.px).max())
                .flatten(),
        };
    }

    /// Fija la cantidad absoluta de un nivel (0 elimina). Devuelve la cantidad previa (0 si no existía).
    #[inline]
    pub fn set(&mut self, side: Side, px: Px, qty: Qty) -> Qty {
        let map = match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };
        if qty.is_zero() {
            // Eliminar un nivel inexistente es normal según Binance.
            map.remove(&px).unwrap_or(Qty::ZERO)
        } else {
            map.insert(px, qty).unwrap_or(Qty::ZERO)
        }
    }

    /// Mejor bid.
    #[inline]
    pub fn best_bid(&self) -> Option<(Px, Qty)> {
        self.bids.last_key_value().map(|(p, q)| (*p, *q))
    }

    /// Mejor ask.
    #[inline]
    pub fn best_ask(&self) -> Option<(Px, Qty)> {
        self.asks.first_key_value().map(|(p, q)| (*p, *q))
    }

    /// Libro cruzado o bloqueado (bid ≥ ask): imposible en un único venue sano.
    #[inline]
    pub fn is_crossed(&self) -> bool {
        matches!((self.best_bid(), self.best_ask()), (Some((b, _)), Some((a, _))) if b >= a)
    }

    /// Cantidad en un nivel (0 si no existe).
    #[inline]
    pub fn qty_at(&self, side: Side, px: Px) -> Qty {
        let map = match side {
            Side::Bid => &self.bids,
            Side::Ask => &self.asks,
        };
        map.get(&px).copied().unwrap_or(Qty::ZERO)
    }

    /// Bids de mejor a peor.
    pub fn bids_desc(&self) -> impl Iterator<Item = (Px, Qty)> + '_ {
        self.bids.iter().rev().map(|(p, q)| (*p, *q))
    }

    /// Asks de mejor a peor.
    pub fn asks_asc(&self) -> impl Iterator<Item = (Px, Qty)> + '_ {
        self.asks.iter().map(|(p, q)| (*p, *q))
    }

    /// Mapa de bids (solo lectura).
    pub fn bids(&self) -> &BTreeMap<Px, Qty> {
        &self.bids
    }

    /// Mapa de asks (solo lectura).
    pub fn asks(&self) -> &BTreeMap<Px, Qty> {
        &self.asks
    }

    /// Número de niveles por lado.
    pub fn depth_len(&self) -> (usize, usize) {
        (self.bids.len(), self.asks.len())
    }

    /// Cobertura garantizada por el último snapshot.
    pub fn coverage(&self) -> Coverage {
        self.coverage
    }

    /// ¿El precio cae en la región conocida del lado indicado?
    #[inline]
    pub fn is_covered(&self, side: Side, px: Px) -> bool {
        match side {
            Side::Bid => self.coverage.bid_floor.is_none_or(|f| px >= f),
            Side::Ask => self.coverage.ask_ceiling.is_none_or(|c| px <= c),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lu_core::{Level, RxStamp};

    fn lv(p: &str, q: &str) -> Level {
        Level {
            px: Px::parse(p).unwrap(),
            qty: Qty::parse(q).unwrap(),
        }
    }

    #[test]
    fn set_devuelve_previo_y_elimina_con_cero() {
        let mut b = L2Book::new();
        let p = Px::parse("100").unwrap();
        assert_eq!(b.set(Side::Bid, p, Qty::parse("2").unwrap()), Qty::ZERO);
        assert_eq!(
            b.set(Side::Bid, p, Qty::parse("3").unwrap()),
            Qty::parse("2").unwrap()
        );
        assert_eq!(b.set(Side::Bid, p, Qty::ZERO), Qty::parse("3").unwrap());
        assert_eq!(b.set(Side::Bid, p, Qty::ZERO), Qty::ZERO); // eliminar inexistente: normal
        assert_eq!(b.depth_len(), (0, 0));
    }

    #[test]
    fn cobertura_solo_si_snapshot_truncado() {
        let s = DepthSnapshot {
            last_update_id: 1,
            limit: 2,
            bids: vec![lv("100", "1"), lv("99", "1")],
            asks: vec![lv("101", "1")],
            exch_ts_ms: None,
            rx: RxStamp::default(),
        };
        let mut b = L2Book::new();
        b.load_snapshot(&s);
        assert_eq!(b.coverage().bid_floor, Some(Px::parse("99").unwrap()));
        assert_eq!(b.coverage().ask_ceiling, None);
        assert!(!b.is_covered(Side::Bid, Px::parse("98").unwrap()));
        assert!(b.is_covered(Side::Ask, Px::parse("500").unwrap()));
        assert_eq!(b.best_bid().unwrap().0, Px::parse("100").unwrap());
        assert_eq!(b.best_ask().unwrap().0, Px::parse("101").unwrap());
        assert!(!b.is_crossed());
    }
}

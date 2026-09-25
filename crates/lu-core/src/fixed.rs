//! Punto fijo exacto con escala universal 1e-8.
//!
//! Decisión de diseño: TODOS los venues se representan en la misma escala
//! (8 decimales), independiente de su tick o step. Así la fusión multi-venue
//! (F-posteriores) no requiere reconversión y ningún `f64` toca el camino
//! crítico: el parseo desde el texto del exchange es exacto o falla.

use core::fmt;
use core::ops::{Add, Sub};
use serde::{Serialize, Serializer};

/// Decimales de la escala universal.
pub const DECIMALS: u32 = 8;
/// 10^DECIMALS.
pub const SCALE: i64 = 100_000_000;

/// Error de parseo de decimal ASCII.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ParseFixedError {
    /// Cadena vacía o sin dígitos.
    #[error("cadena vacía o sin dígitos")]
    Empty,
    /// Carácter no permitido (solo dígitos y un punto).
    #[error("carácter inválido")]
    InvalidChar,
    /// Más de 8 decimales significativos: se rechaza en vez de redondear.
    #[error("precisión mayor a 8 decimales")]
    Precision,
    /// El valor no cabe en i64 escalado.
    #[error("desbordamiento")]
    Overflow,
    /// Precios y cantidades de libro nunca son negativos.
    #[error("valor negativo no permitido")]
    Negative,
}

/// Parseo exacto `"123.4500"` → `12_345_000_000` (escala 1e8). Sin `f64`.
///
/// Acepta ceros finales más allá del octavo decimal (`"1.000000000"`),
/// rechaza cualquier dígito significativo más allá (nunca redondea en silencio).
#[inline]
pub fn parse_fixed8(s: &str) -> Result<i64, ParseFixedError> {
    let b = s.as_bytes();
    if b.is_empty() {
        return Err(ParseFixedError::Empty);
    }
    if b[0] == b'-' {
        return Err(ParseFixedError::Negative);
    }
    let mut i = 0usize;
    let mut int: i64 = 0;
    let mut digits = 0usize;
    while i < b.len() && b[i] != b'.' {
        let c = b[i];
        if !c.is_ascii_digit() {
            return Err(ParseFixedError::InvalidChar);
        }
        int = int
            .checked_mul(10)
            .and_then(|v| v.checked_add(i64::from(c - b'0')))
            .ok_or(ParseFixedError::Overflow)?;
        digits += 1;
        i += 1;
    }
    let mut frac: i64 = 0;
    let mut nfrac: u32 = 0;
    if i < b.len() {
        i += 1; // '.'
        while i < b.len() {
            let c = b[i];
            if !c.is_ascii_digit() {
                return Err(ParseFixedError::InvalidChar);
            }
            if nfrac < DECIMALS {
                frac = frac * 10 + i64::from(c - b'0');
                nfrac += 1;
            } else if c != b'0' {
                return Err(ParseFixedError::Precision);
            }
            digits += 1;
            i += 1;
        }
    }
    if digits == 0 {
        return Err(ParseFixedError::Empty);
    }
    frac *= 10i64.pow(DECIMALS - nfrac);
    int.checked_mul(SCALE)
        .and_then(|v| v.checked_add(frac))
        .ok_or(ParseFixedError::Overflow)
}

/// Formatea un entero escalado 1e8 como decimal mínimo (`12.5`, `3`, `0.00000001`).
pub fn write_fixed8(v: i64, f: &mut impl fmt::Write) -> fmt::Result {
    let neg = v < 0;
    let a = v.unsigned_abs();
    let int = a / SCALE as u64;
    let frac = a % SCALE as u64;
    if neg {
        f.write_char('-')?;
    }
    write!(f, "{int}")?;
    if frac != 0 {
        let mut buf = [b'0'; 8];
        let mut x = frac;
        for slot in buf.iter_mut().rev() {
            *slot = b'0' + (x % 10) as u8;
            x /= 10;
        }
        let end = buf.iter().rposition(|&c| c != b'0').map_or(0, |p| p + 1);
        f.write_char('.')?;
        // buf es ASCII por construcción
        f.write_str(core::str::from_utf8(&buf[..end]).unwrap_or("0"))?;
    }
    Ok(())
}

macro_rules! fixed_newtype {
    ($(#[$m:meta])* $name:ident) => {
        $(#[$m])*
        #[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
        #[repr(transparent)]
        pub struct $name(pub i64);

        impl $name {
            /// Cero.
            pub const ZERO: Self = Self(0);

            /// Parseo exacto desde el texto del exchange.
            #[inline]
            pub fn parse(s: &str) -> Result<Self, ParseFixedError> {
                parse_fixed8(s).map(Self)
            }
            /// Valor crudo escalado 1e8.
            #[inline]
            pub const fn raw(self) -> i64 {
                self.0
            }
            /// Construye desde entero crudo escalado 1e8.
            #[inline]
            pub const fn from_raw(v: i64) -> Self {
                Self(v)
            }
            /// Construye desde unidades enteras (p. ej. `from_units(1)` = 1.0).
            #[inline]
            pub const fn from_units(u: i64) -> Self {
                Self(u * SCALE)
            }
            /// ¿Es cero?
            #[inline]
            pub const fn is_zero(self) -> bool {
                self.0 == 0
            }
            /// ¿Es estrictamente positivo?
            #[inline]
            pub const fn is_positive(self) -> bool {
                self.0 > 0
            }
            /// Aproximación `f64` SOLO para presentación (nunca en cálculos).
            #[inline]
            pub fn to_f64_lossy(self) -> f64 {
                self.0 as f64 / SCALE as f64
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write_fixed8(self.0, f)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(stringify!($name))?;
                f.write_str("(")?;
                write_fixed8(self.0, f)?;
                f.write_str(")")
            }
        }

        /// Se serializa como texto decimal exacto (práctica institucional: sin floats en APIs).
        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.collect_str(self)
            }
        }

        impl Add for $name {
            type Output = Self;
            #[inline]
            fn add(self, o: Self) -> Self {
                Self(self.0.saturating_add(o.0))
            }
        }

        impl Sub for $name {
            type Output = Self;
            #[inline]
            fn sub(self, o: Self) -> Self {
                Self(self.0.saturating_sub(o.0))
            }
        }
    };
}

fixed_newtype!(
    /// Precio en escala 1e8 de la moneda de cotización.
    Px
);
fixed_newtype!(
    /// Cantidad en escala 1e8 del activo base.
    Qty
);

impl Px {
    /// Índice de bucket de ancho `width` (p. ej. `Px::from_units(1)` = niveles de 1 USDT).
    #[inline]
    pub fn bucket(self, width: Px) -> i64 {
        debug_assert!(width.0 > 0);
        self.0.div_euclid(width.0)
    }
}

/// Notional exacto `px * qty` en escala 1e8 de la moneda de cotización (i128: sin desborde).
#[inline]
pub fn notional(px: Px, qty: Qty) -> i128 {
    (i128::from(px.0) * i128::from(qty.0)) / i128::from(SCALE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_exacto() {
        assert_eq!(parse_fixed8("0").unwrap(), 0);
        assert_eq!(parse_fixed8("1").unwrap(), SCALE);
        assert_eq!(parse_fixed8("123.45").unwrap(), 12_345_000_000);
        assert_eq!(parse_fixed8("123.45000000").unwrap(), 12_345_000_000);
        assert_eq!(parse_fixed8("0.00000001").unwrap(), 1);
        assert_eq!(parse_fixed8(".5").unwrap(), SCALE / 2);
        assert_eq!(parse_fixed8("5.").unwrap(), 5 * SCALE);
        assert_eq!(parse_fixed8("1.0000000000").unwrap(), SCALE);
    }

    #[test]
    fn parse_rechaza_sin_redondear() {
        assert_eq!(parse_fixed8(""), Err(ParseFixedError::Empty));
        assert_eq!(parse_fixed8("."), Err(ParseFixedError::Empty));
        assert_eq!(parse_fixed8("-1"), Err(ParseFixedError::Negative));
        assert_eq!(parse_fixed8("1e-8"), Err(ParseFixedError::InvalidChar));
        assert_eq!(parse_fixed8("1.2.3"), Err(ParseFixedError::InvalidChar));
        assert_eq!(parse_fixed8("0.000000001"), Err(ParseFixedError::Precision));
        assert_eq!(
            parse_fixed8("99999999999999999999"),
            Err(ParseFixedError::Overflow)
        );
    }

    #[test]
    fn formato_minimo_y_roundtrip() {
        for s in ["0", "1", "123.45", "0.00000001", "98765.4321"] {
            let v = parse_fixed8(s).unwrap();
            let mut out = String::new();
            write_fixed8(v, &mut out).unwrap();
            assert_eq!(out, s);
        }
        assert_eq!(Px::parse("150.10").unwrap().to_string(), "150.1");
    }

    #[test]
    fn buckets_de_1_usdt() {
        let w = Px::from_units(1);
        assert_eq!(Px::parse("102.99").unwrap().bucket(w), 102);
        assert_eq!(Px::parse("103.00").unwrap().bucket(w), 103);
    }

    #[test]
    fn notional_exacto() {
        let n = notional(Px::parse("150.25").unwrap(), Qty::parse("2.5").unwrap());
        assert_eq!(n, 37_562_500_000); // 375.625 * 1e8
    }
}

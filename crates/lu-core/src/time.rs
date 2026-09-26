//! Tiempo del exchange: parseo exacto de marcas RFC 3339 (UTC) sin dependencias.

/// `2026-09-26T04:14:54.693295Z` → ms UNIX (sin dependencias; solo UTC `Z`).
pub fn rfc3339_ms(s: &str) -> Result<u64, String> {
    let bad = || format!("fecha inválida: {s}");
    let b = s.as_bytes();
    if b.len() < 20 || b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || !s.ends_with('Z') {
        return Err(bad());
    }
    let n =
        |r: std::ops::Range<usize>| s.get(r).and_then(|x| x.parse::<i64>().ok()).ok_or_else(bad);
    let (y, mo, d) = (n(0..4)?, n(5..7)?, n(8..10)?);
    let (h, mi, se) = (n(11..13)?, n(14..16)?, n(17..19)?);
    let mut ms = 0i64;
    if b[19] == b'.' {
        let frac = &s[20..s.len() - 1];
        let digits: String = frac.chars().take(3).collect();
        let v: i64 = digits.parse().map_err(|_| bad())?;
        ms = v * 10i64.pow(3 - digits.len() as u32);
    }
    // Días desde 1970-01-01 (algoritmo civil de H. Hinnant).
    let yy = if mo <= 2 { y - 1 } else { y };
    let era = yy.div_euclid(400);
    let yoe = yy - era * 400;
    let mp = (mo + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let t = ((days * 24 + h) * 60 + mi) * 60 + se;
    u64::try_from(t * 1000 + ms).map_err(|_| bad())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fecha_rfc3339_exacta() {
        assert_eq!(rfc3339_ms("1970-01-01T00:00:00Z").unwrap(), 0);
        assert_eq!(
            rfc3339_ms("2000-03-01T00:00:00.5Z").unwrap(),
            951_868_800_500
        );
        assert_eq!(
            rfc3339_ms("2026-09-26T04:15:00.114692Z").unwrap(),
            1_790_396_100_114
        );
        assert!(rfc3339_ms("2026-09-26 04:15:00Z").is_err());
    }
}

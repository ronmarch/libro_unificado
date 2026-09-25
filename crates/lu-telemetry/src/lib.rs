//! # lu-telemetry
//!
//! Histogramas HDR (propiedad de un solo hilo: sin locks) y un escritor de
//! texto Prometheus sin dependencias. Los motores publican resúmenes
//! inmutables; el servidor HTTP solo lee.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

use hdrhistogram::Histogram;
use serde::Serialize;
use std::fmt::Write as _;

/// Histograma de latencia en microsegundos (1 µs .. 60 s, 3 cifras significativas).
pub struct LatencyHist {
    h: Histogram<u64>,
    negatives: u64,
}

impl Default for LatencyHist {
    fn default() -> Self {
        Self::new()
    }
}

impl LatencyHist {
    /// Nuevo histograma vacío.
    pub fn new() -> Self {
        // Límites fijos válidos: la construcción no puede fallar.
        let h = Histogram::new_with_bounds(1, 60_000_000, 3).expect("límites HDR válidos");
        Self { h, negatives: 0 }
    }

    /// Registra una latencia firmada en µs. Las negativas (desfase de reloj) se cuentan aparte.
    #[inline]
    pub fn record_us(&mut self, us: i64) {
        if us < 0 {
            self.negatives += 1;
            return;
        }
        let v = (us as u64).clamp(1, 60_000_000);
        let _ = self.h.record(v);
    }

    /// Resumen inmutable para publicar.
    pub fn summary(&self) -> LatencySummary {
        let q = |p: f64| {
            if self.h.is_empty() {
                0.0
            } else {
                self.h.value_at_quantile(p) as f64 / 1000.0
            }
        };
        LatencySummary {
            count: self.h.len(),
            negatives: self.negatives,
            p50_ms: q(0.50),
            p90_ms: q(0.90),
            p99_ms: q(0.99),
            p999_ms: q(0.999),
            max_ms: if self.h.is_empty() {
                0.0
            } else {
                self.h.max() as f64 / 1000.0
            },
        }
    }

    /// Reinicia (ventanas de medición).
    pub fn reset(&mut self) {
        self.h.reset();
        self.negatives = 0;
    }
}

/// Resumen de latencia (ms).
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct LatencySummary {
    /// Muestras.
    pub count: u64,
    /// Muestras negativas descartadas (reloj local adelantado al del exchange).
    pub negatives: u64,
    /// Mediana.
    pub p50_ms: f64,
    /// p90.
    pub p90_ms: f64,
    /// p99.
    pub p99_ms: f64,
    /// p99.9.
    pub p999_ms: f64,
    /// Máximo.
    pub max_ms: f64,
}

/// Escritor de exposición Prometheus (text format 0.0.4).
#[derive(Default)]
pub struct PromWriter {
    out: String,
    declared: Vec<&'static str>,
}

/// Tipo de métrica.
#[derive(Clone, Copy)]
pub enum MetricType {
    /// Contador monótono.
    Counter,
    /// Valor instantáneo.
    Gauge,
}

impl PromWriter {
    /// Nuevo escritor.
    pub fn new() -> Self {
        Self::default()
    }

    /// Escribe una muestra; declara `# HELP`/`# TYPE` la primera vez.
    pub fn sample(
        &mut self,
        name: &'static str,
        help: &str,
        ty: MetricType,
        labels: &[(&str, &str)],
        value: f64,
    ) {
        if !self.declared.contains(&name) {
            self.declared.push(name);
            let t = match ty {
                MetricType::Counter => "counter",
                MetricType::Gauge => "gauge",
            };
            let _ = writeln!(self.out, "# HELP {name} {help}");
            let _ = writeln!(self.out, "# TYPE {name} {t}");
        }
        self.out.push_str(name);
        if !labels.is_empty() {
            self.out.push('{');
            for (i, (k, v)) in labels.iter().enumerate() {
                if i > 0 {
                    self.out.push(',');
                }
                let _ = write!(self.out, "{k}=\"");
                for c in v.chars() {
                    match c {
                        '\\' => self.out.push_str("\\\\"),
                        '"' => self.out.push_str("\\\""),
                        '\n' => self.out.push_str("\\n"),
                        c => self.out.push(c),
                    }
                }
                self.out.push('"');
            }
            self.out.push('}');
        }
        if value.is_finite() {
            let _ = writeln!(self.out, " {value}");
        } else {
            let _ = writeln!(self.out, " NaN");
        }
    }

    /// Texto final.
    pub fn finish(self) -> String {
        self.out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histograma_percentiles_y_negativos() {
        let mut h = LatencyHist::new();
        for us in 1..=1000 {
            h.record_us(us * 1000); // 1..1000 ms
        }
        h.record_us(-5);
        let s = h.summary();
        assert_eq!(s.count, 1000);
        assert_eq!(s.negatives, 1);
        assert!((s.p50_ms - 500.0).abs() < 1.0);
        assert!((s.p99_ms - 990.0).abs() < 2.0);
    }

    #[test]
    fn prometheus_escapa_y_declara_una_vez() {
        let mut w = PromWriter::new();
        w.sample(
            "lu_x_total",
            "x",
            MetricType::Counter,
            &[("m", "a\"b")],
            1.0,
        );
        w.sample("lu_x_total", "x", MetricType::Counter, &[("m", "c")], 2.0);
        let t = w.finish();
        assert_eq!(t.matches("# TYPE lu_x_total counter").count(), 1);
        assert!(t.contains("lu_x_total{m=\"a\\\"b\"} 1"));
    }
}

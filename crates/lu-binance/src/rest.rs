//! Cliente REST bloqueante (se ejecuta en `spawn_blocking`): snapshots poco
//! frecuentes no justifican un cliente async pesado. Respeta los límites de
//! peso de Binance: 429 ⇒ esperar `Retry-After`; 418 ⇒ IP baneada, esperar más.

use crate::wire::{parse_instrument, parse_snapshot, WireError};
use lu_core::{DepthSnapshot, InstrumentSpec, MarketId, RxStamp};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Error REST tipado: el llamador decide el backoff según la causa.
#[derive(Debug, thiserror::Error)]
pub enum RestError {
    /// 429: se excedió el peso por minuto.
    #[error("rate limit (429), reintentar en {retry_after:?}")]
    RateLimited {
        /// Espera sugerida por el servidor.
        retry_after: Option<Duration>,
    },
    /// 418: IP baneada temporalmente por ignorar 429.
    #[error("IP baneada (418), reintentar en {retry_after:?}")]
    Banned {
        /// Espera sugerida por el servidor.
        retry_after: Option<Duration>,
    },
    /// 451: bloqueo regional.
    #[error("bloqueo regional (451) en {host}")]
    Restricted {
        /// Host que bloqueó.
        host: String,
    },
    /// Otro estado HTTP.
    #[error("HTTP {status}")]
    Http {
        /// Código.
        status: u16,
    },
    /// Error de red/TLS/timeout.
    #[error("transporte: {0}")]
    Transport(String),
    /// Respuesta inválida.
    #[error(transparent)]
    Wire(#[from] WireError),
}

impl RestError {
    /// ¿Conviene probar el siguiente host?
    fn try_next_host(&self) -> bool {
        matches!(
            self,
            RestError::Restricted { .. } | RestError::Transport(_) | RestError::Http { .. }
        )
    }
}

/// Cliente REST de Binance.
pub struct RestClient {
    agent: ureq::Agent,
    used_weight_1m: AtomicU32,
}

impl RestClient {
    /// Nuevo cliente con timeouts estrictos y la configuración TLS compartida.
    pub fn new(tls: Arc<rustls::ClientConfig>) -> Self {
        let agent = ureq::AgentBuilder::new()
            .tls_config(tls)
            .timeout_connect(Duration::from_secs(5))
            .timeout(Duration::from_secs(15))
            .user_agent(concat!("lu-node/", env!("CARGO_PKG_VERSION")))
            .build();
        Self {
            agent,
            used_weight_1m: AtomicU32::new(0),
        }
    }

    /// Último `X-MBX-USED-WEIGHT-1M` observado.
    pub fn used_weight_1m(&self) -> u32 {
        self.used_weight_1m.load(Ordering::Relaxed)
    }

    fn get(&self, host: &str, path: &str) -> Result<String, RestError> {
        let url = format!("{host}{path}");
        match self.agent.get(&url).call() {
            Ok(resp) => {
                if let Some(w) = resp
                    .header("x-mbx-used-weight-1m")
                    .and_then(|v| v.parse().ok())
                {
                    self.used_weight_1m.store(w, Ordering::Relaxed);
                }
                resp.into_string()
                    .map_err(|e| RestError::Transport(e.to_string()))
            }
            Err(ureq::Error::Status(code, resp)) => {
                let retry_after = resp
                    .header("retry-after")
                    .and_then(|v| v.parse::<u64>().ok())
                    .map(Duration::from_secs);
                Err(match code {
                    429 => RestError::RateLimited { retry_after },
                    418 => RestError::Banned { retry_after },
                    451 => RestError::Restricted {
                        host: host.to_string(),
                    },
                    status => RestError::Http { status },
                })
            }
            Err(ureq::Error::Transport(t)) => Err(RestError::Transport(t.to_string())),
        }
    }

    fn get_any(&self, hosts: &[String], path: &str) -> Result<String, RestError> {
        let mut last = RestError::Transport("sin hosts configurados".into());
        for h in hosts {
            match self.get(h, path) {
                Ok(t) => return Ok(t),
                Err(e) if e.try_next_host() => last = e,
                Err(e) => return Err(e),
            }
        }
        Err(last)
    }

    /// Snapshot de profundidad (prueba hosts en orden).
    pub fn depth_snapshot(
        &self,
        hosts: &[String],
        path: &str,
        limit: usize,
    ) -> Result<DepthSnapshot, RestError> {
        let text = self.get_any(hosts, path)?;
        let rx = RxStamp::now(u8::MAX);
        Ok(parse_snapshot(&text, limit, rx)?)
    }

    /// Reglas del instrumento.
    pub fn instrument(
        &self,
        hosts: &[String],
        path: &str,
        market: &MarketId,
    ) -> Result<InstrumentSpec, RestError> {
        let text = self.get_any(hosts, path)?;
        Ok(parse_instrument(&text, market)?)
    }
}

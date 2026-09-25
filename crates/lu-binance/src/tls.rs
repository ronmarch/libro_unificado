//! Configuración TLS única y explícita (WS y REST comparten la misma).
//!
//! Raíces = Mozilla (webpki-roots, embebidas: el binario estático musl no
//! depende del sistema) ∪ CA adicional opcional (`--ca-file` o `SSL_CERT_FILE`),
//! para redes con proxy corporativo que re-firma TLS.

use rustls::pki_types::{pem::PemObject, CertificateDer};
use rustls::{ClientConfig, RootCertStore};
use std::path::Path;
use std::sync::Arc;

/// Error de configuración TLS.
#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    /// No se pudo leer o parsear el archivo PEM.
    #[error("CA adicional {path}: {msg}")]
    CaFile {
        /// Ruta.
        path: String,
        /// Detalle.
        msg: String,
    },
}

/// Construye la configuración cliente. Devuelve también cuántas CA extra se cargaron.
pub fn client_config(extra_ca: Option<&Path>) -> Result<(Arc<ClientConfig>, usize), TlsError> {
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let mut extra = 0usize;
    if let Some(path) = extra_ca {
        let err = |msg: String| TlsError::CaFile {
            path: path.display().to_string(),
            msg,
        };
        let certs = CertificateDer::pem_file_iter(path).map_err(|e| err(e.to_string()))?;
        for c in certs {
            let c = c.map_err(|e| err(e.to_string()))?;
            if roots.add(c).is_ok() {
                extra += 1;
            }
        }
    }
    let cfg = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok((Arc::new(cfg), extra))
}

//! Internal HTTP primitives shared by model adapters, without vendor policy.

use std::sync::Arc;

pub mod sse;

/// A bounded event stream that could not be decoded.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SseError {
    /// The stream exceeded its byte budget.
    #[error("the provider's answer is longer than {limit} bytes")]
    AnswerTooLong {
        /// The maximum number of bytes.
        limit: usize,
    },
    /// A line exceeded its byte budget.
    #[error("the provider's stream sent a line longer than {limit} bytes")]
    StreamLineTooLong {
        /// The maximum number of bytes.
        limit: usize,
    },
    /// An event exceeded its byte budget.
    #[error("the provider's stream sent an event longer than {limit} bytes")]
    StreamEventTooLong {
        /// The maximum number of bytes.
        limit: usize,
    },
    /// The wire was not valid UTF-8.
    #[error("the provider's answer is malformed: {reason}")]
    Malformed {
        /// The reason, without the response's content.
        reason: String,
    },
}

/// TLS with ring, public web PKI roots, and HTTP/1.1 ALPN.
///
/// # Errors
/// Fails when the cryptography provider has no usable protocol version.
pub fn tls_config() -> Result<rustls::ClientConfig, String> {
    let roots = rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|error| error.to_string())?
    .with_root_certificates(roots)
    .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(config)
}

//! The transport's explicit refusal before execution, never a wire error.

use std::ops::Deref;

use roomkit_ai::{ProviderError, ProviderErrorOptions};

/// A transport's connection refused a prompt before executing any of it.
/// Python's `ACPSessionInvalidatedError`. Raise it only from `prompt` when
/// the session cannot be used. Authorization means the host reserved a safe
/// retry, including any durable admission or deduplication it requires; it
/// cannot be inferred from an agent's error code or message.
#[derive(Clone, Debug, thiserror::Error)]
#[error("{inner}")]
pub struct AcpSessionInvalidatedError {
    /// Whether the host explicitly authorized one rebuild and replay.
    pub recovery_authorized: bool,
    inner: ProviderError,
}

/// Python's keyword arguments, with their defaults.
#[derive(Clone, Copy, Debug, Default)]
pub struct AcpSessionInvalidatedErrorOptions {
    /// Explicit host authorization; false unless granted.
    pub recovery_authorized: bool,
}

impl AcpSessionInvalidatedError {
    /// The session is unusable for `reason`, qualified by the host's authorization.
    pub fn new(reason: impl Into<String>, options: AcpSessionInvalidatedErrorOptions) -> Self {
        Self {
            recovery_authorized: options.recovery_authorized,
            inner: ProviderError::new(
                reason,
                ProviderErrorOptions {
                    provider: "acp".to_owned(),
                    ..ProviderErrorOptions::default()
                },
            ),
        }
    }
}

impl Deref for AcpSessionInvalidatedError {
    type Target = ProviderError;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

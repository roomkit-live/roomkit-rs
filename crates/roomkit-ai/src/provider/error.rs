//! What a provider's call fails with: Python's
//! `roomkit.providers.ai.ProviderError`.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

/// A provider's call that failed (RFC §6.7): Python's
/// `ProviderError(message, *, retryable=False, provider='', status_code=None,
/// context_overflow=None)`. The turn records its message; it never reaches
/// the room nor a model. The error it was raised from, when there is one,
/// is its [`source`](Error::source), as Python's `raise ... from`.
#[derive(Clone, Debug)]
pub struct ProviderError {
    /// What failed.
    pub message: String,
    /// Whether a retry may succeed.
    pub retryable: bool,
    /// The provider that failed, by its name.
    pub provider: String,
    /// The HTTP status the vendor answered, when it answered one.
    pub status_code: Option<u16>,
    /// Whether the request was past the model's context window, when the
    /// vendor said.
    pub context_overflow: Option<bool>,
    source: Option<Arc<dyn Error + Send + Sync>>,
}

/// What [`ProviderError::new`] takes beside the message: Python's
/// keywords, with its defaults.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProviderErrorOptions {
    /// Whether a retry may succeed.
    pub retryable: bool,
    /// The provider that failed, by its name.
    pub provider: String,
    /// The HTTP status the vendor answered.
    pub status_code: Option<u16>,
    /// Whether the request was past the model's context window.
    pub context_overflow: Option<bool>,
}

impl ProviderError {
    /// The failure `message`, as `options` qualify it.
    pub fn new(message: impl Into<String>, options: ProviderErrorOptions) -> Self {
        let ProviderErrorOptions {
            retryable,
            provider,
            status_code,
            context_overflow,
        } = options;
        Self {
            message: message.into(),
            retryable,
            provider,
            status_code,
            context_overflow,
            source: None,
        }
    }

    /// The same failure, raised from `source`: Python's `raise ... from`.
    #[must_use]
    pub fn with_source(mut self, source: impl Error + Send + Sync + 'static) -> Self {
        self.source = Some(Arc::new(source));
        self
    }
}

impl fmt::Display for ProviderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for ProviderError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        let source: &(dyn Error + 'static) = self.source.as_deref()?;
        Some(source)
    }
}

impl PartialEq for ProviderError {
    /// Two failures are the same when they say the same: their causes are
    /// not compared.
    fn eq(&self, other: &Self) -> bool {
        (
            &self.message,
            self.retryable,
            &self.provider,
            self.status_code,
            self.context_overflow,
        ) == (
            &other.message,
            other.retryable,
            &other.provider,
            other.status_code,
            other.context_overflow,
        )
    }
}

impl From<String> for ProviderError {
    /// A failure saying `message`, not retryable.
    fn from(message: String) -> Self {
        Self::new(message, ProviderErrorOptions::default())
    }
}

impl From<&str> for ProviderError {
    fn from(message: &str) -> Self {
        Self::from(message.to_owned())
    }
}

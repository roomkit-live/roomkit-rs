//! Classifier failures and Python's typed-read diagnostics.

use std::error::Error;
use std::sync::Arc;

use roomkit::{ChannelError, ClassedError};

/// A classifier could not answer its questions (RFC §6.8).
#[derive(Clone, Debug, thiserror::Error)]
#[error("{message}")]
pub struct ClassifierError {
    /// Why the classifier could not answer.
    pub message: String,
    #[source]
    source: Option<Arc<dyn Error + Send + Sync>>,
}

impl ClassifierError {
    /// A failure without an underlying cause.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            source: None,
        }
    }

    /// Keeps the original cause along with this diagnostic.
    #[must_use]
    pub fn with_source(mut self, source: impl Error + Send + Sync + 'static) -> Self {
        self.source = Some(Arc::new(source));
        self
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct Diagnostic(String);

pub(crate) fn invalid(class: &str, message: impl Into<String>) -> ChannelError {
    Box::new(ClassedError::new(class, Diagnostic(message.into())))
}

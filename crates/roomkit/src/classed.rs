//! An error a channel raises under its Python class's name (RFC §23.3 step
//! 6): the kit reads a boxed error of a type it does not know as a
//! `ChannelError`, and a crate built on it, roomkit-ai's provider errors
//! among them, names its class with this so that a task, a turn's record
//! and `wait_task` keep it as Python's do (RMK-1208).

use std::error::Error;
use std::fmt;

use roomkit_core::model::Metadata;

/// `error`, raised as an instance of `class`, its Python class, which the
/// kit records by its last name as Python's `error_type_name` reports an
/// error: a task's exception, ON_ERROR's `error_type`, a failed delivery's
/// code. Shown as `error` is, its causes `error`'s.
#[doc(hidden)]
#[derive(Debug)]
pub struct ClassedError {
    class: String,
    error: Box<dyn Error + Send + Sync>,
    attributes: Metadata,
}

impl ClassedError {
    /// `error`, raised as an instance of `class`, with no attributes.
    pub fn new(class: impl Into<String>, error: impl Error + Send + Sync + 'static) -> Self {
        Self {
            class: class.into(),
            error: Box::new(error),
            attributes: Metadata::new(),
        }
    }

    /// The same error, carrying `attributes`, those of its Python class's
    /// instance JSON carries (a provider error's `retryable`, `provider`,
    /// `status_code`, `context_overflow`): its record keeps them, as
    /// Python's `error_record` does (RFC §10.6.8).
    #[must_use]
    pub fn with_attributes(mut self, attributes: Metadata) -> Self {
        self.attributes = attributes;
        self
    }

    /// The attributes the error carries.
    pub fn attributes(&self) -> &Metadata {
        &self.attributes
    }

    /// The name the error is reported under, as Python's `error_type_name`
    /// reads its class: the last name of a qualified one
    /// (`roomkit.providers.ai.base:ProviderError` reads `ProviderError`).
    pub fn class(&self) -> &str {
        let name = self.class.rsplit(':').next().unwrap_or(&self.class);
        name.rsplit('.').next().unwrap_or(name)
    }

    /// The error raised.
    pub fn error(&self) -> &(dyn Error + Send + Sync + 'static) {
        &*self.error
    }
}

impl fmt::Display for ClassedError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.error, formatter)
    }
}

impl Error for ClassedError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.error.source()
    }
}

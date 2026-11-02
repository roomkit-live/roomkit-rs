//! An error a channel raises under its Python class's name (RFC §23.3 step
//! 6): the kit reads a boxed error of a type it does not know as a
//! `ChannelError`, and a crate built on it, roomkit-ai's provider errors
//! among them, names its class with this so that a task, a turn's record
//! and `wait_task` keep it as Python's do (gap G-097, RMK-1208).

use std::error::Error;
use std::fmt;

/// `error`, raised as an instance of `class`, the name Python gives its
/// class (`roomkit.providers.ai.base:ProviderError`): shown as `error` is,
/// its causes `error`'s.
#[doc(hidden)]
#[derive(Debug)]
pub struct ClassedError {
    class: String,
    error: Box<dyn Error + Send + Sync>,
}

impl ClassedError {
    /// `error`, raised as an instance of `class`.
    pub fn new(class: impl Into<String>, error: impl Error + Send + Sync + 'static) -> Self {
        Self {
            class: class.into(),
            error: Box::new(error),
        }
    }

    /// The name of the class the error is raised as.
    pub fn class(&self) -> &str {
        &self.class
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

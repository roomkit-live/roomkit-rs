//! What the image surface refuses before any request: Python's
//! `ValueError` of `parse_size`, `ImageCapabilities.validate_request` and a
//! provider's `generate`.

use std::fmt;

/// A request the image surface cannot express, refused before anything is
/// sent or billed: Python's `ValueError`, its message Python's.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImageRequestError {
    message: String,
}

impl ImageRequestError {
    /// The refusal `message`, as a provider raises one.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for ImageRequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ImageRequestError {}

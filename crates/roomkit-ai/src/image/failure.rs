//! How an image generation fails and tells of its attempts: Python's
//! `ImageGenerationError`, `ImageProgressCallback` and
//! `notify_image_progress` (RFC §25.2, §25.3).
//!
//! A failed generation keeps every attempt and every image it got, and is
//! never tried again on its own: the host decides. A progress callback is
//! told of each attempt; its failure fails the generation, the outcome
//! kept, and repeats no request.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use super::{ImageAttempt, ImageResult};
use crate::tools::BoxFuture;
use crate::{ProviderError, ProviderErrorOptions};

/// A failed generation with every outcome it got: Python's
/// `ImageGenerationError`, a `ProviderError` never retryable, its status
/// the first attempt's that has one. It is never tried again on its own.
/// The error it was raised from, when there is one, is its
/// [`source`](Error::source), as Python's `raise ... from`.
#[derive(Clone, Debug)]
pub struct ImageGenerationError {
    /// What failed.
    pub message: String,
    /// The provider that failed, by its name.
    pub provider: String,
    /// Every attempt, in order.
    pub attempts: Vec<ImageAttempt>,
    /// Every image the attempts got, in order.
    pub results: Vec<ImageResult>,
    source: Option<Arc<dyn Error + Send + Sync>>,
}

impl ImageGenerationError {
    /// The failure `message` of `provider`, after `attempts`.
    pub fn new(
        message: impl Into<String>,
        provider: impl Into<String>,
        attempts: Vec<ImageAttempt>,
    ) -> Self {
        let results = attempts
            .iter()
            .flat_map(|attempt| attempt.results.iter().cloned())
            .collect();
        Self {
            message: message.into(),
            provider: provider.into(),
            attempts,
            results,
            source: None,
        }
    }

    /// The same failure, raised from `source`: Python's `raise ... from`.
    #[must_use]
    pub fn with_source(mut self, source: impl Error + Send + Sync + 'static) -> Self {
        self.source = Some(Arc::new(source));
        self
    }

    /// The status of the first attempt that has one, as Python's
    /// `next(... if attempt.status_code)`: a status of 0 is none.
    fn first_status(&self) -> Option<u16> {
        self.attempts
            .iter()
            .find_map(|attempt| attempt.status_code.filter(|status| *status != 0))
    }

    /// The options of the `ProviderError` the failure is.
    fn options(&self) -> ProviderErrorOptions {
        ProviderErrorOptions {
            provider: self.provider.clone(),
            status_code: self.first_status(),
            ..ProviderErrorOptions::default()
        }
    }
}

impl PartialEq for ImageGenerationError {
    /// The same failure, of the same outcomes, whatever it was raised from.
    fn eq(&self, other: &Self) -> bool {
        self.message == other.message
            && self.provider == other.provider
            && self.attempts == other.attempts
            && self.results == other.results
    }
}

impl From<ImageGenerationError> for ProviderError {
    /// The provider's failure, never retryable, its message and its
    /// provider the generation's, its status the first attempt's that has
    /// one; the generation's failure is its source, every outcome kept.
    fn from(failure: ImageGenerationError) -> Self {
        Self::new(failure.message.clone(), failure.options()).with_source(failure)
    }
}

impl fmt::Display for ImageGenerationError {
    /// As the [`ProviderError`] it is reads, as Python's `str()`:
    /// `gemini (500): boom`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let failure = ProviderError::new(self.message.clone(), self.options());
        fmt::Display::fmt(&failure, f)
    }
}

impl Error for ImageGenerationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        let source: &(dyn Error + 'static) = self.source.as_deref()?;
        Some(source)
    }
}

/// What a progress callback's call ends with: done, or its failure.
type Progress = BoxFuture<'static, Result<(), Box<dyn Error + Send + Sync>>>;

/// What an image generation tells as each attempt moves, a copy of it:
/// Python's `ImageProgressCallback`. A failure of the callback fails the
/// generation, its outcome kept, and repeats no request.
pub type ImageProgressCallback = Arc<dyn Fn(ImageAttempt) -> Progress + Send + Sync>;

/// The failure of a callback, as the source of the generation it fails:
/// the callback's error itself, which a box does not make an `Error`.
#[derive(Debug)]
struct CallbackFailed(Box<dyn Error + Send + Sync>);

impl fmt::Display for CallbackFailed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl Error for CallbackFailed {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.0.source()
    }
}

/// Tells `callback`, when there is one, of `attempt`: Python's
/// `notify_image_progress`. For a provider.
///
/// # Errors
///
/// An [`ImageGenerationError`] of `provider`, holding `attempt`, raised
/// from the callback's failure, for a callback that failed.
#[doc(hidden)]
pub async fn notify_image_progress(
    callback: Option<&ImageProgressCallback>,
    attempt: &ImageAttempt,
    provider: &str,
) -> Result<(), ImageGenerationError> {
    let Some(callback) = callback else {
        return Ok(());
    };
    callback(attempt.clone()).await.map_err(|failed| {
        ImageGenerationError::new(
            format!("Image progress callback failed: {failed}"),
            provider,
            vec![attempt.clone()],
        )
        .with_source(CallbackFailed(failed))
    })
}

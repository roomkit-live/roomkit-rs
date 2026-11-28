//! How an image generation fails and tells of its attempts: Python's
//! `ImageGenerationError`, `ImageProgressCallback` and
//! `notify_image_progress` (RFC §25.2, §25.3).
//!
//! A failed generation keeps every attempt and every image it got, and is
//! never tried again on its own: the host decides. A progress callback is
//! told of each attempt; its failure fails the generation, the outcome
//! kept, and repeats no request.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use super::{ImageAttempt, ImageResult};
use crate::{ProviderError, ProviderErrorOptions};

/// A failed generation with every outcome it got: Python's
/// `ImageGenerationError`, a `ProviderError` never retryable, its status
/// the first attempt's that has one. It is never tried again on its own.
#[derive(Clone, Debug, PartialEq)]
pub struct ImageGenerationError {
    /// What failed.
    pub message: String,
    /// The provider that failed, by its name.
    pub provider: String,
    /// Every attempt, in order.
    pub attempts: Vec<ImageAttempt>,
    /// Every image the attempts got, in order.
    pub results: Vec<ImageResult>,
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
        }
    }

    /// The status of the first attempt that has one, as Python's
    /// `next(... if attempt.status_code)`: a status of 0 is none.
    fn first_status(&self) -> Option<u16> {
        self.attempts
            .iter()
            .find_map(|attempt| attempt.status_code.filter(|status| *status != 0))
    }
}

impl From<ImageGenerationError> for ProviderError {
    /// The provider's failure, never retryable, its message and its
    /// provider the generation's, its status the first attempt's that has
    /// one; the generation's failure is its source, every outcome kept.
    fn from(failure: ImageGenerationError) -> Self {
        let options = ProviderErrorOptions {
            provider: failure.provider.clone(),
            status_code: failure.first_status(),
            ..ProviderErrorOptions::default()
        };
        Self::new(failure.message.clone(), options).with_source(failure)
    }
}

impl fmt::Display for ImageGenerationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ImageGenerationError {}

/// What a progress callback's call ends with: done, or its failure.
type Progress =
    Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send>>;

/// What an image generation tells as each attempt moves, a copy of it:
/// Python's `ImageProgressCallback`. A failure of the callback fails the
/// generation, its outcome kept, and repeats no request.
pub type ImageProgressCallback = Arc<dyn Fn(ImageAttempt) -> Progress + Send + Sync>;

/// Tells `callback`, when there is one, of `attempt`: Python's
/// `notify_image_progress`. For a provider.
///
/// # Errors
///
/// An [`ImageGenerationError`] of `provider`, holding `attempt`, for a
/// callback that failed.
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
    })
}

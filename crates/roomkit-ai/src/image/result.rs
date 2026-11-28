//! What an image generation gives: Python's `ImageResult`, `ImageAttempt`
//! and `ImageGenerationError` (RFC §25.2, §25.3).
//!
//! A result's image is always a `data:` URI with a base64 payload, never
//! bare base64 nor a remote URL: what an [`AiImagePart`] already takes, so a
//! generated image enters a room, or comes back as the next edit's
//! reference, as it is. An attempt is one vendor call, its outcome kept
//! whatever it is; a failed generation keeps every attempt and every image
//! it got, and is never tried again on its own: the host decides.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use roomkit::model::Metadata;
use roomkit_core::rules::transcript::{DataUriError, parse_data_uri};
use serde::{Deserialize, Serialize};

use crate::{AiImagePart, ProviderError, ProviderErrorOptions};

/// Why a result's image is not one: Python's `ValidationError` of
/// `ImageResult.data`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImageResultError {
    /// It is no `data:` URI.
    NotADataUri,
    /// It carries no payload.
    NoPayload,
    /// Its payload is not declared base64.
    NotBase64,
}

impl fmt::Display for ImageResultError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::NotADataUri => {
                "ImageResult.data must be a data URI (data:<mime>;base64,<payload>)"
            }
            Self::NoPayload => "ImageResult.data is missing its base64 payload",
            Self::NotBase64 => "ImageResult.data must carry a base64 payload",
        })
    }
}

impl std::error::Error for ImageResultError {}

/// One generated image: Python's `ImageResult`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "Written")]
pub struct ImageResult {
    /// The image, as a `data:<mime_type>;base64,<payload>` URI.
    pub data: String,
    /// Its media type, the one `data` spells.
    pub mime_type: String,
    /// The prompt as the model rewrote it, where the vendor says; never a
    /// copy of the caller's.
    pub revised_prompt: Option<String>,
    /// The call's counters, disjoint: `input_tokens`, `input_image_tokens`,
    /// `output_tokens`, `output_image_tokens`; empty when the vendor says
    /// nothing.
    pub usage: Metadata,
    /// The attempt that gave the image.
    pub attempt_id: Option<String>,
    /// The vendor's id of the request.
    pub provider_request_id: Option<String>,
    /// The counters as the vendor wrote them.
    pub raw_usage: Metadata,
    /// The options the generation ran with.
    pub effective_options: Metadata,
    /// What else the vendor said of the image.
    pub metadata: Metadata,
    /// Its width, where known.
    pub width: Option<u32>,
    /// Its height, where known.
    pub height: Option<u32>,
}

/// A result as written, before its image is checked.
#[derive(Deserialize)]
struct Written {
    data: String,
    mime_type: String,
    #[serde(default)]
    revised_prompt: Option<String>,
    #[serde(default)]
    usage: Metadata,
    #[serde(default)]
    attempt_id: Option<String>,
    #[serde(default)]
    provider_request_id: Option<String>,
    #[serde(default)]
    raw_usage: Metadata,
    #[serde(default)]
    effective_options: Metadata,
    #[serde(default)]
    metadata: Metadata,
    #[serde(default)]
    width: Option<u32>,
    #[serde(default)]
    height: Option<u32>,
}

impl TryFrom<Written> for ImageResult {
    type Error = ImageResultError;

    fn try_from(written: Written) -> Result<Self, Self::Error> {
        let mut result = Self::new(written.data, written.mime_type)?;
        result.revised_prompt = written.revised_prompt;
        result.usage = written.usage;
        result.attempt_id = written.attempt_id;
        result.provider_request_id = written.provider_request_id;
        result.raw_usage = written.raw_usage;
        result.effective_options = written.effective_options;
        result.metadata = written.metadata;
        result.width = written.width;
        result.height = written.height;
        Ok(result)
    }
}

impl ImageResult {
    /// The image `data`, of the type `mime_type`, nothing else known of it.
    ///
    /// # Errors
    ///
    /// [`ImageResultError`] for `data` that is no `data:` URI, carries no
    /// payload, or does not declare it base64.
    pub fn new(
        data: impl Into<String>,
        mime_type: impl Into<String>,
    ) -> Result<Self, ImageResultError> {
        let data = data.into();
        let rest = data
            .strip_prefix("data:")
            .ok_or(ImageResultError::NotADataUri)?;
        let (header, payload) = rest.split_once(',').ok_or(ImageResultError::NoPayload)?;
        if payload.is_empty() {
            return Err(ImageResultError::NoPayload);
        }
        if !header.contains(";base64") {
            return Err(ImageResultError::NotBase64);
        }
        Ok(Self {
            data,
            mime_type: mime_type.into(),
            revised_prompt: None,
            usage: Metadata::new(),
            attempt_id: None,
            provider_request_id: None,
            raw_usage: Metadata::new(),
            effective_options: Metadata::new(),
            metadata: Metadata::new(),
            width: None,
            height: None,
        })
    }

    /// The image's bytes.
    ///
    /// # Errors
    ///
    /// [`DataUriError`] for a payload that is no base64: a corrupted
    /// result fails where it is read.
    pub fn decoded(&self) -> Result<Vec<u8>, DataUriError> {
        parse_data_uri(&self.data, Some(&self.mime_type)).map(|(_, bytes)| bytes)
    }

    /// The image as a message part: an AI's input, or the next edit's
    /// reference.
    #[must_use]
    pub fn to_image_part(&self) -> AiImagePart {
        AiImagePart {
            url: self.data.clone(),
            mime_type: Some(self.mime_type.clone()),
        }
    }
}

/// How an attempt stands: Python's `Literal` of `ImageAttempt.status`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImageAttemptStatus {
    /// Sent, no outcome yet.
    #[default]
    Started,
    /// A preview came.
    Preview,
    /// Its images came.
    Succeeded,
    /// It failed.
    Failed,
    /// Its outcome is unknown: cut off while the vendor may have run it.
    Unknown,
}

/// One vendor call, an outcome that gave no usable image included: Python's
/// `ImageAttempt`. Its usage is the whole call's; a result's is the same
/// measurement, not to be summed with it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ImageAttempt {
    /// The attempt's id, a random UUID unless given.
    pub id: String,
    /// Its place among the attempts of a generation.
    pub index: u32,
    /// How it stands.
    pub status: ImageAttemptStatus,
    /// The vendor's id of the request.
    pub provider_request_id: Option<String>,
    /// The call's counters.
    pub usage: Metadata,
    /// The counters as the vendor wrote them.
    pub raw_usage: Metadata,
    /// The options it ran with.
    pub effective_options: Metadata,
    /// What else the vendor said.
    pub metadata: Metadata,
    /// The images it gave.
    pub results: Vec<ImageResult>,
    /// What failed, where it did.
    pub error: Option<String>,
    /// The status the vendor answered a failure with.
    pub status_code: Option<u16>,
}

impl Default for ImageAttempt {
    /// An attempt just sent, of a new id.
    fn default() -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            index: 0,
            status: ImageAttemptStatus::Started,
            provider_request_id: None,
            usage: Metadata::new(),
            raw_usage: Metadata::new(),
            effective_options: Metadata::new(),
            metadata: Metadata::new(),
            results: Vec::new(),
            error: None,
            status_code: None,
        }
    }
}

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

#[cfg(test)]
#[path = "result_tests.rs"]
mod tests;

//! What an image generation gives: Python's `ImageResult` and
//! `ImageAttempt` (RFC §25.2, §25.3).
//!
//! A result's image is always a `data:` URI with a base64 payload, never
//! bare base64 nor a remote URL: what an [`AiImagePart`] already takes, so a
//! generated image enters a room, or comes back as the next edit's
//! reference, as it is. An attempt is one vendor call, its outcome kept
//! whatever it is.

use std::fmt;

use roomkit::model::Metadata;
use roomkit_core::rules::transcript::{DataUriError, parse_data_uri};
use serde::{Deserialize, Serialize};

use crate::AiImagePart;

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

#[cfg(test)]
#[path = "result_tests.rs"]
mod tests;

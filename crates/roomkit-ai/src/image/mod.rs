//! Image generation (RFC §25): Python's `roomkit.providers.image`.
//!
//! Image synthesis is a capability few models hold, and the model holding a
//! conversation is rarely one of them, so it has its own surface, as speech
//! has: an agent conversing through one vendor draws through another. A
//! generated image is a `data:` URI an [`AiImagePart`](crate::AiImagePart)
//! takes as it is, so it enters a room, or comes back as the next edit's
//! reference, without conversion.

mod capabilities;
mod catalog;
mod failure;
mod geometry;
mod mock;
mod options;
mod provider;
mod refusal;
mod result;

use roomkit_core::rules::transcript;

pub use self::capabilities::ImageCapabilities;
pub use self::catalog::{ImageCatalogEntry, ImageModelInfo};
pub use self::failure::{ImageGenerationError, ImageProgressCallback, notify_image_progress};
pub use self::geometry::{
    PayloadMimeTypeOptions, SniffMimeTypeOptions, parse_size, payload_mime_type, sniff_mime_type,
};
pub use self::mock::{MockImageProvider, MockImageProviderOptions};
pub use self::options::{
    ImageBackground, ImageInputFidelity, ImageModeration, ImageOptions, ImageOutputFormat,
    ImageQuality, ImageSearchType, ImageSize, ImageThinkingLevel,
};
pub use self::provider::{GenerateOptions, GenerateWithOptionsOptions, ImageError, ImageProvider};
pub use self::refusal::ImageRequestError;
pub use self::result::{ImageAttempt, ImageAttemptStatus, ImageResult, ImageResultError};
pub use roomkit_core::rules::transcript::DataUriError;

/// The capability tag of an image-generating model's catalog entry:
/// Python's `IMAGE_GEN_CAPABILITY`. The image catalog and the conversational
/// one are disjoint; the tag is for a consumer that merges them on purpose.
pub const IMAGE_GEN_CAPABILITY: &str = "image_gen";

/// The statuses a failed image generation may be tried again after, the
/// vendor having run nothing: Python's `PAID_GENERATION_RETRYABLE`. A 500,
/// 502 or 504 may follow a generation the vendor ran and billed, so it
/// stays final and the host decides (RFC §25.2). For a provider.
#[doc(hidden)]
pub const PAID_GENERATION_RETRYABLE: [u16; 4] = [408, 409, 429, 503];

/// What [`parse_data_uri`] takes beside the URI: Python's keyword, with its
/// default.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ParseDataUriOptions {
    /// The media type of a URI whose header names none: `image/png` unless
    /// given.
    pub fallback_mime: Option<String>,
}

/// The media type and the bytes of the `data:` URI `url`: Python's
/// `parse_data_uri`. Whitespace in the payload and a missing padding are
/// repaired; anything else is refused.
///
/// # Errors
///
/// [`DataUriError`] for a URL that is no `data:` URI, carries no payload,
/// or whose payload is no base64.
pub fn parse_data_uri(
    url: &str,
    options: ParseDataUriOptions,
) -> Result<(String, Vec<u8>), DataUriError> {
    let ParseDataUriOptions { fallback_mime } = options;
    transcript::parse_data_uri(url, fallback_mime.as_deref())
}

/// The `data:` URI of `data`, of the media type `mime_type`: Python's
/// `to_data_uri`.
#[must_use]
pub fn to_data_uri(data: &[u8], mime_type: &str) -> String {
    transcript::to_data_uri(data, mime_type)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Python: tests/test_providers/test_image.py::test_parse_data_uri_takes_the_media_type_from_the_header_then_the_fallback
    #[test]
    fn parse_data_uri_takes_the_media_type_from_the_header_then_the_fallback() {
        let fallback = |kind: &str| ParseDataUriOptions {
            fallback_mime: Some(kind.to_owned()),
        };
        let kind = |url, options| parse_data_uri(url, options).map(|(kind, _)| kind);
        assert_eq!(
            kind("data:image/jpeg;base64,QUJD", fallback("image/png")),
            Ok("image/jpeg".into())
        );
        assert_eq!(
            kind("data:;base64,QUJD", fallback("image/webp")),
            Ok("image/webp".into())
        );
        assert_eq!(
            kind("data:;base64,QUJD", ParseDataUriOptions::default()),
            Ok("image/png".into())
        );
    }

    // Python: tests/test_providers/test_image.py::test_to_data_uri_round_trips
    #[test]
    fn to_data_uri_round_trips() {
        let uri = to_data_uri(b"\x89PNG\r\n\x1a\n", "image/png");
        assert!(uri.starts_with("data:image/png;base64,"), "{uri}");
        let back = parse_data_uri(&uri, ParseDataUriOptions::default());
        assert_eq!(
            back,
            Ok(("image/png".to_owned(), b"\x89PNG\r\n\x1a\n".to_vec()))
        );
    }
}

//! Representations of an image already present in a text conversation (RFC §6.7).

use std::error::Error;

use base64::{Engine, engine::general_purpose::STANDARD};
use roomkit_core::rules::transcript::{DataUriError, parse_data_uri, to_data_uri};

use super::{AiImagePart, ProviderError, ProviderErrorOptions};

#[derive(Debug, thiserror::Error)]
#[error("expected a data: URI, got a {0} URL")]
struct RemoteUri(String);

fn invalid_image(provider: &str, cause: impl Error + Send + Sync + 'static) -> ProviderError {
    ProviderError::new(
        format!("invalid image part: {cause}"),
        ProviderErrorOptions {
            provider: provider.into(),
            ..Default::default()
        },
    )
    .with_source(cause)
}

/// The media type and decoded bytes of an inline image. A missing media type
/// falls back to the part's type, then to `image/png`.
///
/// # Errors
///
/// Returns a non-retryable provider error for a remote URL or invalid data URI.
pub fn image_part_payload(
    part: &AiImagePart,
    provider: &str,
) -> Result<(String, Vec<u8>), ProviderError> {
    parse_data_uri(&part.url, part.mime_type.as_deref()).map_err(|cause| match cause {
        DataUriError::NotADataUri => {
            let scheme = part.url.split(':').next().unwrap_or_default();
            invalid_image(provider, RemoteUri(scheme.into()))
        }
        DataUriError::NoPayload | DataUriError::NotBase64 => invalid_image(provider, cause),
    })
}

/// The media type and canonical base64 payload of an inline image.
///
/// # Errors
///
/// Returns the same provider error as [`image_part_payload`].
pub fn image_part_base64(
    part: &AiImagePart,
    provider: &str,
) -> Result<(String, String), ProviderError> {
    let (kind, data) = image_part_payload(part, provider)?;
    Ok((kind, STANDARD.encode(data)))
}

/// A URI suitable for forwarding. Remote URLs pass through untouched. Inline
/// images are decoded and rebuilt with their media type and canonical base64.
///
/// # Errors
///
/// Returns the same provider error as [`image_part_payload`] for invalid data URIs.
pub fn image_part_uri(part: &AiImagePart, provider: &str) -> Result<String, ProviderError> {
    if !part.url.starts_with("data:") {
        return Ok(part.url.clone());
    }
    let (kind, data) = image_part_payload(part, provider)?;
    Ok(to_data_uri(&data, &kind))
}

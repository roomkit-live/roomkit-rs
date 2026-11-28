//! A size as the image surface spells it, and the media type of an image's
//! bytes: Python's `parse_size`, `sniff_mime_type` and `payload_mime_type`
//! (`roomkit.providers.image`).

use std::fmt;

use base64::{Engine, engine::general_purpose::STANDARD};

/// The media type an image whose bytes say nothing is given.
const FALLBACK: &str = "image/png";

/// What [`sniff_mime_type`] takes beside the bytes: Python's keyword, with
/// its default.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SniffMimeTypeOptions {
    /// The media type of bytes whose magic number says nothing:
    /// `image/png` unless given.
    pub fallback: String,
}

impl Default for SniffMimeTypeOptions {
    fn default() -> Self {
        Self {
            fallback: FALLBACK.to_owned(),
        }
    }
}

/// What [`payload_mime_type`] takes beside the declared type and the
/// payload: Python's keyword, with its default.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PayloadMimeTypeOptions {
    /// The media type of a payload that declares none and whose leading
    /// bytes say nothing: `image/png` unless given.
    pub fallback: String,
}

impl Default for PayloadMimeTypeOptions {
    fn default() -> Self {
        Self {
            fallback: FALLBACK.to_owned(),
        }
    }
}

/// Why a size does not read: Python's `ValueError` of `parse_size`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SizeError {
    /// The size given.
    size: String,
    /// Whether it named its two dimensions, one of them not positive.
    not_positive: bool,
}

impl fmt::Display for SizeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let size = &self.size;
        if self.not_positive {
            write!(f, "size must have positive dimensions, got '{size}'")
        } else {
            write!(
                f,
                "size must be 'WIDTHxHEIGHT' (e.g. '1024x1024'), got '{size}'"
            )
        }
    }
}

impl std::error::Error for SizeError {}

/// The width and the height `size` names: two whole numbers joined by `x`
/// (in either case), the whitespace around them ignored.
///
/// # Errors
///
/// [`SizeError`] for anything else, or a dimension that is not positive.
pub fn parse_size(size: &str) -> Result<(u32, u32), SizeError> {
    let refused = |not_positive| SizeError {
        size: size.to_owned(),
        not_positive,
    };
    let written = size.trim().to_lowercase();
    let (width, height) = written.split_once('x').ok_or_else(|| refused(false))?;
    let digits = |text: &str| !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit());
    if !digits(width) || !digits(height) {
        return Err(refused(false));
    }
    // Digits that overflow are still digits: Python reads them, and the
    // dimension is positive.
    let read = |text: &str| text.parse::<u32>().unwrap_or(u32::MAX);
    let (width, height) = (read(width), read(height));
    if width == 0 || height == 0 {
        return Err(refused(true));
    }
    Ok((width, height))
}

/// The media type of an image's bytes, read from their magic number: the
/// formats image APIs return (PNG, JPEG, WebP, GIF), else the options'
/// fallback.
#[must_use]
pub fn sniff_mime_type(data: &[u8], options: SniffMimeTypeOptions) -> String {
    let kind = if data.starts_with(b"\x89PNG\r\n\x1a\n") {
        "image/png"
    } else if data.starts_with(b"\xff\xd8\xff") {
        "image/jpeg"
    } else if data.get(..4) == Some(b"RIFF") && data.get(8..12) == Some(b"WEBP") {
        "image/webp"
    } else if data.starts_with(b"GIF87a") || data.starts_with(b"GIF89a") {
        "image/gif"
    } else {
        return options.fallback;
    };
    kind.to_owned()
}

/// The media type of a base64 image: the one its answer `declared`, else
/// the one its leading bytes show, else the options' fallback.
#[must_use]
pub fn payload_mime_type(
    declared: Option<&str>,
    payload: &str,
    options: PayloadMimeTypeOptions,
) -> String {
    if let Some(declared) = declared.filter(|declared| !declared.is_empty()) {
        return declared.to_owned();
    }
    // 32 base64 characters decode the 24 leading bytes every magic number
    // fits in.
    let head = payload.get(..payload.len().min(32)).unwrap_or(payload);
    let fallback = options.fallback;
    match STANDARD.decode(head) {
        Ok(head) => sniff_mime_type(&head, SniffMimeTypeOptions { fallback }),
        Err(_) => fallback,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Python: tests/test_providers/test_image.py::test_parse_size_accepts_wxh
    #[test]
    fn parse_size_accepts_wxh() {
        for (value, expected) in [
            ("1024x1024", (1024, 1024)),
            ("1536X1024", (1536, 1024)),
            (" 512x512 ", (512, 512)),
        ] {
            assert_eq!(parse_size(value), Ok(expected), "{value}");
        }
    }

    // Python: tests/test_providers/test_image.py::test_parse_size_rejects_everything_else
    #[test]
    fn parse_size_rejects_everything_else() {
        for value in ["1024", "1024*1024", "axb", "", "1024x", "-1x10", "0x100"] {
            let refused = parse_size(value).unwrap_err();
            assert!(refused.to_string().contains("size"), "{value}");
        }
    }

    // Python: tests/test_providers/test_image.py::test_sniff_mime_type_reads_the_magic_number
    #[test]
    fn sniff_mime_type_reads_the_magic_number() {
        let heads: [(&[u8], &str); 4] = [
            (b"\x89PNG\r\n\x1a\n rest", "image/png"),
            (b"\xff\xd8\xff\xe0 rest", "image/jpeg"),
            (b"RIFF\x00\x00\x00\x00WEBPVP8 ", "image/webp"),
            (b"GIF89a rest", "image/gif"),
        ];
        for (head, expected) in heads {
            assert_eq!(
                sniff_mime_type(head, SniffMimeTypeOptions::default()),
                expected
            );
        }
    }

    // Python: tests/test_providers/test_image.py::test_sniff_mime_type_falls_back_when_bytes_say_nothing
    #[test]
    fn sniff_mime_type_falls_back_when_bytes_say_nothing() {
        let jpeg = SniffMimeTypeOptions {
            fallback: "image/jpeg".to_owned(),
        };
        assert_eq!(
            sniff_mime_type(b"not an image", SniffMimeTypeOptions::default()),
            "image/png"
        );
        assert_eq!(sniff_mime_type(b"", jpeg), "image/jpeg");
    }

    // Python: payload_mime_type(None, base64 of a JPEG head) and with a
    // declared type, an unread payload
    #[test]
    fn a_payload_s_type_is_the_declared_one_else_its_magic_number() {
        let by_default = PayloadMimeTypeOptions::default;
        let gif = PayloadMimeTypeOptions {
            fallback: "image/gif".to_owned(),
        };
        let jpeg = STANDARD.encode(b"\xff\xd8\xff\xe0 the rest of a jpeg");
        assert_eq!(payload_mime_type(None, &jpeg, by_default()), "image/jpeg");
        assert_eq!(
            payload_mime_type(Some("image/webp"), &jpeg, by_default()),
            "image/webp"
        );
        assert_eq!(
            payload_mime_type(Some(""), &jpeg, by_default()),
            "image/jpeg"
        );
        assert_eq!(payload_mime_type(None, "!!", gif), "image/gif");
    }
}

//! An author's image as a turn sends it (RFC §6.4). This module never
//! resolves references or reads bytes; application references remain
//! opaque until the runtime asks its host. Which images a turn sends is the
//! channel's to decide.

/// The most bytes an inline image holds, read off its base64's length:
/// Anthropic's documented per-image limit, Python's
/// `MAX_AUTHOR_IMAGE_BYTES`.
const MAX_INLINE_BYTES: usize = 5 * 1024 * 1024;

/// An image URL or opaque application reference, separate from prompt text.
#[derive(Clone, PartialEq, Eq)]
pub struct Image {
    url: String,
    mime_type: String,
}

impl std::fmt::Debug for Image {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Image")
            .field("mime_type", &self.mime_type)
            .finish_non_exhaustive()
    }
}

impl Image {
    /// Accept a supported image with a safe URL/reference (RFC §6.4). Inline
    /// base64 holds some data and at most 5 MiB, read off its length as
    /// Python's `MAX_AUTHOR_IMAGE_BYTES` reads it; local paths and
    /// executable schemes are refused. Invalid or unsupported media returns
    /// `None`, and the channel reads a mention in its place.
    pub fn new(url: &str, mime_type: &str) -> Option<Self> {
        let mime_type = mime_type.split(';').next()?.trim().to_ascii_lowercase();
        let mime_type = if mime_type == "image/jpg" {
            "image/jpeg".to_owned()
        } else {
            mime_type
        };
        if !matches!(
            mime_type.as_str(),
            "image/png" | "image/jpeg" | "image/webp" | "image/gif"
        ) {
            return None;
        }
        let (scheme, rest) = url.split_once(':')?;
        let lower = scheme.to_ascii_lowercase();
        if lower == "data" {
            let (header, data) = rest.split_once(',')?;
            let header = header
                .to_ascii_lowercase()
                .replace("image/jpg;", "image/jpeg;");
            if header != format!("{mime_type};base64")
                || data.is_empty()
                || data.len() % 4 != 0
                || data.len() * 3 / 4 > MAX_INLINE_BYTES
                || !data
                    .trim_end_matches('=')
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'/'))
                || data.len() - data.trim_end_matches('=').len() > 2
            {
                return None;
            }
            return Some(Self {
                url: format!("data:{header},{data}"),
                mime_type,
            });
        }
        if !rest.starts_with("//")
            || rest.len() <= 2
            || scheme.len() < 2
            || matches!(lower.as_str(), "file" | "ftp" | "javascript" | "vbscript")
            || !scheme.bytes().enumerate().all(|(i, b)| {
                b.is_ascii_alphabetic()
                    || i > 0 && (b.is_ascii_digit() || matches!(b, b'+' | b'-' | b'.'))
            })
            || url.contains('\\')
            || url.chars().any(char::is_control)
            || rest.split('/').any(|part| part == "..")
        {
            return None;
        }
        let scheme = if matches!(lower.as_str(), "http" | "https") {
            &lower
        } else {
            scheme
        };
        Some(Self {
            url: format!("{scheme}:{rest}"),
            mime_type,
        })
    }

    /// The canonical URL or opaque application reference. Never a local path.
    pub fn url(&self) -> &str {
        &self.url
    }
    /// Canonical supported image MIME type.
    pub fn mime_type(&self) -> &str {
        &self.mime_type
    }
    /// Whether the provider can receive this image without host resolution.
    pub fn is_resolved(&self) -> bool {
        self.url.starts_with("data:")
            || self.url.starts_with("https://")
            || self.url.starts_with("http://")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn image_urls_never_become_local_paths_and_debug_never_prints_payloads() {
        for url in [
            "file:///private.png",
            "C://private.png",
            "javascript://code",
            "photo.png",
            "media://../secret",
            "media://a\\secret",
        ] {
            assert!(Image::new(url, "image/png").is_none());
        }
        let image = Image::new("DATA:IMAGE/JPG;BASE64,AQIDBA==", "IMAGE/JPG; x=1").unwrap();
        assert_eq!(image.url(), "data:image/jpeg;base64,AQIDBA==");
        assert!(!format!("{image:?}").contains("AQIDBA"));
        assert!(Image::new("data:image/png;base64,@@@@", "image/png").is_none());
        assert!(Image::new("media://safe", "application/pdf").is_none());
        assert!(
            !Image::new("media://safe", "image/png")
                .unwrap()
                .is_resolved()
        );
    }
}

//! Bounded author images (RFC §6.4). This module never resolves references or
//! reads bytes; application references remain opaque until the runtime asks its host.
use crate::model::EventContent;

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
    /// base64 is bounded to 5 MB; local paths and executable schemes are refused.
    /// Invalid or unsupported media returns `None` and keeps its text mention.
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
                || data.len() / 4 * 3 - data.chars().rev().take_while(|c| *c == '=').count().min(2)
                    > 5_000_000
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

pub(super) fn collect(content: &EventContent) -> Vec<Image> {
    let mut images = Vec::new();
    visit(content, &mut images);
    images
}

fn visit(content: &EventContent, images: &mut Vec<Image>) {
    match content {
        EventContent::Media(media) => {
            if let Some(image) = Image::new(&media.url, &media.mime_type) {
                if images.len() == 4 {
                    images.remove(0);
                }
                images.push(image);
            }
        }
        EventContent::Composite(parts) => {
            for part in parts.parts() {
                visit(part, images);
            }
        }
        EventContent::Text(_)
        | EventContent::Rich(_)
        | EventContent::Location(_)
        | EventContent::Audio(_)
        | EventContent::Video(_)
        | EventContent::System(_)
        | EventContent::Template(_)
        | EventContent::Edit(_)
        | EventContent::Delete(_)
        | EventContent::ToolCall(_) => {}
    }
}

pub(super) fn bound(messages: &mut [super::Message]) {
    let mut remaining: usize = 4;
    for message in messages.iter_mut().rev() {
        let remove = message.images.len().saturating_sub(remaining);
        message.images.drain(..remove);
        remaining = remaining.saturating_sub(message.images.len());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rules::transcript::{Message, Role};
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
    #[test]
    fn newest_four_images_are_kept_without_dropping_text_mentions() {
        let mut messages: Vec<_> = (0..6)
            .map(|id| {
                let mut message = Message::new(Role::User, format!("image {id}"));
                message
                    .images
                    .push(Image::new(&format!("media://{id}"), "image/png").unwrap());
                message
            })
            .collect();
        bound(&mut messages);
        assert_eq!(messages.len(), 6);
        assert_eq!(
            messages
                .iter()
                .flat_map(|m| &m.images)
                .map(Image::url)
                .collect::<Vec<_>>(),
            ["media://2", "media://3", "media://4", "media://5"]
        );
        assert_eq!(messages.first().unwrap().text, "image 0");
    }
}

//! An author's image as a turn sends it (RFC §6.4). This module never
//! resolves references or reads bytes; application references remain
//! opaque until the runtime asks its host. Which images a turn sends is the
//! channel's to decide.

use super::data_uri::to_data_uri;

/// The most bytes an image holds: Anthropic's documented per-image limit,
/// Python's `MAX_AUTHOR_IMAGE_BYTES`. An inline image's are read off its
/// base64's length, a resolved image's on its bytes.
const MAX_IMAGE_BYTES: usize = 5 * 1024 * 1024;

/// The image types every vision provider takes: Python's
/// `VISION_IMAGE_TYPES`.
const VISION_IMAGE_TYPES: [&str; 4] = ["image/png", "image/jpeg", "image/webp", "image/gif"];

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
    /// Accept an image of a type every vision provider takes (RFC §6.4), as
    /// Python's `_inline` reads it. A `data:` URI holds some data and at
    /// most 5 MiB, read off its length, and goes as written, its scheme,
    /// type and parameters in lower case (Python's `canonical_data_uri`):
    /// the base64's form is the provider's to judge. A link (`http`,
    /// `https`) goes as written, its scheme in lower case: the provider
    /// fetches it. An application reference is held to what a reader could
    /// not take for something else: a scheme of two characters at least,
    /// none a reader fetches, opens or runs, no `..` segment, backslash or
    /// control character. Anything else returns `None`, and the channel
    /// reads a mention in its place.
    pub fn new(url: &str, mime_type: &str) -> Option<Self> {
        let mime_type = vision_type(mime_type)?;
        let (scheme, rest) = url.split_once(':')?;
        let lower = scheme.to_ascii_lowercase();
        if lower == "data" {
            return inline(url, mime_type);
        }
        if matches!(lower.as_str(), "http" | "https") && rest.starts_with("//") {
            return Some(Self {
                url: format!("{lower}:{rest}"),
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
        Some(Self {
            url: url.to_owned(),
            mime_type,
        })
    }

    /// The image `data` holds, as a base64 `data:` URI of the type
    /// `mime_type`: some data and at most 5 MiB of it, read on the bytes,
    /// as Python's `_resolved` reads what its resolver gives; `None`
    /// otherwise, or for a type no vision provider takes.
    pub fn from_bytes(data: &[u8], mime_type: &str) -> Option<Self> {
        let mime_type = vision_type(mime_type)?;
        if data.is_empty() || data.len() > MAX_IMAGE_BYTES {
            return None;
        }
        Some(Self {
            url: to_data_uri(data, &mime_type),
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

/// `mime_type` as RoomKit compares it: lowercased, its parameters aside,
/// its aliases folded (`image/jpg` and `image/pjpeg` are `image/jpeg`).
/// Python's `media_type`.
#[must_use]
pub fn media_type(mime_type: &str) -> String {
    let bare = mime_type
        .split(';')
        .next()
        .unwrap_or(mime_type)
        .trim()
        .to_lowercase();
    match bare.as_str() {
        "image/jpg" | "image/pjpeg" => "image/jpeg".to_owned(),
        _ => bare,
    }
}

/// `mime_type` folded, when every vision provider takes it.
fn vision_type(mime_type: &str) -> Option<String> {
    let folded = media_type(mime_type);
    VISION_IMAGE_TYPES
        .contains(&folded.as_str())
        .then_some(folded)
}

/// The data URI `url` as Python's `_inline` sends it: refused only when
/// its data, what follows its first comma, is empty or past 5 MiB, read
/// off its length (`len(data) * 3 // 4`); spelled as
/// [`canonical_data_uri`] spells it.
fn inline(url: &str, mime_type: String) -> Option<Image> {
    let data = url.split_once(',').map_or(url, |(_, data)| data);
    let size = data.chars().count() * 3 / 4;
    if size == 0 || size > MAX_IMAGE_BYTES {
        return None;
    }
    Some(Image {
        url: canonical_data_uri(url),
        mime_type,
    })
}

/// The `data:` URI `url` spelled as every vendor reads it: the scheme and
/// the parameters in lower case, the type folded as [`media_type`] folds
/// it, the data as written. Python's `canonical_data_uri`.
fn canonical_data_uri(url: &str) -> String {
    let (header, data) = match url.split_once(',') {
        Some((header, data)) => (header, Some(data)),
        None => (url, None),
    };
    let mut fields = header.get("data:".len()..).unwrap_or_default().split(';');
    let mut spelled = format!("data:{}", media_type(fields.next().unwrap_or_default()));
    for parameter in fields {
        spelled.push(';');
        spelled.push_str(&parameter.trim().to_lowercase());
    }
    if let Some(data) = data {
        spelled.push(',');
        spelled.push_str(data);
    }
    spelled
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
        assert!(Image::new("data:image/png;base64,", "image/png").is_none());
        assert!(Image::new("media://safe", "application/pdf").is_none());
        assert!(
            !Image::new("media://safe", "image/png")
                .unwrap()
                .is_resolved()
        );
    }

    // Python: tests/test_author_images.py::TestConstraints::test_a_data_uri_is_spelled_as_every_vendor_reads_it
    #[test]
    fn a_data_uri_is_spelled_as_every_vendor_reads_it() {
        let spelled = [
            "DATA:image/JPG;BASE64,AA==",
            "data:image/png;base64,AA==",
            "data:image/png;Name=Lamp.png; BASE64 ,AQ-_\nBA",
            "data:image/png,AQIDBA==",
        ]
        .map(|url| Image::new(url, "image/png").map(|image| image.url().to_owned()));
        assert_eq!(
            spelled,
            [
                Some("data:image/jpeg;base64,AA==".to_owned()),
                Some("data:image/png;base64,AA==".to_owned()),
                Some("data:image/png;name=lamp.png;base64,AQ-_\nBA".to_owned()),
                Some("data:image/png,AQIDBA==".to_owned()),
            ]
        );
    }

    /// Python's `_inline` and `_resolved` read 5 MiB apart: an inline
    /// image's size off its base64's length, a resolved one's on its bytes.
    // Python: tests/test_author_images.py::TestMention::test_an_image_that_does_not_resolve_leaves_the_turn_a_mention
    #[test]
    fn five_mib_is_read_off_a_data_uri_and_on_resolved_bytes() {
        let inline = |bytes: usize| {
            let data = "A".repeat(bytes.div_ceil(3) * 4);
            Image::new(&format!("data:image/png;base64,{data}"), "image/png").is_some()
        };
        let resolved = |bytes: usize| Image::from_bytes(&vec![0; bytes], "image/png").is_some();
        let max = MAX_IMAGE_BYTES;
        assert_eq!(
            [
                inline(max - 2),
                inline(max),
                resolved(max - 1),
                resolved(max)
            ],
            [true, false, true, true]
        );
        assert_eq!([resolved(0), resolved(max + 1)], [false, false]);
    }

    // Python: tests/test_author_images.py::TestReference::test_a_link_goes_to_the_provider_as_it_is
    #[test]
    fn a_link_goes_to_the_provider_as_it_is() {
        for link in [
            "https://cdn.example.com/lamp.png",
            "https://cdn.example.com/a/../lamp.png",
            "https://cdn.example.com/a\\lamp.png",
            "http://cdn.example.com/lamp\u{1}.png",
        ] {
            let image = Image::new(link, "image/png").map(|image| image.url().to_owned());
            assert_eq!(image.as_deref(), Some(link));
        }
    }
}

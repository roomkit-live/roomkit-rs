//! How a media URL is read when its content is built (RFC §6.4): a `data:`
//! URI, a link, or a reference, as the Python implementation's
//! `roomkit._media_url` reads it.

use super::ModelError;

/// Why a media content's `url` is refused, Python's message.
pub(super) const MEDIA_URL_RULE: &str = "URL must be a link (http://, https://), a data: URI, or a \
     reference <scheme>://... (two letters of scheme at least, not file, ftp, javascript or \
     vbscript, no '..' segment and no backslash)";

/// Why an audio or a video URL is refused, Python's message.
pub(super) const PLAYABLE_URL_RULE: &str = "URL must start with http://, https://, or data:";

/// The schemes a reader could fetch or open itself, or run.
const REFUSED_SCHEMES: [&str; 4] = ["file", "ftp", "javascript", "vbscript"];

/// A media content's `url`, its scheme lower-cased when it is a link or a
/// `data:` URI, as every reader tests it; a reference keeps its scheme,
/// which is the application's to spell.
pub(super) fn media_url(url: String) -> Result<String, ModelError> {
    if is_link(&url) || is_data_uri(&url) {
        return Ok(lowercase_scheme(&url));
    }
    if is_safe_reference(&url) {
        return Ok(url);
    }
    Err(refused(url, MEDIA_URL_RULE))
}

/// An audio or a video URL: a link or a `data:` URI, as written (Python's
/// prefix test is case-sensitive).
pub(super) fn playable_url(url: String) -> Result<String, ModelError> {
    if ["http://", "https://", "data:"]
        .iter()
        .any(|prefix| url.starts_with(prefix))
    {
        Ok(url)
    } else {
        Err(refused(url, PLAYABLE_URL_RULE))
    }
}

fn refused(value: String, reason: &'static str) -> ModelError {
    ModelError::InvalidMediaUrl { value, reason }
}

/// Whether `url` carries its media inline.
fn is_data_uri(url: &str) -> bool {
    starts_with_ignoring_case(url, "data:")
}

/// Whether `url` is a link a reader fetches.
fn is_link(url: &str) -> bool {
    starts_with_ignoring_case(url, "http://") || starts_with_ignoring_case(url, "https://")
}

fn starts_with_ignoring_case(url: &str, prefix: &str) -> bool {
    url.get(..prefix.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(prefix))
}

fn lowercase_scheme(url: &str) -> String {
    match url.split_once(':') {
        Some((scheme, rest)) => format!("{}:{rest}", scheme.to_ascii_lowercase()),
        None => url.to_owned(),
    }
}

/// Whether `url` is a reference no reader could take for something else:
/// `<scheme>://…` with a scheme of two characters at least (one reads as a
/// Windows drive), a letter then letters, digits, `+`, `.` or `-`, not a
/// scheme a reader fetches, opens or runs, no `..` segment and no
/// backslash.
fn is_safe_reference(url: &str) -> bool {
    let Some((scheme, rest)) = url.split_once("://") else {
        return false;
    };
    let mut characters = scheme.chars();
    let well_formed = characters.next().is_some_and(|c| c.is_ascii_alphabetic())
        && scheme.len() >= 2
        && characters.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '.' | '-'));
    well_formed
        && !REFUSED_SCHEMES.contains(&scheme.to_ascii_lowercase().as_str())
        && !rest.contains('\\')
        && !rest.split('/').any(|segment| segment == "..")
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    /// Python's probe `tasks/probes/step5-lot2/media_urls.py`: what each
    /// URL reads as, or why it is refused.
    #[test]
    fn a_media_url_reads_as_python_reads_it() {
        let read = |url: &str| media_url(url.to_owned()).map_err(|_| ());
        assert_eq!(
            [
                "javascript:alert(1)",
                "file:///etc/passwd",
                "app://a/../b",
                "c://x",
                "app://a\\b",
                "FILE:///x",
                "HTTPS://A/b.png",
                "DATA:image/png;base64,AA",
                "App://a/b",
            ]
            .map(read),
            [
                Err(()),
                Err(()),
                Err(()),
                Err(()),
                Err(()),
                Err(()),
                Ok("https://A/b.png".to_owned()),
                Ok("data:image/png;base64,AA".to_owned()),
                Ok("App://a/b".to_owned()),
            ]
        );
    }

    /// A scheme `://` matches as the first `://` of the URL, so a reference
    /// whose path holds another is read by its first scheme.
    #[test]
    fn a_reference_is_read_by_its_first_scheme() {
        assert!(is_safe_reference("app://a/b?next=https://c"));
        assert!(!is_safe_reference("1app://a"));
    }
}

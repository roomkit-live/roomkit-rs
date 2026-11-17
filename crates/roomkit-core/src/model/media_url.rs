//! How a media URL is read when its content is built (RFC §6.4): a `data:`
//! URI, a link, or a reference, as the Python implementation's
//! `roomkit._media_url` reads it.

use std::borrow::Cow;

use super::ModelError;

/// Why a media content's `url` is refused, Python's message.
pub(super) const MEDIA_URL_RULE: &str = "URL must be a link (http://, https://), a data: URI, or a \
     reference <scheme>://... (two letters of scheme at least, no part of it between '+' a scheme \
     a reader fetches, opens or runs, such as file, ftp, http, smb, sftp, ws or javascript, no \
     '..' segment, backslash or control character, even percent-encoded)";

/// Why an audio or a video URL is refused, Python's message.
pub(super) const PLAYABLE_URL_RULE: &str = "URL must start with http://, https://, or data:";

/// The percent-decodings a reference is read through; one still encoded
/// after them is refused.
const MOST_DECODINGS: usize = 3;

/// The schemes a reader fetches, opens or runs by itself, without the
/// application, in order: files on the machine or on a share, the network
/// protocols generic readers (curl, ffmpeg, GIO) fetch, code to run, and a
/// link's, which a reader that splits a compound scheme on `+` fetches.
#[rustfmt::skip]
const REFUSED_SCHEMES: [&str; 59] = [
    "afp", "cifs", "dav", "davs", "dict", "file", "ftp", "ftps", "gopher", "gophers", "hls", "http",
    "https", "icecast", "imap", "imaps", "ipfs", "ipns", "jar", "javascript", "ldap", "ldaps",
    "mmsh", "mmst", "mqtt", "nfs", "pop3", "pop3s", "rtmp", "rtmpe", "rtmps", "rtmpt", "rtmpte",
    "rtmpts", "rtp", "rtsp", "rtsps", "scp", "sctp", "sftp", "smb", "smbs", "smtp", "smtps", "srt",
    "srtp", "ssh", "tcp", "telnet", "tftp", "tls", "udp", "udplite", "unix", "vbscript", "webdav",
    "ws", "wss", "zmq",
];

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
/// Windows drive), a letter then letters, digits, `+`, `.` or `-`, no part
/// of it between `+` a scheme a reader fetches, opens or runs (a link's
/// included), and nothing a path reader could
/// follow once it has percent-decoded the rest, up to three times, the most
/// an ordinary reference is encoded: no backslash, no control character, no
/// segment that is `..` with the whitespace around it trimmed, and no escape
/// left to decode.
pub(crate) fn is_safe_reference(url: &str) -> bool {
    let Some((scheme, rest)) = url.split_once("://") else {
        return false;
    };
    let mut characters = scheme.chars();
    let well_formed = characters.next().is_some_and(|c| c.is_ascii_alphabetic())
        && scheme.len() >= 2
        && characters.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '.' | '-'));
    if !well_formed || refused_scheme(scheme) {
        return false;
    }
    decoded(rest)
        .is_some_and(|rest| !rest.contains('\\') && !holds_control(&rest) && !holds_parent(&rest))
}

/// Whether `scheme`, or one of its parts joined by `+` (`hls+http`,
/// `crypto+file`), is a scheme a reader fetches, opens or runs.
fn refused_scheme(scheme: &str) -> bool {
    scheme
        .to_ascii_lowercase()
        .split('+')
        .any(|part| REFUSED_SCHEMES.binary_search(&part).is_ok())
}

/// Whether `text` holds a control character (Unicode Cc: C0, DEL and C1),
/// which a path reader may cut a name at or drop. An ASCII text is read by
/// its bytes.
fn holds_control(text: &str) -> bool {
    if text.is_ascii() {
        text.bytes().any(|byte| byte.is_ascii_control())
    } else {
        text.chars().any(char::is_control)
    }
}

/// Whether a segment of `text`, between `/`, `?` and `#`, is `..` once
/// trimmed of whitespace.
fn holds_parent(text: &str) -> bool {
    text.contains("..")
        && text
            .split(['/', '?', '#'])
            .any(|segment| segment.trim() == "..")
}

/// `text` percent-decoded until it no longer changes, as a reader that
/// decodes once or more reads it (`%252e` reads as `.` to one that decodes
/// twice); `None` when it still changes after [`MOST_DECODINGS`], a nesting
/// no reference holds and whose cost would grow with the square of its
/// length. A decoded byte sequence that is no UTF-8 reads as U+FFFD, as
/// Python's `urllib.parse.unquote` reads it.
fn decoded(text: &str) -> Option<Cow<'_, str>> {
    let mut text = Cow::Borrowed(text);
    for _ in 0..MOST_DECODINGS {
        match percent_decoded(&text) {
            Some(decoded) => text = Cow::Owned(decoded),
            None => return Some(text),
        }
    }
    percent_decoded(&text).is_none().then_some(text)
}

/// `text` with each `%` and two hex digits replaced by the byte they name;
/// `None` when it holds none.
fn percent_decoded(text: &str) -> Option<String> {
    if !text.contains('%') {
        return None;
    }
    let bytes = text.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut changed = false;
    let mut index = 0;
    while let Some(&byte) = bytes.get(index) {
        if let Some(escaped) = escaped_byte(bytes, index) {
            decoded.push(escaped);
            changed = true;
            index += 3;
        } else {
            decoded.push(byte);
            index += 1;
        }
    }
    changed.then(|| String::from_utf8_lossy(&decoded).into_owned())
}

/// The byte a `%` and two hex digits at `index` of `bytes` name, if they
/// stand there.
fn escaped_byte(bytes: &[u8], index: usize) -> Option<u8> {
    if *bytes.get(index)? != b'%' {
        return None;
    }
    let high = hex_digit(*bytes.get(index + 1)?)?;
    let low = hex_digit(*bytes.get(index + 2)?)?;
    Some((high << 4) | low)
}

fn hex_digit(byte: u8) -> Option<u8> {
    char::from(byte)
        .to_digit(16)
        .and_then(|digit| u8::try_from(digit).ok())
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

    /// Python's `TestReferenceURL`: a reference a reader that percent-decodes,
    /// once or more, or trims, could follow is refused; an encoded dot or
    /// slash that leads nowhere, a space or `a..b` is not.
    #[test]
    fn a_reference_is_refused_in_every_spelling_a_path_reader_follows() {
        for url in [
            "app-media://a/%2e%2e/secret",
            "app-media://a/%2E%2E%2Fsecret",
            "app-media://a/.%2E/secret",
            "app-media://a/%252e%252e/secret",
            "app-media://a/%25252e%25252E/secret",
            "app-media://a/..%2fsecret",
            "app-media://a/..%5Csecret",
            "app-media://a/ ../secret",
            "app-media://a/%20..%09/secret",
            "app-media://a/\u{a0}../secret",
            "app-media://a/..?secret",
            "app-media://a/b#..",
            "app-media://a/b\nc",
            "app-media://a/b%0Ac",
            "app-media://a/b\u{0}",
            "app-media://a/b\u{7f}",
            "app-media://a/b\u{85}",
            "app-media://photos/%25252541.png",
        ] {
            assert!(!is_safe_reference(url), "{url:?} is accepted");
        }
        for url in [
            "app-media://photos/a..b.png",
            "app-media://photos/./.../1.png",
            "app-media://photos/file%2Ename.png",
            "app-media://bucket/folder%2Ffile.png",
            "app-media://photos/My Photo.png?size=large#top",
            "app-media://photos/100%",
            "app-media://photos/%e9t%C3%A9.png",
            "app-media://photos/%252541.png",
        ] {
            assert!(is_safe_reference(url), "{url:?} is refused");
        }
    }

    /// Python's `TestReferenceURL`: a scheme a reader fetches, opens or runs
    /// by itself is refused, whatever its case and alone or joined by `+`;
    /// an application's own scheme and a cloud store are not.
    #[test]
    fn a_scheme_a_reader_fetches_by_itself_is_no_reference() {
        for url in [
            "smb://host/share/a.png",
            "SFTP://host/a.png",
            "ws://host/a",
            "rtsp://host/live",
            "jar://host/a.jar",
            "hls+http://host/a.m3u8",
            "crypto+file://a",
        ] {
            assert!(!is_safe_reference(url), "{url:?} is accepted");
        }
        for url in [
            "x-app://media/1",
            "store+v2://bucket/a.png",
            "s3://bucket/key.png",
        ] {
            assert!(is_safe_reference(url), "{url:?} is refused");
        }
    }

    /// The refused schemes are in order, which their binary search needs.
    #[test]
    fn the_refused_schemes_are_sorted() {
        assert!(REFUSED_SCHEMES.is_sorted());
    }

    /// A scheme `://` matches as the first `://` of the URL, so a reference
    /// whose path holds another is read by its first scheme.
    #[test]
    fn a_reference_is_read_by_its_first_scheme() {
        assert!(is_safe_reference("app://a/b?next=https://c"));
        assert!(!is_safe_reference("1app://a"));
    }
}

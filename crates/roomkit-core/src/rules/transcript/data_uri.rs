//! A `data:` URI's base64, as the Python implementation's providers write
//! it and read it (`roomkit.providers.utils.to_data_uri` and
//! `parse_data_uri`): standard base64 with its padding written, whitespace
//! and a missing padding repaired when read.

use std::fmt;

/// The standard base64 alphabet.
const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// The type a `data:` URI declaring none is read as, with no fallback.
const DEFAULT_TYPE: &str = "image/png";

/// `data` as a base64 `data:` URI of the type `kind`: standard base64 with
/// its padding. Python's `to_data_uri`.
#[must_use]
pub fn to_data_uri(data: &[u8], kind: &str) -> String {
    let mut encoded = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let byte = |at: usize| chunk.get(at).copied().unwrap_or(0);
        let word = u32::from_be_bytes([0, byte(0), byte(1), byte(2)]);
        for position in 0..4 {
            let index = (word >> (18 - 6 * position)) & 0x3f;
            let digit = usize::try_from(index)
                .ok()
                .and_then(|at| ALPHABET.get(at))
                .copied();
            match digit.filter(|_| position <= chunk.len()) {
                Some(digit) => encoded.push(char::from(digit)),
                None => encoded.push('='),
            }
        }
    }
    format!("data:{kind};base64,{encoded}")
}

/// Why a `data:` URI does not read: Python's `ValueError` of
/// `parse_data_uri`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataUriError {
    /// The URL is no `data:` URI.
    NotADataUri,
    /// It carries no payload, whitespace aside.
    NoPayload,
    /// Its payload is no base64: a character outside the standard
    /// alphabet (the URL-safe one's `-` and `_` included), or a length no
    /// padding completes.
    NotBase64,
}

impl fmt::Display for DataUriError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::NotADataUri => "expected a data: URI",
            Self::NoPayload => "data URI carries no payload",
            Self::NotBase64 => "data URI payload is not valid base64",
        })
    }
}

impl std::error::Error for DataUriError {}

/// The media type and the bytes of the `data:` URI `url`, its type the
/// header's, else `fallback`, else `image/png`. Whitespace in the payload
/// (an encoder that wrapped its lines) and a missing padding are repaired;
/// anything else is refused. Python's `parse_data_uri`.
///
/// # Errors
///
/// [`DataUriError`] for a URL that is no `data:` URI, carries no payload,
/// or whose payload is no base64.
pub fn parse_data_uri(
    url: &str,
    fallback: Option<&str>,
) -> Result<(String, Vec<u8>), DataUriError> {
    let rest = url.strip_prefix("data:").ok_or(DataUriError::NotADataUri)?;
    let (header, payload) = rest.split_once(',').ok_or(DataUriError::NoPayload)?;
    let mut compact: String = payload.chars().filter(|c| !c.is_whitespace()).collect();
    if compact.is_empty() {
        return Err(DataUriError::NoPayload);
    }
    let declared = header.split(';').next().unwrap_or_default();
    let kind = [Some(declared), fallback]
        .into_iter()
        .flatten()
        .find(|kind| !kind.is_empty())
        .unwrap_or(DEFAULT_TYPE);
    while !compact.len().is_multiple_of(4) {
        compact.push('=');
    }
    let data = decode(&compact).ok_or(DataUriError::NotBase64)?;
    Ok((kind.to_owned(), data))
}

/// The bytes of `text`, standard base64 of a length a multiple of four,
/// at most two `=` closing it; `None` for any other text. Bits past the
/// last whole byte are dropped, as Python's `b64decode` drops them.
fn decode(text: &str) -> Option<Vec<u8>> {
    let digits = text.trim_end_matches('=');
    if !text.len().is_multiple_of(4) || text.len() - digits.len() > 2 {
        return None;
    }
    let mut bytes = Vec::with_capacity(digits.len() * 3 / 4);
    let (mut word, mut bits) = (0_u32, 0_u32);
    for digit in digits.bytes() {
        word = (word << 6) | sextet(digit)?;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            bytes.push(u8::try_from((word >> bits) & 0xff).ok()?);
        }
    }
    Some(bytes)
}

/// The six bits a base64 digit stands for.
fn sextet(digit: u8) -> Option<u32> {
    let value = match digit {
        b'A'..=b'Z' => digit - b'A',
        b'a'..=b'z' => digit - b'a' + 26,
        b'0'..=b'9' => digit - b'0' + 52,
        b'+' => 62,
        b'/' => 63,
        _ => return None,
    };
    Some(u32::from(value))
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    // Python: tests/test_providers/test_image.py::test_to_data_uri_round_trips
    #[test]
    fn to_data_uri_round_trips() {
        for data in [&b""[..], b"A", b"AB", b"ABC", b"ABC1", b"\x00\xff\x10"] {
            let url = to_data_uri(data, "image/png");
            assert_eq!(
                parse_data_uri(&url, None).ok(),
                (!data.is_empty()).then(|| ("image/png".to_owned(), data.to_vec())),
                "{url}"
            );
        }
    }

    // Python: tests/test_providers/test_image.py::test_parse_data_uri_repairs_whitespace
    #[test]
    fn parse_data_uri_repairs_whitespace() {
        for payload in ["QUJDMTIz", "QUJD\nMTIz", "QUJDMTIz\r\n", " QUJD MTIz "] {
            assert_eq!(
                parse_data_uri(&format!("data:image/png;base64,{payload}"), None),
                Ok(("image/png".to_owned(), b"ABC123".to_vec())),
                "{payload:?}"
            );
        }
    }

    // Python: tests/test_providers/test_image.py::test_parse_data_uri_repairs_missing_padding
    #[test]
    fn parse_data_uri_repairs_missing_padding() {
        for url in [
            "data:image/png;base64,QUJDMQ",
            "data:image/png;base64,QUJDMQ==",
        ] {
            assert_eq!(
                parse_data_uri(url, None),
                Ok(("image/png".to_owned(), b"ABC1".to_vec()))
            );
        }
    }

    // Python: tests/test_providers/test_image.py::test_parse_data_uri_takes_the_media_type_from_the_header_then_the_fallback
    #[test]
    fn parse_data_uri_takes_the_media_type_from_the_header_then_the_fallback() {
        let kind = |url: &str, fallback| parse_data_uri(url, fallback).map(|(kind, _)| kind);
        assert_eq!(
            [
                kind("data:image/jpeg;base64,QUJD", Some("image/png")),
                kind("data:;base64,QUJD", Some("image/webp")),
                kind("data:;base64,QUJD", None),
            ],
            [
                Ok("image/jpeg".to_owned()),
                Ok("image/webp".to_owned()),
                Ok("image/png".to_owned()),
            ]
        );
    }

    // Python: tests/test_providers/test_image.py::test_parse_data_uri_refuses_what_it_cannot_repair
    #[test]
    fn parse_data_uri_refuses_what_it_cannot_repair() {
        let refused = [
            "https://example.com/a.png",
            "data:image/png;base64,",
            "data:image/png;base64,  \n ",
            "data:image/png;base64,not*base64",
            "data:image/png;base64,QUJDM",
            "data:image/png;base64,QUJD-_==",
        ]
        .map(|url| parse_data_uri(url, None).err());
        assert_eq!(
            refused,
            [
                Some(DataUriError::NotADataUri),
                Some(DataUriError::NoPayload),
                Some(DataUriError::NoPayload),
                Some(DataUriError::NotBase64),
                Some(DataUriError::NotBase64),
                Some(DataUriError::NotBase64),
            ]
        );
    }
}

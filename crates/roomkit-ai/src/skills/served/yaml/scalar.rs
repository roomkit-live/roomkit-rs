//! Python `SafeLoader` scalar resolution before JSON normalization.

use super::Node;
use crate::decimal_digits as decimal;
use base64::Engine;
use std::fmt::Write as _;
mod timestamp;
use base64::engine::{GeneralPurpose, GeneralPurposeConfig};
use saphyr_parser::{ScalarStyle, Tag};
use serde_json::{Number, Value};
pub(super) fn same_instant(left: &str, right: &str) -> Option<bool> {
    timestamp::same_instant(left, right)
}

pub(super) fn tag_name(tag: Option<&Tag>) -> Result<String, String> {
    let Some(tag) = tag else {
        return Ok(String::new());
    };
    if tag.handle != "tag:yaml.org,2002:" && tag.handle != "!!" {
        return Err("the frontmatter is not YAML".to_owned());
    }
    Ok(tag.suffix.clone())
}

pub(super) fn collection_tag(tag: Option<&Tag>) -> String {
    tag_name(tag).unwrap_or_else(|_| {
        tag.map_or_else(String::new, |tag| format!("{}{}", tag.handle, tag.suffix))
    })
}

pub(super) fn read(text: &str, style: ScalarStyle, tag: Option<&Tag>) -> Result<Node, String> {
    let tag = tag_name(tag)?;
    let typed = if tag.is_empty() && style != ScalarStyle::Plain {
        "str".to_owned()
    } else if tag.is_empty() {
        implicit(text)
    } else {
        tag
    };
    if typed == "value" {
        return Ok(Node::ValueTag(text.to_owned()));
    }
    let value = match typed.as_str() {
        "str" | "merge" => Value::String(text.to_owned()),
        "null" => Value::Null,
        "bool" => Value::Bool(match text.to_lowercase().as_str() {
            "true" | "True" | "TRUE" | "yes" | "Yes" | "YES" | "on" | "On" | "ON" => true,
            "false" | "False" | "FALSE" | "no" | "No" | "NO" | "off" | "Off" | "OFF" => false,
            _ => return invalid(),
        }),
        "int" => integer(text)
            .ok_or_else(|| "the frontmatter cannot construct a YAML value".to_owned())?,
        "float" => {
            let number = float(text)
                .ok_or_else(|| "the frontmatter cannot construct a YAML value".to_owned())?;
            if !number.is_finite() {
                return Ok(Node::NonFinite(
                    if number.is_nan() {
                        "NaN"
                    } else if number.is_sign_negative() {
                        "-Infinity"
                    } else {
                        "Infinity"
                    }
                    .to_owned(),
                ));
            }
            Value::Number(Number::from_f64(number).ok_or("the frontmatter is not YAML")?)
        }
        "timestamp" => Value::String(
            timestamp::read(text)
                .ok_or_else(|| "the frontmatter cannot construct a YAML value".to_owned())?,
        ),
        "binary" => {
            let bytes = binary(text).ok_or("the frontmatter is not YAML")?;
            Value::String(bytes_repr(&bytes))
        }
        _ => return invalid(),
    };
    let key = if matches!(typed.as_str(), "binary" | "timestamp") {
        None
    } else {
        Some(match &value {
            Value::String(s) => s.clone(),
            Value::Null => "null".to_owned(),
            Value::Bool(_) | Value::Number(_) | Value::Array(_) | Value::Object(_) => {
                crate::skills::texts::dumps(&value)
            }
        })
    };
    Ok(Node::Scalar(value, key, typed == "merge"))
}
fn invalid<T>() -> Result<T, String> {
    Err("the frontmatter is not YAML".to_owned())
}

/// A plain scalar of a local skill's frontmatter as Python's registry keeps
/// it: `str()` of the value `SafeLoader` resolves it to (YAML 1.1).
#[derive(Debug, PartialEq, Eq)]
pub(in crate::skills) enum PythonText {
    /// A null, which leaves the key out.
    Null,
    /// What Python writes of the value: `True`, `16`, `1.0`, `inf`, a
    /// date or a time as its `str()`, a text as itself.
    Text(String),
    /// A scalar `SafeLoader` constructs no value from (`=`, `<<`, an
    /// impossible date, an integer past 64 bits here): Python then reads
    /// the whole frontmatter line by line, each value as written.
    NotYaml,
}

/// `plain`, a plain scalar whole (no quotes, no comment), as Python's
/// registry keeps it.
pub(in crate::skills) fn python_text(plain: &str) -> PythonText {
    let text = match implicit(plain).as_str() {
        "null" => return PythonText::Null,
        "bool" => Some(
            if matches!(
                plain,
                "yes" | "Yes" | "YES" | "true" | "True" | "TRUE" | "on" | "On" | "ON"
            ) {
                "True"
            } else {
                "False"
            }
            .to_owned(),
        ),
        "int" => integer(plain).map(|number| number.to_string()),
        "float" => float(plain).map(roomkit_core::model::python_float),
        "timestamp" => timestamp::read(plain),
        "merge" | "value" => None,
        _ => Some(plain.to_owned()),
    };
    text.map_or(PythonText::NotYaml, PythonText::Text)
}

fn implicit(text: &str) -> String {
    match text {
        "" | "~" | "null" | "Null" | "NULL" => "null",
        "yes" | "Yes" | "YES" | "no" | "No" | "NO" | "true" | "True" | "TRUE" | "false"
        | "False" | "FALSE" | "on" | "On" | "ON" | "off" | "Off" | "OFF" => "bool",
        "<<" => "merge",
        "=" => "value",
        _ if integer_shape(text) => "int",
        _ if float_shape(text) => "float",
        _ if timestamp::matches(text) => "timestamp",
        _ => "str",
    }
    .to_owned()
}

fn integer(text: &str) -> Option<Value> {
    let cleaned = text.replace('_', "");
    let negative = cleaned.starts_with('-');
    // SafeLoader removes one sign, then Python's int reads the remainder.
    // An explicit tag can therefore carry two signs; resolution is stricter.
    let value = cleaned.strip_prefix(['-', '+']).unwrap_or(&cleaned);
    if value.is_empty() {
        return None;
    }
    let number = if value.contains(':') && !value.starts_with('0') {
        let mut parts = value.split(':');
        let first = parts.next()?;
        let mut result = decimal::ascii(first.trim()).parse::<i128>().ok()?;
        for part in parts {
            let part = decimal::ascii(part.trim()).parse::<i128>().ok()?;
            result = result.checked_mul(60)?.checked_add(part)?;
        }
        result
    } else {
        let (radix, digits) = if let Some(s) = value.strip_prefix("0b") {
            (2, s)
        } else if let Some(s) = value.strip_prefix("0x") {
            (16, s)
        } else if value.len() > 1 && value.starts_with('0') {
            (8, value)
        } else {
            (10, value)
        };
        i128::from_str_radix(decimal::ascii(digits.trim()).as_str(), radix).ok()?
    };
    let number = if negative {
        number.checked_neg()?
    } else {
        number
    };
    if let Ok(value) = i64::try_from(number) {
        Some(Value::from(value))
    } else {
        u64::try_from(number).ok().map(Value::from)
    }
}

fn float(text: &str) -> Option<f64> {
    let cleaned = decimal::ascii(&text.replace('_', "").to_lowercase());
    let negative = cleaned.starts_with('-');
    let value = cleaned.strip_prefix(['-', '+']).unwrap_or(&cleaned);
    let number = if value == ".inf" {
        f64::INFINITY
    } else if value == ".nan" {
        f64::NAN
    } else if value.contains(':') {
        let mut result = 0.0;
        for part in value.split(':') {
            result = result * 60.0 + part.trim().parse::<f64>().ok()?;
        }
        result
    } else {
        value.trim().parse::<f64>().ok()?
    };
    Some(if negative { -number } else { number })
}

fn bytes_repr(bytes: &[u8]) -> String {
    let quote = if bytes.contains(&b'\'') && !bytes.contains(&b'"') {
        '"'
    } else {
        '\''
    };
    let mut text = format!("b{quote}");
    for byte in bytes {
        match *byte {
            b'\n' => text.push_str("\\n"),
            b'\r' => text.push_str("\\r"),
            b'\t' => text.push_str("\\t"),
            b'\\' => text.push_str("\\\\"),
            b if char::from(b) == quote => {
                text.push('\\');
                text.push(quote);
            }
            32..=126 => text.push(char::from(*byte)),
            b => {
                let _infallible = write!(&mut text, "\\x{b:02x}");
            }
        }
    }
    text.push(quote);
    text
}

fn digits(text: &str, radix: u32) -> bool {
    !text.is_empty() && text.chars().all(|c| c == '_' || c.is_digit(radix))
}
fn integer_shape(text: &str) -> bool {
    let value = text.strip_prefix(['-', '+']).unwrap_or(text);
    if let Some(tail) = value.strip_prefix("0b") {
        return digits(tail, 2);
    }
    if let Some(tail) = value.strip_prefix("0x") {
        return digits(tail, 16);
    }
    if let Some(tail) = value.strip_prefix('0') {
        return tail.is_empty() || digits(tail, 8);
    }
    let mut parts = value.split(':');
    let first = parts.next().unwrap_or_default();
    if !first.starts_with(|c: char| ('1'..='9').contains(&c)) || !digits(first, 10) {
        return false;
    }
    parts.all(|part| {
        !part.is_empty()
            && part.len() <= 2
            && part.chars().all(|c| c.is_ascii_digit())
            && part.parse::<u32>().is_ok_and(|n| n < 60)
    })
}
fn float_shape(text: &str) -> bool {
    let unsigned = text.strip_prefix(['-', '+']).unwrap_or(text);
    if matches!(unsigned, ".inf" | ".Inf" | ".INF") || matches!(text, ".nan" | ".NaN" | ".NAN") {
        return true;
    }
    let (mantissa, exponent) = match unsigned.split_once(['e', 'E']) {
        Some((m, e)) => (m, Some(e)),
        None => (unsigned, None),
    };
    if exponent.is_some_and(|e| {
        !(e.starts_with(['-', '+'])
            && e.get(1..)
                .is_some_and(|n| !n.is_empty() && n.chars().all(|c| c.is_ascii_digit())))
    }) {
        return false;
    }
    let Some((whole, fraction)) = mantissa.split_once('.') else {
        return false;
    };
    if !fraction.chars().all(|c| c.is_ascii_digit() || c == '_') {
        return false;
    }
    if whole.is_empty() {
        return text.starts_with('.') && fraction.starts_with(|c: char| c.is_ascii_digit());
    }
    if whole.contains(':') {
        let mut parts = whole.split(':');
        let first = parts.next().unwrap_or_default();
        return exponent.is_none()
            && first.starts_with(|c: char| c.is_ascii_digit())
            && digits(first, 10)
            && parts.all(|p| ascii_digits(p, 1, 2) && p.parse::<u32>().is_ok_and(|n| n < 60));
    }
    whole.starts_with(|c: char| c.is_ascii_digit()) && digits(whole, 10)
}
fn ascii_digits(text: &str, min: usize, max: usize) -> bool {
    (min..=max).contains(&text.len()) && text.bytes().all(|c| c.is_ascii_digit())
}
/// `binascii.a2b_base64` used by `SafeLoader`'s binary constructor permits
/// ignored ASCII bytes and redundant/interior padding; resource blobs do not.
fn binary(text: &str) -> Option<Vec<u8>> {
    if !text.is_ascii() {
        return None;
    }
    let clean: String = text
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '/' | '='))
        .collect();
    let padding = clean.len() - clean.trim_end_matches('=').len();
    let mut encoded: String = clean.chars().filter(|c| *c != '=').collect();
    let needed = match encoded.len() % 4 {
        0 => 0,
        2 => 2,
        3 => 1,
        _ => return None,
    };
    if padding < needed {
        return None;
    }
    encoded.extend(std::iter::repeat_n('=', needed));
    GeneralPurpose::new(
        &base64::alphabet::STANDARD,
        GeneralPurposeConfig::new().with_decode_allow_trailing_bits(true),
    )
    .decode(encoded)
    .ok()
}

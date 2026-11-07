//! A tool call's arguments as the wire carries them back to the model: the
//! JSON text Python's `json.dumps` writes with its defaults, `", "` and
//! `": "` between items, every character outside printable ASCII escaped
//! as `\uXXXX` (UTF-16 units), a float in Python's `repr` (RMK-1508, HITL
//! 9f5e61ba: Rust as Python). The keys keep their order. Python:
//! `providers/ai/chat_request.py`, `json.dumps(call.arguments)`.

use std::io::{self, Write};

use serde::Serialize;
use serde_json::ser::{Formatter, Serializer};
use serde_json::{Map, Value};

/// `arguments` as `json.dumps` writes them.
pub(crate) fn dumps(arguments: &Map<String, Value>) -> String {
    let mut text = Vec::new();
    let mut serializer = Serializer::with_formatter(&mut text, PythonFormatter);
    // Writing a JSON value into memory cannot fail.
    if Value::Object(arguments.clone())
        .serialize(&mut serializer)
        .is_err()
    {
        return Value::Object(arguments.clone()).to_string();
    }
    String::from_utf8(text).unwrap_or_default()
}

/// `serde_json`'s compact form, with `json.dumps`' separators, ASCII
/// escapes and floats.
struct PythonFormatter;

impl Formatter for PythonFormatter {
    fn begin_array_value<W: ?Sized + Write>(
        &mut self,
        writer: &mut W,
        first: bool,
    ) -> io::Result<()> {
        if first {
            Ok(())
        } else {
            writer.write_all(b", ")
        }
    }

    fn begin_object_key<W: ?Sized + Write>(
        &mut self,
        writer: &mut W,
        first: bool,
    ) -> io::Result<()> {
        if first {
            Ok(())
        } else {
            writer.write_all(b", ")
        }
    }

    fn begin_object_value<W: ?Sized + Write>(&mut self, writer: &mut W) -> io::Result<()> {
        writer.write_all(b": ")
    }

    fn write_string_fragment<W: ?Sized + Write>(
        &mut self,
        writer: &mut W,
        fragment: &str,
    ) -> io::Result<()> {
        for character in fragment.chars() {
            if (' '..='~').contains(&character) {
                writer.write_all(&[character as u8])?;
            } else {
                let mut units = [0u16; 2];
                for unit in character.encode_utf16(&mut units) {
                    write!(writer, "\\u{unit:04x}")?;
                }
            }
        }
        Ok(())
    }

    fn write_f64<W: ?Sized + Write>(&mut self, writer: &mut W, value: f64) -> io::Result<()> {
        writer.write_all(python_float(value).as_bytes())
    }
}

/// `value` as Python's `repr` writes a float: the shortest digits, in
/// fixed notation from 1e-4 up to 1e16, with `.0` when integral, else in
/// scientific notation with a signed exponent of two digits at least.
fn python_float(value: f64) -> String {
    let magnitude = value.abs();
    if magnitude != 0.0 && !(1e-4..1e16).contains(&magnitude) {
        let text = format!("{value:e}");
        let Some((mantissa, exponent)) = text.split_once('e') else {
            return text;
        };
        let (sign, digits) = match exponent.strip_prefix('-') {
            Some(digits) => ('-', digits),
            None => ('+', exponent),
        };
        return format!("{mantissa}e{sign}{digits:0>2}");
    }
    let text = value.to_string();
    if text.contains('.') {
        text
    } else {
        format!("{text}.0")
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::dumps;

    // json.dumps of the same value, written by Python 3:
    // python3 -c 'import json; print(json.dumps(v))'.
    #[test]
    fn arguments_are_written_as_json_dumps_writes_them() {
        let value = json!({
            "q": "café", "n": 42,
            "nested": {"list": [1, 2.5, [], {}], "empty": ""},
            "esc": "a\"b\\c\nd\te\u{1}\u{7f}",
            "emoji": "😀",
            "floats": [1.0, 1e16, 1.5e-07, 0.0001, -0.0, 123_456.789, 1e-05, 100.0]
        });
        let Some(arguments) = value.as_object() else {
            panic!("an object");
        };
        assert_eq!(
            dumps(arguments),
            r#"{"q": "caf\u00e9", "n": 42, "nested": {"list": [1, 2.5, [], {}], "empty": ""}, "esc": "a\"b\\c\nd\te\u0001\u007f", "emoji": "\ud83d\ude00", "floats": [1.0, 1e+16, 1.5e-07, 0.0001, -0.0, 123456.789, 1e-05, 100.0]}"#
        );
    }
}

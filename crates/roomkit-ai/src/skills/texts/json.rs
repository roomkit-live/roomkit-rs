//! Python JSON spelling shared by rendering and bounded manifest admission.

use std::fmt::{self, Write};

use roomkit_core::model::python_float;
use serde_json::{Map, Value};

enum Frame<'a> {
    Value(&'a Value),
    Array(std::slice::Iter<'a, Value>, bool),
    Object(serde_json::map::Iter<'a>, bool),
}

pub(super) fn write_value(out: &mut impl Write, value: &Value, ascii: bool) -> fmt::Result {
    let mut pending = vec![Frame::Value(value)];
    while let Some(frame) = pending.pop() {
        match frame {
            Frame::Value(value) => start(out, value, ascii, &mut pending)?,
            Frame::Array(mut items, first) => {
                if let Some(item) = items.next() {
                    if !first {
                        out.write_str(", ")?;
                    }
                    pending.push(Frame::Array(items, false));
                    pending.push(Frame::Value(item));
                } else {
                    out.write_char(']')?;
                }
            }
            Frame::Object(mut fields, first) => {
                if let Some((key, value)) = fields.next() {
                    if !first {
                        out.write_str(", ")?;
                    }
                    string(out, key, ascii)?;
                    out.write_str(": ")?;
                    pending.push(Frame::Object(fields, false));
                    pending.push(Frame::Value(value));
                } else {
                    out.write_char('}')?;
                }
            }
        }
    }
    Ok(())
}

fn start<'a>(
    out: &mut impl Write,
    value: &'a Value,
    ascii: bool,
    pending: &mut Vec<Frame<'a>>,
) -> fmt::Result {
    match value {
        Value::String(text) => string(out, text, ascii),
        Value::Array(items) => {
            out.write_char('[')?;
            pending.push(Frame::Array(items.iter(), true));
            Ok(())
        }
        Value::Object(fields) => {
            out.write_char('{')?;
            pending.push(Frame::Object(fields.iter(), true));
            Ok(())
        }
        Value::Number(number) if number.is_f64() => {
            out.write_str(&python_float(number.as_f64().unwrap_or_default()))
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => out.write_str(&value.to_string()),
    }
}

pub(super) fn write_object<'a>(
    out: &mut impl Write,
    fields: impl Iterator<Item = (&'a str, &'a Value)>,
    ascii: bool,
) -> fmt::Result {
    out.write_char('{')?;
    for (index, (key, value)) in fields.enumerate() {
        if index > 0 {
            out.write_str(", ")?;
        }
        string(out, key, ascii)?;
        out.write_str(": ")?;
        write_value(out, value, ascii)?;
    }
    out.write_char('}')
}

fn string(out: &mut impl Write, text: &str, ascii: bool) -> fmt::Result {
    out.write_char('"')?;
    for character in text.chars() {
        match character {
            '"' => out.write_str("\\\"")?,
            '\\' => out.write_str("\\\\")?,
            '\n' => out.write_str("\\n")?,
            '\r' => out.write_str("\\r")?,
            '\t' => out.write_str("\\t")?,
            '\u{8}' => out.write_str("\\b")?,
            '\u{c}' => out.write_str("\\f")?,
            // Python's ensure_ascii escapes DEL and UTF-16 surrogate pairs too.
            c if u32::from(c) < 0x20 || (ascii && (!c.is_ascii() || c == '\u{7f}')) => {
                let mut units = [0_u16; 2];
                for unit in c.encode_utf16(&mut units) {
                    write!(out, "\\u{unit:04x}")?;
                }
            }
            c => out.write_char(c)?,
        }
    }
    out.write_char('"')
}

/// ASCII output bytes equal Python's JSON character count; stop at the budget.
pub(crate) fn fits_map(map: &Map<String, Value>, limit: usize) -> bool {
    let mut budget = Budget(limit);
    write_object(&mut budget, map.iter().map(|(k, v)| (k.as_str(), v)), true).is_ok()
}

struct Budget(usize);

impl Write for Budget {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        let Some(left) = self.0.checked_sub(text.len()) else {
            return Err(fmt::Error);
        };
        self.0 = left;
        Ok(())
    }
}

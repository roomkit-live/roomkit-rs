//! JSON text as Python's `json.dumps` writes it, for the answers a model
//! reads of the kit's tools: `", "` and `": "` between members, the
//! members in the order given, every character past ASCII escaped unless
//! `ensure_ascii` is off.

use std::fmt::Write as _;

use roomkit_core::model::python_float;
use serde_json::Value;

/// `members`, an object in their order, as Python's `json.dumps` writes
/// it.
pub(crate) fn dumps_object<'a>(
    members: impl IntoIterator<Item = (&'a str, &'a Value)>,
    ensure_ascii: bool,
) -> String {
    let mut written = String::new();
    object(&mut written, members, ensure_ascii);
    written
}

/// Writes `members` as an object.
pub(crate) fn object<'a>(
    written: &mut String,
    members: impl IntoIterator<Item = (&'a str, &'a Value)>,
    ensure_ascii: bool,
) {
    written.push('{');
    for (at, (name, value)) in members.into_iter().enumerate() {
        if at > 0 {
            written.push_str(", ");
        }
        string(written, name, ensure_ascii);
        written.push_str(": ");
        json(written, value, ensure_ascii);
    }
    written.push('}');
}

/// Writes `value`.
pub(crate) fn json(written: &mut String, value: &Value, ensure_ascii: bool) {
    match value {
        Value::String(text) => string(written, text, ensure_ascii),
        Value::Array(items) => {
            written.push('[');
            for (at, item) in items.iter().enumerate() {
                if at > 0 {
                    written.push_str(", ");
                }
                json(written, item, ensure_ascii);
            }
            written.push(']');
        }
        Value::Object(members) => object(
            written,
            members.iter().map(|(name, value)| (name.as_str(), value)),
            ensure_ascii,
        ),
        // A float as Python's `float.__repr__`, which `json.dumps` writes:
        // `1e-05`, not `0.00001` (RMK-1262).
        Value::Number(number) if number.is_f64() => {
            written.push_str(&python_float(number.as_f64().unwrap_or_default()));
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => written.push_str(&value.to_string()),
    }
}

/// Writes `text` as a JSON string: `"` and `\` escaped, the controls as
/// Python writes them, and, with `ensure_ascii`, every character but the
/// printable ASCII ones as `\uXXXX`, a pair past the basic plane.
fn string(written: &mut String, text: &str, ensure_ascii: bool) {
    written.push('"');
    for character in text.chars() {
        match character {
            '"' => written.push_str("\\\""),
            '\\' => written.push_str("\\\\"),
            '\n' => written.push_str("\\n"),
            '\r' => written.push_str("\\r"),
            '\t' => written.push_str("\\t"),
            '\u{8}' => written.push_str("\\b"),
            '\u{c}' => written.push_str("\\f"),
            ' '..='~' => written.push(character),
            _ if character < ' ' || ensure_ascii => {
                let mut units = [0_u16; 2];
                for unit in character.encode_utf16(&mut units) {
                    // Writing to a `String` cannot fail.
                    let _ = write!(written, "\\u{unit:04x}");
                }
            }
            _ => written.push(character),
        }
    }
    written.push('"');
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    /// A float is written as Python's `json.dumps` writes it,
    /// `float.__repr__`: scientific past the exponents -4 to 15, with a
    /// signed exponent of two digits at least; an integer as itself
    /// (RMK-1262).
    #[test]
    fn a_float_is_written_as_python_s_repr() {
        let values = [
            json!(1e-05),
            json!(9.5e-05),
            json!(0.0001),
            json!(1.5),
            json!(1e16),
            json!(3),
        ];
        let members = ["a", "b", "c", "d", "e", "f"]
            .into_iter()
            .zip(values.iter());
        assert_eq!(
            dumps_object(members, true),
            r#"{"a": 1e-05, "b": 9.5e-05, "c": 0.0001, "d": 1.5, "e": 1e+16, "f": 3}"#
        );
    }

    #[test]
    fn members_are_written_as_python_writes_them() {
        let (id, text) = (json!("t-1"), json!("Café \"ok\"\n😀"));
        assert_eq!(
            [
                dumps_object([("task_id", &id), ("message", &text)], true),
                dumps_object([("message", &text)], false),
            ],
            [
                "{\"task_id\": \"t-1\", \"message\": \"Caf\\u00e9 \\\"ok\\\"\\n\\ud83d\\ude00\"}",
                "{\"message\": \"Café \\\"ok\\\"\\n😀\"}",
            ]
        );
    }
}

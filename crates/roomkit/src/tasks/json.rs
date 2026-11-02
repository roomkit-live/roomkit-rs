//! JSON text as Python's `json.dumps` writes it, for the answers a model
//! reads of the kit's tools: `", "` and `": "` between members, the
//! members in the order given, every character past ASCII escaped unless
//! `ensure_ascii` is off.

use std::fmt::Write as _;

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

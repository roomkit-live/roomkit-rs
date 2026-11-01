//! What the model reads of the skills, as the reference implementation
//! writes it: the preamble before the manifest, the two tools, and what
//! they answer, each answer JSON written as Python's `json.dumps` writes
//! it.

use std::fmt::Write as _;

use serde_json::Value;

/// The skills' preamble, before the manifest, with the note that no script
/// runs here (this implementation runs none).
pub(crate) const PREAMBLE: &str = "You have access to Agent Skills — specialized knowledge \
packages. Use the activate_skill tool to load a skill's full instructions before using it. \
Available skills are listed below. The skill loader does not execute scripts. \
Use only tools provided by the host for any execution, subject to its permissions.";

/// The heading of the active skills' instructions in the system prompt.
pub(crate) const ACTIVE_HEADING: &str = "# Active skill instructions (binding rules)";

/// What a repeated activation answers (RFC §24.4: the instructions are
/// already in force).
pub(crate) const ALREADY_ACTIVE: &str = "This skill is already active — its full instructions \
are in your system rules, under 'Active skill instructions'. Follow them. There is nothing more \
to load: use other tools to act.";

/// The activation tool's name.
pub(crate) const ACTIVATE_SKILL: &str = "activate_skill";

/// The reference tool's name.
pub(crate) const READ_SKILL_REFERENCE: &str = "read_skill_reference";

/// What the model is told of `activate_skill`.
pub(crate) const ACTIVATE_DESCRIPTION: &str = "Activate a skill to get its full instructions, \
available scripts, and reference files. This is THE way to load a skill for use — do not fetch \
skill content through management/listing tools.";

/// What the model is told of `read_skill_reference`.
pub(crate) const READ_DESCRIPTION: &str = "Read ONE supplementary reference file bundled inside \
a skill (listed by activate_skill under 'references'). Only for those files: NOT for the \
skill's own instructions (activate_skill already loads them) and NOT for stored tool results \
(read_stored_result).";

/// A JSON object of `members`, in order, as Python's `json.dumps` writes
/// it by default: `", "` between members, `": "` after a name, and every
/// character past ASCII escaped.
pub(crate) fn object(members: &[(&str, Value)]) -> String {
    let mut written = String::from("{");
    for (at, (name, value)) in members.iter().enumerate() {
        if at > 0 {
            written.push_str(", ");
        }
        string(&mut written, name);
        written.push_str(": ");
        json(&mut written, value);
    }
    written.push('}');
    written
}

/// `value` as Python's `json.dumps(value)` writes it by default: `", "`
/// and `": "` between members, every character past ASCII escaped. For
/// the MCP crate, whose `call_tool` answers a failed call as Python's
/// `json.dumps({"error": ...})`.
#[doc(hidden)]
pub fn dumps(value: &Value) -> String {
    let mut written = String::new();
    json(&mut written, value);
    written
}

/// `value`, written as Python's `json.dumps` writes it.
fn json(written: &mut String, value: &Value) {
    match value {
        Value::String(text) => string(written, text),
        Value::Array(items) => {
            written.push('[');
            for (at, item) in items.iter().enumerate() {
                if at > 0 {
                    written.push_str(", ");
                }
                json(written, item);
            }
            written.push(']');
        }
        Value::Object(members) => {
            let members: Vec<(&str, Value)> = members
                .iter()
                .map(|(name, value)| (name.as_str(), value.clone()))
                .collect();
            written.push_str(&object(&members));
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => written.push_str(&value.to_string()),
    }
}

/// `text` as a JSON string, every character past ASCII as `\uXXXX`.
fn string(written: &mut String, text: &str) {
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
            c if u32::from(c) < 0x20 || !c.is_ascii() => {
                let mut units = [0_u16; 2];
                for unit in c.encode_utf16(&mut units) {
                    let _infallible = write!(written, "\\u{unit:04x}");
                }
            }
            c => written.push(c),
        }
    }
    written.push('"');
}

/// `text` written for a message as Python's `repr` writes a text: in
/// single quotes (double ones when it holds a single quote and no double
/// one), its backslashes, its quote and its control characters escaped.
pub(crate) fn quoted_name(text: &str) -> String {
    let quote = if text.contains('\'') && !text.contains('"') {
        '"'
    } else {
        '\''
    };
    let mut written = String::with_capacity(text.len() + 2);
    written.push(quote);
    for character in text.chars() {
        match character {
            '\\' => written.push_str("\\\\"),
            '\n' => written.push_str("\\n"),
            '\r' => written.push_str("\\r"),
            '\t' => written.push_str("\\t"),
            c if c == quote => {
                written.push('\\');
                written.push(c);
            }
            c if c.is_control() => {
                let _infallible = write!(written, "\\x{:02x}", u32::from(c));
            }
            c => written.push(c),
        }
    }
    written.push(quote);
    written
}

/// `text` escaped as an XML element's text or attribute, as Python's
/// `html.escape` does.
pub(crate) fn escaped(text: &str) -> String {
    let mut written = String::with_capacity(text.len());
    for character in text.chars() {
        match character {
            '&' => written.push_str("&amp;"),
            '<' => written.push_str("&lt;"),
            '>' => written.push_str("&gt;"),
            '"' => written.push_str("&quot;"),
            '\'' => written.push_str("&#x27;"),
            c => written.push(c),
        }
    }
    written
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    #[test]
    fn an_object_reads_as_python_writes_it() {
        assert_eq!(
            object(&[
                ("ok", json!(true)),
                ("note", json!("already — active \"x\"\n")),
                ("references", json!(["a.md", "b.md"])),
                ("emoji", json!("\u{1f600}")),
            ]),
            r#"{"ok": true, "note": "already \u2014 active \"x\"\n", "references": ["a.md", "b.md"], "emoji": "\ud83d\ude00"}"#
        );
    }

    #[test]
    fn a_name_is_quoted_as_python_s_repr_quotes_it() {
        assert_eq!(
            [
                quoted_name("nope"),
                quoted_name("it's"),
                quoted_name("C:\\x.md"),
                quoted_name("a\u{1}b")
            ],
            ["'nope'", "\"it's\"", "'C:\\\\x.md'", "'a\\x01b'"]
        );
    }

    #[test]
    fn markup_is_escaped_as_python_s_html_escape_does() {
        assert_eq!(
            escaped(r#"<Acme> & "API" 'x'"#),
            "&lt;Acme&gt; &amp; &quot;API&quot; &#x27;x&#x27;"
        );
    }
}

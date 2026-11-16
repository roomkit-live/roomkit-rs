//! What the model reads of the skills, as the reference implementation
//! writes it: the preamble before the manifest, the two tools, and what
//! they answer, each answer JSON written as Python's `json.dumps` writes
//! it.

mod json;

pub(super) use self::json::fits_map;
use self::json::{write_object, write_value};
/// A name written for a message as Python's `repr` of a `str` writes it.
pub(crate) use roomkit_core::rules::python_repr as quoted_name;
use serde_json::Value;

/// Python's `SKILLS_PREAMBLE`, before the manifest.
pub(crate) const PREAMBLE: &str = "You have access to Agent Skills — specialized knowledge \
packages. Use the activate_skill tool to load a skill's full instructions before using it. \
Available skills are listed below.";

/// Appended when no executor is configured or policy denies its tool.
pub(crate) const NO_SCRIPTS_NOTE: &str =
    " Note: Script execution is not available in this environment.";

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

/// The script tool's name, reserved whenever a registry is configured (RFC §24).
pub(crate) const RUN_SKILL_SCRIPT: &str = "run_skill_script";

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
    let mut written = String::new();
    let _infallible = write_object(
        &mut written,
        members.iter().map(|(name, value)| (*name, value)),
        true,
    );
    written
}

/// `value` as Python's `json.dumps(value)` writes it by default: `", "`
/// and `": "` between members, every character past ASCII escaped. For
/// the MCP crate, whose `call_tool` answers a failed call as Python's
/// `json.dumps({"error": ...})`.
#[doc(hidden)]
pub fn dumps(value: &Value) -> String {
    let mut written = String::new();
    let _infallible = write_value(&mut written, value, true);
    written
}

/// `value` as Python's `json.dumps(value, ensure_ascii=False)` writes it:
/// `", "` and `": "` between members, every character as itself.
pub(crate) fn dumps_unescaped(value: &Value) -> String {
    let mut written = String::new();
    let _infallible = write_value(&mut written, value, false);
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

    // Python: json.dumps("a\x7fb") and json.dumps("a\x7fb", ensure_ascii=False)
    #[test]
    fn dumps_escapes_a_delete_as_python_s_ensure_ascii_does() {
        let text = json!("a\u{7f}b");
        assert_eq!(dumps(&text), "\"a\\u007fb\"");
        assert_eq!(dumps_unescaped(&text), "\"a\u{7f}b\"");
    }

    /// A float is written as Python's `json.dumps` writes it,
    /// `float.__repr__` (RMK-1262).
    #[test]
    fn a_float_reads_as_python_writes_it() {
        assert_eq!(
            dumps(&json!({ "ms": 1e-05, "big": 1e16, "n": 3 })),
            r#"{"ms": 1e-05, "big": 1e+16, "n": 3}"#
        );
    }

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
    fn markup_is_escaped_as_python_s_html_escape_does() {
        assert_eq!(
            escaped(r#"<Acme> & "API" 'x'"#),
            "&lt;Acme&gt; &amp; &quot;API&quot; &#x27;x&#x27;"
        );
    }
}

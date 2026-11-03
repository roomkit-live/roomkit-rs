//! A content as Python's `str()` writes it: what the memory layer reads of
//! a content that carries no text (a location, a tool call, an edit), which
//! still costs tokens (Python's `extract_event_text`).

use std::fmt::Write as _;

use roomkit::model::EventContent;
use serde_json::{Map, Value};

use crate::skills::quoted_name;

/// The fields of each content, in the order Python's model declares them,
/// after `type`, and the name of its class.
fn fields(kind: &str) -> (&'static str, &'static [&'static str]) {
    match kind {
        "text" => ("TextContent", &["body", "language"]),
        "rich" => (
            "RichContent",
            &[
                "body",
                "format",
                "plain_text",
                "buttons",
                "cards",
                "quick_replies",
            ],
        ),
        "media" => (
            "MediaContent",
            &["url", "mime_type", "filename", "size_bytes", "caption"],
        ),
        "location" => (
            "LocationContent",
            &["latitude", "longitude", "label", "address"],
        ),
        "audio" => (
            "AudioContent",
            &["url", "mime_type", "duration_seconds", "transcript"],
        ),
        "video" => (
            "VideoContent",
            &["url", "mime_type", "duration_seconds", "thumbnail_url"],
        ),
        "composite" => ("CompositeContent", &["parts"]),
        "system" => ("SystemContent", &["body", "code", "data"]),
        "template" => (
            "TemplateContent",
            &["template_id", "language", "parameters", "body"],
        ),
        "edit" => (
            "EditContent",
            &["target_event_id", "new_content", "edit_source"],
        ),
        "delete" => (
            "DeleteContent",
            &["target_event_id", "delete_type", "reason"],
        ),
        "tool_call" => (
            "ToolCallContent",
            &[
                "tool_name",
                "tool_id",
                "arguments",
                "result",
                "status",
                "duration_ms",
                "error",
                "structured_content",
                "outcome",
                "refused_but_ran",
                "intent",
            ],
        ),
        _ => ("Content", &[]),
    }
}

/// A field Python types as an enum, which its `repr` writes
/// `<Class.MEMBER: 'value'>`, by the content's kind and the field's name.
fn enum_class(kind: &str, field: &str) -> Option<&'static str> {
    match (kind, field) {
        ("edit", "edit_source") => Some("EditSource"),
        ("delete", "delete_type") => Some("DeleteType"),
        _ => None,
    }
}

/// `content` as Python's `str(content)` writes a model: each field as
/// `name=repr(value)`, a space between.
pub(super) fn python_str(content: &EventContent) -> String {
    let value = serde_json::to_value(content).unwrap_or(Value::Null);
    let Value::Object(members) = value else {
        return String::new();
    };
    members_written(&members, " ")
}

/// `members`, a content's serialized form, as Python writes its fields,
/// `separator` between them.
fn members_written(members: &Map<String, Value>, separator: &str) -> String {
    let kind = members.get("type").and_then(Value::as_str).unwrap_or("");
    let (_, names) = fields(kind);
    let mut written = vec![format!("type={}", quoted_name(kind))];
    for name in names {
        let value = members.get(*name).unwrap_or(&Value::Null);
        let shown = match (enum_class(kind, name), value) {
            (Some(class), Value::String(member)) => {
                format!(
                    "<{class}.{}: {}>",
                    member.to_uppercase(),
                    quoted_name(member)
                )
            }
            _ if *name == "new_content" => model_repr(value),
            _ => repr(value),
        };
        written.push(format!("{name}={shown}"));
    }
    written.join(separator)
}

/// A nested content as Python's `repr` writes a model:
/// `TextContent(type='text', body='x', language=None)`.
fn model_repr(value: &Value) -> String {
    let Value::Object(members) = value else {
        return repr(value);
    };
    let kind = members.get("type").and_then(Value::as_str).unwrap_or("");
    let (class, _) = fields(kind);
    format!("{class}({})", members_written(members, ", "))
}

/// `value` as Python's `repr` writes the value it reads as.
pub(crate) fn repr(value: &Value) -> String {
    match value {
        Value::Null => "None".to_owned(),
        Value::Bool(true) => "True".to_owned(),
        Value::Bool(false) => "False".to_owned(),
        Value::Number(number) if number.is_f64() => {
            number.as_f64().map_or_else(String::new, float_repr)
        }
        Value::Number(number) => number.to_string(),
        Value::String(text) => quoted_name(text),
        Value::Array(items) => {
            let items: Vec<String> = items.iter().map(repr).collect();
            format!("[{}]", items.join(", "))
        }
        Value::Object(members) => {
            let members: Vec<String> = members
                .iter()
                .map(|(name, value)| format!("{}: {}", quoted_name(name), repr(value)))
                .collect();
            format!("{{{}}}", members.join(", "))
        }
    }
}

/// `number` as Python's `repr` writes a float: its shortest digits, in
/// positional notation for an exponent from -4 to 15, `.0` on a whole
/// number, else in scientific notation with a signed exponent of two
/// digits at least (`1e+16`, `1e-05`).
pub(crate) fn float_repr(number: f64) -> String {
    if number.is_nan() {
        return "nan".to_owned();
    }
    if number.is_infinite() {
        return if number > 0.0 { "inf" } else { "-inf" }.to_owned();
    }
    let scientific = format!("{number:e}");
    let (mantissa, exponent) = scientific.split_once('e').unwrap_or((&scientific, "0"));
    let exponent: i32 = exponent.parse().unwrap_or(0);
    let negative = mantissa.starts_with('-');
    let digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
    let sign = if negative { "-" } else { "" };
    if !(-4..16).contains(&exponent) {
        let (first, rest) = digits.split_at(1);
        let fraction = if rest.is_empty() {
            String::new()
        } else {
            format!(".{rest}")
        };
        let exponent_sign = if exponent < 0 { '-' } else { '+' };
        return format!(
            "{sign}{first}{fraction}e{exponent_sign}{:02}",
            exponent.abs()
        );
    }
    let mut written = String::from(sign);
    if exponent < 0 {
        written.push_str("0.");
        for _ in 0..(-exponent - 1) {
            written.push('0');
        }
        written.push_str(&digits);
        return written;
    }
    let whole = usize::try_from(exponent).unwrap_or(0) + 1;
    if digits.len() <= whole {
        let _infallible = write!(written, "{digits:0<whole$}.0");
    } else {
        let (integer, fraction) = digits.split_at(whole);
        let _infallible = write!(written, "{integer}.{fraction}");
    }
    written
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use roomkit::model::{
        DeleteContent, EventId, Latitude, LocationContent, Longitude, Metadata, ToolCallContent,
    };

    use super::*;

    #[test]
    fn a_float_reads_as_python_s_repr() {
        let read: Vec<String> = [45.5, -73.0, 1e16, 1e-5, 0.0001, 123.456, 0.0, 2.5e-7]
            .into_iter()
            .map(float_repr)
            .collect();
        assert_eq!(
            read,
            [
                "45.5", "-73.0", "1e+16", "1e-05", "0.0001", "123.456", "0.0", "2.5e-07"
            ]
        );
    }

    #[test]
    fn a_content_without_text_reads_as_python_s_str() {
        let location =
            LocationContent::new(Latitude::new(45.5).unwrap(), Longitude::new(-73.0).unwrap());
        assert_eq!(
            python_str(&EventContent::Location(location)),
            "type='location' latitude=45.5 longitude=-73.0 label=None address=None"
        );
        let delete = DeleteContent::new(EventId::new("e1").unwrap());
        assert_eq!(
            python_str(&EventContent::Delete(delete)),
            "type='delete' target_event_id='e1' delete_type=<DeleteType.SENDER: 'sender'> \
             reason=None"
        );
        let mut arguments = Metadata::new();
        arguments.insert("q".to_owned(), Value::from("a"));
        let call = ToolCallContent {
            arguments,
            ..ToolCallContent::new("lookup", "c1")
        };
        assert_eq!(
            python_str(&EventContent::ToolCall(call)),
            "type='tool_call' tool_name='lookup' tool_id='c1' arguments={'q': 'a'} result=None \
             status='pending' duration_ms=None error=None structured_content=None outcome=None \
             refused_but_ran=False intent=None"
        );
    }
}

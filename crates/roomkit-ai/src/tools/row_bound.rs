//! What a `TOOL_CALL_END` row keeps of a tool's result (RFC §21.5):
//! Python's `channels/_tool_event_result.py`.
//!
//! The row is stored, broadcast and handed to the hooks of the event
//! pipeline, so a screenshot's base64 must not ride along without bound.
//! The model's own copy of the result is not this: it keeps every image.
//! A result reaches the row as content parts, as a text, or as JSON, a
//! handler's JSON answer, which roomkit-rs keeps as JSON where Python dumps
//! it to a string (and walks JSON only of an ACP agent's output and a
//! structured copy); each binary payload in it is kept while the payloads
//! kept so far, its own included, stay within the bound, else a note
//! naming its kind, type and size stands in its place.

use serde_json::{Map, Value};

/// Data-URL or base64 characters of binary payloads a row keeps: Python's
/// `TOOL_EVENT_IMAGE_MAX_CHARS`.
const ROW_PAYLOAD_MAX_CHARS: usize = 512 * 1024;

/// How far into a text a data URI's header is looked for: Python's
/// `_DATA_URI_HEADER_MAX`.
const DATA_URI_HEADER_MAX: usize = 256;

/// JSON deeper than this is kept as it is: no binary block hides that deep
/// in a real tool result, and the walk must not recurse without bound.
/// Python's `_MAX_DEPTH`.
const MAX_DEPTH: usize = 32;

/// The whitespace Python's `\s` matches in ASCII mode, which ends a data
/// URI parameter's value.
const ASCII_SPACE: &str = " \t\n\r\u{b}\u{c}";

/// The payload characters a row has kept so far: Python's `_Budget`.
#[derive(Debug, Default)]
pub(super) struct Budget {
    used: usize,
}

impl Budget {
    /// Whether a payload of `size` characters still fits, counted kept
    /// when it does.
    pub(super) fn keep(&mut self, size: usize) -> bool {
        if self.used + size > ROW_PAYLOAD_MAX_CHARS {
            return false;
        }
        self.used += size;
        true
    }
}

/// The note standing for a payload of `kind` and `mime`, `size` characters
/// long, the row does not keep: Python's `_note`.
pub(super) fn note(kind: &str, mime: Option<&str>, size: usize) -> String {
    let label = match mime {
        Some(mime) if !mime.is_empty() => format!("{kind} {mime}"),
        _ => kind.to_owned(),
    };
    let decoded_kb = (size * 3 / 4 / 1024).max(1);
    format!("[{label}, {decoded_kb} KB, not kept in the event]")
}

/// `text` as a row keeps it: a data URI past the bound becomes its note.
pub(super) fn text(text: String, budget: &mut Budget) -> String {
    let Some(DataUri { mime }) = data_uri(&text) else {
        return text;
    };
    let size = text.chars().count();
    if budget.keep(size) {
        return text;
    }
    note("data URI", mime, size)
}

/// `value` as a row keeps it: each binary block and each data URI kept
/// while it fits, its note otherwise; past [`MAX_DEPTH`], as it is.
/// Python's `_walk` on JSON.
pub(super) fn json(value: Value, budget: &mut Budget) -> Value {
    walk(value, budget, 0)
}

fn walk(value: Value, budget: &mut Budget, depth: usize) -> Value {
    if depth > MAX_DEPTH {
        return value;
    }
    match value {
        Value::String(string) => Value::String(text(string, budget)),
        Value::Array(items) => Value::Array(
            items
                .into_iter()
                .map(|item| walk(item, budget, depth + 1))
                .collect(),
        ),
        Value::Object(node) => object(node, budget, depth),
        other @ (Value::Null | Value::Bool(_) | Value::Number(_)) => other,
    }
}

/// A JSON object as a row keeps it: a binary block whole while it fits,
/// else a text block (a resource with the note as its text); any other
/// object with each member walked.
fn object(node: Map<String, Value>, budget: &mut Budget, depth: usize) -> Value {
    let Some(block) = binary_block(&node) else {
        let walked = node
            .into_iter()
            .map(|(key, value)| (key, walk(value, budget, depth + 1)))
            .collect();
        return Value::Object(walked);
    };
    if budget.keep(block.size) {
        return Value::Object(node);
    }
    let words = note(block.kind, block.mime.as_deref(), block.size);
    if block.kind == "resource" {
        // Still a resource: its uri, and the note as its text.
        let uri = node.get("uri").cloned().unwrap_or(Value::Null);
        return serde_json::json!({"uri": uri, "mimeType": "text/plain", "text": words});
    }
    serde_json::json!({"type": "text", "text": words})
}

/// A JSON block carrying binary data: its kind, its type, its payload's
/// size in characters.
struct BinaryBlock {
    kind: &'static str,
    mime: Option<String>,
    size: usize,
}

/// The binary block `node` is, if any: an image or audio block (MCP and
/// ACP put the base64 in `data`, Anthropic in `source.data`, a stored image
/// part in `url`) or a blob resource (`blob` beside its `uri`, MCP's
/// `BlobResourceContents`). A `data` or `blob` member of any other object
/// is somebody's text. Python's `_binary_block`.
fn binary_block(node: &Map<String, Value>) -> Option<BinaryBlock> {
    let (kind, payload, mime) = match node.get("type").and_then(Value::as_str) {
        Some(kind @ ("image" | "audio")) => {
            let source = node.get("source").and_then(Value::as_object);
            let from_source = |key: &str| source.and_then(|source| source.get(key));
            let payload = first_set([node.get("data"), node.get("url"), from_source("data")]);
            let mime = first_set([
                node.get("mimeType"),
                node.get("mime_type"),
                from_source("media_type"),
            ]);
            let kind = if kind == "image" { "image" } else { "audio" };
            (kind, payload, mime)
        }
        _ => match (node.get("blob"), node.get("uri")) {
            (Some(Value::String(_)), Some(Value::String(_))) => {
                ("resource", node.get("blob"), node.get("mimeType"))
            }
            _ => return None,
        },
    };
    let payload = payload?.as_str()?;
    Some(BinaryBlock {
        kind,
        mime: mime.and_then(Value::as_str).map(str::to_owned),
        size: payload.chars().count(),
    })
}

/// The first of `values` Python's `or` reads as set: present, and neither
/// null, false, zero, nor an empty string, list or object. Where none is,
/// Python's `or` yields the last, an empty payload kept whole; here the
/// block is none, and its members are walked.
fn first_set<const N: usize>(values: [Option<&Value>; N]) -> Option<&Value> {
    values.into_iter().flatten().find(|value| match value {
        Value::Null => false,
        Value::Bool(set) => *set,
        Value::Number(number) => number.as_f64() != Some(0.0),
        Value::String(text) => !text.is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::Object(members) => !members.is_empty(),
    })
}

/// A data URI's header, as Python's `_DATA_URI` reads it: the type it
/// names, if any.
#[derive(Debug, PartialEq, Eq)]
struct DataUri<'text> {
    mime: Option<&'text str>,
}

/// The data URI `text` starts with, its header within the first
/// [`DATA_URI_HEADER_MAX`] characters; `None` for a text that is no data
/// URI. Python's `_DATA_URI` match: `data:`, an optional `type/subtype`,
/// parameters `;name=value`, an optional `;base64`, then `,`.
fn data_uri(text: &str) -> Option<DataUri<'_>> {
    let head = text
        .char_indices()
        .nth(DATA_URI_HEADER_MAX)
        .and_then(|(end, _)| text.get(..end))
        .unwrap_or(text);
    let rest = head.strip_prefix("data:")?;
    let (mime, mut rest) = media_type(rest);
    loop {
        if rest.starts_with(',') {
            return Some(DataUri { mime });
        }
        let (name, after) = token(rest.strip_prefix(';')?);
        if name.is_empty() {
            return None;
        }
        if let Some(value) = after.strip_prefix('=') {
            let end = value
                .find(|c: char| c == ';' || c == ',' || ASCII_SPACE.contains(c))
                .unwrap_or(value.len());
            rest = value.get(end..)?;
        } else if name == "base64" {
            // `;base64` closes the header: a `,` must follow.
            return after.starts_with(',').then_some(DataUri { mime });
        } else {
            return None;
        }
    }
}

/// The `type/subtype` `header` starts with, if any, and what follows it.
fn media_type(header: &str) -> (Option<&str>, &str) {
    let (kind, after) = token(header);
    let Some(subtype) = after.strip_prefix('/') else {
        return (None, header);
    };
    let (sub, rest) = token(subtype);
    if kind.is_empty() || sub.is_empty() {
        return (None, header);
    }
    let length = kind.len() + 1 + sub.len();
    (header.get(..length), rest)
}

/// The run of `[A-Za-z0-9_.+-]` `text` starts with, and what follows it.
fn token(text: &str) -> (&str, &str) {
    let end = text
        .find(|c: char| !(c.is_ascii_alphanumeric() || "_.+-".contains(c)))
        .unwrap_or(text.len());
    text.split_at_checked(end).unwrap_or((text, ""))
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    const NOTE: &str = "not kept in the event";

    fn b64() -> String {
        "A".repeat(300 * 1024)
    }

    fn kept(value: Value) -> Value {
        json(value, &mut Budget::default())
    }

    // Python: tests/test_tool_event_result.py::TestJsonShapes::test_binary_blocks_share_the_bound
    #[test]
    fn binary_blocks_share_the_bound() {
        let blocks = [
            json!({"type": "image", "data": b64(), "mimeType": "image/png"}),
            json!({"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": b64()}}),
            json!({"type": "image", "url": format!("data:image/png;base64,{}", b64()), "mime_type": "image/png"}),
            json!({"type": "audio", "data": b64(), "mimeType": "audio/wav"}),
        ];
        for block in blocks {
            let row = kept(json!({"items": [block.clone(), block.clone()]}));
            assert_eq!(row["items"][0], block);
            assert_eq!(row["items"][1]["type"], "text");
            assert!(row["items"][1]["text"].as_str().unwrap().contains(NOTE));
        }
    }

    // Python: tests/test_tool_event_result.py::TestJsonShapes::test_a_blob_resource_left_out_stays_a_resource
    #[test]
    fn a_blob_resource_left_out_stays_a_resource() {
        let blob =
            json!({"uri": "file:///report.pdf", "blob": b64(), "mimeType": "application/pdf"});
        let row = kept(json!([blob.clone(), blob.clone()]));
        let note = row[1]["text"].as_str().unwrap();
        assert_eq!(
            (
                &row[0],
                &row[1]["uri"],
                note.contains("application/pdf"),
                note.contains(NOTE)
            ),
            (&blob, &json!("file:///report.pdf"), true, true)
        );
    }

    // Python: tests/test_tool_event_result.py::TestJsonShapes::test_text_under_a_blob_key_without_a_uri_is_left_alone
    #[test]
    fn text_under_a_blob_key_without_a_uri_is_left_alone() {
        let note = json!({"path": "a.txt", "blob": "line\n".repeat(200_000)});
        let rows = json!([note.clone(), note]);
        assert_eq!(kept(rows.clone()), rows);
    }

    // Python: tests/test_tool_event_result.py::TestJsonShapes::test_a_data_uri_string_is_bounded
    #[test]
    fn a_data_uri_string_is_bounded() {
        let uri = format!("data:image/jpeg;base64,{}", b64());
        let row = kept(json!({"a": uri, "b": uri}));
        assert_eq!(
            row,
            json!({"a": uri, "b": "[data URI image/jpeg, 225 KB, not kept in the event]"})
        );
    }

    // Python: tests/test_tool_event_result.py::TestJsonShapes::test_text_that_starts_with_data_is_not_a_data_uri
    #[test]
    fn text_that_starts_with_data_is_not_a_data_uri() {
        let uri = format!("data:image/png;base64,{}", "A".repeat(600 * 1024));
        let log = "data: {\"event\": \"tick\"}\n\n".repeat(30_000);
        let row = kept(json!([uri, log]));
        assert!(row[0].as_str().unwrap().contains(NOTE));
        assert_eq!(row[1].as_str(), Some(log.as_str()));
    }

    // Python: tests/test_tool_event_result.py::TestJsonShapes::test_text_under_a_data_key_is_left_alone
    #[test]
    fn text_under_a_data_key_is_left_alone() {
        let rows = json!({"data": "row,".repeat(400_000)});
        assert_eq!(kept(rows.clone()), rows);
    }

    // Python: tests/test_tool_event_result.py::TestJsonShapes::test_a_deep_json_is_kept_as_it_is_past_the_walk
    #[test]
    fn a_deep_json_is_kept_as_it_is_past_the_walk() {
        let leaf = format!("data:image/png;base64,{}", "A".repeat(600 * 1024));
        let mut deep = json!(leaf);
        for _ in 0..40 {
            deep = json!([deep]);
        }
        let mut row = kept(deep);
        for _ in 0..40 {
            row = row[0].take();
        }
        assert_eq!(row, json!(leaf));
    }

    /// The headers Python's `_DATA_URI` matches, and those it does not.
    #[test]
    fn a_data_uri_s_header_is_read_as_python_s_pattern() {
        let read = |mime| Some(DataUri { mime });
        let cases = [
            ("data:image/png;base64,AAAA", read(Some("image/png"))),
            ("data:,hello", read(None)),
            ("data:text/plain;charset=utf-8,hi", read(Some("text/plain"))),
            ("data:;base64,AAAA", read(None)),
            ("data: {\"event\": 1}", None),
            ("data:text,hi", None),
            ("data:image/png;base64AAAA", None),
            ("plain text", None),
        ];
        for (text, uri) in cases {
            assert_eq!(data_uri(text), uri, "{text}");
        }
    }
}

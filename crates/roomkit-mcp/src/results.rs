//! A server's answer to a call, as the model reads it (RFC §21.2, §21.4):
//! its text, a note for what is not text, its words when it says it
//! failed, within a bound.

use rmcp::model::{CallToolResult, ContentBlock, ResourceContents};
use roomkit_ai::{ToolError, ToolResult, dumps};

use crate::texts::bounded_result;

/// The longest result the model reads, in characters: past it, the text is
/// cut with a note saying how long it was.
pub(crate) const RESULT_LIMIT: usize = 100_000;

/// The longest media type a note names.
const MEDIA_TYPE_LIMIT: usize = 64;

/// `result`, the server's answer to a call of `name`, as the model reads
/// it, within [`RESULT_LIMIT`] characters:
/// - each part's text, one part read as itself and several as a JSON array
///   of them written as Python's `json.dumps` writes it, as the reference
///   implementation's `text_body` reads them;
/// - what is not text (an image, audio, a binary resource, a link to a
///   resource) as a one-line note naming its kind and type, never its
///   encoding, since base64 read as text is noise the model pays for (RFC
///   §21.2);
/// - a result with no part, an empty JSON array (`[]`), whatever its
///   structured content, which the model never reads;
/// - a result that says `isError`, a failed call (the tool ran) whose words
///   the model reads, its parts' texts joined by spaces.
pub(crate) fn read(name: &str, result: CallToolResult) -> Result<ToolResult, ToolError> {
    if result.is_error == Some(true) {
        return Err(ToolError::failed(bounded(name, &error_text(&result))));
    }
    Ok(ToolResult::Text(body(name, result)))
}

/// `result` as Python's `call_tool` returns it: its text, as [`read`]
/// reads it, or for a result that says `isError` the envelope
/// `{"error": "<its words>"}`, written as Python's `json.dumps` writes it.
pub(crate) fn text(name: &str, result: CallToolResult) -> String {
    if result.is_error == Some(true) {
        let words = bounded(name, &error_text(&result));
        return dumps(&serde_json::json!({ "error": words }));
    }
    body(name, result)
}

/// The words of a result that says `isError`: its parts' texts joined by
/// spaces, as Python's `error_text` joins them.
fn error_text(result: &CallToolResult) -> String {
    let texts: Vec<String> = result.content.iter().map(text_of).collect();
    texts.join(" ")
}

/// The text of a result that does not say `isError`, within the bound.
fn body(name: &str, result: CallToolResult) -> String {
    let texts: Vec<String> = result.content.iter().map(text_of).collect();
    let text = match texts.as_slice() {
        [only] => only.clone(),
        texts => dumps(&serde_json::Value::from(texts)),
    };
    bounded(name, &text)
}

/// `text` within [`RESULT_LIMIT`], with a warning naming the tool when it
/// is cut.
fn bounded(name: &str, text: &str) -> String {
    let cut = bounded_result(text, RESULT_LIMIT);
    if cut != text {
        tracing::warn!(
            target: "roomkit::mcp",
            tool = name,
            kept = RESULT_LIMIT,
            "an MCP tool's result is cut"
        );
    }
    cut
}

/// One part of a result as the model reads it; a kind of part a later
/// version of the protocol adds is noted as content of an unknown kind.
fn text_of(part: &ContentBlock) -> String {
    match part {
        ContentBlock::Text(text) => text.text.clone(),
        ContentBlock::Image(image) => note("image", Some(&image.mime_type)),
        ContentBlock::Audio(audio) => note("audio", Some(&audio.mime_type)),
        ContentBlock::Resource(embedded) => resource_text(&embedded.resource),
        ContentBlock::ResourceLink(link) => note("resource_link", link.mime_type.as_deref()),
        _ => note("other", None),
    }
}

/// An embedded resource as the model reads it: its text, or a note for
/// binary content.
fn resource_text(resource: &ResourceContents) -> String {
    match resource {
        ResourceContents::TextResourceContents { text, .. } => text.clone(),
        ResourceContents::BlobResourceContents { mime_type, .. } => {
            note("resource", mime_type.as_deref())
        }
        _ => note("resource", None),
    }
}

/// The note standing for a part the model is not shown, as the reference
/// implementation words it; the type the server named is kept to the
/// characters a media type has, so that the note carries no text of the
/// server's.
fn note(kind: &str, media_type: Option<&str>) -> String {
    let named: String = media_type
        .unwrap_or_default()
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '.' | '+' | '-' | '_'))
        .take(MEDIA_TYPE_LIMIT)
        .collect();
    let named = if named.is_empty() { "unknown" } else { &named };
    format!("[{kind} content ({named}) not shown to the model]")
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    /// `call_tool` answers a failed call as Python's `json.dumps({"error":
    /// error_text(result)})`: the parts' words joined by a space, every
    /// character past ASCII escaped.
    #[test]
    fn a_failed_call_answers_python_s_error_envelope() {
        let result = CallToolResult::error(vec![
            ContentBlock::text("Commande déjà expédiée"),
            ContentBlock::text("\"42\""),
        ]);
        assert_eq!(
            text("cancel_order", result),
            r#"{"error": "Commande d\u00e9j\u00e0 exp\u00e9di\u00e9e \"42\""}"#
        );
    }
}

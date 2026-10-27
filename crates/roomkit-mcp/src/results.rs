//! A server's answer to a call, as the model reads it (RFC §21.2, §21.4):
//! its text, a note for what is not text, its words when it says it
//! failed, within a bound.

use rmcp::model::{CallToolResult, ContentBlock, ResourceContents};
use roomkit_ai::{ToolError, ToolOutput};

use crate::texts::bounded_result;

/// The longest result the model reads, in characters: past it, the text is
/// cut with a note saying how long it was.
pub(crate) const RESULT_LIMIT: usize = 100_000;

/// The longest media type a note names.
const MEDIA_TYPE_LIMIT: usize = 64;

/// `result`, the server's answer to a call of `name`, as the model reads
/// it, within [`RESULT_LIMIT`] characters:
/// - each part's text, one part read as itself and several as a JSON array
///   of them, as the reference implementation's `text_body` reads them;
/// - what is not text (an image, audio, a binary resource, a link to a
///   resource) as a one-line note naming its kind and type, never its
///   encoding, since base64 read as text is noise the model pays for (RFC
///   §21.2);
/// - a result with no part, its structured content as JSON;
/// - a result that says `isError`, a failed call (the tool ran) whose words
///   the model reads, its parts' texts joined by spaces.
pub(crate) fn read(name: &str, result: CallToolResult) -> Result<ToolOutput, ToolError> {
    let texts: Vec<String> = result.content.iter().map(text_of).collect();
    if result.is_error == Some(true) {
        let words = texts.join(" ");
        let words = if words.trim().is_empty() {
            format!("Tool '{name}' failed with no message")
        } else {
            words
        };
        return Err(ToolError::failed(bounded(name, &words)));
    }
    let text = match (texts.as_slice(), result.structured_content) {
        ([only], _) => only.clone(),
        ([], Some(structured)) => structured.to_string(),
        (texts, _) => serde_json::Value::from(texts).to_string(),
    };
    Ok(ToolOutput::Text(bounded(name, &text)))
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

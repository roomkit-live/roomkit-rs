//! A server's answer to a call, as the model reads it (RFC §21.2, §21.4):
//! its text, a note for what is not text, its words when it says it
//! failed, within a bound.

use rmcp::model::{CallToolResult, ContentBlock, ResourceContents};
use roomkit_ai::{AiImagePart, AiTextPart, ToolError, ToolResult, ToolResultPart, dumps};
use roomkit_core::rules::transcript::parse_data_uri;

use crate::texts::bounded_result;

/// The longest result the model reads, in characters: past it, the text is
/// cut with a note saying how long it was.
pub(crate) const RESULT_LIMIT: usize = 100_000;

/// The longest media type a note names.
const MEDIA_TYPE_LIMIT: usize = 64;

/// The raster formats every vision vendor accepts: an image of another
/// type (an SVG, a TIFF) would fail the whole request at the vendor, so it
/// reaches the model as a note. Python's `_MODEL_IMAGE_TYPES`.
const MODEL_IMAGE_TYPES: [&str; 4] = ["image/png", "image/jpeg", "image/gif", "image/webp"];

/// How many images of one result the model is shown, as an agent's
/// default `max_author_images`: the maintainer's bound (RMK-1242), which
/// the reference implementation does not have.
const MODEL_IMAGES_MAX: usize = 4;

/// How many data URL characters the images of one result the model is
/// shown take in all: the maintainer's bound (RMK-1242).
const MODEL_IMAGE_CHARS_MAX: usize = 5 * 1024 * 1024;

/// `result`, the server's answer to a call of `name`, as the model reads
/// it, as the reference implementation's `handler_result`:
/// - a result with an image the vendors accept (PNG, JPEG, GIF or WebP,
///   its base64 decoding), its parts in order, each image an image part
///   the model sees, each other part its text or its note, its texts
///   within [`RESULT_LIMIT`] characters in all, its first
///   [`MODEL_IMAGES_MAX`] images within [`MODEL_IMAGE_CHARS_MAX`] in all,
///   each image past either a note (the reference implementation bounds
///   none of them);
/// - otherwise, within [`RESULT_LIMIT`] characters, each part's text,
///   one part read as itself and several as a JSON array of them written
///   as Python's `json.dumps` writes it, as the reference implementation's
///   handler joins its parts' texts;
/// - what is not text (an image, audio, a binary resource, a link to a
///   resource) as a one-line note naming its kind and type, never its
///   encoding, since base64 read as text is noise the model pays for (RFC
///   §21.2);
/// - a result with no part, an empty JSON array (`[]`), whatever its
///   structured content, which the model never reads;
/// - a result that says `isError`, a failed call (the tool ran) whose words
///   the model reads, its parts' texts joined by spaces.
pub(crate) fn read(name: &str, result: &CallToolResult) -> Result<ToolResult, ToolError> {
    if result.is_error == Some(true) {
        return Err(ToolError::failed(bounded(name, &error_text(result))));
    }
    let parts: Vec<ToolResultPart> = result.content.iter().map(model_part).collect();
    if parts
        .iter()
        .any(|part| matches!(part, ToolResultPart::Image(_)))
    {
        return Ok(ToolResult::Parts(bounded_parts(name, parts)));
    }
    Ok(ToolResult::Text(body(name, result)))
}

/// `parts` with their texts within [`RESULT_LIMIT`] characters in all, as
/// a text result is: each text kept whole while it fits, the one past the
/// bound cut with a note saying how long it was, the texts after it left
/// out; its images within [`MODEL_IMAGES_MAX`] and
/// [`MODEL_IMAGE_CHARS_MAX`], each image past either its note.
fn bounded_parts(name: &str, parts: Vec<ToolResultPart>) -> Vec<ToolResultPart> {
    let mut left = RESULT_LIMIT;
    let mut cut = false;
    let mut images = ImageBudget::default();
    let kept: Vec<ToolResultPart> = parts
        .into_iter()
        .filter_map(|part| {
            let text = match part {
                ToolResultPart::Image(image) => return Some(images.keep(image)),
                ToolResultPart::Text(text) => text,
            };
            let size = text.text.chars().count();
            if size <= left {
                left -= size;
                return Some(ToolResultPart::Text(text));
            }
            cut = true;
            if left == 0 {
                return None;
            }
            let kept = bounded_result(&text.text, left);
            left = 0;
            Some(ToolResultPart::Text(AiTextPart::new(kept)))
        })
        .collect();
    if cut {
        tracing::warn!(
            target: "roomkit::mcp",
            tool = name,
            kept = RESULT_LIMIT,
            "an MCP tool's result is cut"
        );
    }
    kept
}

/// The images of one result the model is shown so far.
#[derive(Default)]
struct ImageBudget {
    count: usize,
    chars: usize,
}

impl ImageBudget {
    /// `image` as the model is shown it: itself while the result's images
    /// stay within their bounds, its note past them.
    fn keep(&mut self, image: AiImagePart) -> ToolResultPart {
        let size = image.url.chars().count();
        if self.count < MODEL_IMAGES_MAX && self.chars + size <= MODEL_IMAGE_CHARS_MAX {
            self.count += 1;
            self.chars += size;
            return ToolResultPart::Image(image);
        }
        let words = note("image", image.mime_type.as_deref());
        ToolResultPart::Text(AiTextPart::new(words))
    }
}

/// One part of a result as the model reads it: an image the vendors
/// accept as an image part, any other part as its text or its note.
/// Python's `_model_part`.
fn model_part(part: &ContentBlock) -> ToolResultPart {
    if let ContentBlock::Image(image) = part {
        let mime = match image.mime_type.as_str() {
            "" => "image/png",
            mime => mime,
        };
        let url = format!("data:{mime};base64,{}", image.data);
        if MODEL_IMAGE_TYPES.contains(&mime) && parse_data_uri(&url, None).is_ok() {
            return ToolResultPart::Image(AiImagePart {
                url,
                mime_type: Some(mime.to_owned()),
            });
        }
    }
    ToolResultPart::Text(AiTextPart::new(text_of(part)))
}

/// `result` as Python's `call_tool` returns it: its text, as [`read`]
/// reads it, or for a result that says `isError` the envelope
/// `{"error": "<its words>"}`, written as Python's `json.dumps` writes it.
pub(crate) fn text(name: &str, result: &CallToolResult) -> String {
    if result.is_error == Some(true) {
        let words = bounded(name, &error_text(result));
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
fn body(name: &str, result: &CallToolResult) -> String {
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

    /// An image of a vendor's type whose base64 lacks its padding, and one
    /// that names no type, read as `image/png`, reach the model as images,
    /// the padding repaired by the check alone.
    // Python: tests/test_mcp_tool_provider.py::test_tool_handler_returns_an_image_as_an_image_part,
    // its unpadded JPEG.
    #[test]
    fn an_unpadded_or_untyped_image_reaches_the_model() {
        let result = CallToolResult::success(vec![
            ContentBlock::text("the page"),
            ContentBlock::image("iVBORw0KGgo", "image/jpeg"),
            ContentBlock::image("iVBORw0KGgo", ""),
        ]);
        let image = |mime: &str| {
            ToolResultPart::Image(AiImagePart {
                url: format!("data:{mime};base64,iVBORw0KGgo"),
                mime_type: Some(mime.to_owned()),
            })
        };
        assert_eq!(
            read("search", &result),
            Ok(ToolResult::Parts(vec![
                ToolResultPart::Text(AiTextPart::new("the page")),
                image("image/jpeg"),
                image("image/png"),
            ]))
        );
    }

    /// The maintainer's bounds (RMK-1242): a result's first four images,
    /// within 5 MiB of data URL in all, reach the model; each image past
    /// either reads as its note.
    #[test]
    fn a_result_s_images_are_bounded_in_number_and_size() {
        let tiny = || ContentBlock::image("iVBORw0KGgo", "image/png");
        let big = ContentBlock::image("A".repeat(5 * 1024 * 1024), "image/jpeg");
        let many = CallToolResult::success((0..6).map(|_| tiny()).collect());
        let large = CallToolResult::success(vec![tiny(), big]);
        let shown = |result: &CallToolResult| match read("draw", result) {
            Ok(ToolResult::Parts(parts)) => parts
                .iter()
                .map(|part| match part {
                    ToolResultPart::Image(_) => "image".to_owned(),
                    ToolResultPart::Text(text) => text.text.clone(),
                })
                .collect::<Vec<_>>(),
            other => panic!("{other:?}"),
        };
        let png = "[image content (image/png) not shown to the model]";
        let jpeg = "[image content (image/jpeg) not shown to the model]";
        assert_eq!(
            (shown(&many), shown(&large)),
            (
                ["image", "image", "image", "image", png, png]
                    .map(str::to_owned)
                    .to_vec(),
                ["image", jpeg].map(str::to_owned).to_vec()
            )
        );
    }

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
            text("cancel_order", &result),
            r#"{"error": "Commande d\u00e9j\u00e0 exp\u00e9di\u00e9e \"42\""}"#
        );
    }
}

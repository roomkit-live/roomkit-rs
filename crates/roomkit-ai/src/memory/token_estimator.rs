//! A fast approximate count of tokens, for a memory that sizes the history
//! to a context window (RFC §20.3): Python's
//! `roomkit.memory.token_estimator`.

use roomkit::model::{EventContent, MediaContent, RoomEvent};
use roomkit_core::rules::transcoding::text_of;
use serde_json::Value;

use super::repr::python_str;
use crate::provider::{AiContentPart, AiContext, AiMessage, AiMessageContent};
use crate::skills::dumps;
use crate::tools::AiTool;

/// What one image costs, flat: a vendor bills an image by its downscaled
/// size, a thousand-odd tokens for a screenshot, whatever its base64's
/// length.
const IMAGE_TOKENS: usize = 1000;

/// What a message costs beyond its parts: its role and its delimiters.
const MESSAGE_OVERHEAD: usize = 4;

/// The image types every vision provider takes (RFC §6.4).
const VISION_IMAGE_TYPES: [&str; 4] = ["image/png", "image/jpeg", "image/gif", "image/webp"];

/// About one token for four characters of English: Python's
/// `estimate_tokens`, `len(text) // 4 + 1`, counted in code points.
pub fn estimate_tokens(text: &str) -> usize {
    text.chars().count() / 4 + 1
}

/// What a tool's definition costs in a request: its name, its description
/// and its parameters' JSON Schema as Python's `json.dumps` writes it.
/// Python's `estimate_tool_tokens`.
pub(crate) fn estimate_tool_tokens(tool: &AiTool) -> usize {
    let mut total = estimate_tokens(&tool.name) + estimate_tokens(&tool.description);
    if truthy(&tool.parameters) {
        total += estimate_tokens(&dumps(&tool.parameters));
    }
    total
}

/// Whether `value` reads as true, as Python's `if value:` reads it.
fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(flag) => *flag,
        Value::Number(number) => number.as_f64().is_some_and(|number| number != 0.0),
        Value::String(text) => !text.is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::Object(members) => !members.is_empty(),
    }
}

/// What `message` costs, its role's overhead included: a text part its
/// text, a call its name and its arguments as JSON, a result its text, an
/// image a flat thousand. Python's `estimate_message_tokens`.
pub fn estimate_message_tokens(message: &AiMessage) -> usize {
    let parts = match &message.content {
        AiMessageContent::Text(text) => return MESSAGE_OVERHEAD + estimate_tokens(text),
        AiMessageContent::Parts(parts) => parts,
    };
    MESSAGE_OVERHEAD + parts.iter().map(part_tokens).sum::<usize>()
}

/// What one part of a message costs.
fn part_tokens(part: &AiContentPart) -> usize {
    match part {
        AiContentPart::Text(text) => estimate_tokens(&text.text),
        AiContentPart::ToolCall(call) => {
            let arguments = Value::Object(call.arguments.clone().into_iter().collect());
            estimate_tokens(&call.name) + estimate_tokens(&dumps(&arguments))
        }
        AiContentPart::ToolResult(result) => estimate_tokens(&result.result),
        AiContentPart::Image(_) => IMAGE_TOKENS,
    }
}

/// The text of `event` as the memory layer reads it (RFC §6.6): the body a
/// transport sends, else a media as its label and a composite as its
/// parts, never a `data:` URI or a reference, else the content as
/// Python's `str()` writes it (a location, a tool call), which still costs
/// tokens and belongs in a summary. Python's `extract_event_text`.
pub fn extract_event_text(event: &RoomEvent) -> String {
    content_text(&event.content)
}

/// The text of `content` as the memory layer reads it.
fn content_text(content: &EventContent) -> String {
    if let Some(body) = transport_text(content)
        && (!body.is_empty() || matches!(content, EventContent::Text(_)))
    {
        return body;
    }
    match content {
        EventContent::Text(_)
        | EventContent::Rich(_)
        | EventContent::Media(_)
        | EventContent::Audio(_)
        | EventContent::Video(_)
        | EventContent::Composite(_) => text_of(content),
        // What carries no text, and a content a later version adds.
        EventContent::Location(_)
        | EventContent::System(_)
        | EventContent::Template(_)
        | EventContent::Edit(_)
        | EventContent::Delete(_)
        | EventContent::ToolCall(_)
        | _ => python_str(content),
    }
}

/// The body a transport sends of `content`: Python's
/// `providers.utils.extract_event_text`; `None` for a content with no
/// body, an empty text for a template with none.
fn transport_text(content: &EventContent) -> Option<String> {
    match content {
        EventContent::Text(text) => Some(text.body.clone()),
        EventContent::Rich(rich) => Some(rich.body.clone()),
        EventContent::System(system) => Some(system.body.clone()),
        EventContent::Template(template) => Some(template.body.clone().unwrap_or_default()),
        EventContent::Media(_)
        | EventContent::Location(_)
        | EventContent::Audio(_)
        | EventContent::Video(_)
        | EventContent::Composite(_)
        | EventContent::Edit(_)
        | EventContent::Delete(_)
        | EventContent::ToolCall(_)
        | _ => None,
    }
}

/// What [`estimate_event_tokens`] takes beside the event: Python's
/// keyword-only `text_only=False`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EstimateEventTokensOptions {
    /// Leaves out the flat cost of an image and what is not text: a
    /// refusal by a message's length cannot rely on the image's estimate,
    /// which may overstate a small one.
    pub text_only: bool,
}

/// What `event` costs, a media's payload never billed as text: a composite
/// its parts, a media no vision model takes its label, an image its
/// caption and a flat thousand, a voice message its transcript, any other
/// its text. Python's `estimate_event_tokens`.
pub fn estimate_event_tokens(event: &RoomEvent, options: EstimateEventTokensOptions) -> usize {
    content_tokens(&event.content, options.text_only)
}

/// What `content`, an event's or one of its parts, costs.
fn content_tokens(content: &EventContent, text_only: bool) -> usize {
    match content {
        EventContent::Composite(composite) => composite
            .parts()
            .iter()
            .map(|part| content_tokens(part, text_only))
            .sum(),
        EventContent::Media(media) if !is_vision_image(media) => estimate_tokens(&text_of(content)),
        EventContent::Media(media) => {
            let image = if text_only { 0 } else { IMAGE_TOKENS };
            spoken_tokens(media.caption.as_deref(), image)
        }
        EventContent::Audio(audio) => spoken_tokens(audio.transcript.as_deref(), 0),
        EventContent::Video(_) => 0,
        _ if text_only => transport_text(content)
            .filter(|text| !text.is_empty())
            .map_or(0, |text| estimate_tokens(&text)),
        EventContent::Text(_)
        | EventContent::Rich(_)
        | EventContent::Location(_)
        | EventContent::System(_)
        | EventContent::Template(_)
        | EventContent::Edit(_)
        | EventContent::Delete(_)
        | EventContent::ToolCall(_)
        | _ => estimate_tokens(&content_text(content)),
    }
}

/// What a media's message costs: its `text` (a caption, a transcript) and
/// its `image`'s flat cost; nothing when it has neither.
fn spoken_tokens(text: Option<&str>, image: usize) -> usize {
    let text = text.filter(|text| !text.is_empty());
    if text.is_none() && image == 0 {
        return 0;
    }
    MESSAGE_OVERHEAD + text.map_or(0, estimate_tokens) + image
}

/// Whether `media` is an image of a type every vision provider takes: its
/// `data:` URI's type, else its own, folded (`image/jpg` is `image/jpeg`).
fn is_vision_image(media: &MediaContent) -> bool {
    let declared = media
        .url
        .get(..5)
        .filter(|scheme| scheme.eq_ignore_ascii_case("data:"))
        .and_then(|_| media.url.get(5..))
        .map(|rest| rest.split(',').next().unwrap_or(rest));
    let kind = folded(declared.unwrap_or(&media.mime_type));
    VISION_IMAGE_TYPES.contains(&kind.as_str())
}

/// `mime_type` as RoomKit compares it: lowercased, its parameters aside,
/// its aliases folded.
fn folded(mime_type: &str) -> String {
    let bare = mime_type
        .split(';')
        .next()
        .unwrap_or(mime_type)
        .trim()
        .to_lowercase();
    match bare.as_str() {
        "image/jpg" | "image/pjpeg" => "image/jpeg".to_owned(),
        _ => bare,
    }
}

/// What `context` costs: its system prompt, its messages and its tools.
/// Python's `estimate_context_tokens`.
pub fn estimate_context_tokens(context: &AiContext) -> usize {
    let prompt = context
        .system_prompt
        .as_deref()
        .filter(|prompt| !prompt.is_empty())
        .map_or(0, estimate_tokens);
    prompt
        + context
            .messages
            .iter()
            .map(estimate_message_tokens)
            .sum::<usize>()
        + context
            .tools
            .iter()
            .map(estimate_tool_tokens)
            .sum::<usize>()
}

/// What a memory's notes for the turn occupy in the window: they ride the
/// turn's input, outside the history, and no trimmer cuts them. Python's
/// `estimate_notes_tokens`.
pub fn estimate_notes_tokens(notes: &[String]) -> usize {
    notes.iter().map(|note| estimate_tokens(note)).sum()
}

/// What [`history_budget`] takes beside the window: Python's keywords and
/// their defaults.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct HistoryBudgetOptions<'a> {
    /// The share of the window kept for the model's reply: 0.15.
    pub safety_margin_ratio: f64,
    /// What the caller knows occupies the window and the trimmer cannot
    /// see, a system prompt and tool schemas: 0 says nothing else does.
    pub reserved_tokens: usize,
    /// The messages a memory injects, passed through untrimmed.
    pub messages: Option<&'a [AiMessage]>,
    /// The turn the channel places after the history, which no trimmer
    /// cuts.
    pub current_event: Option<&'a RoomEvent>,
    /// The reply budget the turn requests, when known: the reply is
    /// reserved once, the larger of the margin and this budget.
    pub reply_tokens: usize,
}

impl Default for HistoryBudgetOptions<'_> {
    fn default() -> Self {
        Self {
            safety_margin_ratio: 0.15,
            reserved_tokens: 0,
            messages: None,
            current_event: None,
            reply_tokens: 0,
        }
    }
}

/// The tokens the conversation's history may occupy in a window of
/// `max_context_tokens`, once the rest of the prompt is paid for (RFC
/// §20.3): Python's `history_budget`.
///
/// What the whole prompt may take is the window less its safety margin,
/// or less the reply budget when that is larger; from it go the reserved
/// tokens, the injected messages and the current turn. Never below 0.
pub fn history_budget(max_context_tokens: usize, options: HistoryBudgetOptions<'_>) -> usize {
    let window = i64::try_from(max_context_tokens).unwrap_or(i64::MAX);
    let reply = i64::try_from(options.reply_tokens).unwrap_or(i64::MAX);
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_precision_loss,
        reason = "Python's int(max_context_tokens * (1 - safety_margin_ratio)), cut toward zero"
    )]
    let margin = (window as f64 * (1.0 - options.safety_margin_ratio)) as i64;
    let prompt = margin.min(window.saturating_sub(reply));
    let injected: usize = options
        .messages
        .unwrap_or_default()
        .iter()
        .map(estimate_message_tokens)
        .sum();
    let current = options.current_event.map_or(0, |event| {
        estimate_event_tokens(event, EstimateEventTokensOptions::default())
    });
    let spent = [options.reserved_tokens, injected, current]
        .into_iter()
        .map(|tokens| i64::try_from(tokens).unwrap_or(i64::MAX))
        .fold(0_i64, i64::saturating_add);
    usize::try_from(prompt.saturating_sub(spent).max(0)).unwrap_or(0)
}

#[cfg(test)]
mod tests;

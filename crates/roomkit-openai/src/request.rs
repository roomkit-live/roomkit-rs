//! What one generation asks, as the wire builders read it, and what they
//! read of an answer before the loop does.

use std::collections::VecDeque;

use roomkit::model::{Metadata, ToolCall};
use roomkit_ai::{
    AiContentPart, AiContext, AiMessage, AiMessageContent, AiRole, AiTool, StreamDone, StreamEvent,
    StreamTextDelta, read_tool_calls,
};
use roomkit_core::rules::transcript::{Image, Message, Part, Role, parse_data_uri};

use crate::error::OpenAiError;

/// What one generation asks: the system prompt, the conversation as the
/// transcript writes it, the tools.
#[derive(Clone, Debug)]
pub(crate) struct Request {
    pub(crate) system_prompt: Option<String>,
    pub(crate) messages: Vec<Message>,
    pub(crate) tools: Vec<AiTool>,
    /// The turn's temperature, when it gives one.
    pub(crate) temperature: Option<f64>,
    /// The turn's token bound, over the configured one.
    pub(crate) max_tokens: Option<u32>,
}

impl Request {
    /// The request `context` stands for.
    ///
    /// # Errors
    ///
    /// [`OpenAiError::Image`] for an image part whose `data:` URI does not
    /// read: the request is never sent.
    pub(crate) fn of(context: &AiContext) -> Result<Self, OpenAiError> {
        let mut written = Vec::new();
        for message in &context.messages {
            written.extend(messages(message)?);
        }
        Ok(Self {
            system_prompt: context.system_prompt.clone(),
            messages: written,
            tools: context.tools.clone(),
            temperature: Some(context.temperature),
            max_tokens: context.max_tokens,
        })
    }
}

/// The transcript messages `message` is: a round's tool message is one per
/// result, each answering its call (Python's `chat_request._rendered`);
/// any other is one.
fn messages(message: &AiMessage) -> Result<Vec<Message>, OpenAiError> {
    let results: Vec<_> = match &message.content {
        AiMessageContent::Parts(parts) => parts
            .iter()
            .filter_map(|part| match part {
                AiContentPart::ToolResult(result) => Some(result),
                AiContentPart::Text(_) | AiContentPart::Image(_) | AiContentPart::ToolCall(_) => {
                    None
                }
            })
            .collect(),
        AiMessageContent::Text(_) => Vec::new(),
    };
    if results.len() < 2 {
        return Ok(vec![self::message(message)?]);
    }
    Ok(results
        .into_iter()
        .map(|result| {
            let mut written = Message::tool_result(&result.tool_call_id, &result.result);
            written.role = Role::Tool;
            written.metadata.clone_from(&message.metadata);
            written
        })
        .collect())
}

/// `message` as the transcript writes it: its text, its images, its calls,
/// the call its result answers; a message of text and images keeps its
/// parts in order, each image at its place (Python's `chat_request._plain`).
fn message(message: &AiMessage) -> Result<Message, OpenAiError> {
    let parts: &[AiContentPart] = match &message.content {
        AiMessageContent::Text(text) => {
            let mut written = Message::new(role(message.role), text.clone());
            written.metadata.clone_from(&message.metadata);
            return Ok(written);
        }
        AiMessageContent::Parts(parts) => parts,
    };
    let mut text = String::new();
    let mut images = Vec::new();
    let mut ordered = Vec::new();
    let mut calls = Vec::new();
    let mut result = None;
    for part in parts {
        match part {
            AiContentPart::Text(part) => {
                text.push_str(&part.text);
                ordered.push(Part::Text(part.text.clone()));
            }
            AiContentPart::Image(image) => {
                let mime_type = image.mime_type.as_deref().unwrap_or_default();
                if let Some(image) = read_image(&image.url, mime_type)? {
                    ordered.push(Part::Image(image.clone()));
                    images.push(image);
                }
            }
            AiContentPart::ToolCall(call) => {
                calls.push(ToolCall::new(&call.id, &call.name, call.arguments.clone()));
            }
            AiContentPart::ToolResult(part) => result = Some(part),
        }
    }
    let plain = calls.is_empty() && result.is_none();
    let mut written = match result {
        Some(result) => Message::tool_result(&result.tool_call_id, &result.result),
        None => Message::assistant_calls(text, calls),
    };
    if plain {
        written.parts = ordered;
    }
    written.role = match result {
        Some(_) => Role::Tool,
        None => role(message.role),
    };
    written.images = images;
    written.metadata.clone_from(&message.metadata);
    Ok(written)
}

/// The image a part names, as Python's `image_part_uri` forwards it: a
/// link as it is, a `data:` URI read and rebuilt from its bytes, so that
/// whitespace and a missing padding are repaired and only the type stays
/// of its header; `None` for one the transcript does not take.
///
/// # Errors
///
/// [`OpenAiError::Image`] for a `data:` URI that does not read (no
/// payload, no base64, the URL-safe alphabet's included): the request is
/// never sent.
fn read_image(url: &str, mime_type: &str) -> Result<Option<Image>, OpenAiError> {
    if !url.starts_with("data:") {
        return Ok(Image::new(url, mime_type));
    }
    let fallback = Some(mime_type).filter(|kind| !kind.is_empty());
    let (kind, data) = parse_data_uri(url, fallback).map_err(OpenAiError::Image)?;
    Ok(Image::from_bytes(&data, &kind))
}

/// The transcript's role of `role`; the system's reads as a user's, the
/// system prompt being the request's own.
fn role(role: AiRole) -> Role {
    match role {
        AiRole::Assistant => Role::Assistant,
        AiRole::Tool => Role::Tool,
        AiRole::User | AiRole::System => Role::User,
    }
}

/// A call as the wire carried it: its id, its tool, its argument text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RawCall {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) arguments: String,
}

impl RawCall {
    pub(crate) fn new(
        id: impl Into<String>,
        name: impl Into<String>,
        arguments: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            arguments: arguments.into(),
        }
    }
}

/// A whole answer as the wire read it: its text, its calls, its end.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Whole {
    pub(crate) text: String,
    pub(crate) calls: Vec<RawCall>,
    pub(crate) done: StreamDone,
}

impl Whole {
    pub(crate) fn new(text: impl Into<String>, calls: Vec<RawCall>, done: StreamDone) -> Self {
        Self {
            text: text.into(),
            calls,
            done,
        }
    }
}

/// The end of an answer that stopped for `finish_reason`, having used
/// `usage`.
pub(crate) fn done(finish_reason: Option<String>, usage: Metadata) -> StreamDone {
    StreamDone {
        finish_reason,
        usage,
        metadata: Metadata::new(),
    }
}

/// Queues `text`, when there is any, as a text event.
pub(crate) fn push_text(ready: &mut VecDeque<StreamEvent>, text: String) {
    if !text.is_empty() {
        ready.push_back(StreamEvent::TextDelta(StreamTextDelta { text }));
    }
}

/// Queues the end of an answer: its calls, read for its ending as every
/// provider reads them (RFC §6.4), then its done event.
pub(crate) fn push_end(ready: &mut VecDeque<StreamEvent>, calls: Vec<RawCall>, done: StreamDone) {
    let raw = calls
        .into_iter()
        .map(|call| (call.id, call.name, call.arguments))
        .collect();
    let calls = read_tool_calls(raw, done.finish_reason.as_deref());
    ready.extend(
        calls
            .into_iter()
            .map(|call| StreamEvent::ToolCall(call.into())),
    );
    ready.push_back(StreamEvent::Done(done));
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use roomkit_ai::AiToolResultPart;

    use super::*;

    // Python: tests/test_providers/test_image_parts.py::test_uri_is_rebuilt_for_a_data_uri_and_untouched_for_a_url
    #[test]
    fn uri_is_rebuilt_for_a_data_uri_and_untouched_for_a_url() {
        let read = |url: &str| {
            read_image(url, "image/png")
                .unwrap()
                .map(|image| image.url().to_owned())
        };
        assert_eq!(
            [
                read("data:;base64,QUJD\nMTIz"),
                read("data:image/png;name=lamp.png;base64,QUJDMTI"),
                read("https://example.com/a.png"),
            ],
            [
                Some("data:image/png;base64,QUJDMTIz".to_owned()),
                Some("data:image/png;base64,QUJDMTI=".to_owned()),
                Some("https://example.com/a.png".to_owned()),
            ]
        );
    }

    // Python: tests/test_providers/test_chat_request.py::test_an_image_result_rides_a_user_message_after_the_tool_messages
    // (its tool messages: one per result of the round's one message)
    #[test]
    fn a_round_s_results_are_a_tool_message_each() {
        let result = |id: &str, text: &str| {
            AiContentPart::ToolResult(AiToolResultPart::new(id, "lookup", text))
        };
        let context = AiContext {
            messages: vec![AiMessage::new(
                AiRole::Tool,
                vec![result("c1", "found"), result("c2", "missing")],
            )],
            ..AiContext::default()
        };
        let written: Vec<(Role, Option<String>, String)> = Request::of(&context)
            .unwrap()
            .messages
            .into_iter()
            .map(|message| (message.role, message.tool_call_id, message.text))
            .collect();
        assert_eq!(
            written,
            [
                (Role::Tool, Some("c1".to_owned()), "found".to_owned()),
                (Role::Tool, Some("c2".to_owned()), "missing".to_owned()),
            ]
        );
    }
}

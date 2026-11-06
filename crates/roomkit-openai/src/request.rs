//! What one generation asks, as the wire builders read it, and what they
//! read of an answer before the loop does.

use std::collections::VecDeque;

use roomkit::model::{Metadata, ToolCall};
use roomkit_ai::{
    AiContentPart, AiContext, AiMessage, AiMessageContent, AiRole, AiTool, CallIds, StreamDone,
    StreamEvent, StreamTextDelta, StreamThinkingDelta, read_tool_calls,
};
use roomkit_core::rules::transcript::{Image, Message, Part, Role, parse_data_uri};

use crate::error::OpenAiError;
use crate::reasoning::thinking_switch;

/// What one generation asks: the system prompt, the conversation as the
/// transcript writes it, the tools.
#[derive(Clone, Debug)]
pub(crate) struct Request {
    pub(crate) system_prompt: Option<String>,
    pub(crate) messages: Vec<Message>,
    /// Each message's reasoning, by its place in `messages`: an assistant's
    /// thinking parts joined, empty for any other. A vendor's dialect reads
    /// it ([`crate::dialect::ChatDialect`]); OpenAI's sends none.
    pub(crate) reasoning: Vec<String>,
    pub(crate) tools: Vec<AiTool>,
    /// The turn's temperature, when it gives one.
    pub(crate) temperature: Option<f64>,
    /// The turn's token bound, over the configured one.
    pub(crate) max_tokens: Option<u32>,
    /// The turn's reasoning effort, over the configured one (RFC §6.7).
    pub(crate) reasoning_effort: Option<String>,
    /// Whether the turn states off (RFC §6.7), its budget or its switch.
    pub(crate) thinking_off: bool,
    /// Whether the turn states the model reasons, as its budget, its
    /// switch or an effort of `none` says (Python's `thinking_switch`).
    pub(crate) thinking_switch: Option<bool>,
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
        let mut reasoning = Vec::new();
        for message in &context.messages {
            let rendered = messages(message)?;
            let thought = reasoning_of(message);
            reasoning.extend(rendered.iter().map(|_| thought.clone()));
            written.extend(rendered);
        }
        let switch = thinking_switch(
            context.thinking_budget,
            context.enable_thinking,
            context.reasoning_effort.as_deref(),
        );
        Ok(Self {
            system_prompt: context.system_prompt.clone(),
            messages: written,
            reasoning,
            tools: context.tools.clone(),
            temperature: Some(context.temperature),
            max_tokens: context.max_tokens,
            reasoning_effort: context.reasoning_effort.clone(),
            thinking_off: switch == Some(false),
            thinking_switch: switch,
        })
    }
}

/// What an assistant's `message` reasoned, its thinking parts joined in
/// order (Python's `chat_request._thinking`); nothing for any other role.
fn reasoning_of(message: &AiMessage) -> String {
    let AiMessageContent::Parts(parts) = &message.content else {
        return String::new();
    };
    if message.role != AiRole::Assistant {
        return String::new();
    }
    parts
        .iter()
        .filter_map(|part| match part {
            AiContentPart::Thinking(part) => Some(part.thinking.as_str()),
            AiContentPart::Text(_)
            | AiContentPart::Image(_)
            | AiContentPart::ToolCall(_)
            | AiContentPart::ToolResult(_) => None,
        })
        .collect()
}

/// The transcript messages `message` is: a round's tool message is one per
/// result, each answering its call with its text, then the results' images
/// on a user message: a tool message carries text only, an image part is a
/// user's (Python's `chat_request._tool_messages`); any other is one.
fn messages(message: &AiMessage) -> Result<Vec<Message>, OpenAiError> {
    let results: Vec<_> = match &message.content {
        AiMessageContent::Parts(parts) => parts
            .iter()
            .filter_map(|part| match part {
                AiContentPart::ToolResult(result) => Some(result),
                AiContentPart::Text(_)
                | AiContentPart::Image(_)
                | AiContentPart::Thinking(_)
                | AiContentPart::ToolCall(_) => None,
            })
            .collect(),
        AiMessageContent::Text(_) => Vec::new(),
    };
    if results.is_empty() {
        return Ok(vec![self::message(message)?]);
    }
    let mut written = Vec::with_capacity(results.len() + 1);
    let mut images = Vec::new();
    for result in results {
        let (text, result_images) = result.split_for_message();
        let mut answer = Message::tool_result(&result.tool_call_id, text);
        answer.role = Role::Tool;
        answer.metadata.clone_from(&message.metadata);
        written.push(answer);
        for image in result_images {
            let mime_type = image.mime_type.as_deref().unwrap_or_default();
            images.extend(read_image(&image.url, mime_type)?);
        }
    }
    if !images.is_empty() {
        let mut shown = Message::new(Role::User, "");
        shown.parts = images.iter().cloned().map(Part::Image).collect();
        shown.images = images;
        written.push(shown);
    }
    Ok(written)
}

/// `message`, which carries no tool result, as the transcript writes it:
/// its text, its images, its calls; a message of text and images keeps its
/// parts in order, each image at its place (Python's `chat_request._plain`).
/// A model's reasoning is not written here (RFC §6.4): a generic Chat
/// Completions server would read it as text the model said, and its prompt
/// cache would not survive it; a vendor's dialect writes it in its own field
/// from [`Request::reasoning`]. Responses keeps its reasoning items in the
/// round's metadata.
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
            // A message with a result is written by `messages`.
            AiContentPart::ToolResult(_) | AiContentPart::Thinking(_) => {}
        }
    }
    let plain = calls.is_empty();
    let mut written = Message::assistant_calls(text, calls);
    if plain {
        written.parts = ordered;
    }
    written.role = role(message.role);
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

/// The transcript's role of `role`: each keeps its own, the system's
/// included, as Python sends `message.role` (`chat_request._rendered`).
fn role(role: AiRole) -> Role {
    match role {
        AiRole::Assistant => Role::Assistant,
        AiRole::Tool => Role::Tool,
        AiRole::User => Role::User,
        AiRole::System => Role::System,
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

/// A whole answer as the wire read it: its reasoning, its text, its calls,
/// its end.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Whole {
    pub(crate) thinking: Option<String>,
    pub(crate) text: String,
    pub(crate) calls: Vec<RawCall>,
    pub(crate) done: StreamDone,
}

impl Whole {
    pub(crate) fn new(text: impl Into<String>, calls: Vec<RawCall>, done: StreamDone) -> Self {
        Self {
            thinking: None,
            text: text.into(),
            calls,
            done,
        }
    }

    /// The same answer, its reasoning `thinking`.
    pub(crate) fn with_thinking(mut self, thinking: Option<String>) -> Self {
        self.thinking = thinking;
        self
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

/// Queues `thinking`, when there is any, as a reasoning event.
pub(crate) fn push_thinking(ready: &mut VecDeque<StreamEvent>, thinking: String) {
    if !thinking.is_empty() {
        ready.push_back(StreamEvent::ThinkingDelta(StreamThinkingDelta::new(
            thinking,
        )));
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
    let mut ids = CallIds::new();
    let raw = calls
        .into_iter()
        .map(|call| (ids.call(&call.id, &call.name), call.name, call.arguments))
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
    use roomkit_ai::{AiImagePart, AiTextPart, AiToolResultPart, ToolResultPart};
    use roomkit_ai::{AiThinkingPart, AiToolCallPart};

    use super::*;

    // Python: tests/test_providers/test_tool_call_handoff.py::TestCallIds: a
    // provider reading its wire gives a call its server left without an id, or
    // gave another's, one of its own, so that the loop runs both.
    #[test]
    fn a_response_s_two_calls_under_one_id_reach_the_loop_with_two_ids() {
        let mut ready = VecDeque::new();
        let calls = vec![
            RawCall::new("c1", "now", "{}"),
            RawCall::new("c1", "now", "{}"),
            RawCall::new("", "later", "{}"),
        ];
        push_end(&mut ready, calls, StreamDone::default());
        let ids: Vec<String> = ready
            .into_iter()
            .filter_map(|event| {
                if let StreamEvent::ToolCall(call) = event {
                    Some(call.id)
                } else {
                    None
                }
            })
            .collect();
        let [first, second, third] = ids.as_slice() else {
            panic!("three calls: {ids:?}");
        };
        assert_eq!(first, "c1");
        assert!(second.starts_with("call_now_"), "{second}");
        assert!(third.starts_with("call_later_"), "{third}");
    }

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

    fn thought() -> AiContentPart {
        AiContentPart::Thinking(AiThinkingPart::new("why"))
    }

    fn call_part() -> AiContentPart {
        AiContentPart::ToolCall(AiToolCallPart::new("c1", "lookup"))
    }

    fn written(parts: Vec<AiContentPart>) -> Message {
        let context = AiContext {
            messages: vec![AiMessage::new(AiRole::Assistant, parts)],
            ..AiContext::default()
        };
        let mut written = Request::of(&context).unwrap().messages;
        written.remove(0)
    }

    // Python: tests/test_providers/test_chat_request.py::test_a_tool_round_sends_its_text_and_its_calls_and_no_reasoning
    // and test_a_round_of_reasoning_and_calls_alone_has_no_content
    #[test]
    fn a_round_sends_its_text_and_its_calls_and_no_reasoning() {
        let say = || AiContentPart::Text(AiTextPart::new("what"));
        let cases = [
            (vec![say(), thought(), call_part()], "what"),
            (vec![thought(), call_part()], ""),
            (vec![say(), call_part()], "what"),
            (
                vec![
                    AiContentPart::Thinking(AiThinkingPart {
                        signature: Some("s".to_owned()),
                        ..AiThinkingPart::new("")
                    }),
                    call_part(),
                ],
                "",
            ),
        ];
        for (parts, read) in cases {
            let message = written(parts);
            assert_eq!(message.text, read);
            assert_eq!(message.tool_calls.len(), 1);
        }
    }

    // Python: tests/test_providers/test_chat_request.py::test_a_plain_answer_sends_no_reasoning
    #[test]
    fn a_plain_answer_sends_no_reasoning() {
        let done = AiContentPart::Text(AiTextPart::new("Done."));
        let message = written(vec![thought(), done]);
        assert_eq!(message.parts, [Part::Text("Done.".to_owned())]);
    }

    // Python: tests/test_providers/test_chat_request.py::test_an_answer_of_reasoning_alone_is_an_empty_text
    #[test]
    fn an_answer_of_reasoning_alone_is_an_empty_text() {
        let message = written(vec![thought()]);
        assert_eq!(message.parts, []);
        assert_eq!(message.text, "");
    }

    // Python: tests/test_providers/test_chat_request.py::test_an_image_result_rides_a_user_message_after_the_tool_messages
    // and tests/test_providers/test_openai.py::test_tool_result_image_splits_to_user_message
    #[test]
    fn a_result_s_image_rides_a_user_message_after_the_tool_messages() {
        let image = || {
            ToolResultPart::Image(AiImagePart {
                url: "data:image/png;base64,QUJD".to_owned(),
                mime_type: Some("image/png".to_owned()),
            })
        };
        let shot = vec![ToolResultPart::Text(AiTextPart::new("shot")), image()];
        let context = AiContext {
            messages: vec![AiMessage::new(
                AiRole::Tool,
                vec![
                    AiContentPart::ToolResult(AiToolResultPart::new("c1", "screen", shot)),
                    AiContentPart::ToolResult(AiToolResultPart::new("c2", "draw", vec![image()])),
                    AiContentPart::ToolResult(AiToolResultPart::new("c3", "lookup", "found")),
                ],
            )],
            ..AiContext::default()
        };
        let written = Request::of(&context).unwrap().messages;
        let read: Vec<(Role, Option<&str>, &str, Vec<&str>)> = written
            .iter()
            .map(|message| {
                let images = message.images.iter().map(Image::url).collect();
                (
                    message.role,
                    message.tool_call_id.as_deref(),
                    message.text.as_str(),
                    images,
                )
            })
            .collect();
        let png = "data:image/png;base64,QUJD";
        assert_eq!(
            read,
            [
                (Role::Tool, Some("c1"), "shot", vec![]),
                (Role::Tool, Some("c2"), "[see image below]", vec![]),
                (Role::Tool, Some("c3"), "found", vec![]),
                (Role::User, None, "", vec![png, png]),
            ]
        );
        assert_eq!(written[3].parts.len(), 2);
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

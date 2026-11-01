//! What one generation asks, as the wire builders read it, and what they
//! read of an answer before the loop does.

use std::collections::VecDeque;

use roomkit::model::{Metadata, ToolCall};
use roomkit_ai::{
    AiContentPart, AiContext, AiMessage, AiMessageContent, AiRole, AiTool, StreamDone, StreamEvent,
    StreamTextDelta, read_tool_calls,
};
use roomkit_core::rules::transcript::{Image, Message, Role};

/// What one generation asks: the system prompt, the conversation as the
/// transcript writes it, the tools.
#[derive(Clone, Debug)]
pub(crate) struct Request {
    pub(crate) system_prompt: Option<String>,
    pub(crate) messages: Vec<Message>,
    pub(crate) tools: Vec<AiTool>,
}

impl Request {
    /// The request `context` stands for.
    pub(crate) fn of(context: &AiContext) -> Self {
        Self {
            system_prompt: context.system_prompt.clone(),
            messages: context.messages.iter().map(message).collect(),
            tools: context.tools.clone(),
        }
    }
}

/// `message` as the transcript writes it: its text, its images, its calls,
/// the call its result answers.
fn message(message: &AiMessage) -> Message {
    let parts: &[AiContentPart] = match &message.content {
        AiMessageContent::Text(text) => {
            let mut written = Message::new(role(message.role), text.clone());
            written.metadata.clone_from(&message.metadata);
            return written;
        }
        AiMessageContent::Parts(parts) => parts,
    };
    let mut text = String::new();
    let mut images = Vec::new();
    let mut calls = Vec::new();
    let mut result = None;
    for part in parts {
        match part {
            AiContentPart::Text(part) => text.push_str(&part.text),
            AiContentPart::Image(image) => {
                let mime_type = image.mime_type.as_deref().unwrap_or_default();
                images.extend(Image::new(&image.url, mime_type));
            }
            AiContentPart::ToolCall(call) => {
                calls.push(ToolCall::new(&call.id, &call.name, call.arguments.clone()));
            }
            AiContentPart::ToolResult(part) => result = Some(part),
        }
    }
    let mut written = match result {
        Some(result) => Message::tool_result(&result.tool_call_id, &result.result),
        None => Message::assistant_calls(text, calls),
    };
    written.role = match result {
        Some(_) => Role::Tool,
        None => role(message.role),
    };
    written.images = images;
    written.metadata.clone_from(&message.metadata);
    written
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

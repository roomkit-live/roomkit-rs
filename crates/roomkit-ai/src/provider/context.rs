//! What one generation is asked: Python's `AIContext` and its messages.

use std::collections::HashMap;

use roomkit::model::Metadata;
use roomkit_core::rules::transcript::{Image, Message, Part, Role};

use crate::tools::{AiTool, ToolResult};

/// What one generation is asked (RFC §6.7): Python's `AIContext(*,
/// messages=[], system_prompt=None, temperature=0.7, max_tokens=None,
/// tools=[], metadata={})`.
#[derive(Clone, Debug, PartialEq)]
pub struct AiContext {
    /// The conversation, oldest first, the turn's rounds so far included.
    pub messages: Vec<AiMessage>,
    /// The system prompt, when there is one.
    pub system_prompt: Option<String>,
    /// How freely the model samples: Python's `float`, 0.7 by default.
    pub temperature: f64,
    /// The most tokens the generation writes; `None` leaves it to the
    /// provider's configuration.
    pub max_tokens: Option<u32>,
    /// The tools the model may call, in the toolset's order.
    pub tools: Vec<AiTool>,
    /// The turn's data for the provider.
    pub metadata: Metadata,
}

impl Default for AiContext {
    fn default() -> Self {
        Self {
            messages: Vec::new(),
            system_prompt: None,
            temperature: 0.7,
            max_tokens: None,
            tools: Vec::new(),
            metadata: Metadata::new(),
        }
    }
}

/// One message of the conversation: Python's `AIMessage(*, role, content,
/// metadata={})`.
#[derive(Clone, Debug, PartialEq)]
pub struct AiMessage {
    /// Whose message it is.
    pub role: AiRole,
    /// What it says: a text, or its parts.
    pub content: AiMessageContent,
    /// Provider-owned data carried within the current tool loop (RFC §6.7),
    /// apart from the visible text and the room's history.
    pub metadata: Metadata,
}

impl AiMessage {
    /// A message of `role` saying `content`, with no metadata.
    pub fn new(role: AiRole, content: impl Into<AiMessageContent>) -> Self {
        Self {
            role,
            content: content.into(),
            metadata: Metadata::new(),
        }
    }

    /// The text the message says, its text parts and its tool results
    /// joined. For a test.
    #[doc(hidden)]
    pub fn text(&self) -> String {
        let results = self.tool_results();
        if results.is_empty() {
            return self.content.text();
        }
        results.iter().map(|result| result.as_text()).collect()
    }

    /// The calls the message makes, in order. For a test.
    #[doc(hidden)]
    pub fn tool_calls(&self) -> Vec<&AiToolCallPart> {
        self.parts()
            .filter_map(|part| match part {
                AiContentPart::ToolCall(call) => Some(call),
                AiContentPart::Text(_) | AiContentPart::Image(_) | AiContentPart::ToolResult(_) => {
                    None
                }
            })
            .collect()
    }

    /// The result the message carries, when it is a tool's: the first of a
    /// round's. For a test.
    #[doc(hidden)]
    pub fn tool_result(&self) -> Option<&AiToolResultPart> {
        self.tool_results().into_iter().next()
    }

    /// The results the message carries, a round's in call order, when it
    /// is a tool's. For a test.
    #[doc(hidden)]
    pub fn tool_results(&self) -> Vec<&AiToolResultPart> {
        self.parts()
            .filter_map(|part| match part {
                AiContentPart::ToolResult(result) => Some(result),
                AiContentPart::Text(_) | AiContentPart::Image(_) | AiContentPart::ToolCall(_) => {
                    None
                }
            })
            .collect()
    }

    /// The message's parts; none for a text alone.
    fn parts(&self) -> impl Iterator<Item = &AiContentPart> {
        match &self.content {
            AiMessageContent::Text(_) => [].iter(),
            AiMessageContent::Parts(parts) => parts.iter(),
        }
    }
}

/// Whose a message is: Python's `role`, `system`, `user`, `assistant` or
/// `tool`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AiRole {
    /// The application's instructions.
    System,
    /// Someone other than the channel.
    User,
    /// The channel itself.
    Assistant,
    /// A tool's result.
    Tool,
}

impl AiRole {
    /// The role as Python writes it.
    #[doc(hidden)]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::User => "user",
            Self::Assistant => "assistant",
            Self::Tool => "tool",
        }
    }
}

/// What a message says: Python's `content`, a text or a list of parts.
#[derive(Clone, Debug, PartialEq)]
pub enum AiMessageContent {
    /// A text alone.
    Text(String),
    /// Its parts, in order.
    Parts(Vec<AiContentPart>),
}

impl AiMessageContent {
    /// The text the content says, its text parts joined.
    #[doc(hidden)]
    pub fn text(&self) -> String {
        match self {
            Self::Text(text) => text.clone(),
            Self::Parts(parts) => parts
                .iter()
                .filter_map(|part| match part {
                    AiContentPart::Text(part) => Some(part.text.as_str()),
                    AiContentPart::Image(_)
                    | AiContentPart::ToolCall(_)
                    | AiContentPart::ToolResult(_) => None,
                })
                .collect(),
        }
    }
}

impl From<String> for AiMessageContent {
    fn from(text: String) -> Self {
        Self::Text(text)
    }
}

impl From<&str> for AiMessageContent {
    fn from(text: &str) -> Self {
        Self::Text(text.to_owned())
    }
}

impl From<Vec<AiContentPart>> for AiMessageContent {
    fn from(parts: Vec<AiContentPart>) -> Self {
        Self::Parts(parts)
    }
}

/// One part of a message: Python's `AITextPart`, `AIImagePart`,
/// `AIToolCallPart` or `AIToolResultPart`, tagged by its `type`.
#[derive(Clone, Debug, PartialEq)]
pub enum AiContentPart {
    /// A text.
    Text(AiTextPart),
    /// An image (RFC §6.4).
    Image(AiImagePart),
    /// A call the assistant made.
    ToolCall(AiToolCallPart),
    /// A tool's result.
    ToolResult(AiToolResultPart),
}

/// A text part: Python's `AITextPart(*, text)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AiTextPart {
    /// The text.
    pub text: String,
}

impl AiTextPart {
    /// The part saying `text`.
    pub fn new(text: impl Into<String>) -> Self {
        Self { text: text.into() }
    }
}

/// An image part: Python's `AIImagePart(*, url, mime_type=None)`, a data or
/// HTTP URL the host resolved (RFC §6.4).
#[derive(Clone, PartialEq, Eq)]
pub struct AiImagePart {
    /// Where the image is: a `data:` or `https:` URL.
    pub url: String,
    /// Its media type, when known.
    pub mime_type: Option<String>,
}

impl AiImagePart {
    /// The image at `url`, its media type unknown.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            mime_type: None,
        }
    }
}

impl std::fmt::Debug for AiImagePart {
    /// The URL is left out: a data URL holds the whole image.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AiImagePart")
            .field("mime_type", &self.mime_type)
            .finish_non_exhaustive()
    }
}

/// A call the assistant made: Python's `AIToolCallPart(*, id, name,
/// arguments={}, metadata={})`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AiToolCallPart {
    /// The call's id.
    pub id: String,
    /// The tool called.
    pub name: String,
    /// Its arguments.
    pub arguments: Metadata,
    /// Provider-owned data of the call.
    pub metadata: Metadata,
}

impl AiToolCallPart {
    /// The call `id` of the tool `name`, with no arguments.
    pub fn new(id: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            arguments: Metadata::new(),
            metadata: Metadata::new(),
        }
    }
}

/// A tool's result: Python's `AIToolResultPart(*, tool_call_id, name,
/// result, is_error=False)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AiToolResultPart {
    /// The call the result answers.
    pub tool_call_id: String,
    /// The tool that answered.
    pub name: String,
    /// What the model reads of the result: a text, or its text and image
    /// parts in order (RFC §6.4). The loop never hands a provider
    /// [`ToolResult::Json`]; a context built elsewhere may, read as its
    /// JSON.
    pub result: ToolResult,
    /// Whether the call failed.
    pub is_error: bool,
}

impl AiToolResultPart {
    /// The result `result` of the call `tool_call_id` to `name`, not an
    /// error.
    pub fn new(
        tool_call_id: impl Into<String>,
        name: impl Into<String>,
        result: impl Into<ToolResult>,
    ) -> Self {
        Self {
            tool_call_id: tool_call_id.into(),
            name: name.into(),
            result: result.into(),
            is_error: false,
        }
    }

    /// The result as a provider that takes no image in a tool's result
    /// reads it: its texts and an `[image]` mark for each image, one per
    /// line. Python's `as_text()`.
    #[must_use]
    pub fn as_text(&self) -> String {
        self.result.as_text()
    }

    /// The text the tool message keeps and the images a provider sends
    /// after it, on a user message: the texts joined, one per line, or
    /// `[see image below]` for images alone. Python's
    /// `split_for_message()`.
    #[must_use]
    pub fn split_for_message(&self) -> (String, Vec<AiImagePart>) {
        self.result.split_for_message()
    }
}

/// `messages`, the conversation a turn reads, as its provider is handed
/// it: a text alone, or, for a message with images, calls or a call's
/// result, its parts; a result's tool is the one its call named. A round's
/// results, one transcript message each, are one tool message holding
/// them all, as Python's `_execute_round_tools` and `_tool_history._round`
/// build it.
pub(crate) fn ai_messages(messages: &[Message]) -> Vec<AiMessage> {
    let mut tools: HashMap<&str, &str> = HashMap::new();
    let mut read: Vec<AiMessage> = Vec::with_capacity(messages.len());
    for message in messages {
        for call in &message.tool_calls {
            tools.insert(&call.id, &call.name);
        }
        let content = content(message, &tools);
        if message.tool_call_id.is_some()
            && let Some(results) = read.last_mut().filter(|last| last.role == AiRole::Tool)
            && let (AiMessageContent::Parts(parts), AiMessageContent::Parts(more)) =
                (&mut results.content, &content)
        {
            parts.extend(more.iter().cloned());
            results.metadata.extend(message.metadata.clone());
            continue;
        }
        read.push(AiMessage {
            role: role(message.role),
            content,
            metadata: message.metadata.clone(),
        });
    }
    read
}

fn role(role: Role) -> AiRole {
    match role {
        Role::Assistant => AiRole::Assistant,
        Role::Tool => AiRole::Tool,
        Role::System => AiRole::System,
        // A role the transcript adds later reads as someone else's.
        Role::User | _ => AiRole::User,
    }
}

/// What `message` says, a result's tool read in `tools`.
fn content(message: &Message, tools: &HashMap<&str, &str>) -> AiMessageContent {
    if let Some(call_id) = &message.tool_call_id {
        let name = tools.get(call_id.as_str()).copied().unwrap_or_default();
        return AiMessageContent::Parts(vec![AiContentPart::ToolResult(AiToolResultPart {
            tool_call_id: call_id.clone(),
            name: name.to_owned(),
            result: ToolResult::Text(message.text.clone()),
            is_error: message.is_error,
        })]);
    }
    if message.parts.is_empty() && message.images.is_empty() && message.tool_calls.is_empty() {
        return AiMessageContent::Text(message.text.clone());
    }
    let mut parts = Vec::new();
    if message.parts.is_empty() {
        if !message.text.is_empty() {
            parts.push(AiContentPart::Text(AiTextPart {
                text: message.text.clone(),
            }));
        }
        parts.extend(message.images.iter().map(image_part));
    } else {
        parts.extend(message.parts.iter().map(|part| match part {
            Part::Text(text) => AiContentPart::Text(AiTextPart { text: text.clone() }),
            Part::Image(image) => image_part(image),
            // A part the transcript adds later reads as nothing.
            _ => AiContentPart::Text(AiTextPart::new("")),
        }));
    }
    parts.extend(message.tool_calls.iter().map(|call| {
        AiContentPart::ToolCall(AiToolCallPart {
            id: call.id.clone(),
            name: call.name.clone(),
            arguments: call.arguments.clone(),
            metadata: Metadata::new(),
        })
    }));
    AiMessageContent::Parts(parts)
}

/// `image` as a message's part, its type given.
fn image_part(image: &Image) -> AiContentPart {
    AiContentPart::Image(AiImagePart {
        url: image.url().to_owned(),
        mime_type: Some(image.mime_type().to_owned()),
    })
}

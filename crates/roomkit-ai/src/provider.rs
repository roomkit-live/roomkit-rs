//! The provider a model is reached through (RFC §6.7): one call, a request
//! in, the response's events out.

use std::error::Error;
use std::future::Future;
use std::pin::Pin;

use futures_core::Stream;
use roomkit::model::Metadata;
use roomkit::transcript::Message;

use crate::stream::{Yielder, items};
use crate::tools::ToolDefinition;

/// What a provider's call failed with. The turn records its message; it
/// never reaches the room nor a model.
pub type ProviderError = Box<dyn Error + Send + Sync>;

/// The events of one generation, as its provider reads them: text as it
/// comes, the calls, then the done event.
pub type ProviderStream<'a> =
    Pin<Box<dyn Stream<Item = Result<ProviderEvent, ProviderError>> + Send + 'a>>;

/// A model provider (RFC §6.7): one generation per call, read as a stream
/// of events, so that a provider that streams hands its text as it comes
/// and one that does not hands its whole response
/// ([`ProviderResponse::into_stream`]).
///
/// The agent's tool loop is the same for every provider (RFC §6.4 "One
/// loop, streamed or not"): what a provider hands it of a call, its raw
/// argument text included, is read by one rule, the loop's.
pub trait AiProvider: Send + Sync + 'static {
    /// The provider's name, as the agent's channel declares it.
    fn name(&self) -> Option<&str> {
        None
    }

    /// Configured model identity for usage attribution, when known. This is
    /// not a model capacity or a token estimate.
    fn model(&self) -> Option<&str> {
        None
    }

    /// One generation answering `request`.
    fn generate<'a>(&'a self, request: &'a AiRequest) -> ProviderStream<'a>;
}

/// What one generation is asked (RFC §6.7 `AIContext`): the system prompt,
/// the conversation, the turn's rounds so far included, and the tools the
/// round declares, in the toolset's order.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct AiRequest {
    /// The system prompt, when there is one.
    pub system_prompt: Option<String>,
    /// The conversation, oldest first.
    pub messages: Vec<Message>,
    /// The tools the model may call.
    pub tools: Vec<ToolDefinition>,
}

impl AiRequest {
    /// A request for `messages` after `system_prompt`, declaring `tools`.
    pub fn new(
        system_prompt: Option<String>,
        messages: Vec<Message>,
        tools: Vec<ToolDefinition>,
    ) -> Self {
        Self {
            system_prompt,
            messages,
            tools,
        }
    }
}

/// One event of a generation.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ProviderEvent {
    /// Text the model wrote, as it came.
    Text(String),
    /// A call the model made.
    ToolCall(ProviderToolCall),
    /// The generation ended.
    Done(Done),
}

/// A call as the provider received it (RFC §6.4 "What a provider hands the
/// loop for a call"): its id, which the loop replaces when it is empty or
/// another call's of the response; the tool's name; and its arguments as
/// the wire carried them, as text, which the loop reads: none or `null`
/// reads as `{}`, text that reads as no object as `{"raw": text}`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ProviderToolCall {
    /// The call's id.
    pub id: String,
    /// The tool called.
    pub name: String,
    /// The arguments' text.
    pub arguments: String,
}

impl ProviderToolCall {
    /// The call `id` of `name` with the argument text `arguments`.
    pub fn new(
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

/// How a generation ended: why it stopped, in the provider's own word
/// (`stop`, `tool_calls`, `length`...), `None` when it gave none, and what
/// it used, the counters the turn's record sums (RFC §6.4 "The turn's
/// record").
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Done {
    /// Why the generation stopped.
    pub finish_reason: Option<String>,
    /// What it used: integer counters by name (`input_tokens`,
    /// `output_tokens`...).
    pub usage: Metadata,
    /// Provider-owned data for replaying a tool round (RFC §6.7,
    /// `AIResponse.metadata`). The loop copies it to the assistant message,
    /// never to the room's displayed or stored text.
    pub metadata: Metadata,
}

impl Done {
    /// A generation that stopped for `finish_reason`, having used `usage`.
    pub fn new(finish_reason: Option<String>, usage: Metadata) -> Self {
        Self {
            finish_reason,
            usage,
            metadata: Metadata::new(),
        }
    }
}

/// A whole response, as a provider that does not stream reads it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ProviderResponse {
    /// Its text.
    pub text: String,
    /// Its calls, in order.
    pub calls: Vec<ProviderToolCall>,
    /// How it ended.
    pub done: Done,
}

impl ProviderResponse {
    /// A response saying `text`, making `calls`, ended as `done`.
    pub fn new(text: impl Into<String>, calls: Vec<ProviderToolCall>, done: Done) -> Self {
        Self {
            text: text.into(),
            calls,
            done,
        }
    }

    /// The generation of `response`, a future giving the whole response,
    /// as a stream: its text, its calls, then its done event (RFC §6.4: a
    /// provider that does not stream hands the loop everything it read).
    pub fn into_stream<'a, F>(response: F) -> ProviderStream<'a>
    where
        F: Future<Output = Result<Self, ProviderError>> + Send + 'a,
    {
        Box::pin(items(
            move |out: Yielder<Result<ProviderEvent, ProviderError>>| async move {
                let response = match response.await {
                    Ok(response) => response,
                    Err(error) => return out.give(Err(error)).await,
                };
                if !response.text.is_empty() {
                    out.give(Ok(ProviderEvent::Text(response.text))).await;
                }
                for call in response.calls {
                    out.give(Ok(ProviderEvent::ToolCall(call))).await;
                }
                out.give(Ok(ProviderEvent::Done(response.done))).await;
            },
        ))
    }
}

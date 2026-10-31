//! What an agent's tool loop calls (RFC §6.4, §21): toolsets, each serving
//! the tools it declares behind one call, what a call answers with, and the
//! policy that says which tools the agent may call.
//!
//! - [`Toolset`]: many tools behind one handler, as an MCP server or the
//!   skills serve theirs; [`Tools`] holds the host's closures.
//! - [`ToolOutput`] and [`ToolError`]: a call's answer, text or JSON, and
//!   how it fails, which the model reads by its rule (RFC §9.3).
//! - [`ToolPolicy`]: allow and deny patterns (RFC §21.1).
//! - [`arguments`]: a call's arguments read into a type, the refusal the
//!   model reads when they do not fit.

mod builder;
mod policy;
pub(crate) mod schema;
pub(crate) mod toolbox;

use std::any::type_name;
use std::error::Error;
use std::future::Future;

use roomkit::TurnHost;
use roomkit::model::{ChannelId, RoomId, ToolCall, ToolCallOutcome};
use serde::de::DeserializeOwned;
use serde_json::Value;

pub use self::builder::Tools;
pub use self::policy::ToolPolicy;
use crate::texts::error_envelope;

/// A tool as the model is told of it (RFC §6.7 `AITool`): its name, what it
/// does, and its parameters as a JSON Schema object.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ToolDefinition {
    /// The tool's name, one per agent (RFC §21.1 "One tool per name").
    pub name: String,
    /// What it does, for the model.
    pub description: String,
    /// Its parameters: a JSON Schema object.
    pub parameters: Value,
}

impl ToolDefinition {
    /// The tool `name`, doing `description`, taking `parameters`.
    pub fn new(name: impl Into<String>, description: impl Into<String>, parameters: Value) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            parameters,
        }
    }
}

/// What a tool's call answers with (RFC §21.4): text, which the model reads
/// as it is, or a JSON value, which it reads as its JSON.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum ToolOutput {
    /// Text.
    Text(String),
    /// A JSON value.
    Json(Value),
}

impl ToolOutput {
    /// The output as its hooks read it: a text as a JSON string.
    pub(crate) fn into_value(self) -> Value {
        match self {
            Self::Text(text) => Value::String(text),
            Self::Json(value) => value,
        }
    }
}

impl From<String> for ToolOutput {
    fn from(text: String) -> Self {
        Self::Text(text)
    }
}

impl From<&str> for ToolOutput {
    fn from(text: &str) -> Self {
        Self::Text(text.to_owned())
    }
}

impl From<Value> for ToolOutput {
    fn from(value: Value) -> Self {
        Self::Json(value)
    }
}

/// How a tool's call failed (RFC §9.3). The model reads a refusal's and a
/// failure's words as they are; an error the tool raised it reads as
/// `{"error": "Tool 'x' failed (<kind>)"}`, its message going to the
/// call's observers alone, never to the model nor to the room, since it can
/// hold anything the failing code held.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ToolError {
    /// Nothing ran: the call is refused, and the model reads these words.
    #[error("{0}")]
    Refused(String),
    /// The tool ran and could not do it: the model reads these words, and
    /// the observers too.
    #[error("{0}")]
    Failed(String),
    /// The call is not this tool's to serve, Python's
    /// `UnservedToolCallError`: nothing served it, and a SYNC `ON_TOOL_CALL`
    /// hook may serve it; when none does, the call fails and the model reads
    /// `{"error": "No handler for tool x"}` (RFC §9.3, §21.4). The message
    /// goes to the log alone.
    #[error("{0}")]
    Unserved(String),
    /// The tool raised an error of `kind`, saying `message`.
    #[error("{kind}: {message}")]
    Raised {
        /// The error's kind: its type's name.
        kind: String,
        /// Its message, with its causes.
        message: String,
    },
}

impl ToolError {
    /// A refusal the model reads as `words`.
    pub fn refused(words: impl Into<String>) -> Self {
        Self::Refused(words.into())
    }

    /// A failure the model reads as `words`.
    pub fn failed(words: impl Into<String>) -> Self {
        Self::Failed(words.into())
    }

    /// A call that is not this tool's to serve, saying why for the log.
    pub fn unserved(message: impl Into<String>) -> Self {
        Self::Unserved(message.into())
    }

    /// `error`, raised by the tool: the model reads its kind, the name of
    /// its type (`Tool 'x' failed (Error)` for an `io::Error`), its
    /// observers its message and causes. An error that is no `Error`
    /// itself, a `Box<dyn Error>` or an `anyhow::Error`, is raised with
    /// [`raised_as`](Self::raised_as), its kind named.
    pub fn raised<E: Error>(error: &E) -> Self {
        Self::Raised {
            kind: short_type_name::<E>().to_owned(),
            message: with_causes(error),
        }
    }

    /// An error of `kind` saying `message`, raised by the tool.
    pub fn raised_as(kind: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Raised {
            kind: kind.into(),
            message: message.into(),
        }
    }
}

/// The arguments of `call` read into `T`; a refusal the model reads as
/// `{"error": "Invalid arguments for 'x': <why>"}` when they do not fit, as
/// the agent's own check refuses them (RFC §21.1).
///
/// ```
/// use roomkit::model::{Metadata, ToolCall};
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Lookup {
///     order: String,
/// }
///
/// let call = ToolCall::new("call_1", "lookup_order", Metadata::new());
/// let refused = roomkit_ai::arguments::<Lookup>(&call).err().unwrap();
/// assert!(refused.to_string().contains("Invalid arguments for 'lookup_order'"));
/// ```
///
/// # Errors
///
/// Returns [`ToolError::Refused`] when the arguments do not fit `T`.
pub fn arguments<T: DeserializeOwned>(call: &ToolCall) -> Result<T, ToolError> {
    serde_json::from_value(Value::Object(call.arguments.clone())).map_err(|error| {
        ToolError::Refused(error_envelope(&format!(
            "Invalid arguments for '{}': {error}",
            call.name
        )))
    })
}

/// What a tool's call is told of its turn (RFC §21.4): the room, the agent,
/// and the turn's host, through which it writes to the room at the turn's
/// depth, without a kit of its own.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ToolContext {
    /// The turn's room.
    pub room_id: RoomId,
    /// The agent whose turn calls.
    pub channel_id: ChannelId,
    turn: Option<TurnHost>,
    /// Whether the turn is an instruction that stands alone, which reads
    /// none of the room's working state (RFC §10.1.1 rule 7).
    pub(crate) stands_alone: bool,
}

impl ToolContext {
    /// The context of a call in `room_id` by the agent `channel_id`,
    /// outside any turn the kit runs: in a toolset's own tests.
    pub fn new(room_id: RoomId, channel_id: ChannelId) -> Self {
        Self {
            room_id,
            channel_id,
            turn: None,
            stands_alone: false,
        }
    }

    /// The same context, within `turn`.
    pub(crate) fn within(mut self, turn: Option<TurnHost>) -> Self {
        self.turn = turn;
        self
    }

    /// The same context, for a turn that stands alone or not.
    pub(crate) fn standing_alone(mut self, stands_alone: bool) -> Self {
        self.stands_alone = stands_alone;
        self
    }

    /// The turn's host, when the kit runs the turn.
    pub fn turn(&self) -> Option<&TurnHost> {
        self.turn.as_ref()
    }
}

/// Tools behind one handler (step 4, decision 7): the tools it declares,
/// and the call that serves any of them. An MCP server and the skills are
/// toolsets by nature; the host's closures register in [`Tools`].
///
/// Implement the call with `async fn`. It is called only with a call to a
/// tool it declares, its arguments checked against the tool's schema and
/// gated by the kit's `BEFORE_TOOL_USE` hooks, within the agent's bound
/// per call (RFC §21.6).
pub trait Toolset: Send + Sync + 'static {
    /// The tools it serves, in the order they are declared to the model.
    fn tools(&self) -> Vec<ToolDefinition>;

    /// Serves `call`.
    fn call(
        &self,
        call: ToolCall,
        context: ToolContext,
    ) -> impl Future<Output = Result<ToolOutput, ToolError>> + Send;

    /// Releases what the toolset holds, an MCP server's process say: the
    /// agent serving it calls it when its kit shuts down
    /// ([`Channel::close`](roomkit::Channel::close)), and a clone of the
    /// agent shares its toolsets. It must not wait without bound.
    ///
    /// The default releases nothing.
    fn close(&self) -> impl Future<Output = ()> + Send {
        async {}
    }
}

/// A boxed, sendable future.
pub(crate) type BoxFuture<'a, T> = std::pin::Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A [`Toolset`] behind a trait object.
pub(crate) trait DynToolset: Send + Sync {
    fn call(
        &self,
        call: ToolCall,
        context: ToolContext,
    ) -> BoxFuture<'_, Result<ToolOutput, ToolError>>;

    /// `call` ended `outcome`, the kit's `ON_TOOL_CALL` hooks run: what a
    /// toolset of the agent's own that keeps state on its calls records
    /// then, once nothing can withhold the result any more (RFC §24.4: an
    /// activation counts once its call is served).
    fn settled(&self, _call: &ToolCall, _outcome: ToolCallOutcome, _context: &ToolContext) {}

    /// Releases what the toolset holds ([`Toolset::close`]).
    fn close(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

impl<T: Toolset> DynToolset for T {
    fn call(
        &self,
        call: ToolCall,
        context: ToolContext,
    ) -> BoxFuture<'_, Result<ToolOutput, ToolError>> {
        Box::pin(Toolset::call(self, call, context))
    }

    fn close(&self) -> BoxFuture<'_, ()> {
        Box::pin(Toolset::close(self))
    }
}

/// The last segment of `T`'s name, without its generic arguments.
fn short_type_name<T: ?Sized>() -> &'static str {
    let full = type_name::<T>();
    let plain = full.split('<').next().unwrap_or(full);
    plain.rsplit("::").next().unwrap_or(plain)
}

/// `error`'s message, then each of its causes', joined by `: `.
fn with_causes(error: &dyn Error) -> String {
    let mut message = error.to_string();
    let mut cause = error.source();
    while let Some(error) = cause {
        message.push_str(": ");
        message.push_str(&error.to_string());
        cause = error.source();
    }
    message
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn a_raised_error_is_named_by_its_type() {
        let error = std::io::Error::other("disk full");
        assert_eq!(
            ToolError::raised(&error),
            ToolError::Raised {
                kind: "Error".to_owned(),
                message: "disk full".to_owned()
            }
        );
    }
}

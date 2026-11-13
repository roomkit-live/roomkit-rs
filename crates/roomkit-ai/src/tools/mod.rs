//! What an AI channel's tool loop calls (RFC §6.4, §21), in the Python
//! implementation's shape: the tools a channel declares, the handler and
//! the tools that serve their calls, what a call answers with, and the
//! policy that says which tools the model may call.
//!
//! - [`AiTool`]: what the model is told of a tool.
//! - [`ToolHandler`], [`Tool`] and [`ToolEntry`]: a handler serving tools
//!   by name, a tool serving itself, and either of them in a channel's
//!   tools; [`compose_tool_handlers`] chains handlers.
//! - [`ToolResult`] and [`ToolError`]: a call's answer, text or JSON, and
//!   how it fails, which the model reads by its rule (RFC §9.3).
//! - [`ToolCallContext`] and [`current_tool_call`]: the call a handler
//!   runs within.
//! - [`ToolPolicy`]: allow and deny patterns (RFC §21.1).

mod builder;
mod external;
mod external_rows;
mod handler;
pub(crate) mod owner;
mod policy;
pub(crate) mod repair;
mod result;
mod row_bound;
pub(crate) mod schema;
pub(crate) mod toolbox;

use std::any::type_name;
use std::error::Error;
use std::future::Future;
use std::sync::Arc;

use roomkit::TurnHost;
use roomkit::model::{ChannelId, Metadata, RoomId, ToolCall, ToolCallOutcome};
use serde::de::DeserializeOwned;
use serde_json::Value;

#[doc(hidden)]
pub use self::builder::Tools;
pub use self::external::{
    BeforeToolDecision, ExternalToolFuture, ExternalToolHandler, ExternalToolHandlerState,
    FireBeforeHookOptions, FireOnToolHookOptions, OnToolCancelledOptions, OnToolRefusedOptions,
    OnToolResultOptions, PolicyExternalToolHandler, PolicyExternalToolHandlerOptions,
    ProcessToolCallOptions, ToolDecision,
};
pub use self::handler::{
    Tool, ToolCallContext, ToolEntry, ToolFuture, ToolHandler, compose_tool_handlers,
    current_response_metadata, current_tool_actor_id, current_tool_call,
};
pub(crate) use self::handler::{current_tool_room_metadata, served_by};
pub use self::policy::ToolPolicy;
#[doc(hidden)]
pub use self::policy::{alias_server, served_tool_name};
pub(crate) use self::result::read as read_tool_result;
pub use self::result::{ToolResult, ToolResultPart};
use crate::error::AiError;
use crate::texts::error_envelope;
use crate::tool_search::TurnSearch;

/// A tool as the model is told of it (RFC §6.7): Python's `AITool(*, name,
/// description, parameters={}, tags=[], changes_state=False)`, its
/// parameters a JSON Schema object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AiTool {
    /// The tool's name, one per channel (RFC §21.1 "One tool per name").
    pub name: String,
    /// What it does, for the model.
    pub description: String,
    /// Its parameters: a JSON Schema object.
    pub parameters: Value,
    /// Keywords `find_tools` searches beside its name and description (RFC
    /// §6.4), the bridge to a tool written in another language.
    pub tags: Vec<String>,
    /// A call it answers may change what later calls read (a file edited,
    /// a row written): the turn's repeat counts start again from it, so a
    /// test rerun after an edit is no loop (RFC §6.4, the anti-loop guard).
    /// False by default.
    pub changes_state: bool,
    /// Declare this tool unseen until a result references it.
    pub defer_loading: bool,
}

impl AiTool {
    /// The tool `name`, doing `description`, taking no parameters.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::ToolName`] for a name no vendor accepts (RFC
    /// §6.7): an empty one, or one with a character other than a letter, a
    /// digit, `_`, `.`, `:` or `-`. A name some vendors refuse is refused by
    /// their provider when it declares the tool.
    ///
    /// ```
    /// use roomkit_ai::AiTool;
    /// use serde_json::json;
    ///
    /// let lookup = AiTool {
    ///     parameters: json!({ "type": "object", "properties": { "order": { "type": "string" } } }),
    ///     ..AiTool::new("lookup_order", "Find an order by its number")?
    /// };
    /// assert!(AiTool::new("files read", "Reads files").is_err());
    /// # let _ = lookup;
    /// # Ok::<(), roomkit_ai::AiError>(())
    /// ```
    pub fn new(name: impl Into<String>, description: impl Into<String>) -> Result<Self, AiError> {
        let name = name.into();
        Self::checked(&name)?;
        Ok(Self {
            name,
            description: description.into(),
            parameters: Value::Object(serde_json::Map::new()),
            tags: Vec::new(),
            changes_state: false,
            defer_loading: false,
        })
    }

    /// Refuses `name` when no vendor accepts it (RFC §6.7).
    pub(crate) fn checked(name: &str) -> Result<(), AiError> {
        let accepted = !name.is_empty()
            && name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | ':' | '-'));
        if accepted {
            Ok(())
        } else {
            Err(AiError::ToolName {
                name: name.to_owned(),
            })
        }
    }

    /// The tool `name`, a name this crate gives, doing `description`,
    /// taking `parameters`.
    pub(crate) fn own(name: &str, description: &str, parameters: Value) -> Self {
        Self {
            name: name.to_owned(),
            description: description.to_owned(),
            parameters,
            tags: Vec::new(),
            changes_state: false,
            defer_loading: false,
        }
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
    UnservedToolCall(String),
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
        Self::UnservedToolCall(message.into())
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
#[doc(hidden)]
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
#[doc(hidden)]
pub struct ToolContext {
    /// The turn's room.
    pub room_id: RoomId,
    /// The agent whose turn calls.
    pub channel_id: ChannelId,
    /// The room snapshot the turn read, for names in tool answers.
    pub(crate) room_metadata: Option<Arc<Metadata>>,
    turn: Option<TurnHost>,
    /// Whether the turn is an instruction that stands alone, which reads
    /// none of the room's working state (RFC §10.1.1 rule 7).
    pub(crate) stands_alone: bool,
    /// The participant whose event woke the turn, Python's tool loop
    /// context's `actor_id`, read with [`current_tool_actor_id`].
    pub(crate) actor_id: Option<String>,
    /// What a skill activation's hint may name, when the call asks to
    /// activate: the tools the turn reaches, its own and a person's aside (RFC
    /// §24.4).
    pub(crate) hintable: Vec<String>,
    /// The turn's Tool Search, which reveals what such a hint names once its
    /// call is served.
    pub(crate) search: Option<Arc<TurnSearch>>,
}

impl ToolContext {
    /// The context of a call in `room_id` by the agent `channel_id`,
    /// outside any turn the kit runs: in a toolset's own tests.
    pub fn new(room_id: RoomId, channel_id: ChannelId) -> Self {
        Self {
            room_id,
            channel_id,
            room_metadata: None,
            turn: None,
            stands_alone: false,
            actor_id: None,
            hintable: Vec::new(),
            search: None,
        }
    }

    /// The room as it stood when the turn began (RFC §21.4).
    pub(crate) fn reading_room(mut self, metadata: &Metadata) -> Self {
        self.room_metadata = Some(Arc::new(metadata.clone()));
        self
    }

    /// The same context, its turn woken by `actor_id`'s event.
    pub(crate) fn acting_for(mut self, actor_id: Option<String>) -> Self {
        self.actor_id = actor_id;
        self
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

/// A boxed, sendable future.
pub(crate) type BoxFuture<'a, T> = std::pin::Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A set of tools behind one call, as a trait object: the channel's
/// handler and its skills.
pub(crate) trait DynToolset: Send + Sync {
    fn call(
        &self,
        call: ToolCall,
        context: ToolContext,
    ) -> BoxFuture<'_, Result<ToolResult, ToolError>>;

    /// `call` ended `outcome`, the kit's `ON_TOOL_CALL` hooks run: what a
    /// toolset of the agent's own that keeps state on its calls records
    /// then, once nothing can withhold the result any more (RFC §24.4: an
    /// activation counts once its call is served).
    fn settled(&self, _call: &ToolCall, _outcome: ToolCallOutcome, _context: &ToolContext) {}

    /// Whether its calls carry a bound of their own, in place of the
    /// channel's bound per call (RFC §21.6): a person's answer, under its
    /// handler's timeout.
    fn bounds_itself(&self) -> bool {
        false
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

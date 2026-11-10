//! The tools a host gives an AI channel, in the Python implementation's
//! shape (RFC §21): a tool handler serving calls by name, the tool objects
//! that carry their own, what a call answers, and the call a handler runs
//! within.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use roomkit::model::{ChannelId, Metadata, ResponseMetadata, RoomId};
use roomkit_core::rules::python_repr;
use serde_json::Value;

use roomkit::model::ToolCall;

use super::{AiTool, BoxFuture, DynToolset, ToolContext, ToolError, ToolResult};
use crate::error::AiError;

/// What a tool handler's call is: a future of its answer.
pub type ToolFuture = Pin<Box<dyn Future<Output = Result<ToolResult, ToolError>> + Send>>;

/// A handler serving tools by name (RFC §21.4): Python's `ToolHandler`, an
/// `async (name, arguments) -> ToolResult`. A call to a tool it does not
/// serve declines with [`ToolError::UnservedToolCall`].
///
/// ```
/// use roomkit_ai::{ToolError, ToolHandler, ToolResult};
///
/// let handler = ToolHandler::new(|name, arguments| async move {
///     match name.as_str() {
///         "lookup_order" => Ok(ToolResult::Text(format!("{arguments:?} shipped"))),
///         _ => Err(ToolError::unserved(format!("not mine: {name}"))),
///     }
/// });
/// # let _ = handler;
/// ```
#[derive(Clone)]
pub struct ToolHandler(Arc<dyn Fn(String, Metadata) -> ToolFuture + Send + Sync>);

impl ToolHandler {
    /// The handler calling `handler` with each call's tool and arguments.
    pub fn new<F, Fut>(handler: F) -> Self
    where
        F: Fn(String, Metadata) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ToolResult, ToolError>> + Send + 'static,
    {
        Self(Arc::new(move |name, arguments| {
            Box::pin(handler(name, arguments))
        }))
    }

    /// The call of the tool `name` with `arguments`: Python's
    /// `await handler(name, arguments)`.
    pub fn call(&self, name: String, arguments: Metadata) -> ToolFuture {
        (self.0)(name, arguments)
    }
}

impl std::fmt::Debug for ToolHandler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ToolHandler")
    }
}

/// A tool that serves its own calls (RFC §21): Python's `Tool` protocol, a
/// definition and a handler.
pub trait Tool: Send + Sync + 'static {
    /// What the model is told of the tool.
    fn definition(&self) -> AiTool;

    /// The call of the tool `name` with `arguments`.
    fn handler(&self, name: String, arguments: Metadata) -> ToolFuture;
}

/// One of the tools an AI channel declares: Python's `AITool | Tool`, a
/// definition its tool handler serves, or a tool that serves itself.
#[derive(Clone)]
pub enum ToolEntry {
    /// A definition, which the channel's tool handler serves.
    Definition(AiTool),
    /// A tool serving its own calls.
    Tool(Arc<dyn Tool>),
}

impl From<AiTool> for ToolEntry {
    fn from(definition: AiTool) -> Self {
        Self::Definition(definition)
    }
}

impl From<Arc<dyn Tool>> for ToolEntry {
    fn from(tool: Arc<dyn Tool>) -> Self {
        Self::Tool(tool)
    }
}

impl std::fmt::Debug for ToolEntry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Definition(definition) => formatter
                .debug_tuple("Definition")
                .field(definition)
                .finish(),
            Self::Tool(tool) => formatter
                .debug_tuple("Tool")
                .field(&tool.definition())
                .finish(),
        }
    }
}

/// The handlers `handlers` chained, the first that serves a call winning:
/// Python's `compose_tool_handlers(*handlers)`. A handler that declines a
/// call, with [`ToolError::UnservedToolCall`] or the answer
/// `{"error": "Unknown tool: ..."}` (RFC §21.4), hands it to the next; the
/// last handler's answer is the composition's, a decline included, which
/// the channel reads as a call nothing served.
///
/// # Errors
///
/// Returns [`AiError::TooFewHandlers`] for fewer than two handlers, as
/// Python's raises `ValueError`.
pub fn compose_tool_handlers(handlers: Vec<ToolHandler>) -> Result<ToolHandler, AiError> {
    if handlers.len() < 2 {
        return Err(AiError::TooFewHandlers {
            given: handlers.len(),
        });
    }
    let handlers = Arc::new(handlers);
    Ok(ToolHandler::new(move |name, arguments| {
        let handlers = Arc::clone(&handlers);
        async move {
            let (last, earlier) = handlers
                .split_last()
                .ok_or_else(|| ToolError::unserved("compose_tool_handlers holds no handler"))?;
            for handler in earlier {
                match handler.call(name.clone(), arguments.clone()).await {
                    Err(ToolError::UnservedToolCall(_)) => {}
                    Ok(answer) if is_unknown_tool_answer(&answer) => {}
                    served => return served,
                }
            }
            last.call(name, arguments).await
        }
    }))
}

/// Whether `answer` says the call is not its handler's to serve: the
/// envelope `{"error": "Unknown tool: ..."}`, as text or as JSON, its error
/// starting `unknown tool` in any case, as Python's
/// `is_unknown_tool_answer` reads it.
fn is_unknown_tool_answer(answer: &ToolResult) -> bool {
    const UNKNOWN: &str = "unknown tool";
    let unknown = |value: &Value| {
        value
            .get("error")
            .and_then(Value::as_str)
            .and_then(|error| error.get(..UNKNOWN.len()))
            .is_some_and(|start| start.eq_ignore_ascii_case(UNKNOWN))
    };
    match answer {
        ToolResult::Json(value) => unknown(value),
        ToolResult::Text(text) => {
            serde_json::from_str::<Value>(text).is_ok_and(|value| unknown(&value))
        }
        ToolResult::Parts(_) => false,
    }
}

/// The call a tool handler runs within (RFC §21.4): Python's
/// `ToolCallContext(room_id='', tool_call_id='', channel_id='')`, read with
/// [`current_tool_call`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ToolCallContext {
    /// The room the call is made in.
    pub room_id: Option<RoomId>,
    /// The call's id.
    pub tool_call_id: String,
    /// The channel whose model made the call.
    pub channel_id: Option<ChannelId>,
}

/// The call the current tool handler runs within: Python's
/// `current_tool_call()`, `None` outside a tool's call. A task the handler
/// spawns reads it when spawned with [`roomkit::keep_context`], as
/// Python's `asyncio.create_task` copies the call's context; spawned
/// plainly, it reads `None`.
pub fn current_tool_call() -> Option<ToolCallContext> {
    let scope = roomkit::current_tool_call_scope()?;
    scope
        .downcast_ref::<CallScope>()
        .map(|scope| scope.call.clone())
}

/// The participant whose event woke the turn the current tool handler runs
/// within: Python's `current_tool_actor_id()`. `None` outside a tool's
/// call, for a turn no participant woke (a system injection, a webhook),
/// and for a sender the room reached through other channels only, whose
/// id read back would name them (RFC §5.5). A task the handler spawns with
/// [`roomkit::keep_context`] reads it too.
///
/// It names the turn; it does not authenticate it: a handler that reaches
/// a person's data with it loads the participant and requires its
/// identification first (RFC §11).
pub fn current_tool_actor_id() -> Option<String> {
    let scope = roomkit::current_tool_call_scope()?;
    scope.downcast_ref::<CallScope>()?.actor_id.clone()
}

/// The record of the turn the caller runs within (RFC §6.7): Python's
/// `current_response_metadata()`. The one record a memory writing it while
/// the context is built, a generation hook writing
/// `event.ai_context.response_metadata`, and a tool handler or a
/// `BEFORE_TOOL_USE` hook writing here all reach, the one
/// `InboundResult::response_metadata` gives the caller, the turn answered or
/// failed. A tool handler is the case this exists for: a document it read
/// is a fact about the turn, not about the tool's result.
///
/// `None` outside a turn, and in a tool the room's owner serves, whose turn's
/// record is on its executor. A task the handler spawns reads it when spawned
/// with [`roomkit::keep_context`].
pub fn current_response_metadata() -> Option<ResponseMetadata> {
    roomkit::current_turn_record()
}

/// What a tool's call runs within: the call, and who woke its turn.
struct CallScope {
    call: ToolCallContext,
    actor_id: Option<String>,
    room_metadata: Option<Arc<Metadata>>,
}

/// The room snapshot this tool's turn read, not a fresh store read.
pub(crate) fn current_tool_room_metadata() -> Option<Arc<Metadata>> {
    let scope = roomkit::current_tool_call_scope()?;
    scope.downcast_ref::<CallScope>()?.room_metadata.clone()
}

/// Runs `work`, a tool's call, within `context`, for the turn `actor_id`
/// woke, which the kit carries into a task spawned with
/// [`roomkit::keep_context`].
pub(crate) async fn within_call<T>(
    context: ToolCallContext,
    actor_id: Option<String>,
    room_metadata: Option<Arc<Metadata>>,
    work: impl Future<Output = T>,
) -> T {
    let scope = CallScope {
        call: context,
        actor_id,
        room_metadata,
    };
    roomkit::within_tool_call_scope(Arc::new(scope), work).await
}

/// The toolset `tool_handler` and `tools` make, Python's
/// `compose_tool_handlers(tool_handler, extract_tools(tools))`: every
/// definition, a tool serving itself declaring its own, and one handler,
/// the tool handler first, then each tool, the first that serves a call
/// winning. None when nothing serves a tool.
pub(crate) fn served_by(
    tool_handler: Option<ToolHandler>,
    tools: Vec<ToolEntry>,
) -> Result<Option<Served>, AiError> {
    let mut definitions = Vec::with_capacity(tools.len());
    let mut handlers: Vec<ToolHandler> = tool_handler.into_iter().collect();
    for entry in tools {
        match entry {
            ToolEntry::Definition(definition) => definitions.push(definition),
            ToolEntry::Tool(tool) => {
                definitions.push(tool.definition());
                handlers.push(ToolHandler::new(move |name, arguments| {
                    tool.handler(name, arguments)
                }));
            }
        }
    }
    for definition in &definitions {
        AiTool::checked(&definition.name)?;
    }
    let handler = match handlers.len() {
        0 if definitions.is_empty() => return Ok(None),
        0 => ToolHandler::new(|name, _arguments| async move {
            Err(ToolError::unserved(format!(
                "no tool handler serves {name}"
            )))
        }),
        1 => handlers.remove(0),
        _ => compose_tool_handlers(handlers)?,
    };
    let toolset: Arc<dyn DynToolset> = Arc::new(ServedBy { handler });
    Ok(Some((toolset, definitions)))
}

/// What serves a channel's tools: its one handler, as a toolset, and the
/// definitions it declares.
pub(crate) type Served = (Arc<dyn DynToolset>, Vec<AiTool>);

/// The channel's tools behind its one handler.
struct ServedBy {
    handler: ToolHandler,
}

impl DynToolset for ServedBy {
    fn call(
        &self,
        call: ToolCall,
        context: ToolContext,
    ) -> BoxFuture<'_, Result<ToolResult, ToolError>> {
        let within = ToolCallContext {
            room_id: Some(context.room_id.clone()),
            tool_call_id: call.id.clone(),
            channel_id: Some(context.channel_id.clone()),
        };
        // The handler is called within its call, as Python sets the call's
        // context before it calls the handler; its answer may decline the
        // call, as Python's `declined_answer` reads it (RFC §21.4).
        let handler = self.handler.clone();
        let actor_id = context.actor_id.clone();
        Box::pin(within_call(
            within,
            actor_id,
            context.room_metadata,
            async move {
                match handler.call(call.name.clone(), call.arguments).await {
                    Ok(answer) if is_unknown_tool_answer(&answer) => Err(ToolError::unserved(
                        format!("tool {} is not served here", python_repr(&call.name)),
                    )),
                    answered => answered,
                }
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    /// Python's `raise_if_unknown_tool` (`tools/result.py`): a handler's
    /// unknown-tool answer declines the call,
    /// `UnservedToolCallError(f"tool {name!r} is not served here")`.
    #[tokio::test]
    async fn a_call_its_handler_does_not_know_is_declined_naming_it_as_python_s_repr() {
        let handler = ToolHandler::new(|name, _| async move {
            Ok(ToolResult::Text(
                json!({ "error": format!("Unknown tool: {name}") }).to_string(),
            ))
        });
        let (toolset, _) = served_by(Some(handler), Vec::new()).unwrap().unwrap();
        let context = ToolContext::new(RoomId::new("r").unwrap(), ChannelId::new("ai").unwrap());
        let answer = toolset
            .call(ToolCall::new("c1", "o'k", Metadata::new()), context)
            .await;
        assert_eq!(
            answer,
            Err(ToolError::unserved(
                "tool \"o'k\" is not served here".to_owned()
            ))
        );
    }
}

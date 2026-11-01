//! The tools a host gives an AI channel, in the Python implementation's
//! shape (RFC §21): a tool handler serving calls by name, the tool objects
//! that carry their own, what a call answers, and the call a handler runs
//! within.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use roomkit::model::{ChannelId, Metadata, RoomId};
use serde_json::Value;

use roomkit::model::ToolCall;

use super::{AiTool, BoxFuture, DynToolset, ToolContext, ToolError};
use crate::error::AiError;

/// What a tool's call answers with (RFC §21.4): Python's `ToolResult`, a
/// text, which the model reads as it is, or a JSON value, which it reads as
/// its JSON, as Python dumps a mapping.
#[derive(Clone, Debug, PartialEq)]
pub enum ToolResult {
    /// Text.
    Text(String),
    /// A JSON value.
    Json(Value),
}

impl ToolResult {
    /// The result as its hooks read it: a text as a JSON string.
    pub(crate) fn into_value(self) -> Value {
        match self {
            Self::Text(text) => Value::String(text),
            Self::Json(value) => value,
        }
    }
}

impl From<String> for ToolResult {
    fn from(text: String) -> Self {
        Self::Text(text)
    }
}

impl From<&str> for ToolResult {
    fn from(text: &str) -> Self {
        Self::Text(text.to_owned())
    }
}

impl From<Value> for ToolResult {
    fn from(value: Value) -> Self {
        Self::Json(value)
    }
}

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
/// last handler's answer is the composition's, a decline included.
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
/// envelope `{"error": "Unknown tool: ..."}`, as text or as JSON.
fn is_unknown_tool_answer(answer: &ToolResult) -> bool {
    let unknown = |value: &Value| {
        value
            .get("error")
            .and_then(Value::as_str)
            .is_some_and(|error| error.starts_with("Unknown tool:"))
    };
    match answer {
        ToolResult::Json(value) => unknown(value),
        ToolResult::Text(text) => {
            serde_json::from_str::<Value>(text).is_ok_and(|value| unknown(&value))
        }
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

tokio::task_local! {
    static CURRENT_TOOL_CALL: ToolCallContext;
}

/// The call the current tool handler runs within: Python's
/// `current_tool_call()`, `None` outside a tool's call.
pub fn current_tool_call() -> Option<ToolCallContext> {
    CURRENT_TOOL_CALL.try_with(Clone::clone).ok()
}

/// Runs `work`, a tool's call, within `context`.
pub(crate) async fn within_call<T>(context: ToolCallContext, work: impl Future<Output = T>) -> T {
    CURRENT_TOOL_CALL.scope(context, work).await
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
        // context before it calls the handler.
        let handler = self.handler.clone();
        Box::pin(within_call(within, async move {
            handler.call(call.name, call.arguments).await
        }))
    }
}

//! The host's tools, as closures (step 4, decision 7), for tests.

use std::future::Future;
use std::sync::Arc;

use roomkit::current_turn_host;
use roomkit::model::{Metadata, ToolCall};

use super::{
    AiTool, BoxFuture, Tool, ToolContext, ToolEntry, ToolError, ToolFuture, ToolResult,
    current_tool_call,
};

/// A tool's handler, boxed.
type Handler = dyn Fn(ToolCall, ToolContext) -> BoxFuture<'static, Result<ToolResult, ToolError>>
    + Send
    + Sync;

/// The host's tools, each a definition and the closure that serves it,
/// given to a channel as its tools (`Vec<ToolEntry>::from(tools)`), each a
/// tool serving itself. For tests: the public form is a [`ToolHandler`]
/// or a [`Tool`](super::Tool). Cloning it shares its closures.
///
/// [`ToolHandler`]: super::ToolHandler
#[doc(hidden)]
#[derive(Clone, Default)]
pub struct Tools {
    tools: Vec<(AiTool, Arc<Handler>)>,
}

impl Tools {
    /// No tool yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// The same tools, and `definition`, served by `handler`. A name given
    /// twice is refused when the agent takes the toolset (RFC §21.1 "One
    /// tool per name").
    #[must_use]
    pub fn tool<F, Fut>(mut self, definition: AiTool, handler: F) -> Self
    where
        F: Fn(ToolCall, ToolContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ToolResult, ToolError>> + Send + 'static,
    {
        let handler: Arc<Handler> = Arc::new(move |call, context| Box::pin(handler(call, context)));
        self.tools.push((definition, handler));
        self
    }
}

impl std::fmt::Debug for Tools {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = self
            .tools
            .iter()
            .map(|(definition, _)| definition.name.as_str())
            .collect();
        formatter
            .debug_struct("Tools")
            .field("tools", &names)
            .finish()
    }
}

/// Each tool, a tool serving itself through its closure, which reads its
/// call from the call it runs within.
impl From<Tools> for Vec<ToolEntry> {
    fn from(tools: Tools) -> Self {
        tools
            .tools
            .into_iter()
            .map(|(definition, handler)| {
                let tool: Arc<dyn Tool> = Arc::new(Closure {
                    definition,
                    handler,
                });
                ToolEntry::Tool(tool)
            })
            .collect()
    }
}

/// A tool of [`Tools`]: its definition and its closure.
struct Closure {
    definition: AiTool,
    handler: Arc<Handler>,
}

impl Tool for Closure {
    fn definition(&self) -> AiTool {
        self.definition.clone()
    }

    fn handler(&self, name: String, arguments: Metadata) -> ToolFuture {
        // Another tool's call is not this one's to serve: the next handler
        // of the channel's chain serves it.
        if name != self.definition.name {
            return Box::pin(std::future::ready(Err(ToolError::unserved(format!(
                "{name} is not {}",
                self.definition.name
            )))));
        }
        let Some(within) = current_tool_call() else {
            return Box::pin(std::future::ready(Err(ToolError::unserved(
                "a tool of Tools is called outside a tool's call",
            ))));
        };
        let (Some(room_id), Some(channel_id)) = (within.room_id, within.channel_id) else {
            return Box::pin(std::future::ready(Err(ToolError::unserved(
                "a tool of Tools is called outside a room's turn",
            ))));
        };
        let call = ToolCall::new(within.tool_call_id, name, arguments);
        let context = ToolContext::new(room_id, channel_id).within(current_turn_host());
        (self.handler)(call, context)
    }
}

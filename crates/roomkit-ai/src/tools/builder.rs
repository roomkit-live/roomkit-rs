//! The host's tools, as closures (step 4, decision 7).

use std::future::Future;
use std::sync::Arc;

use roomkit::model::ToolCall;

use super::{BoxFuture, ToolContext, ToolDefinition, ToolError, ToolOutput, Toolset};
use crate::texts::error_envelope;

/// A tool's handler, boxed.
type Handler = dyn Fn(ToolCall, ToolContext) -> BoxFuture<'static, Result<ToolOutput, ToolError>>
    + Send
    + Sync;

/// The host's tools, each a definition and the closure that serves it: a
/// [`Toolset`] built one tool at a time. Cloning it shares its closures.
///
/// ```
/// use roomkit_ai::{ToolDefinition, ToolError, ToolOutput, Tools};
/// use serde::Deserialize;
/// use serde_json::json;
///
/// #[derive(Deserialize)]
/// struct Lookup {
///     order: String,
/// }
///
/// let lookup = ToolDefinition::new(
///     "lookup_order",
///     "Find an order by its number",
///     json!({
///         "type": "object",
///         "properties": { "order": { "type": "string" } },
///         "required": ["order"],
///     }),
/// );
/// let tools = Tools::new().tool(lookup, |call, _context| async move {
///     let Lookup { order } = roomkit_ai::arguments(&call)?;
///     if order == "0" {
///         return Err(ToolError::refused("There is no order 0."));
///     }
///     Ok(ToolOutput::Json(json!({ "order": order, "status": "shipped" })))
/// });
/// # let _ = tools;
/// ```
#[derive(Clone, Default)]
pub struct Tools {
    tools: Vec<(ToolDefinition, Arc<Handler>)>,
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
    pub fn tool<F, Fut>(mut self, definition: ToolDefinition, handler: F) -> Self
    where
        F: Fn(ToolCall, ToolContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ToolOutput, ToolError>> + Send + 'static,
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

impl Toolset for Tools {
    fn tools(&self) -> Vec<ToolDefinition> {
        self.tools
            .iter()
            .map(|(definition, _)| definition.clone())
            .collect()
    }

    fn call(
        &self,
        call: ToolCall,
        context: ToolContext,
    ) -> impl Future<Output = Result<ToolOutput, ToolError>> + Send {
        let handler = self
            .tools
            .iter()
            .find(|(definition, _)| definition.name == call.name)
            .map(|(_, handler)| Arc::clone(handler));
        async move {
            match handler {
                Some(handler) => handler(call, context).await,
                // The agent calls a toolset only with a tool it declares.
                None => Err(ToolError::Refused(error_envelope(&format!(
                    "Tool '{}' is not declared.",
                    call.name
                )))),
            }
        }
    }
}

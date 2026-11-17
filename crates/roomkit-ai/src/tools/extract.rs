//! Python's public tool extraction, separate from channel admission.

use super::{AiTool, ToolEntry, ToolHandler, compose_tool_handlers, definition};
use crate::AiError;

/// Split a mixed list of tools into ordered definitions and an optional handler
/// (RFC §21.1, §21.4): Python's `extract_tools(tools)`.
///
/// Definitions and raw schemas have no handler. One tool keeps its handler;
/// two or more compose theirs in input order. Extraction preserves duplicate
/// names: the channel, rather than this helper, admits its catalogue.
/// Existing [`AiTool`] values pass through unchanged. A [`super::Tool`]'s
/// definition is rebuilt like a schema, including its tags and state marker,
/// but without `defer_loading`, which Python's extractor does not copy.
///
/// # Errors
///
/// A raw schema or tool object's invalid definition returns
/// [`AiError::ToolDefinition`] or [`AiError::ToolName`]. Existing typed
/// definitions are not revalidated at this boundary.
///
/// ```
/// use roomkit::model::Metadata;
/// use roomkit_ai::{ToolEntry, extract_tools};
///
/// let mut schema = Metadata::new();
/// schema.insert("name".into(), "lookup".into());
/// let (definitions, handler) = extract_tools(vec![ToolEntry::Schema(schema)])?;
/// assert_eq!(definitions[0].name, "lookup");
/// assert!(handler.is_none());
/// # Ok::<(), roomkit_ai::AiError>(())
/// ```
pub fn extract_tools(tools: Vec<ToolEntry>) -> Result<(Vec<AiTool>, Option<ToolHandler>), AiError> {
    let mut definitions = Vec::with_capacity(tools.len());
    let mut handlers = Vec::new();
    for entry in tools {
        match entry {
            ToolEntry::Definition(given) => definitions.push(given),
            ToolEntry::Schema(schema) => definitions.push(definition::schema(&schema)?),
            ToolEntry::Tool(tool) => {
                definitions.push(definition::tool(tool.definition())?);
                handlers.push(ToolHandler::new(move |name, arguments| {
                    tool.handler(name, arguments)
                }));
            }
        }
    }
    let handler = match handlers.len() {
        0 => None,
        1 => handlers.pop(),
        _ => Some(compose_tool_handlers(handlers)?),
    };
    Ok((definitions, handler))
}

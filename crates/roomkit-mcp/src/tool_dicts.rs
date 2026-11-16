//! The discovered catalogue as plain host-owned dictionaries (RFC §21.2).

use roomkit::model::Metadata;
use roomkit_ai::AiTool;
use serde_json::Value;

use crate::{McpError, McpToolProvider};

impl McpToolProvider {
    /// The discovered tools as plain dictionaries, in catalogue order.
    ///
    /// Python's `get_tools_as_dicts()`: all six `AiTool` fields, including
    /// defaults, with owned nested values. Reading them makes no request.
    ///
    /// # Errors
    /// Returns [`McpError::NotConnected`] outside a live connection.
    pub fn get_tools_as_dicts(&self) -> Result<Vec<Metadata>, McpError> {
        Ok(self.get_tools()?.into_iter().map(tool_dict).collect())
    }
}

fn tool_dict(tool: AiTool) -> Metadata {
    Metadata::from_iter([
        ("name".into(), Value::String(tool.name)),
        ("description".into(), Value::String(tool.description)),
        ("parameters".into(), tool.parameters),
        (
            "tags".into(),
            Value::Array(tool.tags.into_iter().map(Value::String).collect()),
        ),
        ("defer_loading".into(), Value::Bool(tool.defer_loading)),
        ("changes_state".into(), Value::Bool(tool.changes_state)),
    ])
}

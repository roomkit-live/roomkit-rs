//! Read the host catalogue and route only its declared names.

use std::collections::HashSet;
use std::sync::Arc;

use roomkit::model::{Metadata, ToolCall};
use serde_json::Value;

use super::{SANDBOX_TOOL_PREFIX, SandboxExecutor};
use crate::tools::{AiTool, BoxFuture, DynToolset, ToolContext, ToolError, ToolResult};
use crate::{AiError, ToolEntry, extract_tools};

pub(crate) struct SandboxTools(pub(crate) Arc<dyn SandboxExecutor>);

impl SandboxTools {
    pub(crate) fn install(
        toolbox: &mut crate::tools::toolbox::Toolbox,
        executor: Option<Arc<dyn SandboxExecutor>>,
    ) -> Result<Option<Arc<Self>>, AiError> {
        let Some(executor) = executor else {
            return Ok(None);
        };
        let sandbox = Arc::new(Self(executor));
        let tools: Arc<dyn DynToolset> = Arc::<Self>::clone(&sandbox);
        toolbox.add_sandbox(tools, sandbox.names()?)?;
        Ok(Some(sandbox))
    }

    /// Constructor admission reads names, before the context validates schemas.
    pub(crate) fn names(&self) -> Result<Vec<AiTool>, AiError> {
        let entries = self.0.tool_definitions().map_err(failed)?;
        let mut seen = HashSet::new();
        entries
            .iter()
            .filter_map(|entry| match name(entry) {
                Ok(name) if name.starts_with(SANDBOX_TOOL_PREFIX) && seen.insert(name) => {
                    Some(Ok(AiTool::own(name, "", Value::Object(Metadata::new()))))
                }
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect()
    }

    pub(crate) fn definitions(&self) -> Result<Vec<AiTool>, AiError> {
        let entries = self.0.tool_definitions().map_err(failed)?;
        let mut definitions = Vec::new();
        for entry in entries {
            let name = name(&entry)?;
            if !name.starts_with(SANDBOX_TOOL_PREFIX) {
                tracing::warn!(target: "roomkit::ai", tool = name, "Sandbox tool does not start with sandbox_ — skipping");
                continue;
            }
            let mut schema = Metadata::new();
            schema.insert("name".to_owned(), Value::String(name.to_owned()));
            for field in ["description", "parameters"] {
                if let Some(value) = entry.get(field) {
                    schema.insert(field.to_owned(), value.clone());
                }
            }
            definitions.push(ToolEntry::Schema(schema));
        }
        extract_tools(definitions).map(|(tools, _)| tools)
    }
}

fn name(entry: &Metadata) -> Result<&str, AiError> {
    let given = entry
        .get("name")
        .ok_or_else(|| failed(ToolError::raised_as("KeyError", "'name'")))?;
    let name = given.as_str().ok_or_else(|| {
        let kind = match given {
            Value::Null => "NoneType",
            Value::Bool(_) => "bool",
            Value::Number(number) if number.is_f64() => "float",
            Value::Number(_) => "int",
            Value::Array(_) => "list",
            Value::Object(_) => "dict",
            Value::String(_) => "str",
        };
        failed(ToolError::raised_as(
            "AttributeError",
            format!("'{kind}' object has no attribute 'startswith'"),
        ))
    })?;
    Ok(name)
}

fn failed(error: ToolError) -> AiError {
    AiError::SandboxDefinition { error }
}

impl DynToolset for SandboxTools {
    fn call(
        &self,
        call: ToolCall,
        _context: ToolContext,
    ) -> BoxFuture<'_, Result<ToolResult, ToolError>> {
        Box::pin(async move {
            let command = call
                .name
                .strip_prefix(SANDBOX_TOOL_PREFIX)
                .unwrap_or(&call.name);
            let result = self.0.execute(command, Some(&call.arguments)).await?;
            serde_json::to_string(&result)
                .map(ToolResult::Text)
                .map_err(|error| ToolError::raised(&error))
        })
    }
}

/// The runtime cannot inspect an adapter's enum to recover a host exception class.
pub(crate) fn channel_error(error: AiError) -> roomkit::ChannelError {
    let class = if let AiError::SandboxDefinition {
        error: ToolError::Raised { kind, .. },
    } = &error {
        Some(kind.clone())
    } else {
        None
    };
    match class {
        Some(class) => Box::new(roomkit::ClassedError::new(class, error)),
        None => Box::new(error),
    }
}

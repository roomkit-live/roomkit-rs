//! Local script execution belongs to the host; the framework checks its path.

use std::io::ErrorKind;

use roomkit::model::{Metadata, ToolCall};
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::{Skill, SkillFileError, SkillRegistry, texts};
use crate::tools::{AiTool, BoxFuture, ToolError, ToolResult};

#[cfg(test)]
mod tests;

/// Python's `ScriptResult`: a host's script outcome (RFC §24.6).
/// `success` defaults to true independently of the required exit code.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScriptResult {
    /// The script's exit code, required when constructing or deserializing.
    pub exit_code: i64,
    /// Its standard output, empty by default.
    #[serde(default)]
    pub stdout: String,
    /// Its standard error, empty by default.
    #[serde(default)]
    pub stderr: String,
    /// The host's success flag, true by default, regardless of `exit_code`.
    #[serde(default = "succeeded")]
    pub success: bool,
    /// Additional host data, empty by default.
    #[serde(default)]
    pub metadata: Metadata,
}

const fn succeeded() -> bool {
    true
}

impl ScriptResult {
    /// Constructs an outcome with Python's defaults and the required exit code.
    pub fn new(exit_code: i64) -> Self {
        Self {
            exit_code,
            stdout: String::new(),
            stderr: String::new(),
            success: true,
            metadata: Metadata::new(),
        }
    }
}

/// Python's `ScriptExecutor`: the integrator's execution policy (RFC §24.6).
/// The host chooses sandboxing, interpreters and timeouts. RoomKit provides
/// no process runner and checks containment before handing a script to it.
pub trait ScriptExecutor: Send + Sync {
    /// Executes `script_name` of `skill`; omitted arguments remain `None`.
    ///
    /// # Errors
    /// A host failure reaches the channel's ordinary tool error and observer path.
    fn execute<'a>(
        &'a self,
        skill: &'a Skill,
        script_name: &'a str,
        arguments: Option<&'a Metadata>,
    ) -> BoxFuture<'a, Result<ScriptResult, ToolError>>;

    /// Releases host resources; by default, nothing needs closing.
    /// As for `Channel::close`, a panic propagates from direct channel close
    /// and is aggregated by kit close.
    fn close(&self) -> BoxFuture<'_, ()> {
        Box::pin(std::future::ready(()))
    }
}

pub(super) fn definition() -> AiTool {
    AiTool::own(
        texts::RUN_SKILL_SCRIPT,
        "Run a script from a skill's scripts/ directory.",
        json!({
            "type": "object",
            "properties": {
                "skill_name": {"type": "string", "description": "Name of the skill."},
                "script_name": {"type": "string", "description": "Script filename to run."},
                "arguments": {
                    "type": "object", "description": "Optional key-value arguments.",
                    "additionalProperties": {"type": "string"}
                }
            },
            "required": ["skill_name", "script_name"]
        }),
    )
}

pub(super) async fn run(
    registry: &SkillRegistry,
    executor: &dyn ScriptExecutor,
    call: &ToolCall,
) -> Result<ToolResult, ToolError> {
    let name = argument(call, "skill_name");
    let script_name = argument(call, "script_name");
    let Some(skill) = registry.get_skill(name) else {
        let message = registry.get_unavailable_reason(name).map_or_else(
            || format!("Skill {} not found", texts::quoted_name(name)),
            |reason| super::tools::unavailable(name, reason),
        );
        return Err(ToolError::Refused(texts::object(&[(
            "error",
            json!(message),
        )])));
    };
    let checked = skill.clone();
    let filename = script_name.to_owned();
    let resolved = tokio::task::spawn_blocking(move || checked.resolve_script(&filename))
        .await
        .map_err(|error| ToolError::raised(&error))?;
    if let Err(error) = resolved {
        return path_error(&error);
    }
    let arguments = call
        .arguments
        .get("arguments")
        .and_then(serde_json::Value::as_object);
    let result = executor.execute(skill, script_name, arguments).await?;
    serde_json::to_string(&result)
        .map(ToolResult::Text)
        .map_err(|error| ToolError::raised(&error))
}

fn argument<'a>(call: &'a ToolCall, name: &str) -> &'a str {
    call.arguments
        .get(name)
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
}

fn path_error(error: &SkillFileError) -> Result<ToolResult, ToolError> {
    let served = match error {
        SkillFileError::Path(_) => true,
        SkillFileError::Io(error) => error.kind() == ErrorKind::NotFound,
        SkillFileError::Encoding(_) => false,
    };
    if served {
        return Ok(ToolResult::Text(texts::object(&[(
            "error",
            json!(error.to_string()),
        )])));
    }
    let kind = match error {
        SkillFileError::Io(error) if error.kind() == ErrorKind::PermissionDenied => {
            "PermissionError"
        }
        SkillFileError::Io(_) => "OSError",
        SkillFileError::Encoding(_) => "UnicodeDecodeError",
        SkillFileError::Path(_) => "SkillPathError",
    };
    Err(ToolError::raised_as(kind, error.to_string()))
}

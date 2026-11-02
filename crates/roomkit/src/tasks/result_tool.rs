//! The tool a delegated agent must call to hand its work back (RFC §23.3):
//! Python's `roomkit.ResultTool`.

use std::sync::Arc;

use roomkit_core::model::Metadata;
use serde_json::Value;

/// The tool a delegated agent must call to hand its work back, and how it
/// is read: Python's `ResultTool(tool, normalize, on_missing, reminder)`.
///
/// A delegation run with
/// [`require_structured_result`](crate::DelegateOptions::require_structured_result)
/// serves [`tool`](Self::tool) in its child room alone, reads the payload
/// of the agent's served call with [`normalize`](Self::normalize), reminds
/// it with [`reminder`](Self::reminder) when a turn ends without the call,
/// and gives up after the retries with [`on_missing`](Self::on_missing).
/// `submit_result` (a worker's result) is the default.
///
/// ```
/// use std::sync::Arc;
/// use roomkit::ResultTool;
/// use roomkit::model::Metadata;
/// use serde_json::json;
///
/// let verdict = ResultTool::new(
///     json!({
///         "name": "submit_verdict",
///         "description": "Give your verdict.",
///         "parameters": { "type": "object" },
///     }),
///     Arc::new(|arguments: &Metadata| arguments.clone()),
///     Arc::new(|_role: &str, _last_output: &str, _attempts: u32| {
///         let mut verdict = Metadata::new();
///         verdict.insert("approved".to_owned(), json!(false));
///         verdict
///     }),
///     "Call submit_verdict with your verdict.",
/// );
/// assert!(verdict.matches("mcp__gateway__submit_verdict"));
/// ```
#[derive(Clone)]
pub struct ResultTool {
    /// The tool served for the delegation, as the model is told of it:
    /// `{"name", "description", "parameters"}`, Python's `AITool`, as the
    /// turn backend carries the tools the room's owner serves.
    pub tool: Value,
    /// Turns the call's arguments into the payload returned.
    pub normalize: Arc<dyn Fn(&Metadata) -> Metadata + Send + Sync>,
    /// Builds the payload returned when the agent never called the tool,
    /// from its role, its last output and the attempts made.
    #[expect(
        clippy::type_complexity,
        reason = "Python's callable written out: a public alias would be a name Python lacks"
    )]
    pub on_missing: Arc<dyn Fn(&str, &str, u32) -> Metadata + Send + Sync>,
    /// The message sent when a turn ends without the call.
    pub reminder: String,
}

impl ResultTool {
    /// The tool `tool`, read by `normalize`, given up on with
    /// `on_missing`, reminded with `reminder`.
    #[expect(
        clippy::type_complexity,
        reason = "Python's callable written out: a public alias would be a name Python lacks"
    )]
    pub fn new(
        tool: Value,
        normalize: Arc<dyn Fn(&Metadata) -> Metadata + Send + Sync>,
        on_missing: Arc<dyn Fn(&str, &str, u32) -> Metadata + Send + Sync>,
        reminder: impl Into<String>,
    ) -> Self {
        Self {
            tool,
            normalize,
            on_missing,
            reminder: reminder.into(),
        }
    }

    /// The tool's name, its definition's `name`.
    pub fn name(&self) -> &str {
        self.tool
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
    }

    /// Whether `tool_name` is a call of this tool: its name, or its name
    /// as an MCP server exposes it (`mcp__<server>__<name>`).
    pub fn matches(&self, tool_name: &str) -> bool {
        let name = self.name();
        tool_name == name || tool_name.ends_with(&format!("__{name}"))
    }
}

impl std::fmt::Debug for ResultTool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResultTool")
            .field("tool", &self.tool)
            .field("reminder", &self.reminder)
            .finish_non_exhaustive()
    }
}

//! A call a model makes to a tool (RFC §6.4, §6.7).

use serde::{Deserialize, Serialize};

use super::Metadata;

/// A call a model made to a tool, as the tool loop runs it (RFC §6.4 "What
/// a provider hands the loop for a call", §6.7 `AIToolCallPart`): the id no
/// other call of its response carries, the tool's name, and its arguments
/// as a mapping, never an error (`{}` for none, `{"raw": text}` for text
/// that reads as no object).
///
/// ```
/// use roomkit_core::model::{Metadata, ToolCall};
///
/// let mut arguments = Metadata::new();
/// arguments.insert("order".to_owned(), "42".into());
/// let call = ToolCall::new("call_1", "lookup_order", arguments);
/// assert_eq!(call.arguments["order"], "42");
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ToolCall {
    /// The call's id.
    pub id: String,
    /// The tool called.
    pub name: String,
    /// The call's arguments.
    #[serde(default)]
    pub arguments: Metadata,
    /// What the call does, in the sentence its model wrote for the person,
    /// with an agent's `tool_call_intents` on (RFC §6.4): taken out of its
    /// arguments, display text only; `None` when the model wrote none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub intent: Option<String>,
}

impl ToolCall {
    /// The call `id` of the tool `name` with `arguments`.
    pub fn new(id: impl Into<String>, name: impl Into<String>, arguments: Metadata) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            arguments,
            intent: None,
        }
    }
}

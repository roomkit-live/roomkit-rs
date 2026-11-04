//! The tools the room's owner serves a turn (RFC §10.6.5): declared beside
//! the agent's own, each call crossing to the owner, whose handler runs in
//! the room of the call. Python's `TurnOwnerTools`, which the registry of
//! the channel reads in a turn the backend runs.

use std::sync::Arc;

use roomkit::model::ToolCall;
use roomkit::{OwnerToolAnswer, TurnHost};

use super::toolbox::Toolbox;
use super::{AiTool, BoxFuture, DynToolset, ToolContext, ToolError, ToolResult};
use crate::error::AiError;

/// What serves the owner's tools of one turn: its host, which carries each
/// call to the owner; those whose calls wait by design (a strategy's, RFC
/// §21.6) apart, the channel's bound per call not applying to them.
struct OwnerToolset {
    turn: TurnHost,
    waits: bool,
}

impl DynToolset for OwnerToolset {
    fn call(
        &self,
        call: ToolCall,
        _context: ToolContext,
    ) -> BoxFuture<'_, Result<ToolResult, ToolError>> {
        Box::pin(async move {
            let answer = self
                .turn
                .call_owner_tool(&call.name, call.arguments, &call.id)
                .await;
            match answer {
                OwnerToolAnswer::Served(serde_json::Value::String(text)) => {
                    Ok(ToolResult::Text(text))
                }
                OwnerToolAnswer::Served(value) => Ok(ToolResult::Json(value)),
                OwnerToolAnswer::Refused(words) => Err(ToolError::Refused(words)),
                OwnerToolAnswer::Unserved(message) => Err(ToolError::UnservedToolCall(message)),
                OwnerToolAnswer::Failed { kind, message } => {
                    Err(ToolError::Raised { kind, message })
                }
            }
        })
    }

    fn bounds_itself(&self) -> bool {
        self.waits
    }
}

/// `toolbox`, with the tools the owner serves `turn` after the host's and
/// before a person's and the skills'; `toolbox` itself outside a turn, or for a
/// turn served none.
///
/// # Errors
///
/// [`AiError::DuplicateTool`] for an owner's tool named as one of the
/// agent's own: the turn is refused, as Python's executor refuses a turn
/// whose owner's tools clash with the channel's (RFC §10.6.5).
pub(crate) fn with_owner_tools(
    toolbox: &Arc<Toolbox>,
    turn: Option<&TurnHost>,
) -> Result<Arc<Toolbox>, AiError> {
    let Some(turn) = turn else {
        return Ok(Arc::clone(toolbox));
    };
    let declared: Vec<AiTool> = turn
        .owner_tools()
        .iter()
        .filter_map(|definition| {
            let name = definition.get("name")?.as_str()?;
            Some(AiTool {
                name: name.to_owned(),
                description: definition
                    .get("description")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                parameters: definition
                    .get("parameters")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!({ "type": "object" })),
                tags: Vec::new(),
            })
        })
        .collect();
    if declared.is_empty() {
        return Ok(Arc::clone(toolbox));
    }
    let waiting = turn.owner_tools_waiting();
    let (waits, bounded): (Vec<AiTool>, Vec<AiTool>) = declared
        .into_iter()
        .partition(|tool| waiting.contains(&tool.name));
    let mut extended = Toolbox::clone(toolbox);
    for (waits, tools) in [(false, bounded), (true, waits)] {
        if tools.is_empty() {
            continue;
        }
        let served = Arc::new(OwnerToolset {
            turn: turn.clone(),
            waits,
        });
        extended.add_injected(served, tools)?;
    }
    Ok(Arc::new(extended))
}

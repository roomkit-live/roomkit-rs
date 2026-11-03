//! The handoff tool's definition (RFC §19.6), JSON here since `AiTool` is
//! `roomkit_ai`'s: Python's `HANDOFF_TOOL` and `build_handoff_tool`.

use serde_json::{Value, json};

/// The handoff tool's name: Python's `HANDOFF_TOOL_NAME`.
pub(crate) const HANDOFF_TOOL_NAME: &str = "handoff_conversation";

/// The handoff tool's definition, `{name, description, parameters}`:
/// Python's `HANDOFF_TOOL` for no `targets`, else what its
/// `build_handoff_tool(targets)` builds. Hidden: `roomkit_ai` gives them as
/// `AiTool`s.
#[doc(hidden)]
#[must_use]
pub fn handoff_tool_definition(targets: &[(String, Option<String>)]) -> Value {
    let properties = |target: Value| {
        json!({
            "target": target,
            "reason": {"type": "string", "description": "Why the handoff is needed"},
            "summary": {
                "type": "string",
                "description": "Summary of the conversation so far and what the next agent \
                                needs to know to continue effectively",
            },
            "next_phase": {
                "type": "string",
                "description": "Optional: conversation phase to transition to (e.g., \
                                'handling', 'review', 'escalation')",
            },
            "channel_escalation": {
                "type": "string",
                "enum": ["same", "voice", "email", "sms"],
                "description": "Whether to escalate to a different channel. 'same' keeps the \
                                current channel.",
            },
        })
    };
    let parameters = |target: Value| {
        json!({
            "type": "object",
            "properties": properties(target),
            "required": ["target", "reason", "summary"],
        })
    };
    if targets.is_empty() {
        return json!({
            "name": HANDOFF_TOOL_NAME,
            "description": "Transfer this conversation to another agent or specialist. Use \
                            when: the user asks to speak with someone else, the user wants to \
                            be transferred or go back to a previous agent, the conversation \
                            needs expertise you don't have, or your task is complete and the \
                            next step requires a different agent. Always provide a clear \
                            summary of the conversation so far.",
            "parameters": parameters(json!({
                "type": "string",
                "description": "Target agent ID or alias (e.g., 'agent-advisor', 'agent-coder', \
                                'human')",
            })),
        });
    }
    let lines: Vec<String> = targets
        .iter()
        .map(|(agent_id, description)| match description.as_deref() {
            Some(description) if !description.is_empty() => {
                format!("  - {agent_id}: {description}")
            }
            _ => format!("  - {agent_id}"),
        })
        .collect();
    let ids: Vec<&str> = targets
        .iter()
        .map(|(agent_id, _)| agent_id.as_str())
        .collect();
    json!({
        "name": HANDOFF_TOOL_NAME,
        "description": format!(
            "Transfer this conversation to another agent.\nAvailable targets:\n{}",
            lines.join("\n")
        ),
        "parameters": parameters(json!({
            "type": "string",
            "enum": ids,
            "description": "Target agent ID to transfer to",
        })),
    })
}

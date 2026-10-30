//! Preserve the vendor's item order and message phases without bypassing the
//! engine's normalization and bound on tool calls (RFC §6.4, §6.7).

use super::wire::{malformed, message_text, validate_reasoning};
use crate::error::OpenAiError;
use roomkit::model::ToolCall;
use roomkit::transcript::{Message, Role};
use serde_json::{Value, json};

pub(super) const OUTPUT: &str = "openai.responses.output";

pub(super) fn append_message(input: &mut Vec<Value>, message: &Message) -> Result<(), OpenAiError> {
    match message.role {
        Role::Assistant => {
            if let Some(output) = message.metadata.get(OUTPUT) {
                replay(input, message, output)?;
            } else {
                if !message.text.is_empty() {
                    input.push(json!({"role":"assistant", "content": message.text}));
                }
                input.extend(message.tool_calls.iter().map(function_call));
            }
        }
        Role::Tool => {
            let id = message
                .tool_call_id
                .as_ref()
                .ok_or_else(|| malformed("a tool result has no call id"))?;
            input.push(json!({"type":"function_call_output", "call_id":id, "output":message.text}));
        }
        // Future roles stay ordinary input, never promoted to instructions.
        Role::User | _ => input.push(json!({"role":"user", "content":message.text})),
    }
    Ok(())
}

/// Replayed items cannot introduce instructions or calls the engine dropped.
/// Calls keep their original item ids, but use the engine's normalized call
/// ids and arguments. Text must still be the round's actual assistant text.
fn replay(input: &mut Vec<Value>, message: &Message, output: &Value) -> Result<(), OpenAiError> {
    let items = output
        .as_array()
        .ok_or_else(|| malformed("invalid Responses output metadata"))?;
    let mut text = String::new();
    let mut calls = message.tool_calls.iter();
    for item in items {
        match item.get("type").and_then(Value::as_str) {
            Some("reasoning") => {
                validate_reasoning(item)?;
                input.push(item.clone());
            }
            Some("message") => {
                message_text(item, &mut text)?;
                input.push(item.clone());
            }
            Some("function_call") => {
                // The engine can drop calls past its per-round limit.
                if let Some(call) = calls.next() {
                    if item.get("name").and_then(Value::as_str) != Some(call.name.as_str()) {
                        return Err(malformed("Responses replay changed a tool name"));
                    }
                    let mut replayed = function_call(call);
                    if let Some(id) = item.get("id").and_then(Value::as_str)
                        && let Some(fields) = replayed.as_object_mut()
                    {
                        fields.insert("id".into(), id.into());
                    }
                    input.push(replayed);
                }
            }
            _ => return Err(malformed("unsupported Responses replay item")),
        }
    }
    if text != message.text || calls.next().is_some() {
        return Err(malformed(
            "Responses replay does not match the assistant message",
        ));
    }
    Ok(())
}

fn function_call(call: &ToolCall) -> Value {
    json!({"type":"function_call", "call_id":call.id, "name":call.name, "arguments":Value::Object(call.arguments.clone()).to_string()})
}

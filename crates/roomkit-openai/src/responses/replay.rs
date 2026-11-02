//! Preserve the vendor's item order and message phases without bypassing the
//! engine's normalization and bound on tool calls (RFC §6.4, §6.7).

use super::wire::{malformed, message_text, validate_reasoning};
use crate::error::OpenAiError;
use roomkit::model::ToolCall;
use roomkit_core::rules::transcript::{Message, Part, Role};
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
        Role::User | _ => {
            let content = if !message.parts.is_empty() {
                Value::Array(message.parts.iter().filter_map(input_part).collect())
            } else if message.images.is_empty() {
                Value::String(message.text.clone())
            } else {
                let mut parts = vec![json!({"type":"input_text","text":message.text})];
                parts.extend(
                    message
                        .images
                        .iter()
                        .filter(|image| image.is_resolved())
                        .map(|image| json!({"type":"input_image","image_url":image.url()})),
                );
                Value::Array(parts)
            };
            input.push(json!({"role":"user", "content":content}));
        }
    }
    Ok(())
}

/// A part of a user's message as the Responses input writes it, in its
/// place: a text, or an image the provider can fetch.
fn input_part(part: &Part) -> Option<Value> {
    match part {
        Part::Text(text) => Some(json!({"type": "input_text", "text": text})),
        Part::Image(image) if image.is_resolved() => {
            Some(json!({"type": "input_image", "image_url": image.url()}))
        }
        // An image not resolved, or a part the transcript adds later.
        Part::Image(_) | _ => None,
    }
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

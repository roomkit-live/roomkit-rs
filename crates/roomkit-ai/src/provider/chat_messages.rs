//! The Chat Completions message format and its provider-selected dialect (RFC §6.7).

use roomkit_core::rules::transcript::{DataUriError, parse_data_uri, to_data_uri};
use serde_json::{Value, json};

use super::chat_dialect::ChatDialect;
use super::{
    AiContentPart, AiImagePart, AiMessage, AiMessageContent, AiRole, ProviderError,
    ProviderErrorOptions,
};
use crate::skills::dumps;

/// Render a conversation as Chat Completions messages, with its system prompt first.
///
/// # Errors
///
/// Returns a non-retryable error naming `provider` if an image in the messages
/// to send contains an invalid data URI.
pub fn chat_messages(
    messages: &[AiMessage],
    system_prompt: Option<&str>,
    dialect: &ChatDialect,
    provider: &str,
) -> Result<Vec<Value>, ProviderError> {
    let mut rows = Vec::new();
    if let Some(prompt) = system_prompt.filter(|text| !text.is_empty()) {
        rows.push(json!({"role": "system", "content": prompt}));
    }
    for message in messages {
        render(&mut rows, message, dialect).map_err(|error| {
            ProviderError::new(
                format!("invalid image part: {error}"),
                ProviderErrorOptions {
                    provider: provider.into(),
                    ..Default::default()
                },
            )
            .with_source(error)
        })?;
    }
    Ok(rows)
}

fn render(
    rows: &mut Vec<Value>,
    message: &AiMessage,
    dialect: &ChatDialect,
) -> Result<(), DataUriError> {
    let role = message.role.as_str();
    let parts = match &message.content {
        AiMessageContent::Text(text) => {
            if !text.is_empty() || !dialect.flattens_text() {
                rows.push(json!({"role": role, "content": text}));
            }
            return Ok(());
        }
        AiMessageContent::Parts(parts) => parts,
    };
    let calls = tool_calls(parts);
    if !calls.is_empty() {
        let content = if dialect.flattens_text() {
            flat(parts)?
        } else {
            Value::String(text(parts))
        };
        let content = if content.as_str() == Some("") {
            Value::Null
        } else {
            content
        };
        let mut row = json!({"role": "assistant", "content": content, "tool_calls": calls});
        attach_thinking(
            &mut row,
            message.role,
            parts,
            dialect,
            dialect.round_thinking_required(),
        );
        rows.push(row);
    } else if parts
        .iter()
        .any(|part| matches!(part, AiContentPart::ToolResult(_)))
    {
        append_results(rows, parts, dialect)?;
    } else if dialect.flattens_text() {
        let content = flat(parts)?;
        if content.as_str() != Some("") {
            rows.push(json!({"role": role, "content": content}));
        }
    } else {
        let blocks = blocks(parts)?;
        let content = if blocks.is_empty() && !parts.is_empty() {
            json!("")
        } else {
            Value::Array(blocks)
        };
        let mut row = json!({"role": role, "content": content});
        attach_thinking(&mut row, message.role, parts, dialect, false);
        rows.push(row);
    }
    Ok(())
}

fn tool_calls(parts: &[AiContentPart]) -> Vec<Value> {
    parts
        .iter()
        .filter_map(|part| {
            let AiContentPart::ToolCall(call) = part else {
                return None;
            };
            Some(json!({"id": call.id, "type": "function", "function": {
                "name": call.name, "arguments": dumps(&Value::Object(call.arguments.clone()))
            }}))
        })
        .collect()
}

fn append_results(
    rows: &mut Vec<Value>,
    parts: &[AiContentPart],
    dialect: &ChatDialect,
) -> Result<(), DataUriError> {
    let mut images = Vec::new();
    for part in parts {
        if let AiContentPart::ToolResult(result) = part {
            let (text, shown) = result.split_for_message();
            let mut row =
                json!({"role":"tool", "tool_call_id":result.tool_call_id, "content":text});
            if dialect.names_tool_results()
                && let Some(fields) = row.as_object_mut()
            {
                fields.insert("name".into(), json!(result.name));
            }
            rows.push(row);
            for part in shown {
                images.push(image(&part)?);
            }
        }
    }
    if !images.is_empty() {
        rows.push(json!({"role":"user", "content":images}));
    }
    Ok(())
}

fn text(parts: &[AiContentPart]) -> String {
    parts
        .iter()
        .filter_map(|p| {
            if let AiContentPart::Text(t) = p {
                Some(t.text.as_str())
            } else {
                None
            }
        })
        .collect()
}

fn attach_thinking(
    row: &mut Value,
    role: AiRole,
    parts: &[AiContentPart],
    dialect: &ChatDialect,
    required: bool,
) {
    let Some(field) = dialect
        .thinking_field()
        .filter(|_| role == AiRole::Assistant)
    else {
        return;
    };
    let thinking: String = parts
        .iter()
        .filter_map(|p| {
            if let AiContentPart::Thinking(t) = p {
                Some(t.thinking.as_str())
            } else {
                None
            }
        })
        .collect();
    if !thinking.is_empty() {
        row[field] = Value::String(thinking);
    } else if required {
        row[field] = json!(dialect.round_thinking_placeholder());
    }
}

fn image(part: &AiImagePart) -> Result<Value, DataUriError> {
    let url = if part.url.starts_with("data:") {
        let (kind, bytes) = parse_data_uri(&part.url, part.mime_type.as_deref())?;
        to_data_uri(&bytes, &kind)
    } else {
        part.url.clone()
    };
    Ok(json!({"type":"image_url", "image_url":{"url":url}}))
}

fn blocks(parts: &[AiContentPart]) -> Result<Vec<Value>, DataUriError> {
    parts
        .iter()
        .filter_map(|part| match part {
            AiContentPart::Text(text) => Some(Ok(json!({"type":"text", "text":text.text}))),
            AiContentPart::Image(part) => Some(image(part)),
            AiContentPart::ToolCall(_)
            | AiContentPart::ToolResult(_)
            | AiContentPart::Thinking(_) => None,
        })
        .collect()
}

fn flat(parts: &[AiContentPart]) -> Result<Value, DataUriError> {
    if parts
        .iter()
        .any(|part| matches!(part, AiContentPart::Image(_)))
    {
        Ok(Value::Array(blocks(parts)?))
    } else {
        Ok(Value::String(text(parts)))
    }
}

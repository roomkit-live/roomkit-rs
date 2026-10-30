use roomkit::model::Metadata;
use roomkit_ai::{AiRequest, Done, ProviderResponse, ProviderToolCall};
use serde_json::{Value, json};

use crate::config::OpenAiConfig;
use crate::error::OpenAiError;
use crate::wire::cleaned;

use super::replay::{OUTPUT, append_message};

pub(super) fn malformed(reason: &str) -> OpenAiError {
    OpenAiError::Malformed {
        reason: reason.to_owned(),
    }
}

/// Translate the public transcript; replay metadata preserves reasoning and
/// output layout without introducing instructions or additional tool calls.
pub(crate) fn request(config: &OpenAiConfig, request: &AiRequest) -> Result<Value, OpenAiError> {
    let mut input = Vec::new();
    for message in &request.messages {
        append_message(&mut input, message)?;
    }
    let tools: Vec<_> = request
        .tools
        .iter()
        .map(|tool| {
            json!({
                "type": "function", "name": tool.name, "description": tool.description,
                "parameters": tool.parameters, "strict": false,
            })
        })
        .collect();
    let tools = if let Some((namespace, description)) = &config.tool_namespace {
        if namespace.is_empty()
            || namespace.len() > 64
            || !namespace
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            return Err(malformed("invalid Responses tool namespace"));
        }
        for item in &mut input {
            if item.get("type").and_then(Value::as_str) == Some("function_call") {
                item["namespace"] = namespace.clone().into();
            }
        }
        if tools.is_empty() {
            Vec::new()
        } else {
            vec![
                json!({"type":"namespace", "name":namespace, "description":description, "tools":tools}),
            ]
        }
    } else {
        tools
    };
    let mut body = json!({ "model": config.model, "input": input, "stream": true, "store": false,
        "include": ["reasoning.encrypted_content"], "tools": tools });
    if let Some(fields) = body.as_object_mut() {
        if let Some(prompt) = &request.system_prompt {
            fields.insert("instructions".into(), prompt.clone().into());
        }
        if let Some(effort) = &config.reasoning_effort {
            fields.insert("reasoning".into(), json!({ "effort": effort }));
        }
    }
    Ok(body)
}

pub(super) fn validate_reasoning(item: &Value) -> Result<(), OpenAiError> {
    if item.get("type").and_then(Value::as_str) != Some("reasoning")
        || item
            .get("encrypted_content")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
    {
        return Err(malformed(
            "stateless reasoning replay requires an encrypted reasoning item",
        ));
    }
    Ok(())
}

pub(super) fn failed(value: &Value, secret: &str) -> OpenAiError {
    let error = value.get("error").filter(|v| !v.is_null()).unwrap_or(value);
    let code = error
        .get("code")
        .and_then(Value::as_str)
        .unwrap_or("provider_error");
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("Responses generation failed");
    OpenAiError::StreamFailed {
        message: cleaned(&format!("{code}: {message}"), secret, 500),
    }
}

pub(crate) fn body(
    body: &[u8],
    secret: &str,
    namespace: Option<&str>,
) -> Result<ProviderResponse, OpenAiError> {
    let value = serde_json::from_slice(body).map_err(|_| malformed("invalid Responses JSON"))?;
    validate_namespace(&value, namespace)?;
    response(&value, secret)
}

/// Only a terminal response can release tool calls to the engine.
pub(super) fn response(value: &Value, secret: &str) -> Result<ProviderResponse, OpenAiError> {
    let status = value
        .get("status")
        .and_then(Value::as_str)
        .ok_or_else(|| malformed("Responses answer has no status"))?;
    if matches!(status, "failed" | "cancelled") {
        return Err(failed(value, secret));
    }
    if !matches!(status, "completed" | "incomplete") {
        return Err(malformed("Responses answer is not terminal"));
    }
    let output = value
        .get("output")
        .and_then(Value::as_array)
        .ok_or_else(|| malformed("Responses answer has no output array"))?;
    let mut text = String::new();
    let mut calls = Vec::new();
    let mut replay = Vec::new();
    for item in output {
        match item.get("type").and_then(Value::as_str) {
            Some("message") => message_text(item, &mut text)?,
            Some("function_call") => {
                let name = item
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or_else(|| malformed("function call has no name"))?;
                let id = item
                    .get("call_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let arguments = match item.get("arguments") {
                    Some(Value::String(text)) => text.clone(),
                    None | Some(Value::Null) => String::new(),
                    Some(value) => value.to_string(),
                };
                calls.push(ProviderToolCall::new(id, name, arguments));
            }
            Some("reasoning") => {
                // A cut reasoning item is not replayable. A complete item needs
                // its opaque payload; an id alone cannot be resolved with store=false.
                if status == "incomplete"
                    && item.get("encrypted_content").is_none_or(Value::is_null)
                {
                    continue;
                }
                validate_reasoning(item)?;
            }
            _ => return Err(malformed("unsupported Responses output item")),
        }
        replay.push(item.clone());
    }
    let finish_reason = if status == "incomplete" {
        match value
            .pointer("/incomplete_details/reason")
            .and_then(Value::as_str)
        {
            Some("max_output_tokens") => "max_tokens",
            Some(reason) => reason,
            None => "length",
        }
    } else if calls.is_empty() {
        "stop"
    } else {
        "tool_calls"
    };
    let mut done = Done::new(Some(finish_reason.to_owned()), usage(value.get("usage")));
    if let Some(model) = value.get("model").and_then(Value::as_str) {
        done.metadata.insert("model".into(), model.into());
    }
    done.metadata.insert(OUTPUT.into(), replay.into());
    Ok(ProviderResponse::new(text, calls, done))
}

pub(super) fn message_text(message: &Value, text: &mut String) -> Result<(), OpenAiError> {
    if message.get("role").and_then(Value::as_str) != Some("assistant") {
        return Err(malformed("Responses output message has an invalid role"));
    }
    let parts = message
        .get("content")
        .and_then(Value::as_array)
        .ok_or_else(|| malformed("Responses output message has no content array"))?;
    for part in parts {
        let field = match part.get("type").and_then(Value::as_str) {
            Some("output_text") => "text",
            Some("refusal") => "refusal",
            _ => return Err(malformed("unsupported Responses output content")),
        };
        text.push_str(
            part.get(field)
                .and_then(Value::as_str)
                .ok_or_else(|| malformed("Responses text part has no text"))?,
        );
    }
    Ok(())
}

fn usage(value: Option<&Value>) -> Metadata {
    let mut counted = Metadata::new();
    let Some(value) = value else {
        return counted;
    };
    let cached = value
        .pointer("/input_tokens_details/cached_tokens")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    if let Some(input) = value.get("input_tokens").and_then(Value::as_u64) {
        counted.insert("input_tokens".into(), input.saturating_sub(cached).into());
        if cached > 0 {
            counted.insert("cache_read_input_tokens".into(), cached.min(input).into());
        }
    }
    if let Some(output) = value.get("output_tokens").and_then(Value::as_u64) {
        counted.insert("output_tokens".into(), output.into());
    }
    if let Some(reasoning) = value
        .pointer("/output_tokens_details/reasoning_tokens")
        .and_then(Value::as_u64)
    {
        counted.insert("reasoning_tokens".into(), reasoning.into());
    }
    counted
}

/// Namespace mismatches fail before a terminal snapshot releases any calls.
pub(super) fn validate_namespace(value: &Value, expected: Option<&str>) -> Result<(), OpenAiError> {
    if let Some(items) = value.get("output").and_then(Value::as_array) {
        for item in items {
            if item.get("type").and_then(Value::as_str) == Some("function_call")
                && (item
                    .get("namespace")
                    .is_some_and(|v| !v.is_null() && !v.is_string())
                    || item
                        .get("namespace")
                        .and_then(Value::as_str)
                        .is_some_and(|namespace| Some(namespace) != expected))
            {
                return Err(malformed(
                    "Responses function call has an unexpected namespace",
                ));
            }
        }
    }
    Ok(())
}

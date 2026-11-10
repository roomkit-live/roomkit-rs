//! Native Messages content blocks and prompt caching (RFC §§6.4, 6.7).
use crate::{AnthropicConfig, error, models, reasoning};
use roomkit_ai::{
    AiContentPart, AiContext, AiImagePart, AiMessage, AiMessageContent, AiRole, AiToolResultPart,
    ProviderError, ToolResult, ToolResultPart,
};
use roomkit_core::rules::transcript::{fence, identifier, parse_data_uri, to_data_uri};
use serde_json::{Map, Value, json};

pub(crate) fn body(config: &AnthropicConfig, context: &AiContext) -> Result<Value, ProviderError> {
    let mut fields = Map::new();
    fields.insert("model".into(), config.model.clone().into());
    fields.insert(
        "max_tokens".into(),
        context
            .max_tokens
            .filter(|n| *n > 0)
            .unwrap_or(config.max_tokens)
            .into(),
    );
    fields.insert("stream".into(), true.into());
    fields.insert("messages".into(), messages(&context.messages)?);
    if let Some(prompt) = context
        .system_prompt
        .as_ref()
        .filter(|prompt| !prompt.is_empty())
    {
        fields.insert("system".into(), prompt.clone().into());
    }
    let model = models::entry(&config.model);
    fields.extend(reasoning::sampling(config, context, model));
    if !context.tools.is_empty() {
        fields.insert("tools".into(), tools(config, context)?);
    }
    let mut output = Map::new();
    if let Some(effort) = reasoning::effort(context, model) {
        output.insert("effort".into(), effort.into());
    }
    if let Some(schema) = &context.response_schema {
        output.insert(
            "format".into(),
            json!({"type":"json_schema", "schema":schema}),
        );
    }
    if !output.is_empty() {
        fields.insert("output_config".into(), output.into());
    }
    let mut body = Value::Object(fields);
    if config.enable_prompt_caching {
        cache(&mut body);
    }
    Ok(body)
}

pub(crate) fn messages(messages: &[AiMessage]) -> Result<Value, ProviderError> {
    messages
        .iter()
        .map(|message| {
            let role = match message.role {
                AiRole::Assistant => "assistant",
                AiRole::User | AiRole::System | AiRole::Tool => "user",
            };
            Ok(json!({"role":role, "content":content(&message.content)?}))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Value::Array)
}

fn content(content: &AiMessageContent) -> Result<Value, ProviderError> {
    let parts = match content {
        AiMessageContent::Text(text) => return Ok(text.clone().into()),
        AiMessageContent::Parts(parts) => parts,
    };
    let mut blocks = Vec::new();
    let mut trailing = Vec::new();
    for part in parts {
        match part {
            AiContentPart::Text(part) => blocks.push(json!({"type":"text", "text":part.text})),
            AiContentPart::Image(part) => blocks.push(image(part)?),
            AiContentPart::ToolCall(part) => blocks.push(
                json!({"type":"tool_use", "id":part.id, "name":part.name, "input":part.arguments}),
            ),
            AiContentPart::ToolResult(part) => {
                let mut block = json!({"type":"tool_result", "tool_use_id":part.tool_call_id, "content":if part.references.is_empty() { result(part)? } else { json!(part.references.iter().map(|name| json!({"type":"tool_reference", "tool_name":name})).collect::<Vec<_>>()) }});
                if part.is_error {
                    put(&mut block, "is_error", true.into());
                }
                blocks.push(block);
                trailing.extend(beside_references(part)?);
            }
            AiContentPart::Thinking(part) => {
                if let Some(data) = &part.redacted {
                    blocks.push(json!({"type":"redacted_thinking", "data":data}));
                } else if let Some(signature) =
                    part.signature.as_ref().filter(|value| !value.is_empty())
                {
                    blocks.push(
                        json!({"type":"thinking", "thinking":part.thinking, "signature":signature}),
                    );
                }
            }
        }
    }
    blocks.extend(trailing);
    Ok(blocks.into())
}

fn beside_references(part: &AiToolResultPart) -> Result<Vec<Value>, ProviderError> {
    if part.references.is_empty() {
        return Ok(Vec::new());
    }
    let label = format!("[Result of {}]", identifier(&part.name, "tool"));
    match result(part)? {
        Value::String(text) if !text.is_empty() => Ok(vec![
            json!({"type":"text", "text":format!("{label}\n{}", fence("tool_result", &text))}),
        ]),
        Value::Array(mut blocks) if !blocks.is_empty() => {
            for block in &mut blocks {
                if let Some(text) = block.get("text").and_then(Value::as_str) {
                    let fenced = fence("tool_result", text);
                    put(block, "text", fenced.into());
                }
            }
            blocks.insert(0, json!({"type":"text", "text":label}));
            Ok(blocks)
        }
        Value::Null
        | Value::Bool(_)
        | Value::Number(_)
        | Value::String(_)
        | Value::Array(_)
        | Value::Object(_) => Ok(Vec::new()),
    }
}

fn image(part: &AiImagePart) -> Result<Value, ProviderError> {
    if !part.url.starts_with("data:") {
        return Ok(json!({"type":"image", "source":{"type":"url", "url":part.url}}));
    }
    let (kind, bytes) = parse_data_uri(&part.url, part.mime_type.as_deref())
        .map_err(|cause| error::failed(format!("invalid image part: {cause}")))?;
    let canonical = to_data_uri(&bytes, &kind);
    let (_, payload) = canonical
        .split_once(',')
        .ok_or_else(|| error::failed("invalid image payload"))?;
    Ok(json!({"type":"image", "source":{"type":"base64", "media_type":kind, "data":payload}}))
}

fn result(part: &AiToolResultPart) -> Result<Value, ProviderError> {
    match &part.result {
        ToolResult::Text(text) => Ok(text.clone().into()),
        ToolResult::Json(_) => Ok(part.as_text().into()),
        ToolResult::Parts(parts) => parts
            .iter()
            .map(|part| match part {
                ToolResultPart::Text(part) => Ok(json!({"type":"text", "text":part.text})),
                ToolResultPart::Image(part) => image(part),
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
    }
}

fn tools(config: &AnthropicConfig, context: &AiContext) -> Result<Value, ProviderError> {
    let defers = context.tools.iter().any(|tool| !tool.defer_loading);
    context
        .tools
        .iter()
        .map(|tool| {
            if config.official()
                && (tool.name.is_empty()
                    || tool.name.len() > 128
                    || !tool
                        .name
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-'))
            {
                let mut refused = error::failed(format!(
                    "tool '{}': anthropic accepts tool names matching [A-Za-z0-9_-]{{1,128}}",
                    tool.name
                ));
                refused.before_request = true;
                return Err(refused);
            }
            let mut parameters = tool.parameters.clone();
            if parameters.as_object().is_some_and(Map::is_empty) {
                parameters = json!({"type":"object", "properties":{}});
            }
            if let Some(fields) = parameters.as_object_mut()
                && fields.get("type").is_none_or(Value::is_null)
            {
                fields.insert("type".into(), "object".into());
            }
            let mut declared = json!({"name":tool.name, "description":tool.description, "input_schema":parameters});
            if defers && tool.defer_loading { put(&mut declared, "defer_loading", true.into()); }
            Ok(declared)
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Value::Array)
}

fn cache(body: &mut Value) {
    let marker = json!({"type":"ephemeral"});
    if let Some(last) = body
        .get_mut("tools")
        .and_then(Value::as_array_mut)
        .and_then(|tools| {
            tools
                .iter_mut()
                .rev()
                .find(|tool| tool.get("defer_loading") != Some(&Value::Bool(true)))
        })
    {
        put(last, "cache_control", marker.clone());
    }
    if let Some(system) = body.get("system").and_then(Value::as_str) {
        let system = json!([{"type":"text", "text":system, "cache_control":marker}]);
        put(body, "system", system);
    }
    let Some(messages) = body.get_mut("messages").and_then(Value::as_array_mut) else {
        return;
    };
    let mut count = 0;
    for message in messages.iter_mut().rev() {
        if count == 2 {
            break;
        }
        let Some(content) = message.get_mut("content") else {
            continue;
        };
        if mark_content(content, &marker) {
            count += 1;
        }
    }
}

fn mark_content(content: &mut Value, marker: &Value) -> bool {
    if let Some(text) = content.as_str().filter(|text| !text.is_empty()) {
        *content = json!([{"type":"text", "text":text, "cache_control":marker}]);
        return true;
    }
    let Some(blocks) = content.as_array_mut() else {
        return false;
    };
    if let Some(block) = blocks.iter_mut().rev().find(|block| {
        matches!(
            block.get("type").and_then(Value::as_str),
            Some("text" | "tool_result" | "tool_use" | "image")
        )
    }) {
        put(block, "cache_control", marker.clone());
        return true;
    }
    false
}

fn put(object: &mut Value, key: &str, value: Value) {
    if let Some(fields) = object.as_object_mut() {
        fields.insert(key.to_owned(), value);
    }
}

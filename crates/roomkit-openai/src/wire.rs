//! The Chat Completions wire: the request a turn sends, and what it reads
//! of the answer.

use reqwest::StatusCode;
use roomkit::model::Metadata;
use roomkit::transcript::{Message, Role};
use roomkit_ai::{AiRequest, Done, ProviderResponse, ProviderToolCall};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::OpenAiError;

/// The longest provider message an error keeps, in characters.
const MESSAGE_LIMIT: usize = 500;

/// The longest error type or code an error keeps, in characters.
const NAME_LIMIT: usize = 64;

/// A Chat Completions request: one completion, not streamed, declaring the
/// round's tools when it has any, and neither `tool_choice` nor
/// `parallel_tool_calls`.
#[derive(Debug, Serialize)]
pub(crate) struct CompletionRequest<'a> {
    model: &'a str,
    messages: Vec<WireMessage<'a>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tools: Vec<WireTool<'a>>,
}

/// One message of a request.
#[derive(Debug, Serialize)]
struct WireMessage<'a> {
    role: &'static str,
    /// The text; `null` for an assistant's message that only calls.
    content: Option<&'a str>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tool_calls: Vec<WireCall<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_call_id: Option<&'a str>,
}

/// A call an assistant's message made, as the wire writes it: its
/// arguments as JSON text.
#[derive(Debug, Serialize)]
struct WireCall<'a> {
    id: &'a str,
    #[serde(rename = "type")]
    kind: &'static str,
    function: WireFunction<'a>,
}

#[derive(Debug, Serialize)]
struct WireFunction<'a> {
    name: &'a str,
    arguments: String,
}

/// A tool a request declares.
#[derive(Debug, Serialize)]
struct WireTool<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    function: WireToolFunction<'a>,
}

#[derive(Debug, Serialize)]
struct WireToolFunction<'a> {
    name: &'a str,
    description: &'a str,
    parameters: &'a Value,
}

impl<'a> CompletionRequest<'a> {
    /// The request asking `model` to answer `request`: its system prompt
    /// when it has one, its conversation, its tools.
    pub(crate) fn new(model: &'a str, request: &'a AiRequest) -> Self {
        let system = request
            .system_prompt
            .as_deref()
            .filter(|prompt| !prompt.is_empty())
            .map(|prompt| WireMessage {
                role: "system",
                content: Some(prompt),
                tool_calls: Vec::new(),
                tool_call_id: None,
            });
        let messages = system
            .into_iter()
            .chain(request.messages.iter().map(wire_message))
            .collect();
        let tools = request
            .tools
            .iter()
            .map(|tool| WireTool {
                kind: "function",
                function: WireToolFunction {
                    name: &tool.name,
                    description: &tool.description,
                    parameters: &tool.parameters,
                },
            })
            .collect();
        Self {
            model,
            messages,
            tools,
        }
    }
}

/// `message` as the wire writes it.
fn wire_message(message: &Message) -> WireMessage<'_> {
    let calls: Vec<WireCall<'_>> = message
        .tool_calls
        .iter()
        .map(|call| WireCall {
            id: &call.id,
            kind: "function",
            function: WireFunction {
                name: &call.name,
                arguments: Value::Object(call.arguments.clone()).to_string(),
            },
        })
        .collect();
    // An assistant's message that only calls has no text.
    let content = Some(message.text.as_str()).filter(|text| calls.is_empty() || !text.is_empty());
    WireMessage {
        role: role(message.role),
        content,
        tool_calls: calls,
        tool_call_id: message.tool_call_id.as_deref(),
    }
}

/// The wire's name of `role`.
fn role(role: Role) -> &'static str {
    match role {
        Role::Assistant => "assistant",
        Role::Tool => "tool",
        // A role this agent does not know yet reads as a user's: never as
        // the model's own words, nor as the system's.
        Role::User | _ => "user",
    }
}

/// A chat completion, as far as a turn reads it. An OpenAI-compatible
/// server may write `null` for any member it leaves out: each reads as
/// absent ([`nullable`]).
#[derive(Debug, Deserialize)]
struct Completion {
    #[serde(default, deserialize_with = "nullable")]
    choices: Vec<Choice>,
    #[serde(default)]
    usage: Option<Usage>,
}

/// One choice of a completion.
#[derive(Debug, Deserialize)]
struct Choice {
    message: AnswerMessage,
    #[serde(default)]
    finish_reason: Option<String>,
}

/// The message a choice answers with.
#[derive(Debug, Deserialize)]
struct AnswerMessage {
    #[serde(default)]
    content: Option<String>,
    #[serde(default, deserialize_with = "nullable")]
    tool_calls: Vec<AnswerCall>,
}

/// A call the answer makes.
#[derive(Debug, Deserialize)]
struct AnswerCall {
    /// Empty when the server gave none: the loop makes one.
    #[serde(default, deserialize_with = "nullable")]
    id: String,
    function: AnswerFunction,
}

#[derive(Debug, Deserialize)]
struct AnswerFunction {
    #[serde(default, deserialize_with = "nullable")]
    name: String,
    /// The arguments' JSON text; some servers send an object instead.
    #[serde(default)]
    arguments: Value,
}

/// What a completion used.
#[derive(Debug, Deserialize)]
struct Usage {
    #[serde(default, deserialize_with = "nullable")]
    prompt_tokens: i64,
    #[serde(default, deserialize_with = "nullable")]
    completion_tokens: i64,
    #[serde(default)]
    prompt_tokens_details: Option<PromptDetails>,
}

#[derive(Debug, Deserialize)]
struct PromptDetails {
    #[serde(default, deserialize_with = "nullable")]
    cached_tokens: i64,
}

/// A member that reads `null` as absent: its type's default, a count of 0,
/// an empty text or list, as the reference implementation reads it.
fn nullable<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

/// What a completion's first choice answers: its text, empty when it has
/// none (an empty answer, or a refusal the provider gives apart), its
/// calls, why it stopped, and what it used, as the turn's record counts it
/// (`input_tokens` without the cache's, `output_tokens`,
/// `cache_read_input_tokens`).
///
/// # Errors
///
/// Returns [`OpenAiError::Malformed`] when `body` is no chat completion,
/// saying why without `secret`, the API key.
pub(crate) fn response(body: &[u8], secret: &str) -> Result<ProviderResponse, OpenAiError> {
    let malformed = |reason: String| OpenAiError::Malformed {
        reason: cleaned(&reason, secret, MESSAGE_LIMIT),
    };
    let completion: Completion =
        serde_json::from_slice(body).map_err(|error| malformed(error.to_string()))?;
    let usage = completion.usage.map(usage_of).unwrap_or_default();
    let choice = completion
        .choices
        .into_iter()
        .next()
        .ok_or_else(|| malformed("it has no choice".to_owned()))?;
    let text = choice
        .message
        .content
        .filter(|text| !text.trim().is_empty())
        .unwrap_or_default();
    let calls = choice
        .message
        .tool_calls
        .into_iter()
        .map(|call| {
            let arguments = match call.function.arguments {
                Value::String(text) => text,
                Value::Null => String::new(),
                other
                @ (Value::Bool(_) | Value::Number(_) | Value::Array(_) | Value::Object(_)) => {
                    other.to_string()
                }
            };
            ProviderToolCall::new(call.id, call.function.name, arguments)
        })
        .collect();
    Ok(ProviderResponse::new(
        text,
        calls,
        Done::new(choice.finish_reason, usage),
    ))
}

/// A completion's usage as the turn's record counts it.
fn usage_of(usage: Usage) -> Metadata {
    let cached = usage
        .prompt_tokens_details
        .map(|details| details.cached_tokens)
        .unwrap_or_default();
    let mut counted = Metadata::new();
    counted.insert(
        "input_tokens".to_owned(),
        Value::from(usage.prompt_tokens.saturating_sub(cached).max(0)),
    );
    counted.insert(
        "output_tokens".to_owned(),
        Value::from(usage.completion_tokens),
    );
    if cached > 0 {
        counted.insert("cache_read_input_tokens".to_owned(), Value::from(cached));
    }
    counted
}

/// The error an answer of HTTP `status` holding `body` fails a turn with:
/// the provider's type, code and message, as OpenAI writes them under
/// `error`, or vLLM at the top level, else the status's reason. Each is on
/// one line, bounded, and holds no `secret`, the API key: a provider may
/// echo the request. The rest of the answer is left out.
pub(crate) fn status_error(status: StatusCode, body: &[u8], secret: &str) -> OpenAiError {
    let parsed: Option<Value> = serde_json::from_slice(body).ok();
    let field = |name: &str| {
        let value = parsed.as_ref()?;
        let error = value.get("error").filter(|error| error.is_object());
        let found = error
            .and_then(|error| error.get(name))
            .or_else(|| value.get(name));
        match found? {
            Value::String(text) => Some(text.clone()),
            Value::Number(number) => Some(number.to_string()),
            Value::Null | Value::Bool(_) | Value::Array(_) | Value::Object(_) => None,
        }
    };
    let message = field("message")
        .or_else(|| {
            let value = parsed.as_ref()?;
            value.get("error")?.as_str().map(str::to_owned)
        })
        .unwrap_or_else(|| status.canonical_reason().unwrap_or("no message").to_owned());
    let named = |text: String| Some(cleaned(&text, secret, NAME_LIMIT)).filter(|t| !t.is_empty());
    OpenAiError::Status {
        status: status.as_u16(),
        error_type: field("type").and_then(named),
        code: field("code").and_then(named),
        message: cleaned(&message, secret, MESSAGE_LIMIT),
    }
}

/// `text` as an error carries it: `secret` removed wherever it appears,
/// on one line, within `limit` characters.
fn cleaned(text: &str, secret: &str, limit: usize) -> String {
    let removed = if secret.is_empty() {
        text.to_owned()
    } else {
        text.replace(secret, "[removed]")
    };
    removed
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(limit)
        .collect()
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn an_error_answer_gives_the_provider_s_type_code_and_message() {
        let openai = br#"{"error": {"message": "Incorrect API key", "type": "invalid_request_error", "code": "invalid_api_key"}}"#;
        let vllm = br#"{"object": "error", "message": "The model does not exist.", "type": "NotFoundError", "code": 404}"#;
        assert_eq!(
            status_error(StatusCode::UNAUTHORIZED, openai, "sk-1").to_string(),
            "the provider answered HTTP 401 (invalid_request_error, invalid_api_key): Incorrect API key"
        );
        assert_eq!(
            status_error(StatusCode::NOT_FOUND, vllm, "sk-1").to_string(),
            "the provider answered HTTP 404 (NotFoundError, 404): The model does not exist."
        );
    }

    #[test]
    fn an_error_answer_that_is_no_json_gives_the_status_s_reason_only() {
        let error = status_error(StatusCode::BAD_GATEWAY, b"<html>echo: sk-1</html>", "sk-1");
        assert_eq!(
            error.to_string(),
            "the provider answered HTTP 502: Bad Gateway"
        );
    }

    #[test]
    fn a_completion_without_text_says_nothing() {
        let body = br#"{"choices": [{"message": {"role": "assistant", "content": null}}]}"#;
        assert_eq!(
            response(body, "sk-1").map(|read| read.text),
            Ok(String::new())
        );
    }

    #[test]
    fn a_completion_s_calls_keep_their_argument_text() {
        let body = br#"{"choices": [{"finish_reason": "tool_calls", "message": {"content": null,
            "tool_calls": [{"id": "call_1", "type": "function",
                "function": {"name": "lookup", "arguments": "{\"order\": \"42\"}"}}]}}],
            "usage": {"prompt_tokens": 100, "completion_tokens": 7,
                "prompt_tokens_details": {"cached_tokens": 40}}}"#;
        let read = response(body, "sk-1").unwrap();
        assert_eq!(
            (
                read.calls,
                read.done.finish_reason,
                Value::Object(read.done.usage)
            ),
            (
                vec![ProviderToolCall::new(
                    "call_1",
                    "lookup",
                    r#"{"order": "42"}"#
                )],
                Some("tool_calls".to_owned()),
                serde_json::json!({
                    "input_tokens": 60,
                    "output_tokens": 7,
                    "cache_read_input_tokens": 40
                })
            )
        );
    }

    /// An OpenAI-compatible server may write `null` for any member it
    /// leaves out: each reads as absent, a count as 0, a call's id made by
    /// the loop.
    #[test]
    fn a_completion_s_nulls_read_as_absent() {
        let body = br#"{"id": null, "choices": [{"index": 0, "finish_reason": null,
            "message": {"role": "assistant", "content": null, "refusal": null,
                "tool_calls": [{"id": null, "type": "function",
                    "function": {"name": "lookup", "arguments": null}}]}}],
            "usage": {"prompt_tokens": null, "completion_tokens": 7, "total_tokens": null,
                "prompt_tokens_details": {"cached_tokens": null}}}"#;
        let read = response(body, "sk-1").unwrap();
        assert_eq!(
            (read.calls, Value::Object(read.done.usage)),
            (
                vec![ProviderToolCall::new("", "lookup", "")],
                serde_json::json!({ "input_tokens": 0, "output_tokens": 7 })
            )
        );
    }

    #[test]
    fn a_message_with_null_calls_says_its_text() {
        let body = br#"{"choices": [{"message": {"content": "Hi", "tool_calls": null}}],
            "usage": null}"#;
        let read = response(body, "sk-1").unwrap();
        assert_eq!((read.text.as_str(), read.calls.len()), ("Hi", 0));
    }

    #[test]
    fn a_completion_with_no_choice_is_malformed() {
        assert_eq!(
            response(br#"{"id": "chatcmpl-1", "choices": []}"#, "sk-1"),
            Err(OpenAiError::Malformed {
                reason: "it has no choice".to_owned()
            })
        );
    }

    #[test]
    fn a_malformed_answer_s_reason_holds_no_key() {
        let body = br#"{"choices": "sk-1"}"#;
        let error = response(body, "sk-1").unwrap_err();
        assert!(!error.to_string().contains("sk-1"), "{error}");
    }
}

//! The Chat Completions wire: the request a turn sends, and what it reads
//! of the answer.

use crate::config::OpenAiConfig;
use crate::request::{RawCall, Request, Whole, done as finished};
use reqwest::StatusCode;
use roomkit::model::Metadata;
use roomkit_ai::StreamDone;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::OpenAiError;
use roomkit_core::rules::transcript::{Message, Role};

/// The longest provider message an error keeps, in characters.
const MESSAGE_LIMIT: usize = 500;

/// The longest error type or code an error keeps, in characters.
const NAME_LIMIT: usize = 64;

/// A Chat Completions request: one completion, streamed as server-sent
/// events with its usage in the last chunk (`stream_options.include_usage`),
/// declaring the round's tools when it has any, and neither `tool_choice`
/// nor `parallel_tool_calls`.
#[derive(Debug, Serialize)]
pub(crate) struct CompletionRequest<'a> {
    model: &'a str,
    messages: Vec<WireMessage<'a>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tools: Vec<WireTool<'a>>,
    stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream_options: Option<StreamOptions>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_completion_tokens: Option<u32>,
}

/// What a streamed completion sends beside its deltas.
#[derive(Debug, Serialize)]
struct StreamOptions {
    include_usage: bool,
}

/// One message of a request.
#[derive(Debug, Serialize)]
struct WireMessage<'a> {
    role: &'static str,
    /// The text; `null` for an assistant's message that only calls.
    content: Option<Value>,
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
    /// The request asking `config`'s model to answer `request`: its system
    /// prompt when it has one, its conversation, its tools, and Python's
    /// sampling keywords: the turn's temperature unless the model takes
    /// none, the turn's token bound over the configured one, under
    /// `max_completion_tokens` when the configuration says, the stream's
    /// usage when it asks for it.
    pub(crate) fn new(config: &'a OpenAiConfig, request: &'a Request) -> Self {
        let model = config.model.as_str();
        let system = request
            .system_prompt
            .as_deref()
            .filter(|prompt| !prompt.is_empty())
            .map(|prompt| WireMessage {
                role: "system",
                content: Some(Value::String(prompt.into())),
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
        let bound = request.max_tokens.unwrap_or(config.max_tokens);
        let (max_tokens, max_completion_tokens) = if config.use_max_completion_tokens {
            (None, Some(bound))
        } else {
            (Some(bound), None)
        };
        Self {
            model,
            messages,
            tools,
            stream: true,
            stream_options: config.include_stream_usage.then_some(StreamOptions {
                include_usage: true,
            }),
            temperature: request
                .temperature
                .filter(|_| config.supports_custom_temperature),
            max_tokens,
            max_completion_tokens,
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
    let content = if message.images.is_empty() || message.role != Role::User {
        Some(message.text.as_str())
            .filter(|text| calls.is_empty() || !text.is_empty())
            .map(|text| Value::String(text.into()))
    } else {
        let mut parts = vec![serde_json::json!({"type":"text","text":message.text})];
        parts.extend(
            message
                .images
                .iter()
                .filter(|image| image.is_resolved())
                .map(
                    |image| serde_json::json!({"type":"image_url","image_url":{"url":image.url()}}),
                ),
        );
        Some(Value::Array(parts))
    };
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
    #[serde(default)]
    prompt_tokens: Option<u64>,
    #[serde(default)]
    completion_tokens: Option<u64>,
    #[serde(default)]
    prompt_tokens_details: Option<PromptDetails>,
}

#[derive(Debug, Deserialize)]
struct PromptDetails {
    #[serde(default, deserialize_with = "nullable")]
    cached_tokens: u64,
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
pub(crate) fn response(body: &[u8], secret: &str) -> Result<Whole, OpenAiError> {
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
            RawCall::new(call.id, call.function.name, arguments)
        })
        .collect();
    Ok(Whole::new(
        text,
        calls,
        finished(choice.finish_reason, usage),
    ))
}

/// One chunk of a streamed completion, as far as a turn reads it: its first
/// choice's delta and end, and, in the last chunk, the completion's usage.
#[derive(Debug, Deserialize)]
struct Chunk {
    #[serde(default, deserialize_with = "nullable")]
    choices: Vec<ChunkChoice>,
    #[serde(default)]
    usage: Option<Usage>,
    /// What a server that fails mid-stream sends instead.
    #[serde(default)]
    error: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct ChunkChoice {
    #[serde(default, deserialize_with = "nullable")]
    index: u64,
    #[serde(default)]
    delta: Option<Delta>,
    #[serde(default)]
    finish_reason: Option<String>,
}

/// What one chunk adds to the answer.
#[derive(Debug, Deserialize)]
struct Delta {
    #[serde(default)]
    content: Option<String>,
    #[serde(default, deserialize_with = "nullable")]
    tool_calls: Vec<DeltaCall>,
}

/// A fragment of a call, which the call's `index` names across chunks.
#[derive(Debug, Deserialize)]
struct DeltaCall {
    #[serde(default, deserialize_with = "nullable")]
    index: u64,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    function: Option<DeltaFunction>,
}

#[derive(Debug, Deserialize)]
struct DeltaFunction {
    #[serde(default)]
    name: Option<String>,
    /// A fragment of the arguments' JSON text; some servers send an object.
    #[serde(default)]
    arguments: Value,
}

/// A call as its fragments built it so far.
#[derive(Debug, Default)]
struct Folded {
    id: String,
    name: String,
    arguments: String,
}

/// A streamed completion, folded as its chunks come: the calls' fragments
/// by their index, why it stopped, what it used (RFC §6.4 "What a provider
/// hands the loop for a call").
#[derive(Debug, Default)]
pub(crate) struct Folding {
    calls: std::collections::BTreeMap<u64, Folded>,
    finish_reason: Option<String>,
    usage: Metadata,
}

impl Folding {
    /// Takes the chunk `data`: the text it adds, if any.
    ///
    /// # Errors
    ///
    /// [`OpenAiError::Malformed`] when `data` is no chunk,
    /// [`OpenAiError::StreamFailed`] when it reports the provider's error,
    /// each without `secret`, the API key.
    pub(crate) fn take(&mut self, data: &str, secret: &str) -> Result<Option<String>, OpenAiError> {
        let chunk: Chunk = serde_json::from_str(data).map_err(|error| OpenAiError::Malformed {
            reason: cleaned(&error.to_string(), secret, MESSAGE_LIMIT),
        })?;
        if let Some(error) = chunk.error.filter(|error| !error.is_null()) {
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .map_or_else(|| error.to_string(), str::to_owned);
            return Err(OpenAiError::StreamFailed {
                message: cleaned(&message, secret, MESSAGE_LIMIT),
            });
        }
        if let Some(usage) = chunk.usage {
            self.usage = usage_of(usage);
        }
        let mut text = String::new();
        for choice in chunk.choices.into_iter().filter(|choice| choice.index == 0) {
            if let Some(reason) = choice.finish_reason {
                self.finish_reason = Some(reason);
            }
            let Some(delta) = choice.delta else {
                continue;
            };
            text.push_str(delta.content.as_deref().unwrap_or_default());
            for call in delta.tool_calls {
                self.fold(call);
            }
        }
        Ok(Some(text).filter(|text| !text.is_empty()))
    }

    /// Adds a call's fragment to the call its index names: its id and name
    /// once given, its arguments' text appended.
    fn fold(&mut self, fragment: DeltaCall) {
        let folded = self.calls.entry(fragment.index).or_default();
        if let Some(id) = fragment.id.filter(|id| !id.is_empty()) {
            folded.id = id;
        }
        let Some(function) = fragment.function else {
            return;
        };
        if let Some(name) = function.name.filter(|name| !name.is_empty()) {
            folded.name = name;
        }
        match function.arguments {
            Value::String(text) => folded.arguments.push_str(&text),
            Value::Null => {}
            other @ (Value::Bool(_) | Value::Number(_) | Value::Array(_) | Value::Object(_)) => {
                folded.arguments.push_str(&other.to_string());
            }
        }
    }

    /// The completion once its stream is over: its calls in their index's
    /// order, each with its argument text, which the loop reads, and how it
    /// ended.
    pub(crate) fn finish(self) -> (Vec<RawCall>, StreamDone) {
        let calls = self
            .calls
            .into_values()
            .map(|call| RawCall::new(call.id, call.name, call.arguments))
            .collect();
        (calls, finished(self.finish_reason, self.usage))
    }
}

/// A completion's usage as the turn's record counts it.
fn usage_of(usage: Usage) -> Metadata {
    let cached = usage
        .prompt_tokens_details
        .map(|details| details.cached_tokens)
        .unwrap_or_default();
    let mut counted = Metadata::new();
    if let Some(prompt) = usage.prompt_tokens {
        let cached = cached.min(prompt);
        counted.insert("input_tokens".to_owned(), Value::from(prompt - cached));
        if cached > 0 {
            counted.insert("cache_read_input_tokens".to_owned(), Value::from(cached));
        }
    }
    if let Some(output) = usage.completion_tokens {
        counted.insert("output_tokens".to_owned(), Value::from(output));
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
pub(crate) fn cleaned(text: &str, secret: &str, limit: usize) -> String {
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
                vec![RawCall::new("call_1", "lookup", r#"{"order": "42"}"#)],
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
    /// leaves out: each reads as absent, a call's id made by
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
                vec![RawCall::new("", "lookup", "")],
                serde_json::json!({ "output_tokens": 7 })
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

    /// RFC §6.4: a streamed call's fragments are folded by their index,
    /// its id and name kept from the first that gives them, its arguments'
    /// text joined; the usage comes in the last chunk.
    #[test]
    fn a_streamed_completion_s_call_fragments_fold_by_index() {
        let mut folding = Folding::default();
        let chunks = [
            r#"{"choices": [{"index": 0, "delta": {"role": "assistant", "content": "Let me "}}]}"#,
            r#"{"choices": [{"index": 0, "delta": {"content": "check."}}]}"#,
            r#"{"choices": [{"index": 0, "delta": {"tool_calls": [{"index": 1, "id": "call_b",
                "function": {"name": "notify", "arguments": ""}}]}}]}"#,
            r#"{"choices": [{"index": 0, "delta": {"tool_calls": [{"index": 0, "id": "call_a",
                "function": {"name": "lookup", "arguments": "{\"or"}}]}}]}"#,
            r#"{"choices": [{"index": 0, "delta": {"tool_calls": [{"index": 0,
                "function": {"arguments": "der\": \"42\"}"}}]}}]}"#,
            r#"{"choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls"}]}"#,
            r#"{"choices": [], "usage": {"prompt_tokens": 9, "completion_tokens": 4}}"#,
        ];
        let text: Vec<String> = chunks
            .iter()
            .filter_map(|chunk| folding.take(chunk, "sk-1").unwrap())
            .collect();
        let (calls, done) = folding.finish();
        assert_eq!(text, ["Let me ", "check."]);
        assert_eq!(
            calls,
            [
                RawCall::new("call_a", "lookup", r#"{"order": "42"}"#),
                RawCall::new("call_b", "notify", ""),
            ]
        );
        assert_eq!(
            (done.finish_reason.as_deref(), Value::Object(done.usage)),
            (
                Some("tool_calls"),
                serde_json::json!({ "input_tokens": 9, "output_tokens": 4 })
            )
        );
    }

    #[test]
    fn a_chunk_reporting_an_error_fails_without_the_key() {
        let mut folding = Folding::default();
        let failed = folding.take(r#"{"error": {"message": "overloaded, key sk-1"}}"#, "sk-1");
        assert_eq!(
            failed,
            Err(OpenAiError::StreamFailed {
                message: "overloaded, key [removed]".to_owned()
            })
        );
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

#[cfg(test)]
mod image_tests {
    use super::*;
    #[test]
    fn author_images_use_native_parts_in_both_protocols() {
        let mut message = Message::new(Role::User, "Describe this image");
        message.images.push(
            roomkit_core::rules::transcript::Image::new(
                "data:image/png;base64,AQIDBA==",
                "image/png",
            )
            .unwrap(),
        );
        let request = Request {
            system_prompt: None,
            messages: vec![message],
            tools: vec![],
            temperature: None,
            max_tokens: None,
        };
        let config = OpenAiConfig {
            base_url: Some("http://localhost/v1".into()),
            vision: true,
            ..OpenAiConfig::new("", "vision-fixture")
        };
        let chat = serde_json::to_value(CompletionRequest::new(&config, &request)).unwrap();
        assert_eq!(
            chat.pointer("/messages/0/content/0/text"),
            Some(&Value::String("Describe this image".into()))
        );
        assert_eq!(
            chat.pointer("/messages/0/content/1/type"),
            Some(&Value::String("image_url".into()))
        );
        let response = crate::responses::request(&config, &request).unwrap();
        assert_eq!(
            response.pointer("/input/0/content/1/type"),
            Some(&Value::String("input_image".into()))
        );
        assert_eq!(
            response.pointer("/input/0/content/1/image_url"),
            chat.pointer("/messages/0/content/1/image_url/url")
        );
    }
}

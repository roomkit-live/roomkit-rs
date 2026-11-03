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
use roomkit_core::rules::transcript::{Message, Part, Role};

/// The longest provider message an error keeps, in characters.
const MESSAGE_LIMIT: usize = 500;

/// The longest error type or code an error keeps, in characters.
const NAME_LIMIT: usize = 64;

/// A Chat Completions request: one completion, streamed as server-sent
/// events with its usage in the last chunk (`stream_options.include_usage`)
/// or answered whole, declaring the round's tools when it has any, and
/// neither `tool_choice` nor `parallel_tool_calls`.
#[derive(Debug, Serialize)]
pub(crate) struct CompletionRequest<'a> {
    model: &'a str,
    messages: Vec<WireMessage<'a>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tools: Vec<WireTool<'a>>,
    /// Sent only when true: a completion answered whole names no `stream`,
    /// as Python's `generate` sends none.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
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
    /// usage when it asks for it. `streamed` false asks for the completion
    /// whole, as Python's `generate` does.
    pub(crate) fn new(config: &'a OpenAiConfig, request: &'a Request, streamed: bool) -> Self {
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
        // No bound from the turn nor the configuration sends none.
        let bound = request.max_tokens.or(config.max_tokens);
        let (max_tokens, max_completion_tokens) = if config.takes_max_completion_tokens() {
            (None, bound)
        } else {
            (bound, None)
        };
        Self {
            model,
            messages,
            tools,
            stream: streamed,
            stream_options: (streamed && config.include_stream_usage).then_some(StreamOptions {
                include_usage: true,
            }),
            temperature: request.temperature.filter(|_| config.takes_temperature()),
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
    // An assistant's message that only calls has no text; a message of
    // parts sends them in order, whatever its role (Python's
    // `chat_request._plain`).
    let content = if !message.parts.is_empty() {
        Some(Value::Array(
            message.parts.iter().filter_map(part_block).collect(),
        ))
    } else if message.images.is_empty() || message.role != Role::User {
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

/// A part of a message as the wire writes it: a text, or an
/// image the provider can fetch; an image the host has not resolved, or a
/// part the transcript adds later, is not sent.
fn part_block(part: &Part) -> Option<Value> {
    match part {
        Part::Text(text) => Some(serde_json::json!({"type": "text", "text": text})),
        Part::Image(image) if image.is_resolved() => {
            Some(serde_json::json!({"type": "image_url", "image_url": {"url": image.url()}}))
        }
        // An image not resolved, or a part the transcript adds later.
        Part::Image(_) | _ => None,
    }
}

/// The wire's name of `role`.
fn role(role: Role) -> &'static str {
    match role {
        Role::Assistant => "assistant",
        Role::Tool => "tool",
        Role::System => "system",
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
        // An error member Python's client reads as true raises (`if
        // data.get("error")`), with the status its object names and the
        // client's words when it gives none of its own.
        if let Some(error) = chunk.error.filter(truthy) {
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .filter(|message| !message.is_empty())
                .unwrap_or(STREAM_ERROR);
            return Err(OpenAiError::StreamFailed {
                message: cleaned(&message, secret, MESSAGE_LIMIT),
                status: described_status(&error),
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

/// What Python's client says of an error inside a stream that brings no
/// message of its own (`openai._streaming`).
const STREAM_ERROR: &str = "An error occurred during streaming";

/// The status an error object names, Python's `_ERROR_TYPE_STATUS`: where
/// a server wrote the failure into its body rather than its status line (a
/// stream already answered 200, a gateway answering 200), and the
/// `google.rpc` codes a Google error body names in `status`.
const ERROR_TYPE_STATUS: [(&str, u16); 21] = [
    ("invalid_request_error", 400),
    ("authentication_error", 401),
    ("permission_error", 403),
    ("not_found_error", 404),
    ("request_too_large", 413),
    ("rate_limit_error", 429),
    ("rate_limit_exceeded", 429),
    ("api_error", 500),
    ("server_error", 500),
    ("internal_error", 500),
    ("service_unavailable", 503),
    ("timeout_error", 504),
    ("overloaded_error", 529),
    ("INVALID_ARGUMENT", 400),
    ("UNAUTHENTICATED", 401),
    ("PERMISSION_DENIED", 403),
    ("NOT_FOUND", 404),
    ("RESOURCE_EXHAUSTED", 429),
    ("INTERNAL", 500),
    ("UNAVAILABLE", 503),
    ("DEADLINE_EXCEEDED", 504),
];

/// The status an error body describes: its `code`, `type` or `status`, read
/// on the innermost error object first, one nested under `error` or `data`
/// (Python's `_described_status`); a body given as text is read as JSON.
pub(crate) fn described_status(body: &Value) -> Option<u16> {
    let parsed;
    let body = if let Value::String(text) = body {
        parsed = serde_json::from_str::<Value>(text).ok()?;
        &parsed
    } else {
        body
    };
    error_objects(body, 3).into_iter().find_map(|item| {
        ["code", "type", "status"]
            .into_iter()
            .find_map(|key| item.get(key).and_then(status_in))
    })
}

/// The error objects of `body`: what it nests under `error` or `data`,
/// innermost first, then itself (Python's `_error_objects`).
fn error_objects(body: &Value, depth: usize) -> Vec<&serde_json::Map<String, Value>> {
    let Some(fields) = body.as_object().filter(|_| depth > 0) else {
        return Vec::new();
    };
    let mut found: Vec<_> = ["error", "data"]
        .into_iter()
        .filter_map(|key| fields.get(key))
        .flat_map(|inner| error_objects(inner, depth - 1))
        .collect();
    found.push(fields);
    found
}

/// `value` as an HTTP error status, if it is one (Python's `_status_in`): a
/// number or its digits between 400 and 599, or a type the table names.
fn status_in(value: &Value) -> Option<u16> {
    let number = match value {
        Value::Number(number) => number.as_u64(),
        Value::String(text) if !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()) => {
            text.parse().ok()
        }
        Value::String(text) => {
            return ERROR_TYPE_STATUS
                .iter()
                .find(|(name, _)| name == text)
                .map(|(_, status)| *status);
        }
        Value::Null | Value::Bool(_) | Value::Array(_) | Value::Object(_) => None,
    }?;
    u16::try_from(number)
        .ok()
        .filter(|status| (400..=599).contains(status))
}

/// Whether `value` reads as true, as Python's `if error:` reads it.
fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(flag) => *flag,
        Value::Number(number) => number.as_f64().is_some_and(|number| number != 0.0),
        Value::String(text) => !text.is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::Object(fields) => !fields.is_empty(),
    }
}

/// What a completion asked whole answers, as [`response`] reads it, or the
/// failure an error object answered in its place stands for, as a gateway
/// answers 200 (Python's `_completion_of`).
///
/// # Errors
///
/// [`OpenAiError::Answered`] for an error object, else as [`response`].
pub(crate) fn completion(body: &[u8], secret: &str) -> Result<Whole, OpenAiError> {
    let value: Option<Value> = serde_json::from_slice(body).ok();
    if let Some(error) = value
        .as_ref()
        .and_then(|value| value.get("error"))
        .filter(|error| truthy(error))
    {
        return Err(answered(error, secret));
    }
    response(body, secret)
}

/// The failure an error object answered in place of a completion stands
/// for (Python's `error_body_failure`): the status it names, and the
/// message of its innermost error object that has one, else the object.
fn answered(error: &Value, secret: &str) -> OpenAiError {
    let message = error_objects(error, 3)
        .into_iter()
        .find_map(|item| item.get("message").and_then(Value::as_str))
        .map_or_else(
            || {
                error
                    .as_str()
                    .map_or_else(|| error.to_string(), str::to_owned)
            },
            str::to_owned,
        );
    OpenAiError::Answered {
        status: described_status(error),
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

    /// Python's `OpenAIConfig.model_post_init`: on OpenAI's own endpoint a
    /// modern model takes `max_completion_tokens`, and the reasoning ones no
    /// temperature, unless the caller set the switch; behind another
    /// `base_url` the switches alone say
    /// (`tasks/probes/step5-review/mcp-openai/py_openai_wire.out`,
    /// `fixes/py_openai_explicit_switch.out`).
    /// Python: `tests/test_providers/test_openai.py::TestOpenAIAIProvider::test_no_cap_is_sent_when_none_is_set`.
    /// `max_tokens: None` sends no bound under either name; a turn's own
    /// bound still goes (RMK-1177).
    #[test]
    fn no_cap_is_sent_when_none_is_set() {
        let sent = |completion_tokens: bool, turn: Option<u32>| {
            let config = OpenAiConfig {
                base_url: Some("http://localhost:8000/v1".to_owned()),
                max_tokens: None,
                use_max_completion_tokens: Some(completion_tokens),
                ..OpenAiConfig::new("sk-1", "local")
            };
            let request = Request {
                system_prompt: None,
                messages: Vec::new(),
                tools: Vec::new(),
                temperature: None,
                max_tokens: turn,
            };
            let body =
                serde_json::to_value(CompletionRequest::new(&config, &request, true)).unwrap();
            (
                body.get("max_tokens").cloned(),
                body.get("max_completion_tokens").cloned(),
            )
        };
        assert_eq!(
            [
                sent(false, None),
                sent(false, Some(321)),
                sent(true, None),
                sent(true, Some(321))
            ],
            [
                (None, None),
                (Some(serde_json::json!(321)), None),
                (None, None),
                (None, Some(serde_json::json!(321))),
            ]
        );
    }

    #[test]
    fn a_modern_model_on_openai_s_endpoint_takes_python_s_profile() {
        let request = Request {
            system_prompt: None,
            messages: Vec::new(),
            tools: Vec::new(),
            temperature: Some(0.7),
            max_tokens: None,
        };
        let set = |base_url: Option<&str>, model: &str, switches: (Option<bool>, Option<bool>)| {
            let config = OpenAiConfig {
                base_url: base_url.map(str::to_owned),
                use_max_completion_tokens: switches.0,
                supports_custom_temperature: switches.1,
                ..OpenAiConfig::new("sk-1", model)
            };
            let body =
                serde_json::to_value(CompletionRequest::new(&config, &request, true)).unwrap();
            (
                body.get("max_completion_tokens").is_some(),
                body.get("temperature").is_some(),
            )
        };
        let shape = |base_url: Option<&str>, model: &str| set(base_url, model, (None, None));
        assert_eq!(
            [
                shape(None, "gpt-6-luna"),
                shape(Some("https://API.openai.com:443/v1/"), "o3-mini"),
                shape(None, "gpt-4.1-mini"),
                shape(None, "gpt-4o"),
                shape(Some("http://localhost:8000/v1"), "gpt-6-luna"),
                set(None, "gpt-5-chat-latest", (None, Some(true))),
                set(None, "gpt-4.1-mini", (Some(false), None)),
                set(None, "gpt-4o", (Some(true), Some(false))),
            ],
            [
                (true, false),
                (true, false),
                (true, true),
                (false, true),
                (false, true),
                (true, true),
                (false, true),
                (true, false)
            ]
        );
    }

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
                message: "overloaded, key [removed]".to_owned(),
                status: None,
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
        let chat = serde_json::to_value(CompletionRequest::new(&config, &request, true)).unwrap();
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

    // Python: tests/test_providers/test_chat_request.py::test_a_plain_answer_keeps_its_reasoning_inline
    // (a message of parts goes as its blocks in order: here a caption, its
    // image, then the next text, RMK-1191)
    #[test]
    fn a_message_s_text_and_image_parts_keep_their_order() {
        use roomkit_ai::{AiContentPart, AiContext, AiImagePart, AiMessage, AiRole, AiTextPart};

        let image = AiImagePart {
            mime_type: Some("image/png".to_owned()),
            ..AiImagePart::new("data:image/png;base64,AQIDBA==")
        };
        let context = AiContext {
            messages: vec![AiMessage::new(
                AiRole::User,
                vec![
                    AiContentPart::Text(AiTextPart::new("my lamp")),
                    AiContentPart::Image(image),
                    AiContentPart::Text(AiTextPart::new("compare with mine")),
                ],
            )],
            ..AiContext::default()
        };
        let request = Request::of(&context).unwrap();
        let config = OpenAiConfig {
            base_url: Some("http://localhost/v1".into()),
            vision: true,
            ..OpenAiConfig::new("", "vision-fixture")
        };
        let chat = serde_json::to_value(CompletionRequest::new(&config, &request, true)).unwrap();
        assert_eq!(
            chat.pointer("/messages/0/content"),
            Some(&serde_json::json!([
                {"type": "text", "text": "my lamp"},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,AQIDBA=="}},
                {"type": "text", "text": "compare with mine"},
            ]))
        );
        let response = crate::responses::request(&config, &request).unwrap();
        assert_eq!(
            response.pointer("/input/0/content"),
            Some(&serde_json::json!([
                {"type": "input_text", "text": "my lamp"},
                {"type": "input_image", "image_url": "data:image/png;base64,AQIDBA=="},
                {"type": "input_text", "text": "compare with mine"},
            ]))
        );
    }
}

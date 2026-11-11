//! The Chat Completions wire: the request a turn sends, and what it reads
//! of the answer.

use crate::call_slots::{CallSlots, Fragment, FragmentId};
use crate::config::OpenAiConfig;
use crate::dialect::ChatDialect;
use crate::request::{RawCall, Request, Whole, done as finished};
use crate::request::{push_text, push_thinking};
use reqwest::StatusCode;
use roomkit::model::Metadata;
use roomkit_ai::{StreamDone, StreamEvent, StreamToolCallDelta};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::VecDeque;

use crate::error::OpenAiError;
use crate::think_tags::{
    Segment, ThinkTagParser, extract_think_tags, field_reasoning, merge_thinking,
};
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
    /// The vendor's reasoning field, when its dialect writes one.
    #[serde(flatten)]
    reasoning: serde_json::Map<String, Value>,
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
    pub(crate) fn new(
        config: &'a OpenAiConfig,
        request: &'a Request,
        streamed: bool,
        dialect: ChatDialect<'_>,
    ) -> Self {
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
                reasoning: serde_json::Map::new(),
            });
        let thinking = request.reasoning.iter().map(Option::as_deref);
        let messages = system
            .into_iter()
            .chain(
                request
                    .messages
                    .iter()
                    .zip(thinking.chain(std::iter::repeat(None)))
                    .map(|(message, thinking)| wire_message(message, thinking, dialect)),
            )
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

/// `message` as the wire writes it, an assistant's `thinking` in the
/// field `dialect` reads it from, if any; a message no assistant wrote
/// (`thinking` `None`) carries none.
fn wire_message<'a>(
    message: &'a Message,
    thinking: Option<&str>,
    dialect: ChatDialect<'_>,
) -> WireMessage<'a> {
    let calls: Vec<WireCall<'_>> = message
        .tool_calls
        .iter()
        .map(|call| WireCall {
            id: &call.id,
            kind: "function",
            function: WireFunction {
                name: &call.name,
                arguments: roomkit_ai::dumps(&Value::Object(call.arguments.clone())),
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
    let mut reasoning = serde_json::Map::new();
    if message.role == Role::Assistant
        && let Some(thinking) = thinking
        && let Some((field, value)) = dialect.field(thinking, !calls.is_empty())
    {
        reasoning.insert(field.to_owned(), Value::String(value));
    }
    WireMessage {
        role: role(message.role),
        content,
        tool_calls: calls,
        tool_call_id: message.tool_call_id.as_deref(),
        reasoning,
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
    /// The model that answered, as the server names it.
    #[serde(default)]
    model: Option<Value>,
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
    #[serde(default)]
    reasoning_content: Option<Value>,
    #[serde(default)]
    reasoning: Option<Value>,
    #[serde(default, deserialize_with = "nullable")]
    tool_calls: Vec<AnswerCall>,
    /// Why the model withheld its answer, where a structured output's
    /// refusal lands on this dialect.
    #[serde(default)]
    refusal: Option<Value>,
}

/// A call the answer makes.
#[derive(Debug, Deserialize)]
struct AnswerCall {
    /// Empty when the server gave none: `CallIds` mints one.
    #[serde(default, deserialize_with = "server_id")]
    id: String,
    /// None for an entry with no function, a custom tool's call: no call.
    #[serde(default)]
    function: Option<AnswerFunction>,
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
    /// DeepSeek's cache hits and misses, beside OpenAI's details.
    #[serde(default)]
    prompt_cache_hit_tokens: Option<u64>,
    #[serde(default)]
    prompt_cache_miss_tokens: Option<u64>,
    /// The cache writes a server may report beside the details, as
    /// Python's reader takes them: read only as a whole number.
    #[serde(default)]
    cache_write_tokens: Option<Value>,
    /// What the output counts, its `reasoning_tokens`: read only as a whole
    /// number, any other shape left unread.
    #[serde(default)]
    completion_tokens_details: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct PromptDetails {
    #[serde(default, deserialize_with = "nullable")]
    cached_tokens: u64,
    /// The prompt's tokens written to the cache, which it counts: read only
    /// as a whole number.
    #[serde(default)]
    cache_write_tokens: Option<Value>,
}

/// A whole answer's call id as text, whatever the server wrote: Python's
/// `CallIds` reads `str(server_id) if server_id else ""`.
fn server_id<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    if reads_false(&value) {
        return Ok(String::new());
    }
    Ok(match value {
        Value::String(id) => id,
        Value::Bool(_) => "True".to_owned(),
        other @ (Value::Null | Value::Number(_) | Value::Array(_) | Value::Object(_)) => {
            other.to_string()
        }
    })
}

/// A stream's call id as Python's `ToolCallSlots` reads what the server
/// wrote: none for `null` or empty text, the text, and an id that is no text
/// kept as it reads, false or true, for the fold to judge.
fn fragment_id(id: Option<&Value>) -> FragmentId<'_> {
    match id {
        None | Some(Value::Null) => FragmentId::None,
        Some(Value::String(id)) if id.is_empty() => FragmentId::None,
        Some(Value::String(id)) => FragmentId::Text(id),
        Some(id) if reads_false(id) => FragmentId::False,
        Some(id) => FragmentId::True(id),
    }
}

/// Whether Python reads `value` as false.
fn reads_false(value: &Value) -> bool {
    match value {
        Value::Null | Value::Bool(false) => true,
        Value::Number(number) => number.as_f64() == Some(0.0),
        Value::Array(items) => items.is_empty(),
        Value::Object(members) => members.is_empty(),
        Value::Bool(true) | Value::String(_) => false,
    }
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
/// ([`usage_of`]: `input_tokens` without the cache's, `output_tokens`,
/// `cache_read_input_tokens`, `cache_creation_input_tokens`,
/// `reasoning_tokens`); its end names the model that answered, else
/// `asked` ([`answered_by`]). A completion with no choice is an empty
/// answer, which still billed its input (Python's `_response_of`); a body
/// that is no JSON object is no completion (Python's `_completion_of`).
///
/// # Errors
///
/// Returns [`OpenAiError::Malformed`] when `body` is no chat completion,
/// saying why without `secret`, the API key.
pub(crate) fn response(
    body: &[u8],
    secret: &str,
    cache_hits: bool,
    asked: &str,
) -> Result<Whole, OpenAiError> {
    let malformed = |reason: String| OpenAiError::Malformed {
        reason: cleaned(&reason, secret, MESSAGE_LIMIT),
    };
    // An array would fill a completion with its members' defaults.
    if body.trim_ascii_start().first() != Some(&b'{') {
        return Err(malformed("not a chat completion".to_owned()));
    }
    let completion: Completion =
        serde_json::from_slice(body).map_err(|error| malformed(error.to_string()))?;
    let usage = completion
        .usage
        .map(|usage| usage_of(&usage, cache_hits))
        .unwrap_or_default();
    let Some(choice) = completion.choices.into_iter().next() else {
        let mut done = finished(None, usage);
        answered_by(&mut done, completion.model, asked);
        return Ok(Whole::new(String::new(), Vec::new(), done));
    };
    let (inline, text) = extract_think_tags(choice.message.content.as_deref().unwrap_or_default());
    let thinking = merge_thinking(
        inline,
        field_reasoning(
            choice.message.reasoning_content.as_ref(),
            choice.message.reasoning.as_ref(),
        ),
    );
    let mut done = finished(choice.finish_reason, usage);
    answered_by(&mut done, completion.model, asked);
    keep_refusal(
        &mut done,
        choice.message.refusal.as_ref().and_then(Value::as_str),
    );
    let calls = choice
        .message
        .tool_calls
        .into_iter()
        .filter_map(|call| {
            let function = call.function?;
            let arguments = match function.arguments {
                Value::String(text) => text,
                Value::Null => String::new(),
                other
                @ (Value::Bool(_) | Value::Number(_) | Value::Array(_) | Value::Object(_)) => {
                    other.to_string()
                }
            };
            Some(RawCall::new(call.id, function.name, arguments))
        })
        .collect();
    Ok(Whole::new(text, calls, done).with_thinking(thinking))
}

/// What a stream answered with a completion's JSON in place of its events
/// gives: the completion as [`response`] reads it, but one with no choice
/// is no answer, as an error object there is none. Python's client finds no
/// event in any such body and fails it, a completion with a choice too:
/// that one is read here as the answer (RMK-1446).
///
/// # Errors
///
/// As [`response`], and [`OpenAiError::Malformed`] for a completion with no
/// choice.
pub(crate) fn streamed_completion(
    body: &[u8],
    secret: &str,
    cache_hits: bool,
    asked: &str,
) -> Result<Whole, OpenAiError> {
    let chosen = serde_json::from_slice::<Value>(body).ok().map(|value| {
        value
            .get("choices")
            .and_then(Value::as_array)
            .is_some_and(|choices| !choices.is_empty())
    });
    if chosen == Some(false) {
        return Err(OpenAiError::Malformed {
            reason: "it has no choice".to_owned(),
        });
    }
    response(body, secret, cache_hits, asked)
}

/// Names on `done` the model that answered: `answered`, as the server names
/// it, when Python reads it as true, else `asked` (Python's `answered_by`,
/// one rule for a whole answer and for a stream's end).
fn answered_by(done: &mut StreamDone, answered: Option<Value>, asked: &str) {
    let model = answered
        .filter(truthy)
        .unwrap_or_else(|| Value::from(asked));
    done.metadata.insert("model".to_owned(), model);
}

/// Keeps on `done` the refusal an answer carried as text, when it carried
/// one (Python's `answered_by`).
fn keep_refusal(done: &mut StreamDone, refusal: Option<&str>) {
    if let Some(refusal) = refusal.filter(|refusal| !refusal.is_empty()) {
        done.metadata
            .insert("refusal".to_owned(), Value::from(refusal));
    }
}

/// The `response_format` that constrains an answer to `schema`, strictly
/// (RFC §6.7, Python's `json_schema_format`).
pub(crate) fn json_schema_format(schema: &serde_json::Map<String, Value>) -> Value {
    serde_json::json!({
        "type": "json_schema",
        "json_schema": { "name": "response", "schema": schema, "strict": true },
    })
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
    /// The model that answered, as the server names it.
    #[serde(default)]
    model: Option<Value>,
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
    content: Option<Value>,
    #[serde(default)]
    reasoning_content: Option<Value>,
    #[serde(default)]
    reasoning: Option<Value>,
    #[serde(default, deserialize_with = "nullable")]
    tool_calls: Vec<Value>,
    /// A piece of the refusal the model gives in place of its answer.
    #[serde(default)]
    refusal: Option<Value>,
}

/// A fragment of a call, which the call's `index` names across chunks.
#[derive(Debug, Deserialize)]
struct DeltaCall {
    #[serde(default, deserialize_with = "nullable")]
    index: u64,
    /// The server's id, as it wrote it: [`fragment_id`] reads it.
    #[serde(default)]
    id: Option<Value>,
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

/// A streamed completion, folded as its chunks come: the calls' fragments
/// by their index, why it stopped, what it used (RFC §6.4 "What a provider
/// hands the loop for a call").
#[derive(Debug, Default)]
pub(crate) struct Folding {
    calls: CallSlots,
    finish_reason: Option<String>,
    usage: Metadata,
    /// Sorts the text into reasoning and answer, a tag split across chunks
    /// included.
    tags: ThinkTagParser,
    /// Whether the usage reports its cache hits as DeepSeek does.
    cache_hits: bool,
    /// The refusal the deltas carried, in their order.
    refusal: String,
    /// The model the last chunk naming one says answered.
    model: Option<Value>,
    /// The model the request asked for.
    asked: String,
}

impl Folding {
    /// The folding of a stream asking `asked`, whose usage reports its cache
    /// hits as DeepSeek does when `cache_hits`.
    pub(crate) fn new(cache_hits: bool, asked: impl Into<String>) -> Self {
        Self {
            cache_hits,
            asked: asked.into(),
            ..Self::default()
        }
    }

    /// Takes the chunk `data`: composition, reasoning and text in order.
    /// Events read before a later malformed call remain queued, as Python's
    /// `generate_structured_stream` reads them.
    ///
    /// # Errors
    ///
    /// [`OpenAiError::Malformed`] when `data` is no chunk,
    /// [`OpenAiError::StreamFailed`] when it reports the provider's error,
    /// each without `secret`, the API key.
    pub(crate) fn take(
        &mut self,
        data: &str,
        secret: &str,
        ready: &mut VecDeque<StreamEvent>,
    ) -> Result<(), OpenAiError> {
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
                message: cleaned(message, secret, MESSAGE_LIMIT),
                status: described_status(&error),
            });
        }
        if let Some(model) = chunk.model.filter(truthy) {
            self.model = Some(model);
        }
        if let Some(usage) = chunk.usage {
            self.usage = usage_of(&usage, self.cache_hits);
        }
        for choice in chunk.choices.into_iter().filter(|choice| choice.index == 0) {
            self.read_choice(choice, secret, ready)?;
        }
        Ok(())
    }

    fn read_choice(
        &mut self,
        choice: ChunkChoice,
        secret: &str,
        ready: &mut VecDeque<StreamEvent>,
    ) -> Result<(), OpenAiError> {
        if let Some(reason) = choice.finish_reason {
            self.finish_reason = Some(reason);
        }
        let Some(delta) = choice.delta else {
            return Ok(());
        };
        if let Some(refusal) = delta.refusal.as_ref().and_then(Value::as_str) {
            self.refusal.push_str(refusal);
        }
        for call in delta.tool_calls {
            let call = serde_json::from_value(call).map_err(|error| OpenAiError::Malformed {
                reason: cleaned(&error.to_string(), secret, MESSAGE_LIMIT),
            })?;
            if let Some(composition) = self.fold(call).map_err(|reason| OpenAiError::Malformed {
                reason: cleaned(&reason, secret, MESSAGE_LIMIT),
            })? {
                ready.push_back(StreamEvent::ToolCallDelta(composition));
            }
        }
        if let Some(reasoning) =
            field_reasoning(delta.reasoning_content.as_ref(), delta.reasoning.as_ref())
        {
            push_thinking(ready, reasoning);
        }
        self.read_text(delta.content, ready)?;
        Ok(())
    }

    /// Validate content only when it is read, after this chunk's calls and
    /// reasoning. Python's SDK permits malformed members until that point.
    fn read_text(
        &mut self,
        content: Option<Value>,
        ready: &mut VecDeque<StreamEvent>,
    ) -> Result<(), OpenAiError> {
        let Some(content) = content.filter(truthy) else {
            return Ok(());
        };
        let Value::String(text) = content else {
            return Err(OpenAiError::Malformed {
                reason: "chat delta content is not text".to_owned(),
            });
        };
        queue_segments(ready, self.tags.feed(&text));
        Ok(())
    }

    /// What the tags still hold once the stream is over.
    pub(crate) fn flush(&mut self, ready: &mut VecDeque<StreamEvent>) {
        queue_segments(ready, self.tags.flush());
    }

    /// Adds a call's fragment to the call it belongs to; an object for its
    /// arguments reads as the JSON text it stands for. Fails, saying why, on
    /// an id the calls cannot hold, or a call named under an id that is no
    /// text ([`CallSlots::fold`]).
    fn fold(&mut self, fragment: DeltaCall) -> Result<Option<StreamToolCallDelta>, String> {
        let DeltaCall {
            index,
            id,
            function,
        } = fragment;
        let (name, arguments) = function.as_ref().map_or((None, Value::Null), |function| {
            (function.name.as_deref(), function.arguments.clone())
        });
        let arguments = match arguments {
            Value::String(text) => text,
            Value::Null => String::new(),
            object @ Value::Object(_) => roomkit_ai::dumps(&object),
            other @ (Value::Bool(_) | Value::Number(_) | Value::Array(_)) => other.to_string(),
        };
        self.calls.fold(&Fragment {
            index,
            id: fragment_id(id.as_ref()),
            name: name.filter(|name| !name.is_empty()),
            arguments: &arguments,
            function: function.is_some(),
        })
    }

    /// The completion once its stream is over: its calls in the order they
    /// came, each with its argument text, which the loop reads, and how it
    /// ended, naming the model that answered ([`answered_by`]).
    ///
    /// # Errors
    ///
    /// [`OpenAiError::Malformed`] for a call that took an id that is no text
    /// ([`CallSlots::finish`]).
    pub(crate) fn finish(self) -> Result<(Vec<RawCall>, StreamDone), OpenAiError> {
        let calls = self
            .calls
            .finish()
            .map_err(|reason| OpenAiError::Malformed { reason })?;
        let mut done = finished(self.finish_reason, self.usage);
        answered_by(&mut done, self.model, &self.asked);
        keep_refusal(&mut done, Some(&self.refusal));
        Ok((calls, done))
    }
}

/// Text and reasoning retain the tag parser's order.
fn queue_segments(ready: &mut VecDeque<StreamEvent>, segments: Vec<Segment>) {
    for segment in segments {
        match segment {
            Segment::Thinking(thinking) => push_thinking(ready, thinking),
            Segment::Text(text) => push_text(ready, text),
        }
    }
}

/// A completion's usage as the turn's record counts it (Python's
/// `OpenAIAIProvider._usage_from`): `input_tokens` the prompt less its cache
/// reads and its cache writes, and `output_tokens`, each where the server
/// gives it (a count it leaves out stays unknown, RMK-1029);
/// `cache_read_input_tokens`, bounded by the prompt, and
/// `cache_creation_input_tokens` apart, when they are not 0, a write read
/// from `prompt_tokens_details`, else from the usage itself; and
/// `reasoning_tokens`, a detail of the output, which counts it, when it is
/// not 0. With `cache_hits`, the cache as DeepSeek reports it, where it does
/// (Python's `DeepSeekAIProvider._usage_from`): its misses are the input, its
/// hits the cache read, a miss count it leaves out the prompt less the hits;
/// a count it leaves out, and a miss count with no prompt to derive it from,
/// stay unknown (RMK-1441).
fn usage_of(usage: &Usage, cache_hits: bool) -> Metadata {
    let mut counted = Metadata::new();
    if cache_hits && let Some(hit) = usage.prompt_cache_hit_tokens {
        let miss = usage
            .prompt_cache_miss_tokens
            .or_else(|| usage.prompt_tokens.map(|prompt| prompt.saturating_sub(hit)));
        if let Some(miss) = miss {
            counted.insert("input_tokens".to_owned(), Value::from(miss));
        }
        if let Some(output) = usage.completion_tokens {
            counted.insert("output_tokens".to_owned(), Value::from(output));
        }
        if hit > 0 {
            counted.insert("cache_read_input_tokens".to_owned(), Value::from(hit));
        }
        return with_reasoning(counted, usage);
    }
    let details = usage.prompt_tokens_details.as_ref();
    let cached = details
        .map(|details| details.cached_tokens)
        .unwrap_or_default();
    let written = details
        .and_then(|details| counter(details.cache_write_tokens.as_ref()))
        .filter(|written| *written > 0)
        .or_else(|| counter(usage.cache_write_tokens.as_ref()))
        .unwrap_or_default();
    if let Some(prompt) = usage.prompt_tokens {
        let cached = cached.min(prompt);
        let fresh = prompt.saturating_sub(cached).saturating_sub(written);
        counted.insert("input_tokens".to_owned(), Value::from(fresh));
        if cached > 0 {
            counted.insert("cache_read_input_tokens".to_owned(), Value::from(cached));
        }
    }
    if let Some(output) = usage.completion_tokens {
        counted.insert("output_tokens".to_owned(), Value::from(output));
    }
    if written > 0 {
        counted.insert(
            "cache_creation_input_tokens".to_owned(),
            Value::from(written),
        );
    }
    with_reasoning(counted, usage)
}

/// A counter the server may write as anything: a whole number, else none.
fn counter(value: Option<&Value>) -> Option<u64> {
    value.and_then(Value::as_u64)
}

/// `counted` with the usage's `reasoning_tokens`, when it reports any (Python's
/// `_reasoning_detail`): a detail of the output, never priced apart.
fn with_reasoning(mut counted: Metadata, usage: &Usage) -> Metadata {
    let reasoning = usage
        .completion_tokens_details
        .as_ref()
        .and_then(|details| counter(details.get("reasoning_tokens")))
        .unwrap_or_default();
    if reasoning > 0 {
        counted.insert("reasoning_tokens".to_owned(), Value::from(reasoning));
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
pub(crate) fn completion(
    body: &[u8],
    secret: &str,
    cache_hits: bool,
    asked: &str,
) -> Result<Whole, OpenAiError> {
    let value: Option<Value> = serde_json::from_slice(body).ok();
    if let Some(error) = value
        .as_ref()
        .and_then(|value| value.get("error"))
        .filter(|error| truthy(error))
    {
        return Err(answered(error, secret));
    }
    response(body, secret, cache_hits, asked)
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
                reasoning: Vec::new(),
                tools: Vec::new(),
                temperature: None,
                max_tokens: turn,
                reasoning_effort: None,
                thinking_off: false,
                thinking_switch: None,
                response_schema: None,
            };
            let body = serde_json::to_value(CompletionRequest::new(
                &config,
                &request,
                true,
                crate::dialect::OPENAI_CHAT,
            ))
            .unwrap();
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
            reasoning: Vec::new(),
            tools: Vec::new(),
            temperature: Some(0.7),
            max_tokens: None,
            reasoning_effort: None,
            thinking_off: false,
            thinking_switch: None,
            response_schema: None,
        };
        let set = |base_url: Option<&str>, model: &str, switches: (Option<bool>, Option<bool>)| {
            let config = OpenAiConfig {
                base_url: base_url.map(str::to_owned),
                use_max_completion_tokens: switches.0,
                supports_custom_temperature: switches.1,
                ..OpenAiConfig::new("sk-1", model)
            };
            let body = serde_json::to_value(CompletionRequest::new(
                &config,
                &request,
                true,
                crate::dialect::OPENAI_CHAT,
            ))
            .unwrap();
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
            response(body, "sk-1", false, "m").map(|read| read.text),
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
        let read = response(body, "sk-1", false, "m").unwrap();
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

    // Python: tests/test_providers/test_chat_request.py::test_a_replayed_call_s_arguments_on_the_chat_wire
    // A replayed call's arguments as the chat wire writes them: json.dumps'
    // default form, as Python's, the keys in their order (RMK-1508, HITL
    // 9f5e61ba).
    #[test]
    fn a_replayed_call_s_arguments_on_the_chat_wire() {
        use roomkit_ai::{AiContentPart, AiContext, AiMessage, AiRole, AiToolCallPart};
        let config = OpenAiConfig::new("sk-1", "m");
        let mut call = AiToolCallPart::new("c1", "lookup");
        call.arguments.insert("q".to_owned(), Value::from("café"));
        call.arguments.insert("n".to_owned(), Value::from(42));
        let context = AiContext {
            messages: vec![AiMessage::new(
                AiRole::Assistant,
                vec![AiContentPart::ToolCall(call)],
            )],
            ..AiContext::default()
        };
        let request = Request::of(&context).unwrap();
        let body = serde_json::to_value(CompletionRequest::new(
            &config,
            &request,
            false,
            crate::dialect::OPENAI_CHAT,
        ))
        .unwrap();
        assert_eq!(
            body["messages"][0]["tool_calls"][0]["function"]["arguments"],
            Value::from("{\"q\": \"caf\\u00e9\", \"n\": 42}")
        );
    }

    // Python: tests/test_providers/test_chat_request.py::test_a_call_round_s_required_reasoning_field_is_the_assistants_alone
    #[test]
    fn a_call_round_s_required_reasoning_field_is_the_assistants_alone() {
        use roomkit_ai::AiToolResultPart;
        use roomkit_ai::{AiContentPart, AiContext, AiMessage, AiRole, AiToolCallPart};
        let config = OpenAiConfig::new("sk-1", "m");
        let field = |role: AiRole| {
            let held = vec![
                AiContentPart::ToolResult(AiToolResultPart::new("c1", "lookup", "found")),
                AiContentPart::ToolCall(AiToolCallPart::new("c1", "lookup")),
            ];
            let context = AiContext {
                messages: vec![AiMessage::new(role, held)],
                ..AiContext::default()
            };
            let request = Request::of(&context).unwrap();
            let body = serde_json::to_value(CompletionRequest::new(
                &config,
                &request,
                false,
                crate::dialect::DEEPSEEK_CHAT,
            ))
            .unwrap();
            (
                body["messages"][0]["role"].clone(),
                body["messages"][0].get("reasoning_content").cloned(),
            )
        };
        assert_eq!(
            [field(AiRole::Assistant), field(AiRole::Tool)],
            [
                (Value::from("assistant"), Some(Value::from(""))),
                (Value::from("assistant"), None),
            ]
        );
    }

    // Python: tests/test_providers/test_openai.py::TestOpenAIAIProvider::test_a_count_the_server_leaves_out_stays_unknown
    #[test]
    fn a_count_the_server_leaves_out_stays_unknown() {
        let read = |usage: Value| {
            let body = serde_json::json!({
                "choices": [{"message": {"content": "Hi"}, "finish_reason": "stop"}],
                "usage": usage,
            });
            let read = response(body.to_string().as_bytes(), "sk-1", false, "m").unwrap();
            Value::Object(read.done.usage)
        };
        let cases = [
            (
                serde_json::json!({"completion_tokens": 7}),
                serde_json::json!({"output_tokens": 7}),
            ),
            (
                serde_json::json!({"prompt_tokens": 42}),
                serde_json::json!({"input_tokens": 42}),
            ),
            // A cache read the prompt cannot hold is bounded by it.
            (
                serde_json::json!({"prompt_tokens": 10, "completion_tokens": 7,
                    "prompt_tokens_details": {"cached_tokens": 40}}),
                serde_json::json!({"input_tokens": 0, "output_tokens": 7,
                    "cache_read_input_tokens": 10}),
            ),
            // The read is a share of the prompt: unknown with it. A write is
            // reported apart, and kept.
            (
                serde_json::json!({"completion_tokens": 7,
                    "prompt_tokens_details": {"cached_tokens": 40}}),
                serde_json::json!({"output_tokens": 7}),
            ),
            (
                serde_json::json!({"completion_tokens": 7,
                    "prompt_tokens_details": {"cache_write_tokens": 5}}),
                serde_json::json!({"output_tokens": 7, "cache_creation_input_tokens": 5}),
            ),
            // A counter of another shape is none.
            (
                serde_json::json!({"prompt_tokens": 100, "completion_tokens": 7,
                    "prompt_tokens_details": {"cache_write_tokens": "12"}}),
                serde_json::json!({"input_tokens": 100, "output_tokens": 7}),
            ),
        ];
        for (usage, counted) in cases {
            assert_eq!(read(usage), counted);
        }
    }

    /// An OpenAI-compatible server may write `null` for any member it
    /// leaves out: each reads as absent, a call's id left to `CallIds`.
    #[test]
    fn a_completion_s_nulls_read_as_absent() {
        let body = br#"{"id": null, "choices": [{"index": 0, "finish_reason": null,
            "message": {"role": "assistant", "content": null, "refusal": null,
                "tool_calls": [{"id": null, "type": "function",
                    "function": {"name": "lookup", "arguments": null}}]}}],
            "usage": {"prompt_tokens": null, "completion_tokens": 7, "total_tokens": null,
                "prompt_tokens_details": {"cached_tokens": null}}}"#;
        let read = response(body, "sk-1", false, "m").unwrap();
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
        let read = response(body, "sk-1", false, "m").unwrap();
        assert_eq!((read.text.as_str(), read.calls.len()), ("Hi", 0));
    }

    /// The calls `fragments`, each an index and a `tool_calls` entry's
    /// members, fold into.
    fn folded(fragments: &[(u64, Value)]) -> Vec<RawCall> {
        let mut folding = Folding::default();
        for (index, fragment) in fragments {
            let mut call = fragment.clone();
            call["index"] = (*index).into();
            let chunk = serde_json::json!({
                "choices": [{ "index": 0, "delta": { "tool_calls": [call] } }]
            });
            folding
                .take(&chunk.to_string(), "sk-1", &mut VecDeque::new())
                .unwrap();
        }
        let ids = fragments
            .iter()
            .filter_map(|(_, f)| f["id"].as_str())
            .collect();
        normalize_minted_ids(folding.finish().unwrap().0, &ids)
    }

    /// These slot-shape tests use an empty id as a placeholder for a minted
    /// one. Check its uniqueness before normalization; the public composition
    /// tests separately require the exact same id on every fragment and call.
    fn normalize_minted_ids(
        mut calls: Vec<RawCall>,
        server_ids: &std::collections::HashSet<&str>,
    ) -> Vec<RawCall> {
        assert!(calls.iter().all(|call| !call.id.is_empty()));
        let unique: std::collections::HashSet<_> = calls.iter().map(|call| &call.id).collect();
        assert_eq!(unique.len(), calls.len());
        for call in &mut calls {
            if !server_ids.contains(call.id.as_str()) {
                assert!(call.id.starts_with("call_"), "{}", call.id);
                call.id.clear();
            }
        }
        calls
    }

    fn fragment(id: Option<&str>, name: Option<&str>, arguments: &str) -> Value {
        serde_json::json!({ "id": id, "function": { "name": name, "arguments": arguments } })
    }

    // Python: tests/test_providers/test_tool_call_handoff.py::TestStreamedSlots::test_whole_calls_on_one_index_stay_two_calls
    #[test]
    fn whole_calls_on_one_index_stay_two_calls() {
        let calls = folded(&[
            (0, fragment(Some("a"), Some("now"), r#"{"tz": "A"}"#)),
            (0, fragment(Some("b"), Some("later"), r#"{"tz": "B"}"#)),
        ]);
        assert_eq!(
            calls,
            [
                RawCall::new("a", "now", r#"{"tz": "A"}"#),
                RawCall::new("b", "later", r#"{"tz": "B"}"#),
            ]
        );
    }

    // Python: tests/test_providers/test_tool_call_handoff.py::TestStreamedSlots::test_whole_calls_without_ids_on_one_index_stay_two_calls
    #[test]
    fn whole_calls_without_ids_on_one_index_stay_two_calls() {
        let calls = folded(&[
            (0, fragment(None, Some("roll_die"), r#"{"sides": 6}"#)),
            (0, fragment(None, Some("roll_die"), r#"{"sides": 6}"#)),
        ]);
        assert_eq!(
            calls,
            [
                RawCall::new("", "roll_die", r#"{"sides": 6}"#),
                RawCall::new("", "roll_die", r#"{"sides": 6}"#),
            ]
        );
    }

    // Python: tests/test_providers/test_tool_call_handoff.py::TestStreamedSlots::test_fragments_of_one_call_fold_together
    #[test]
    fn fragments_of_one_call_fold_together() {
        let calls = folded(&[
            (0, fragment(Some("a"), Some("now"), r#"{"tz": "#)),
            (0, fragment(None, None, r#""A"}"#)),
        ]);
        assert_eq!(calls, [RawCall::new("a", "now", r#"{"tz": "A"}"#)]);
    }

    // Python: providers/ai/openai_dialect.py ToolCallSlots._starts_another_call:
    // a server may repeat a call's name on every fragment; after whole arguments
    // a new JSON object under no name opens a second call to the same tool.
    #[test]
    fn what_starts_another_call_on_an_occupied_index() {
        let repeated = folded(&[
            (0, fragment(None, Some("now"), r#"{"tz": "#)),
            (0, fragment(None, Some("now"), r#""A"}"#)),
        ]);
        assert_eq!(repeated, [RawCall::new("", "now", r#"{"tz": "A"}"#)]);
        let nameless = folded(&[
            (0, fragment(Some("a"), Some("now"), r#"{"tz": "A"}"#)),
            (0, fragment(None, None, r#"{"tz": "B"}"#)),
        ]);
        assert_eq!(
            nameless,
            [
                RawCall::new("a", "now", r#"{"tz": "A"}"#),
                RawCall::new("", "now", r#"{"tz": "B"}"#),
            ]
        );
        let other_name_mid_arguments = folded(&[
            (0, fragment(Some("a"), Some("now"), r#"{"tz": "#)),
            (0, fragment(None, Some("later"), r#""A"}"#)),
        ]);
        assert_eq!(
            other_name_mid_arguments,
            [RawCall::new("a", "later", r#"{"tz": "A"}"#)]
        );
    }

    // Python: providers/ai/openai_dialect.py ToolCallSlots.calls: an entry with
    // no function is no call (a custom tool's), its id kept for a function a
    // later fragment brings; one whose function has no name and no arguments is
    // a call, for the loop to refuse.
    #[test]
    fn an_entry_with_no_function_is_no_call() {
        let no_function = serde_json::json!({ "id": "a" });
        assert_eq!(folded(&[(0, no_function.clone())]), []);
        let empty = serde_json::json!({ "id": "a", "function": {} });
        assert_eq!(folded(&[(0, empty)]), [RawCall::new("a", "", "")]);
        assert_eq!(
            folded(&[(0, no_function), (0, fragment(None, Some("now"), "{}"))]),
            [RawCall::new("a", "now", "{}")]
        );
    }

    // Python: providers/ai/openai_dialect.py ToolCallSlots, its answers read from
    // Python: each fragment an id, a name and its arguments, and the calls they
    // fold into.
    #[test]
    fn what_a_fragment_on_an_occupied_index_folds_into() {
        type Part = (Option<&'static str>, Option<&'static str>, &'static str);
        let cases: [(&[Part], Vec<RawCall>); 9] = [
            (
                &[
                    (Some("a"), Some("now"), r#"{"o": "#),
                    (None, None, r#"{"k": 1}}"#),
                ],
                vec![RawCall::new("a", "now", r#"{"o": {"k": 1}}"#)],
            ),
            (
                &[(Some("a"), Some("now"), "{}"), (None, None, "\n")],
                vec![RawCall::new("a", "now", "{}\n")],
            ),
            (
                &[(Some("a"), Some("now"), "{}"), (None, Some("now"), " ")],
                vec![RawCall::new("a", "now", "{} ")],
            ),
            (
                &[(Some("a"), Some("now"), ""), (None, Some("now"), "{}")],
                vec![RawCall::new("a", "now", "{}")],
            ),
            (
                &[(Some("a"), Some("now"), ""), (None, Some("later"), "{}")],
                vec![
                    RawCall::new("a", "now", ""),
                    RawCall::new("", "later", "{}"),
                ],
            ),
            (
                &[(Some("a"), Some("now"), "{}"), (Some("b"), None, "{}")],
                vec![RawCall::new("a", "now", "{}"), RawCall::new("b", "", "{}")],
            ),
            (
                &[(Some("a"), Some("now"), "{}"), (Some("a"), None, "{}")],
                vec![
                    RawCall::new("a", "now", "{}"),
                    RawCall::new("", "now", "{}"),
                ],
            ),
            // Named before its server gave an id, with no arguments yet.
            (
                &[(None, Some("now"), ""), (Some("a"), None, "{}")],
                vec![RawCall::new("", "now", "{}")],
            ),
            // The call was named before its server gave an id: no id starts
            // another call, none is taken.
            (
                &[
                    (None, Some("now"), r#"{"tz": "#),
                    (Some("a"), None, r#""A""#),
                    (Some("b"), None, "}"),
                ],
                vec![RawCall::new("", "now", r#"{"tz": "A"}"#)],
            ),
        ];
        for (parts, read) in cases {
            let fragments: Vec<(u64, Value)> = parts
                .iter()
                .map(|(id, name, arguments)| (0, fragment(*id, *name, arguments)))
                .collect();
            assert_eq!(folded(&fragments), read, "{parts:?}");
        }
    }

    // Python: tests/test_providers/test_tool_call_handoff.py::TestCallIds::test_a_server_id_that_is_not_text_becomes_text
    #[test]
    fn a_server_id_that_is_not_text_becomes_text() {
        let body = br#"{"choices": [{"index": 0, "finish_reason": "tool_calls",
            "message": {"tool_calls": [
                {"id": 7, "function": {"name": "now", "arguments": "{}"}},
                {"id": 0, "function": {"name": "now", "arguments": "{}"}}]}}]}"#;
        assert_eq!(
            response(body, "sk-1", false, "m").unwrap().calls,
            [
                RawCall::new("7", "now", "{}"),
                RawCall::new("", "now", "{}")
            ]
        );
    }

    /// The calls a stream of one chunk per `fragments`, each a `tool_calls`
    /// entry of index 0, folds into, or why its read failed.
    fn streamed(fragments: &[Value]) -> Result<Vec<RawCall>, OpenAiError> {
        let mut folding = Folding::default();
        for fragment in fragments {
            let chunk = serde_json::json!({
                "choices": [{ "index": 0, "delta": { "tool_calls": [fragment] } }]
            });
            folding.take(&chunk.to_string(), "sk-1", &mut VecDeque::new())?;
        }
        let ids = fragments.iter().filter_map(|f| f["id"].as_str()).collect();
        folding
            .finish()
            .map(|(calls, _)| normalize_minted_ids(calls, &ids))
    }

    // Python: providers/ai/openai_dialect.py ToolCallSlots: an id Python reads as
    // false is no id, and the read goes on.
    #[test]
    fn a_streamed_id_that_reads_as_false_is_no_id() {
        for id in ["0", "0.0", "false", "[]", "{}"] {
            let entry = serde_json::json!({
                "index": 0, "id": serde_json::from_str::<Value>(id).unwrap(),
                "function": { "name": "now", "arguments": "{}" },
            });
            assert_eq!(
                streamed(&[entry]),
                Ok(vec![RawCall::new("", "now", "{}")]),
                "{id}"
            );
        }
    }

    // Python: tests/test_providers/test_chat_response_reading.py::test_an_id_that_is_no_text_on_a_new_call_fails_the_read
    #[test]
    fn an_id_that_is_no_text_on_a_new_call_fails_the_read() {
        let named = |id: Value| {
            serde_json::json!({ "index": 0, "id": id, "type": "function",
                "function": { "name": "lookup", "arguments": "{}" } })
        };
        let entries = [
            named(serde_json::json!(7)),
            named(serde_json::json!(true)),
            named(serde_json::json!([1])),
            serde_json::json!({ "index": 0, "id": { "a": 1 }, "type": "custom" }),
        ];
        for entry in entries {
            let read = streamed(std::slice::from_ref(&entry));
            assert!(
                matches!(read, Err(OpenAiError::Malformed { .. })),
                "{entry}: {read:?}"
            );
        }
    }

    // Python: tests/test_providers/test_chat_response_reading.py::test_a_body_that_is_no_object_is_no_completion
    #[test]
    fn a_body_that_is_no_object_is_no_completion() {
        for body in ["[]", "[[]]"] {
            assert_eq!(
                response(body.as_bytes(), "sk-1", false, "m"),
                Err(OpenAiError::Malformed {
                    reason: "not a chat completion".to_owned()
                }),
                "{body}"
            );
        }
    }

    /// Python's `NAMED`: a whole call to `lookup` on index 1, under no id.
    fn named() -> Value {
        serde_json::json!({ "index": 1, "type": "function",
            "function": { "name": "lookup", "arguments": "{}" } })
    }

    // Python: tests/test_providers/test_chat_response_reading.py::test_an_id_that_is_no_text_another_entry_holds_is_not_taken_again
    #[test]
    fn an_id_that_is_no_text_another_entry_holds_is_not_taken_again() {
        let pairs = [
            (serde_json::json!(1), serde_json::json!(true)),
            (serde_json::json!(true), serde_json::json!(1)),
            (serde_json::json!(7), serde_json::json!(7.0)),
            (serde_json::json!(7), serde_json::json!(7)),
        ];
        for (held, again) in pairs {
            let entry = serde_json::json!({ "index": 0, "id": held, "type": "custom" });
            let mut call = named();
            call["id"] = again.clone();
            assert_eq!(
                streamed(&[entry, call]),
                Ok(vec![RawCall::new("", "lookup", "{}")]),
                "{held} then {again}"
            );
        }
    }

    // Python: tests/test_providers/test_chat_response_reading.py::test_a_call_named_under_an_id_that_is_no_text_fails_where_it_is_named
    #[test]
    fn a_call_named_under_an_id_that_is_no_text_fails_where_it_is_named() {
        let pairs = [
            (serde_json::json!(7), Value::Null),
            (serde_json::json!(7), serde_json::json!(7)),
            (serde_json::json!(7), serde_json::json!(7.0)),
            (serde_json::json!(1), serde_json::json!(true)),
            (serde_json::json!(true), serde_json::json!(1)),
        ];
        let chunk = |call: &Value| {
            serde_json::json!({ "choices": [{ "index": 0, "delta": { "tool_calls": [call] } }] })
                .to_string()
        };
        for (held, again) in pairs {
            let mut folding = Folding::default();
            let entry = serde_json::json!({ "index": 0, "id": held, "type": "custom" });
            let mut call = named();
            call["index"] = 0.into();
            call["id"] = again.clone();
            assert_eq!(
                folding.take(&chunk(&entry), "sk-1", &mut VecDeque::new()),
                Ok(())
            );
            let read = folding.take(&chunk(&call), "sk-1", &mut VecDeque::new());
            assert!(
                matches!(read, Err(OpenAiError::Malformed { .. })),
                "{held} then {again}: {read:?}"
            );
        }
    }

    /// A whole call to `lookup` under the server's id `c1`, Python's `WHOLE`.
    fn whole_call() -> Value {
        serde_json::json!({ "index": 0, "id": "c1", "type": "function",
            "function": { "name": "lookup", "arguments": "{}" } })
    }

    // Python: tests/test_providers/test_chat_response_reading.py::test_a_nameless_call_under_an_id_that_is_no_text_takes_no_name
    #[test]
    fn a_nameless_call_under_an_id_that_is_no_text_takes_no_name() {
        for id in [
            serde_json::json!(0),
            serde_json::json!([]),
            serde_json::json!({}),
            serde_json::json!(false),
        ] {
            let second = serde_json::json!({ "index": 0, "id": id,
                "function": { "arguments": "{\"q\": 1}" } });
            assert_eq!(
                streamed(&[whole_call(), second]),
                Ok(vec![
                    RawCall::new("c1", "lookup", "{}"),
                    RawCall::new("", "", r#"{"q": 1}"#),
                ]),
                "{id}"
            );
        }
    }

    /// Under no id, or the held call's own, a nameless fragment opening
    /// another JSON object is a second call to the held call's tool.
    #[test]
    fn a_nameless_call_under_no_id_or_the_held_one_s_takes_its_name() {
        for id in [Value::Null, serde_json::json!(""), serde_json::json!("c1")] {
            let second = serde_json::json!({ "index": 0, "id": id,
                "function": { "arguments": "{\"q\": 1}" } });
            assert_eq!(
                streamed(&[whole_call(), second]),
                Ok(vec![
                    RawCall::new("c1", "lookup", "{}"),
                    RawCall::new("", "lookup", r#"{"q": 1}"#),
                ]),
                "{id}"
            );
        }
    }

    // Python: tests/test_providers/test_chat_response_reading.py::test_an_id_that_is_no_text_on_a_call_already_named_is_ignored
    #[test]
    fn an_id_that_is_no_text_on_a_call_already_named_is_ignored() {
        for id in [
            serde_json::json!(7),
            serde_json::json!(true),
            serde_json::json!([1]),
            serde_json::json!({ "a": 1 }),
        ] {
            let first = serde_json::json!({ "index": 0, "type": "function",
                "function": { "name": "lookup", "arguments": "{\"q\"" } });
            let second = serde_json::json!({ "index": 0, "id": id,
                "function": { "arguments": ": 1}" } });
            assert_eq!(
                streamed(&[first, second]),
                Ok(vec![RawCall::new("", "lookup", r#"{"q": 1}"#)]),
                "{id}"
            );
        }
    }

    // Python: tests/test_providers/test_chat_response_reading.py::test_an_id_that_is_no_text_on_an_entry_without_a_function_is_no_call
    #[test]
    fn an_id_that_is_no_text_on_an_entry_without_a_function_is_no_call() {
        for id in [serde_json::json!(7), serde_json::json!(true)] {
            let entry = serde_json::json!({ "index": 0, "id": id, "type": "custom" });
            let named = serde_json::json!({ "index": 1, "id": "c2", "type": "function",
                "function": { "name": "lookup", "arguments": "{}" } });
            assert_eq!(
                streamed(&[entry, named]),
                Ok(vec![RawCall::new("c2", "lookup", "{}")]),
                "{id}"
            );
        }
    }

    // Python: providers/ai/openai_dialect.py message_tool_calls: an entry with no
    // function is no call, whole as in a stream.
    #[test]
    fn a_whole_answer_s_entry_with_no_function_is_no_call() {
        let body = br#"{"choices": [{"index": 0, "finish_reason": "tool_calls",
            "message": {"tool_calls": [
                {"id": "a", "type": "custom", "custom": {"name": "x"}},
                {"id": "b", "function": {"name": "now", "arguments": "{}"}}]}}]}"#;
        assert_eq!(
            response(body, "sk-1", false, "m").unwrap().calls,
            [RawCall::new("b", "now", "{}")]
        );
    }

    // Python: providers/ai/openai_dialect.py ToolCallSlots._adopt_server_id: an
    // id another call of the response holds stays with that call.
    #[test]
    fn a_server_id_another_call_holds_is_not_taken() {
        let calls = folded(&[
            (0, fragment(Some("a"), Some("now"), "{}")),
            (1, fragment(Some("a"), Some("later"), "{}")),
        ]);
        assert_eq!(
            calls,
            [
                RawCall::new("a", "now", "{}"),
                RawCall::new("", "later", "{}"),
            ]
        );
    }

    /// RFC §6.4: a streamed call's fragments are folded by their index,
    /// its id and name kept from the first that gives them, its arguments'
    /// text joined; the usage comes in the last chunk.
    // Python: providers/ai/openai_dialect.py ToolCallSlots: each call in the
    // order it came, whatever index it names.
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
        let mut ready = VecDeque::new();
        for chunk in chunks {
            folding.take(chunk, "sk-1", &mut ready).unwrap();
        }
        let text: Vec<_> = ready
            .into_iter()
            .filter_map(|e| match e {
                StreamEvent::TextDelta(delta) => Some(Segment::Text(delta.text)),
                StreamEvent::ThinkingDelta(delta) => Some(Segment::Thinking(delta.thinking)),
                StreamEvent::ToolCall(_) | StreamEvent::ToolCallDelta(_) | StreamEvent::Done(_) => {
                    None
                }
            })
            .collect();
        let (calls, done) = folding.finish().unwrap();
        assert_eq!(
            text,
            [
                Segment::Text("Let me ".to_owned()),
                Segment::Text("check.".to_owned())
            ]
        );
        assert_eq!(
            calls,
            [
                RawCall::new("call_b", "notify", ""),
                RawCall::new("call_a", "lookup", r#"{"order": "42"}"#),
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
        let failed = folding.take(
            r#"{"error": {"message": "overloaded, key sk-1"}}"#,
            "sk-1",
            &mut VecDeque::new(),
        );
        assert_eq!(
            failed,
            Err(OpenAiError::StreamFailed {
                message: "overloaded, key [removed]".to_owned(),
                status: None,
            })
        );
    }

    #[test]
    fn a_malformed_answer_s_reason_holds_no_key() {
        let body = br#"{"choices": "sk-1"}"#;
        let error = response(body, "sk-1", false, "m").unwrap_err();
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
            reasoning: Vec::new(),
            tools: vec![],
            temperature: None,
            max_tokens: None,
            reasoning_effort: None,
            thinking_off: false,
            thinking_switch: None,
            response_schema: None,
        };
        let config = OpenAiConfig {
            base_url: Some("http://localhost/v1".into()),
            vision: true,
            ..OpenAiConfig::new("", "vision-fixture")
        };
        let chat = serde_json::to_value(CompletionRequest::new(
            &config,
            &request,
            true,
            crate::dialect::OPENAI_CHAT,
        ))
        .unwrap();
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

    // Python: tests/test_providers/test_chat_request.py::test_a_message_s_text_and_images_keep_their_order
    // (RMK-1191)
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
        let chat = serde_json::to_value(CompletionRequest::new(
            &config,
            &request,
            true,
            crate::dialect::OPENAI_CHAT,
        ))
        .unwrap();
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

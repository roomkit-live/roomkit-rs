//! What a generation answers: Python's `AIResponse`, `AIToolCall` and the
//! events of a structured stream, and the one reading of a call's
//! arguments every provider applies (RFC §6.4).

use std::collections::HashSet;

use roomkit::model::Metadata;
use serde_json::Value;

use super::context::AiThinkingPart;

/// The endings under which a response ran out of room: the output cap, the
/// context window filling up mid-answer, in each provider's word.
const OUT_OF_ROOM: [&str; 4] = [
    "length",
    "max_tokens",
    "model_context_window_exceeded",
    "model_length",
];

/// The endings beside running out of room that can stop a call
/// mid-arguments: a generation error, a content filter, a refusal.
const CUTTING: [&str; 3] = ["error", "content_filter", "refusal"];

/// A whole generation (RFC §6.7): Python's `AIResponse(*, content,
/// finish_reason=None, usage={}, metadata={}, tool_calls=[])`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AiResponse {
    /// Its text.
    pub content: String,
    /// Why it stopped, in the provider's own word (`stop`, `tool_calls`,
    /// `length`...); `None` when it gave none.
    pub finish_reason: Option<String>,
    /// What it used: integer counters by name (`input_tokens`,
    /// `output_tokens`...), which the turn's record sums.
    pub usage: Metadata,
    /// What the response says of itself, `model` the model that answered
    /// and `refusal` the refusal it carried (Python's `answered_by`), and
    /// provider-owned data for replaying a tool round, which the loop copies
    /// to the assistant message without those two keys, never to the room.
    pub metadata: Metadata,
    /// The calls it made, in order.
    pub tool_calls: Vec<AiToolCall>,
    /// All the model's reasoning text (RFC §6.7), none when it showed none.
    pub thinking: Option<String>,
    /// The last signature of its reasoning (RFC §6.4).
    pub thinking_signature: Option<String>,
    /// The reasoning blocks to replay, in the order the vendor sent them,
    /// each with its signature or redacted data (RFC §6.4); `None` for a
    /// provider whose reasoning has no blocks. A block the response cut before
    /// its signature is not among them.
    pub thinking_parts: Option<Vec<AiThinkingPart>>,
}

impl AiResponse {
    /// A response saying `content`, with no call, ending for no reason.
    pub fn new(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            finish_reason: None,
            usage: Metadata::new(),
            metadata: Metadata::new(),
            tool_calls: Vec::new(),
            thinking: None,
            thinking_signature: None,
            thinking_parts: None,
        }
    }
}

/// A response's reasoning blocks (RFC §6.4): its `thinking_parts`, or for a
/// provider that reports one block its `thinking` with its signature. Python's
/// `thinking_parts_of(response)`.
#[must_use]
pub fn thinking_parts_of(response: &AiResponse) -> Vec<AiThinkingPart> {
    if let Some(parts) = &response.thinking_parts {
        return parts.clone();
    }
    let thinking = response.thinking.as_deref().unwrap_or_default();
    let signature = response.thinking_signature.as_deref().unwrap_or_default();
    if thinking.is_empty() && signature.is_empty() {
        return Vec::new();
    }
    vec![AiThinkingPart {
        signature: response.thinking_signature.clone(),
        ..AiThinkingPart::new(thinking)
    }]
}

/// A call the model made (RFC §6.4 "What a provider hands the loop for a
/// call"): Python's `AIToolCall(*, id, name, arguments={}, metadata={},
/// partial=False, garbled=False, intent=None)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AiToolCall {
    /// The call's id, unique in its response ([`CallIds`]): the loop refuses a
    /// second call of a round under it (RFC §12.4).
    pub id: String,
    /// The tool called.
    pub name: String,
    /// Its arguments, a mapping ([`tool_arguments`]).
    pub arguments: Metadata,
    /// Provider-owned data of the call.
    pub metadata: Metadata,
    /// The call never runs: its arguments do not read, or the response cut
    /// them short.
    pub partial: bool,
    /// The model wrote the arguments unreadable, where the response did not
    /// cut them: a `partial` call the model is told to write again.
    pub garbled: bool,
    /// What the call does, in the sentence the model wrote for the person
    /// (RFC §6.4); the loop takes it out of the arguments.
    pub intent: Option<String>,
}

impl AiToolCall {
    /// The call `id` of the tool `name`, with no arguments.
    pub fn new(id: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            arguments: Metadata::new(),
            metadata: Metadata::new(),
            partial: false,
            garbled: false,
            intent: None,
        }
    }

    /// The call `id` of `name`, its arguments the text `raw` the wire
    /// carried, read by Python's rules for a response that ended for
    /// `finish_reason`, `last` when it is the response's last call: its
    /// arguments a mapping ([`tool_arguments`]), `partial` when they do not
    /// read or the response cut them short, `garbled` when they do not
    /// read and nothing cut them. For a provider reading its wire.
    #[doc(hidden)]
    pub fn read(
        id: impl Into<String>,
        name: impl Into<String>,
        raw: &str,
        finish_reason: Option<&str>,
        last: bool,
    ) -> Self {
        let unreadable = read_arguments(raw).is_none();
        let cut = last && cuts_calls(finish_reason) && (unreadable || raw.trim().is_empty());
        Self {
            arguments: tool_arguments(raw),
            partial: unreadable || cut,
            garbled: unreadable && !cut,
            ..Self::new(id, name)
        }
    }
}

/// The calls `raw`, each its id, its tool and its argument text, of a
/// response that ended for `finish_reason`, read by [`AiToolCall::read`].
/// For a provider reading its wire.
#[doc(hidden)]
pub fn read_tool_calls(
    raw: Vec<(String, String, String)>,
    finish_reason: Option<&str>,
) -> Vec<AiToolCall> {
    let last = raw.len().checked_sub(1);
    raw.into_iter()
        .enumerate()
        .map(|(position, (id, name, arguments))| {
            AiToolCall::read(id, name, &arguments, finish_reason, Some(position) == last)
        })
        .collect()
}

/// Hands each call of one response its id: the server's when it gave one no
/// earlier call of the response took, a minted one otherwise (`call_<name>_<12
/// hex digits>`, unique across turns). Results, cuts and a call's rows all find
/// a call by its id, so a provider reading its wire gives two calls two ids
/// (RFC §6.4): the loop refuses a second call of a round under an id. Python's
/// `CallIds`.
///
/// ```
/// use roomkit_ai::CallIds;
///
/// let mut ids = CallIds::new();
/// assert_eq!(ids.call("c1", "now"), "c1");
/// assert!(ids.call("c1", "now").starts_with("call_now_"));
/// assert!(ids.call("", "now").starts_with("call_now_"));
/// ```
#[derive(Clone, Debug, Default)]
pub struct CallIds {
    taken: HashSet<String>,
}

impl CallIds {
    /// No id taken yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// The id of the response's next call, to the tool `name`, its server's
    /// `server_id` empty when it gave none. Python's `ids(server_id, name)`:
    /// Rust has no object to call.
    pub fn call(&mut self, server_id: &str, name: &str) -> String {
        let id = if !server_id.is_empty() && !self.taken.contains(server_id) {
            server_id.to_owned()
        } else {
            minted_call_id(name)
        };
        self.taken.insert(id.clone());
        id
    }
}

/// An id for a call to `name` its server gave none, unique across turns:
/// Python's `minted_call_id`.
fn minted_call_id(name: &str) -> String {
    // The 48 high bits of a v4 UUID are random: uuid4().hex[:12].
    let random = uuid::Uuid::new_v4().as_u128() >> 80;
    format!("call_{name}_{random:012x}")
}

/// A call's arguments as a mapping, never an error (RFC §6.4): Python's
/// `tool_arguments(raw)`. No arguments (nothing, blank text, JSON `null`)
/// are `{}`; text that parses to a JSON object is that object; anything
/// else (invalid JSON, an array, a fragment the output cap cut) is kept
/// whole under `raw`.
///
/// ```
/// use roomkit_ai::tool_arguments;
/// use serde_json::json;
///
/// assert_eq!(json!(tool_arguments(r#"{"order": "42"}"#)), json!({ "order": "42" }));
/// assert_eq!(json!(tool_arguments(" null ")), json!({}));
/// assert_eq!(json!(tool_arguments("[1, 2]")), json!({ "raw": "[1, 2]" }));
/// ```
pub fn tool_arguments(raw: &str) -> Metadata {
    read_arguments(raw).unwrap_or_else(|| {
        let mut kept = Metadata::new();
        kept.insert("raw".to_owned(), Value::String(raw.to_owned()));
        kept
    })
}

/// `raw` as a mapping, `None` when it does not read as one.
fn read_arguments(raw: &str) -> Option<Metadata> {
    let text = raw.trim();
    if text.is_empty() {
        return Some(Metadata::new());
    }
    match serde_json::from_str::<Value>(text) {
        Ok(Value::Object(arguments)) => Some(arguments),
        Ok(Value::Null) => Some(Metadata::new()),
        Ok(_) | Err(_) => None,
    }
}

/// Whether a response that ended for `finish_reason` may have stopped its
/// last call mid-arguments: it ran out of room, a filter or a refusal
/// stopped it, or its stream ended with no reason at all.
fn cuts_calls(finish_reason: Option<&str>) -> bool {
    finish_reason.is_none_or(|reason| {
        ran_out_of_room(Some(reason)) || CUTTING.contains(&reason.to_ascii_lowercase().as_str())
    })
}

/// The endings of a response whose call its provider did not hand over:
/// one it could not parse, or one to a tool the request did not enable.
const MALFORMED_CALL: [&str; 2] = ["malformed_function_call", "unexpected_tool_call"];

/// Whether a response ended on a call its provider did not hand over:
/// Python's `is_malformed_call`.
pub(crate) fn is_malformed_call(finish_reason: Option<&str>) -> bool {
    finish_reason.is_some_and(|reason| {
        MALFORMED_CALL
            .iter()
            .any(|malformed| reason.eq_ignore_ascii_case(malformed))
    })
}

/// Whether a response that ended for `finish_reason` ran out of room.
pub(crate) fn ran_out_of_room(finish_reason: Option<&str>) -> bool {
    finish_reason.is_some_and(|reason| OUT_OF_ROOM.contains(&reason.to_ascii_lowercase().as_str()))
}

/// One event of a structured stream (RFC §6.7, Appendix A.9): Python's
/// `StreamEvent`, its classes tagged by their `type`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamEvent {
    /// Text the model wrote, as it came.
    TextDelta(StreamTextDelta),
    /// A call the model made.
    ToolCall(StreamToolCall),
    /// The generation ended.
    Done(StreamDone),
    /// A piece of the model's reasoning, before its text (RFC §6.7).
    ThinkingDelta(StreamThinkingDelta),
    /// The opening or an argument fragment of a call, before its whole value.
    ToolCallDelta(StreamToolCallDelta),
}

/// A streaming call fragment: Python's `StreamToolCallDelta`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StreamToolCallDelta {
    /// The stable identity of the call throughout this response.
    pub id: String,
    /// The tool being called.
    pub name: String,
    /// The provider's content block index.
    pub index: u32,
    /// An incremental fragment of the JSON arguments; empty at opening.
    pub arguments_delta: String,
}

impl StreamToolCallDelta {
    /// Open a call with no arguments received yet.
    pub fn new(id: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            ..Self::default()
        }
    }
}

/// A piece of a model's reasoning: Python's `StreamThinkingDelta(*, thinking,
/// signature=None, block=None, redacted=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StreamThinkingDelta {
    /// The reasoning text; empty for a delta that only carries a signature.
    pub thinking: String,
    /// The vendor's opaque token for the block, which it wants back with
    /// the block when a round is replayed.
    pub signature: Option<String>,
    /// The reasoning block the delta belongs to, for a vendor whose
    /// reasoning comes in blocks each with its own signature; `None` for a
    /// single block per round.
    pub block: Option<u32>,
    /// A block the vendor redacted: its opaque data, replayed as received.
    pub redacted: Option<String>,
}

impl StreamThinkingDelta {
    /// The delta `thinking`.
    pub fn new(thinking: impl Into<String>) -> Self {
        Self {
            thinking: thinking.into(),
            ..Self::default()
        }
    }
}

/// Text the model wrote: Python's `StreamTextDelta(*, text)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamTextDelta {
    /// The text.
    pub text: String,
}

impl StreamTextDelta {
    /// The delta `text`.
    pub fn new(text: impl Into<String>) -> Self {
        Self { text: text.into() }
    }
}

/// A call the model made: Python's `StreamToolCall`, the fields of
/// [`AiToolCall`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamToolCall {
    /// The call's id.
    pub id: String,
    /// The tool called.
    pub name: String,
    /// Its arguments, a mapping.
    pub arguments: Metadata,
    /// Provider-owned data of the call.
    pub metadata: Metadata,
    /// The call never runs.
    pub partial: bool,
    /// The model wrote the arguments unreadable.
    pub garbled: bool,
    /// What the call does, for the person.
    pub intent: Option<String>,
}

impl StreamToolCall {
    /// The call `id` of the tool `name`, with no arguments.
    pub fn new(id: impl Into<String>, name: impl Into<String>) -> Self {
        AiToolCall::new(id, name).into()
    }
}

impl From<AiToolCall> for StreamToolCall {
    fn from(call: AiToolCall) -> Self {
        let AiToolCall {
            id,
            name,
            arguments,
            metadata,
            partial,
            garbled,
            intent,
        } = call;
        Self {
            id,
            name,
            arguments,
            metadata,
            partial,
            garbled,
            intent,
        }
    }
}

impl From<StreamToolCall> for AiToolCall {
    fn from(call: StreamToolCall) -> Self {
        let StreamToolCall {
            id,
            name,
            arguments,
            metadata,
            partial,
            garbled,
            intent,
        } = call;
        Self {
            id,
            name,
            arguments,
            metadata,
            partial,
            garbled,
            intent,
        }
    }
}

/// How a generation ended: Python's `StreamDone(*, finish_reason=None,
/// usage={}, metadata={})`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StreamDone {
    /// Why it stopped.
    pub finish_reason: Option<String>,
    /// What it used.
    pub usage: Metadata,
    /// As [`AiResponse::metadata`]: what the response says of itself
    /// (`model`, `refusal`) and provider-owned data for replaying a tool
    /// round, which the loop keeps without those two keys.
    pub metadata: Metadata,
}

/// The events `response` stands for: each reasoning block with its signature
/// or redacted data (a signature alone too), its text, its calls, then its
/// end, as Python's `response_stream_events` wraps a whole response.
pub(crate) fn response_events(response: AiResponse) -> Vec<StreamEvent> {
    let blocks = thinking_parts_of(&response);
    let named = response.thinking_parts.is_some();
    let AiResponse {
        content,
        finish_reason,
        usage,
        metadata,
        tool_calls,
        ..
    } = response;
    let mut events: Vec<StreamEvent> = blocks
        .into_iter()
        .enumerate()
        .map(|(block, part)| {
            StreamEvent::ThinkingDelta(StreamThinkingDelta {
                thinking: part.thinking,
                signature: part.signature,
                redacted: part.redacted,
                block: if named {
                    u32::try_from(block).ok()
                } else {
                    None
                },
            })
        })
        .collect();
    if !content.is_empty() {
        events.push(StreamEvent::TextDelta(StreamTextDelta { text: content }));
    }
    events.extend(
        tool_calls
            .into_iter()
            .map(|call| StreamEvent::ToolCall(call.into())),
    );
    events.push(StreamEvent::Done(StreamDone {
        finish_reason,
        usage,
        metadata,
    }));
    events
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    // Python: tests/test_providers/test_tool_call_handoff.py::TestCallIds::test_every_call_of_a_response_gets_its_own_id
    #[test]
    fn every_call_of_a_response_gets_its_own_id() {
        let mut ids = CallIds::new();
        let handed = ["c1", "", "", "c1"].map(|id| ids.call(id, "now"));
        let distinct: HashSet<&String> = handed.iter().collect();
        let minted = handed.get(1..).unwrap_or_default();
        assert_eq!(
            (handed.first().map(String::as_str), distinct.len()),
            (Some("c1"), 4)
        );
        assert!(
            minted.iter().all(|id| id.starts_with("call_now_")
                && id.len() == "call_now_".len() + 12
                && id.bytes().skip(9).all(|byte| byte.is_ascii_hexdigit())),
            "{handed:?}"
        );
    }

    // Python: tests/test_providers/test_stream_adapter.py::test_a_wrapped_response_keeps_its_thinking_signature_and_call_metadata
    // (its thinking): a response read whole reaches the loop as a stream,
    // its reasoning first; none is no event.
    #[test]
    fn a_wrapped_response_hands_its_reasoning_first() {
        let response = AiResponse {
            thinking: Some("Weighing.".to_owned()),
            ..AiResponse::new("42.")
        };
        let events = response_events(response);
        assert_eq!(
            events.first(),
            Some(&StreamEvent::ThinkingDelta(StreamThinkingDelta::new(
                "Weighing."
            )))
        );
        assert_eq!(
            events.get(1),
            Some(&StreamEvent::TextDelta(StreamTextDelta::new("42.")))
        );
        let empty = AiResponse {
            thinking: Some(String::new()),
            ..AiResponse::new("42.")
        };
        assert!(
            !response_events(empty)
                .iter()
                .any(|event| matches!(event, StreamEvent::ThinkingDelta(_)))
        );
    }

    // Python: tests/test_providers/test_thinking_blocks.py::test_a_wrapped_response_hands_each_block_on
    #[test]
    fn a_wrapped_response_hands_each_block_on() {
        let response = AiResponse {
            thinking_parts: Some(vec![
                AiThinkingPart {
                    signature: Some("S0".to_owned()),
                    ..AiThinkingPart::new("a")
                },
                AiThinkingPart {
                    redacted: Some("RRR".to_owned()),
                    ..AiThinkingPart::new("")
                },
            ]),
            ..AiResponse::new("")
        };
        let events = response_events(response);
        assert_eq!(
            events.first(),
            Some(&StreamEvent::ThinkingDelta(StreamThinkingDelta {
                block: Some(0),
                signature: Some("S0".to_owned()),
                ..StreamThinkingDelta::new("a")
            }))
        );
        assert_eq!(
            events.get(1),
            Some(&StreamEvent::ThinkingDelta(StreamThinkingDelta {
                block: Some(1),
                redacted: Some("RRR".to_owned()),
                ..StreamThinkingDelta::new("")
            }))
        );
    }

    // Python: tests/test_providers/test_stream_adapter.py::test_a_wrapped_response_keeps_its_thinking_signature_and_call_metadata
    // (its signature, a signature alone too)
    #[test]
    fn a_response_with_one_block_hands_its_signature_with_it() {
        let response = AiResponse {
            thinking: Some("Look.".to_owned()),
            thinking_signature: Some("SIG".to_owned()),
            ..AiResponse::new("")
        };
        assert_eq!(
            thinking_parts_of(&response),
            [AiThinkingPart {
                signature: Some("SIG".to_owned()),
                ..AiThinkingPart::new("Look.")
            }]
        );
        let alone = AiResponse {
            thinking_signature: Some("SIG".to_owned()),
            ..AiResponse::new("")
        };
        assert_eq!(
            response_events(alone).first(),
            Some(&StreamEvent::ThinkingDelta(StreamThinkingDelta {
                signature: Some("SIG".to_owned()),
                ..StreamThinkingDelta::new("")
            }))
        );
    }

    fn read(calls: &[(&str, &str)], finish_reason: &str) -> Vec<(String, Value, bool, bool)> {
        let raw = calls
            .iter()
            .map(|(id, arguments)| {
                (
                    (*id).to_owned(),
                    "lookup".to_owned(),
                    (*arguments).to_owned(),
                )
            })
            .collect();
        read_tool_calls(raw, Some(finish_reason))
            .into_iter()
            .map(|call| {
                (
                    call.id,
                    Value::Object(call.arguments),
                    call.partial,
                    call.garbled,
                )
            })
            .collect()
    }

    /// RFC §6.4: arguments reach the loop as a mapping, never as an error;
    /// arguments that do not read make a garbled call.
    #[test]
    fn arguments_read_as_a_mapping_whatever_the_wire_carried() {
        let calls = [
            ("a", r#"{"order": "42"}"#),
            ("b", ""),
            ("c", "null"),
            ("d", "[1, 2]"),
        ];
        assert_eq!(
            read(&calls, "tool_calls"),
            vec![
                ("a".to_owned(), json!({ "order": "42" }), false, false),
                ("b".to_owned(), json!({}), false, false),
                ("c".to_owned(), json!({}), false, false),
                ("d".to_owned(), json!({ "raw": "[1, 2]" }), true, true),
            ]
        );
    }

    /// RFC §6.4: only the last call can be cut, and a call whose arguments
    /// arrived and read runs, the response cut over it or not.
    #[test]
    fn only_the_last_call_of_a_cut_response_with_no_argument_text_is_cut() {
        let calls = [("a", ""), ("b", r#"{"order": "1"}"#), ("c", "")];
        let marks: Vec<(bool, bool)> = read(&calls, "length")
            .into_iter()
            .map(|(_, _, partial, garbled)| (partial, garbled))
            .collect();
        assert_eq!(marks, [(false, false), (false, false), (true, false)]);
    }
}

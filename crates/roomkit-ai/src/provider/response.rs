//! What a generation answers: Python's `AIResponse`, `AIToolCall` and the
//! events of a structured stream, and the one reading of a call's
//! arguments every provider applies (RFC §6.4).

use roomkit::model::Metadata;
use serde_json::Value;

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
#[derive(Clone, Debug, PartialEq)]
pub struct AiResponse {
    /// Its text.
    pub content: String,
    /// Why it stopped, in the provider's own word (`stop`, `tool_calls`,
    /// `length`...); `None` when it gave none.
    pub finish_reason: Option<String>,
    /// What it used: integer counters by name (`input_tokens`,
    /// `output_tokens`...), which the turn's record sums.
    pub usage: Metadata,
    /// Provider-owned data for replaying a tool round, which the loop
    /// copies to the assistant message, never to the room.
    pub metadata: Metadata,
    /// The calls it made, in order.
    pub tool_calls: Vec<AiToolCall>,
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
        }
    }
}

/// A call the model made (RFC §6.4 "What a provider hands the loop for a
/// call"): Python's `AIToolCall(*, id, name, arguments={}, metadata={},
/// partial=False, garbled=False, intent=None)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AiToolCall {
    /// The call's id; the loop gives one of its own to a call with none or
    /// another's.
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

/// Whether a response that ended for `finish_reason` ran out of room.
pub(crate) fn ran_out_of_room(finish_reason: Option<&str>) -> bool {
    finish_reason.is_some_and(|reason| OUT_OF_ROOM.contains(&reason.to_ascii_lowercase().as_str()))
}

/// One event of a structured stream (RFC §6.7, Appendix A.9): Python's
/// `StreamEvent`, its classes tagged by their `type`.
#[derive(Clone, Debug, PartialEq)]
pub enum StreamEvent {
    /// Text the model wrote, as it came.
    TextDelta(StreamTextDelta),
    /// A call the model made.
    ToolCall(StreamToolCall),
    /// The generation ended.
    Done(StreamDone),
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
    /// Provider-owned data for replaying a tool round.
    pub metadata: Metadata,
}

/// The events `response` stands for: its text, its calls, then its end,
/// as Python's `response_stream_events` wraps a whole response.
pub(crate) fn response_events(response: AiResponse) -> Vec<StreamEvent> {
    let AiResponse {
        content,
        finish_reason,
        usage,
        metadata,
        tool_calls,
    } = response;
    let mut events = Vec::new();
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

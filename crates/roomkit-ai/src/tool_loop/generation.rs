//! One generation, read from its provider's stream, and what the loop reads
//! of its calls (RFC §6.4 "What a provider hands the loop for a call").

use std::collections::HashSet;

use roomkit::model::{Metadata, ToolCall};
use serde_json::Value;

use super::TurnItem;
use crate::provider::{
    AiProvider, AiRequest, Done, ProviderError, ProviderEvent, ProviderToolCall,
};
use crate::stream::{Yielder, next};

/// The endings under which a response ran out of room: the output cap, the
/// context window filling up mid-answer, in each provider's word.
const OUT_OF_ROOM: [&str; 4] = [
    "length",
    "max_tokens",
    "model_context_window_exceeded",
    "model_length",
];

/// The endings that can stop a call mid-arguments: out of room, a content
/// filter, a refusal, a generation error.
const CUTTING: [&str; 3] = ["error", "content_filter", "refusal"];

/// A generation, read whole: its text, its calls, how it ended.
#[derive(Debug)]
pub(super) struct Generation {
    pub(super) text: String,
    pub(super) calls: Vec<Called>,
    pub(super) done: Done,
}

/// A call as the loop reads it.
#[derive(Clone, Debug)]
pub(super) struct Called {
    /// The call, its id unique in the response, its arguments a mapping.
    pub(super) call: ToolCall,
    /// Why it never runs, when it does not (RFC §6.4).
    pub(super) partial: Option<Partial>,
}

/// Why a call never runs, whatever its tool.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Partial {
    /// The response was cut short over its arguments.
    Cut,
    /// The model wrote arguments that read as no object.
    Unreadable,
}

impl Generation {
    /// Reads `provider`'s answer to `request`, the `round`-th generation of
    /// the turn, handing its text to `out` as it comes.
    ///
    /// # Errors
    ///
    /// Returns the provider's error, or one saying its stream ended without
    /// a done event.
    pub(super) async fn read<P: AiProvider>(
        provider: &P,
        request: &AiRequest,
        round: u32,
        out: &Yielder<Result<TurnItem, ProviderError>>,
    ) -> Result<Self, ProviderError> {
        let mut stream = provider.generate(request);
        let mut text = String::new();
        let mut calls = Vec::new();
        while let Some(event) = next(&mut stream).await {
            match event? {
                ProviderEvent::Text(delta) => {
                    text.push_str(&delta);
                    out.give(Ok(TurnItem::Text(delta))).await;
                }
                ProviderEvent::ToolCall(call) => calls.push(call),
                ProviderEvent::Done(done) => {
                    let calls = read_calls(calls, done.finish_reason.as_deref(), round);
                    return Ok(Self { text, calls, done });
                }
            }
        }
        Err("the provider's response ended before its done event".into())
    }
}

/// The calls of a response that ended for `finish_reason`, as the loop reads
/// them: each with an id no other carries (`call_<round>_<n>` for one the
/// provider gave empty or twice), its arguments a mapping, and, for one that
/// never runs, why.
fn read_calls(
    calls: Vec<ProviderToolCall>,
    finish_reason: Option<&str>,
    round: u32,
) -> Vec<Called> {
    let last = calls.len().checked_sub(1);
    let mut ids = HashSet::new();
    calls
        .into_iter()
        .enumerate()
        .map(|(position, raw)| {
            let id = if raw.id.is_empty() || !ids.insert(raw.id.clone()) {
                format!("call_{round}_{position}")
            } else {
                raw.id
            };
            let (arguments, readable) = read_arguments(&raw.arguments);
            let cuts = Some(position) == last && cuts_calls(finish_reason);
            let partial = if cuts && (!readable || raw.arguments.trim().is_empty()) {
                Some(Partial::Cut)
            } else if !readable {
                Some(Partial::Unreadable)
            } else {
                None
            };
            Called {
                call: ToolCall::new(id, raw.name, arguments),
                partial,
            }
        })
        .collect()
}

/// `raw`, a call's argument text, as a mapping, and whether it read: none
/// or `null` is `{}`; text that reads as no object is kept whole under
/// `raw`, and does not read.
fn read_arguments(raw: &str) -> (Metadata, bool) {
    let text = raw.trim();
    if text.is_empty() {
        return (Metadata::new(), true);
    }
    match serde_json::from_str::<Value>(text) {
        Ok(Value::Object(arguments)) => (arguments, true),
        Ok(Value::Null) => (Metadata::new(), true),
        Ok(_) | Err(_) => {
            let mut kept = Metadata::new();
            kept.insert("raw".to_owned(), Value::String(raw.to_owned()));
            (kept, false)
        }
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
pub(super) fn ran_out_of_room(finish_reason: Option<&str>) -> bool {
    finish_reason.is_some_and(|reason| OUT_OF_ROOM.contains(&reason.to_ascii_lowercase().as_str()))
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    fn raw(id: &str, arguments: &str) -> ProviderToolCall {
        ProviderToolCall::new(id, "lookup", arguments)
    }

    fn read(
        calls: Vec<ProviderToolCall>,
        finish_reason: &str,
    ) -> Vec<(String, Value, Option<Partial>)> {
        read_calls(calls, Some(finish_reason), 1)
            .into_iter()
            .map(|called| {
                (
                    called.call.id,
                    Value::Object(called.call.arguments),
                    called.partial,
                )
            })
            .collect()
    }

    /// RFC §6.4: arguments reach the loop as a mapping, never as an error.
    #[test]
    fn arguments_read_as_a_mapping_whatever_the_wire_carried() {
        let calls = vec![
            raw("a", r#"{"order": "42"}"#),
            raw("b", ""),
            raw("c", "null"),
            raw("d", "[1, 2]"),
        ];
        assert_eq!(
            read(calls, "tool_calls"),
            vec![
                ("a".to_owned(), json!({ "order": "42" }), None),
                ("b".to_owned(), json!({}), None),
                ("c".to_owned(), json!({}), None),
                (
                    "d".to_owned(),
                    json!({ "raw": "[1, 2]" }),
                    Some(Partial::Unreadable)
                ),
            ]
        );
    }

    /// RFC §6.4: only the last call can be cut, and a call whose arguments
    /// arrived and read runs, the response cut over it or not.
    #[test]
    fn only_the_last_call_of_a_cut_response_with_no_argument_text_is_cut() {
        let calls = vec![raw("a", ""), raw("b", r#"{"order": "1"}"#), raw("c", "")];
        let partials: Vec<Option<Partial>> = read(calls, "length")
            .into_iter()
            .map(|(_, _, partial)| partial)
            .collect();
        assert_eq!(partials, [None, None, Some(Partial::Cut)]);
    }

    #[test]
    fn a_call_with_no_id_or_another_s_gets_one_of_its_own() {
        let ids: Vec<String> = read(vec![raw("", "{}"), raw("x", "{}"), raw("x", "{}")], "stop")
            .into_iter()
            .map(|(id, _, _)| id)
            .collect();
        assert_eq!(ids, ["call_1_0", "x", "call_1_2"]);
    }
}

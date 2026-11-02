//! One generation, read from its provider's stream, and what the loop reads
//! of its calls (RFC §6.4 "What a provider hands the loop for a call").

use std::collections::{BTreeSet, HashSet};

use roomkit::model::ToolCall;

use super::TurnItem;
use crate::provider::{
    AiContext, AiProvider, ProviderError, StreamDone, StreamEvent, StreamToolCall,
};
use crate::steering::Steering;
use crate::stream::{Yielder, next};

/// The most calls one generation's round runs: far above a model's
/// legitimate fan-out, far below a looping one's, which can fill its whole
/// output with calls (roomkit's `_MAX_TOOL_CALLS_PER_ROUND`).
const MAX_CALLS_PER_ROUND: usize = 32;

/// A generation, read whole: its text, its calls, how it ended.
#[derive(Debug)]
pub(super) struct Generation {
    pub(super) text: String,
    pub(super) calls: Vec<Called>,
    pub(super) done: StreamDone,
    /// A `Cancel` stopped it before its end (RFC §21.3).
    pub(super) cancelled: bool,
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
    /// Reads `provider`'s answer to `context`, handing its text to `out` as
    /// it comes. A stream that ends without its done event is read whole
    /// all the same, its calls kept, with no finish reason and no usage, as
    /// Python's stream round reads it. A `Cancel` `steering` received stops
    /// the read at the next event, which is dropped with the stream (RFC
    /// §21.3).
    ///
    /// # Errors
    ///
    /// Returns the provider's error.
    pub(super) async fn read<P: AiProvider>(
        provider: &P,
        context: &AiContext,
        steering: &Steering,
        out: &Yielder<Result<TurnItem, ProviderError>>,
    ) -> Result<Self, ProviderError> {
        let mut stream = provider.generate_structured_stream(context);
        let mut text = String::new();
        let mut calls = Vec::new();
        while let Some(event) = next(&mut stream).await {
            if steering.cancelled() {
                return Ok(Self::cut(text));
            }
            match event? {
                StreamEvent::TextDelta(delta) => {
                    text.push_str(&delta.text);
                    out.give(Ok(TurnItem::Text(delta.text))).await;
                }
                StreamEvent::ToolCall(call) => calls.push(call),
                StreamEvent::Done(done) => {
                    let calls = read_calls(calls);
                    return Ok(Self {
                        text,
                        calls,
                        done,
                        cancelled: false,
                    });
                }
            }
        }
        let calls = read_calls(calls);
        Ok(Self {
            text,
            calls,
            done: StreamDone::default(),
            cancelled: false,
        })
    }

    /// A generation a `Cancel` stopped after it wrote `text`.
    fn cut(text: String) -> Self {
        Self {
            text,
            calls: Vec::new(),
            done: StreamDone::default(),
            cancelled: true,
        }
    }
}

/// The calls of a response, as the loop reads them: the first
/// [`MAX_CALLS_PER_ROUND`], each with an id no other carries (one minted
/// for a call the provider gave none, or another's, as Python's `CallIds`
/// hands them), and, for one its provider marked `partial`, why it never
/// runs. The calls past the bound are dropped before the round's assistant
/// message is written, so that no call reaches the model without its
/// result, and logged.
fn read_calls(calls: Vec<StreamToolCall>) -> Vec<Called> {
    log_dropped(&calls);
    let mut ids = HashSet::new();
    calls
        .into_iter()
        .take(MAX_CALLS_PER_ROUND)
        .map(|given| {
            let id = if !given.id.is_empty() && ids.insert(given.id.clone()) {
                given.id
            } else {
                minted_id(&mut ids, &given.name)
            };
            let partial = match (given.partial, given.garbled) {
                (false, _) => None,
                (true, true) => Some(Partial::Unreadable),
                (true, false) => Some(Partial::Cut),
            };
            let mut call = ToolCall::new(id, given.name, given.arguments);
            call.intent = given.intent;
            Called { call, partial }
        })
        .collect()
}

/// An id for a call to `name` its provider gave none, unique across turns:
/// Python's `minted_call_id`, `call_<name>_<12 hex digits>`, taken among
/// `ids`.
fn minted_id(ids: &mut HashSet<String>, name: &str) -> String {
    loop {
        // The 48 high bits of a v4 UUID are random: uuid4().hex[:12].
        let random = uuid::Uuid::new_v4().as_u128() >> 80;
        let id = format!("call_{name}_{random:012x}");
        if ids.insert(id.clone()) {
            return id;
        }
    }
}

/// Logs the calls of a round past [`MAX_CALLS_PER_ROUND`], which never run:
/// how many, and the tools they named.
fn log_dropped(calls: &[StreamToolCall]) {
    let Some(dropped) = calls
        .get(MAX_CALLS_PER_ROUND..)
        .filter(|rest| !rest.is_empty())
    else {
        return;
    };
    let tools: BTreeSet<&str> = dropped.iter().map(|call| call.name.as_str()).collect();
    tracing::warn!(
        target: "roomkit::ai",
        requested = calls.len(),
        kept = MAX_CALLS_PER_ROUND,
        dropped = dropped.len(),
        tools = ?tools,
        "a round asked more tool calls than it runs, the rest dropped: the model is looping"
    );
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::provider::AiToolCall;

    // Python: tests/test_providers/test_tool_call_handoff.py::TestCallIds::test_every_call_of_a_response_gets_its_own_id
    #[test]
    fn every_call_of_a_response_gets_its_own_id() {
        let calls = ["c1", "", "", "c1"]
            .iter()
            .map(|id| AiToolCall::new(*id, "now").into())
            .collect();
        let handed: Vec<String> = read_calls(calls)
            .into_iter()
            .map(|called| called.call.id)
            .collect();
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
}

//! One generation, read from its provider's stream, and what the loop reads
//! of its calls (RFC §6.4 "What a provider hands the loop for a call").

use std::collections::{BTreeSet, HashSet};

use roomkit::model::ToolCall;

use super::TurnItem;
use crate::provider::{
    AiContext, AiProvider, ProviderError, StreamDone, StreamEvent, StreamToolCall,
};
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
    /// Reads `provider`'s answer to `context`, the `round`-th generation of
    /// the turn, handing its text to `out` as it comes.
    ///
    /// # Errors
    ///
    /// Returns the provider's error, or one saying its stream ended without
    /// a done event.
    pub(super) async fn read<P: AiProvider>(
        provider: &P,
        context: &AiContext,
        round: u32,
        out: &Yielder<Result<TurnItem, ProviderError>>,
    ) -> Result<Self, ProviderError> {
        let mut stream = provider.generate_structured_stream(context);
        let mut text = String::new();
        let mut calls = Vec::new();
        while let Some(event) = next(&mut stream).await {
            match event? {
                StreamEvent::TextDelta(delta) => {
                    text.push_str(&delta.text);
                    out.give(Ok(TurnItem::Text(delta.text))).await;
                }
                StreamEvent::ToolCall(call) => calls.push(call),
                StreamEvent::Done(done) => {
                    let calls = read_calls(calls, round);
                    return Ok(Self { text, calls, done });
                }
            }
        }
        Err("the provider's response ended before its done event".into())
    }
}

/// The calls of a response, as the loop reads them: the first
/// [`MAX_CALLS_PER_ROUND`], each with an id no other carries
/// (`call_<round>_<n>`, made free, for one the provider gave empty or
/// twice), and, for
/// one its provider marked `partial`, why it never runs. The calls past the
/// bound are dropped before the round's assistant message is written, so
/// that no call reaches the model without its result, and logged.
fn read_calls(calls: Vec<StreamToolCall>, round: u32) -> Vec<Called> {
    log_dropped(&calls);
    let mut ids = HashSet::new();
    calls
        .into_iter()
        .take(MAX_CALLS_PER_ROUND)
        .enumerate()
        .map(|(position, given)| {
            let id = if !given.id.is_empty() && ids.insert(given.id.clone()) {
                given.id
            } else {
                free_id(&mut ids, round, position)
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

/// `call_<round>_<position>`, with `_` after it until no call of the round
/// carries it, taken among `ids`.
fn free_id(ids: &mut HashSet<String>, round: u32, position: usize) -> String {
    let mut id = format!("call_{round}_{position}");
    while !ids.insert(id.clone()) {
        id.push('_');
    }
    id
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

    /// An id given empty or twice is replaced by one no other call of the
    /// round carries, a given one included: `call_1_1` given then minted
    /// was carried twice, and `call_1_0` minted then given.
    #[test]
    fn a_call_with_no_id_or_another_s_gets_one_of_its_own() {
        let ids = |given: &[&str]| -> Vec<String> {
            let calls = given
                .iter()
                .map(|id| AiToolCall::new(*id, "lookup").into())
                .collect();
            read_calls(calls, 1)
                .into_iter()
                .map(|called| called.call.id)
                .collect()
        };
        assert_eq!(
            [
                ids(&["", "x", "x"]),
                ids(&["call_1_1", ""]),
                ids(&["", "call_1_0"])
            ],
            [
                vec!["call_1_0", "x", "call_1_2"],
                vec!["call_1_1", "call_1_1_"],
                vec!["call_1_0", "call_1_1"]
            ]
        );
    }
}

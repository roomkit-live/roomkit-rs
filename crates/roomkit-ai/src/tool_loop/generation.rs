//! One generation, read from its provider's stream, and what the loop reads
//! of its calls (RFC §6.4 "What a provider hands the loop for a call").

use std::collections::{BTreeSet, HashSet};

use roomkit::model::ToolCall;

use super::TurnItem;
use super::resilience::Reading;
use crate::provider::{ProviderError, ProviderStream, StreamDone, StreamEvent, StreamToolCall};
use crate::stream::{Yielder, next};

/// The most calls one generation's round runs: far above a model's
/// legitimate fan-out, far below a looping one's, which can fill its whole
/// output with calls (roomkit's `_MAX_TOOL_CALLS_PER_ROUND`).
const MAX_CALLS_PER_ROUND: usize = 32;

/// A generation, read whole: its text, its calls, how it ended.
#[derive(Debug)]
pub(super) struct Generation {
    /// What the model wrote, which it reads back.
    pub(super) text: String,
    /// What the room was handed of it: its text less a replayed prefix.
    pub(super) delivered: String,
    pub(super) calls: Vec<Called>,
    pub(super) done: StreamDone,
    /// A `Cancel` stopped it before its end (RFC §21.3).
    pub(super) cancelled: bool,
    /// The fallback provider served it, not the channel's own (RFC §6.4).
    pub(super) by_fallback: bool,
    /// The round's reasoning, once the read returned.
    pub(super) thinking: String,
}

/// A call as the loop reads it.
#[derive(Clone, Debug)]
pub(super) struct Called {
    /// The call, its id unique in the response, its arguments a mapping.
    pub(super) call: ToolCall,
    /// Why it never runs, when it does not (RFC §6.4).
    pub(super) partial: Option<Partial>,
}

/// A generation its provider failed: the error, whether the stream had
/// handed anything before it, and what the room was handed of its text.
pub(super) struct Failed {
    pub(super) error: ProviderError,
    pub(super) streamed: bool,
    /// The text delivered before the failure, which the room keeps: the
    /// turn's last segment when the failure ends it `error` (RFC §6.4).
    pub(super) delivered: String,
}

impl Failed {
    /// A failure before the provider handed anything.
    pub(super) fn before_streaming(error: ProviderError) -> Self {
        Self {
            error,
            streamed: false,
            delivered: String::new(),
        }
    }
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
    /// Reads a provider's answer from its `stream`, handing its text to
    /// `out` as it comes. A stream that ends without its done event is read whole
    /// all the same, its calls kept, with no finish reason and no usage, as
    /// Python's stream round reads it. A `Cancel` `steering` received stops
    /// the read at the next event, which is dropped with the stream (RFC
    /// §21.3).
    ///
    /// # Errors
    ///
    /// Returns the provider's error, whether the stream had handed
    /// anything before it, and the text the room was handed.
    pub(super) async fn read(
        mut stream: ProviderStream<'_>,
        (steering, out, prefix, window): Reading<'_>,
    ) -> Result<Self, Failed> {
        let mut text = String::new();
        let mut delivered = String::new();
        let mut replayed = Replayed::new(prefix);
        let mut calls = Vec::new();
        let mut done = StreamDone::default();
        let mut streamed = false;
        while let Some(event) = next(&mut stream).await {
            if steering.cancelled() {
                window.end();
                return Ok(Self::cut(text, delivered));
            }
            let event = match event {
                Ok(event) => event,
                Err(error) => {
                    window.end();
                    return Err(Failed {
                        error,
                        streamed,
                        delivered,
                    });
                }
            };
            streamed = true;
            match event {
                StreamEvent::TextDelta(delta) => {
                    window.end();
                    text.push_str(&delta.text);
                    let released = replayed.add(delta.text);
                    deliver(released, &mut delivered, out).await;
                }
                // A whole call closes no window: a fragment would (Python's
                // `StreamToolCallDelta`), which no provider here hands.
                StreamEvent::ToolCall(call) => calls.push(call),
                StreamEvent::Done(ended) => {
                    done = ended;
                    break;
                }
                // A delta with a signature alone reads as nothing here.
                StreamEvent::ThinkingDelta(delta) => {
                    if !delta.thinking.is_empty() {
                        window.add(&delta.thinking);
                        out.give(Ok(TurnItem::Thinking(delta.thinking))).await;
                    }
                }
            }
        }
        window.end();
        deliver(replayed.finish(), &mut delivered, out).await;
        Ok(Self {
            text,
            delivered,
            calls: read_calls(calls),
            done,
            cancelled: false,
            by_fallback: false,
            thinking: String::new(),
        })
    }

    /// A generation a `Cancel` stopped after it wrote `text`, `delivered`
    /// of it handed to the room.
    fn cut(text: String, delivered: String) -> Self {
        Self {
            text,
            delivered,
            calls: Vec::new(),
            done: StreamDone::default(),
            cancelled: true,
            by_fallback: false,
            thinking: String::new(),
        }
    }
}

/// Hands `pieces` to the room, each in turn, and keeps them in
/// `delivered`.
async fn deliver(
    pieces: Vec<String>,
    delivered: &mut String,
    out: &Yielder<Result<TurnItem, ProviderError>>,
) {
    for piece in pieces.into_iter().filter(|piece| !piece.is_empty()) {
        delivered.push_str(&piece);
        out.give(Ok(TurnItem::Text(piece))).await;
    }
}

/// Withholds the text a round replays of the round of calls before it (a
/// model repeating `Let me look.` after a call) until new text or a
/// difference settles it: a prefix followed by new text is dropped, while
/// a generation that ends inside it gets it back, as it may be the whole
/// answer (RFC §6.4). Python's `_PrefixDeduplicator`.
struct Replayed<'a> {
    prefix: &'a str,
    active: bool,
    offset: usize,
    held: Vec<String>,
}

impl<'a> Replayed<'a> {
    fn new(prefix: &'a str) -> Self {
        Self {
            prefix,
            active: !prefix.is_empty(),
            offset: 0,
            held: Vec::new(),
        }
    }

    /// The pieces ready to hand to the room once `text` came.
    fn add(&mut self, text: String) -> Vec<String> {
        if !self.active {
            return vec![text];
        }
        let end = self.offset + text.len();
        let released = if end <= self.prefix.len() {
            if self.prefix.get(self.offset..end) == Some(text.as_str()) {
                self.offset = end;
                self.held.push(text);
                return Vec::new();
            }
            std::mem::take(&mut self.held)
                .into_iter()
                .chain([text])
                .collect()
        } else {
            let tail = self.prefix.get(self.offset..).unwrap_or_default();
            match text.strip_prefix(tail) {
                Some(rest) => vec![rest.to_owned()],
                None => std::mem::take(&mut self.held)
                    .into_iter()
                    .chain([text])
                    .collect(),
            }
        };
        self.active = false;
        self.held.clear();
        released
    }

    /// What it still holds, when no new text followed the prefix.
    fn finish(&mut self) -> Vec<String> {
        std::mem::take(&mut self.held)
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

//! One generation, read from its provider's stream, and what the loop reads
//! of its calls (RFC §6.4 "What a provider hands the loop for a call").

use std::collections::{BTreeSet, HashSet};

use roomkit::model::{Metadata, ToolCall};

use super::TurnItem;
use super::resilience::Reading;
use crate::provider::{
    AiToolCall, ProviderError, ProviderStream, RoundTranscript, StreamDone, StreamEvent,
    StreamToolCall,
};
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
    /// The usage each of its done events reported, those that did, in
    /// order: each one's priced as one response (RFC §6.4).
    pub(super) reports: Vec<Metadata>,
    /// A `Cancel` stopped it before its end (RFC §21.3).
    pub(super) cancelled: bool,
    /// The fallback provider served it, not the channel's own (RFC §6.4).
    pub(super) by_fallback: bool,
    /// The round's reasoning, once the read returned.
    pub(super) thinking: String,
    /// What it said, in the order it came, which its replay keeps (RFC §6.4).
    pub(super) transcript: RoundTranscript,
}

/// A call as the loop reads it.
#[derive(Clone, Debug)]
pub(super) struct Called {
    /// The call, its id unique in the response, its arguments a mapping.
    pub(super) call: ToolCall,
    /// Why it never runs, when it does not (RFC §6.4).
    pub(super) partial: Option<Partial>,
    /// Provider-owned data of the call, which its replay carries.
    pub(super) metadata: Metadata,
}

impl Called {
    /// The call as its round's replay reads it: its intent and its metadata
    /// its own.
    fn replayed(&self) -> AiToolCall {
        AiToolCall {
            arguments: self.call.arguments.clone(),
            metadata: self.metadata.clone(),
            intent: self.call.intent.clone(),
            ..AiToolCall::new(self.call.id.clone(), self.call.name.clone())
        }
    }
}

/// A generation its provider failed: the error, whether the stream had
/// handed anything before it, and what the room was handed of its text.
pub(super) struct Failed {
    pub(super) error: ProviderError,
    pub(super) streamed: bool,
    /// The text delivered before the failure, which the room keeps: the
    /// turn's last segment when the failure ends it `error` (RFC §6.4).
    pub(super) delivered: String,
    /// The usage each done event it handed before the failure reported,
    /// which the turn's usage keeps (RFC §6.4).
    pub(super) reports: Vec<Metadata>,
}

impl Failed {
    /// A failure before the provider handed anything.
    pub(super) fn before_streaming(error: ProviderError) -> Self {
        Self {
            error,
            streamed: false,
            delivered: String::new(),
            reports: Vec::new(),
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
    /// Reads a provider's answer from its `stream`, to the stream's end,
    /// handing its text to `out` as it comes. A stream that ends without its
    /// done event is read whole all the same, its calls kept, with no finish
    /// reason and no usage, and one that goes on past its done event is read
    /// on, a failure there failing the generation as streamed, as Python's
    /// stream round reads it. A `Cancel` `steering` received stops the read
    /// at the next event, which is dropped with the stream (RFC §21.3); a
    /// done event whose usage the turn's price cannot read stops it there,
    /// the count that raises as Python's does at that event following.
    ///
    /// # Errors
    ///
    /// Returns the provider's error, whether the stream had handed
    /// anything before it, and the text the room was handed.
    pub(super) async fn read(
        mut stream: ProviderStream<'_>,
        (steering, out, prefix, window, limits): Reading<'_>,
    ) -> Result<Self, Failed> {
        let mut transcript = RoundTranscript::new();
        let mut delivered = String::new();
        let mut replayed = Replayed::new(prefix);
        let mut calls = Vec::new();
        let mut ids = HashSet::new();
        let mut done = StreamDone::default();
        let mut reports = Vec::new();
        let mut streamed = false;
        while let Some(event) = next(&mut stream).await {
            if steering.cancelled() {
                window.end();
                return Ok(Self::cut(transcript, delivered, done, reports));
            }
            let event = match event {
                Ok(event) => event,
                Err(error) => {
                    window.end();
                    return Err(Failed {
                        error,
                        streamed,
                        delivered,
                        reports,
                    });
                }
            };
            streamed = true;
            match event {
                StreamEvent::TextDelta(delta) => {
                    window.end();
                    transcript.add_text(&delta.text);
                    let released = replayed.add(delta.text);
                    deliver(released, &mut delivered, out).await;
                }
                // A whole call closes no window: a fragment would (Python's
                // `StreamToolCallDelta`), which no provider here hands.
                StreamEvent::ToolCall(call) => {
                    let call = with_its_own_id(call, &mut ids);
                    transcript.add_call(call.id.clone());
                    calls.push(call);
                }
                StreamEvent::Done(ended) => {
                    let unpriced = limits.cannot_price(&ended.usage);
                    take_done(ended, &mut done, &mut reports);
                    if unpriced {
                        break;
                    }
                }
                // A delta with a signature or redacted data alone reaches no
                // reader; the round replays it all the same (RFC §6.4).
                StreamEvent::ThinkingDelta(delta) => {
                    transcript.add_thinking(&delta);
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
            text: transcript.text(),
            delivered,
            calls: read_calls(calls),
            done,
            reports,
            cancelled: false,
            by_fallback: false,
            thinking: String::new(),
            transcript,
        })
    }

    /// A generation a `Cancel` stopped after it said what `transcript`
    /// holds, `delivered` of its text handed to the room, and after the end
    /// `done` it handed, if any, its usage among `reports`.
    fn cut(
        transcript: RoundTranscript,
        delivered: String,
        done: StreamDone,
        reports: Vec<Metadata>,
    ) -> Self {
        Self {
            text: transcript.text(),
            delivered,
            calls: Vec::new(),
            done,
            reports,
            cancelled: true,
            by_fallback: false,
            thinking: String::new(),
            transcript,
        }
    }

    /// The round as the next one replays it, its calls as the loop kept them,
    /// their intents taken: its reasoning, its text and its calls, in the
    /// order they came (RFC §6.4). `None` for calls it never made, which the
    /// loop's own never are: the round then reads from its text and its calls.
    pub(super) fn said(&self) -> Option<Vec<crate::provider::AiContentPart>> {
        let calls: Vec<AiToolCall> = self.calls.iter().map(Called::replayed).collect();
        match self.transcript.parts(&calls) {
            Ok(parts) => Some(parts),
            Err(error) => {
                tracing::error!(target: "roomkit::ai", %error, "a round's replay lost its reasoning");
                None
            }
        }
    }
}

/// Takes a done event `ended` as Python's stream round does: the last one
/// says how the generation ended, one with no usage leaving what an earlier
/// one reported, and each one's usage is kept among `reports`.
fn take_done(mut ended: StreamDone, done: &mut StreamDone, reports: &mut Vec<Metadata>) {
    if ended.usage.is_empty() {
        ended.usage = std::mem::take(&mut done.usage);
    } else {
        reports.push(ended.usage.clone());
    }
    *done = ended;
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

/// `given` with an id no other call of its response carries: one minted for
/// a call the provider gave none, or another's, as Python's `CallIds` hands
/// them. Read as the call comes, so that the round's transcript names it.
fn with_its_own_id(mut given: StreamToolCall, ids: &mut HashSet<String>) -> StreamToolCall {
    if given.id.is_empty() || !ids.insert(given.id.clone()) {
        given.id = minted_id(ids, &given.name);
    }
    given
}

/// The calls of a response, as the loop reads them: the first
/// [`MAX_CALLS_PER_ROUND`], and, for one its provider marked `partial`, why
/// it never runs. The calls past the bound are dropped before the round's
/// assistant message is written, so that no call reaches the model without
/// its result, and logged.
fn read_calls(calls: Vec<StreamToolCall>) -> Vec<Called> {
    log_dropped(&calls);
    calls
        .into_iter()
        .take(MAX_CALLS_PER_ROUND)
        .map(|given| {
            let partial = match (given.partial, given.garbled) {
                (false, _) => None,
                (true, true) => Some(Partial::Unreadable),
                (true, false) => Some(Partial::Cut),
            };
            let mut call = ToolCall::new(given.id, given.name, given.arguments);
            call.intent = given.intent;
            Called {
                call,
                partial,
                metadata: given.metadata,
            }
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
        let mut ids = HashSet::new();
        let handed: Vec<String> = ["c1", "", "", "c1"]
            .iter()
            .map(|id| with_its_own_id(AiToolCall::new(*id, "now").into(), &mut ids).id)
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

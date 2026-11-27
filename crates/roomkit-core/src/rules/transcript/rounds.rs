//! An intelligence channel's earlier rounds of tool calls, replayed in its
//! history (RFC §6.4 "The tools of earlier turns").
//!
//! A turn's rounds are stored as the channel's `TOOL_CALL_START` and
//! `TOOL_CALL_END` rows, between the turn's input and its answer. A later
//! turn reads them back at that place, the way the turn itself sent them:
//! the assistant's message making the calls, then a tool message per
//! result. A model answers a follow-up from a result it reads there; a
//! recall riding the participant's message instead reads as something the
//! participant asked, and models answer it.
//!
//! A round replays what the model was given, as it was given, so that a
//! later turn's request extends the one before it and a provider's prompt
//! cache keeps it (RMK-1370): the arguments it wrote (the start row's, never
//! a `BEFORE_TOOL_USE` rewrite, which may hold the very value a hook kept
//! from it) and the result it read (the end row's, an `ON_TOOL_CALL` rewrite
//! included), whole, under the id the model gave it. Every call the turn
//! ended is replayed, whatever its tool or its outcome, but a call cancelled
//! before it answered, whose result no model read, and a refused call under
//! a name vendors do not take, which only a call the model invented carries
//! and which would fail every later request. Nothing shortens a round
//! from turn to turn: a compaction does, when a provider refuses the context
//! as too long (RFC §6.4 "A turn compacted").

use super::open_calls::OpenCalls;
use std::borrow::Cow;

use serde_json::Value;

use super::Message;
use crate::model::{
    ChannelId, EventContent, EventDirection, EventType, RoomEvent, ToolCall, ToolCallContent,
    ToolCallOutcome, ToolCallStatus,
};
use crate::rules::edits::is_deleted;

/// What a part of a stored result other than text reads as.
const NON_TEXT_PART: &str = "[non-text part]";

/// How the placeholder of an oversized result starts, the result stored for
/// the turn's re-read: Python's `EVICTION_PLACEHOLDER_PREFIX`.
#[doc(hidden)]
pub const EVICTION_PLACEHOLDER_PREFIX: &str = "Result too large (";

/// What a replayed placeholder whose result the room no longer stores says
/// after its size: Python's `_NOT_SHOWN`.
const NOT_SHOWN: &str = "the rest is not shown here: call the tool again for it";

/// One round of an earlier turn, as the history replays it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ToolRound {
    /// The timeline index of its first replayed call's start row.
    pub(super) index: u64,
    /// The turn it belongs to: its rows' `correlation_id`.
    pub(super) turn: Option<String>,
    /// The assistant's message making its calls, then a tool message per
    /// result, in call order.
    pub(super) messages: Vec<Message>,
}

/// A call the channel's rows hold: its start row, and its end row once
/// paired.
struct Call<'a> {
    index: u64,
    turn: Option<&'a str>,
    start: &'a ToolCallContent,
    end: Option<&'a ToolCallContent>,
}

/// The rounds of `channel_id`'s earlier tool calls that `events`, a room's
/// timeline oldest first, hold, as its history replays them: its own rows
/// only (written by it, outbound, none deleted), each call its turn ended as
/// it was sent, but one cancelled before it answered and one refused under
/// a name vendors do not take. `resolves` says whether the room still
/// stores the result an oversized result's placeholder points to (Python's
/// `tool_rounds(rows, resolves=...)`).
pub(super) fn tool_rounds(
    events: &[RoomEvent],
    channel_id: &ChannelId,
    resolves: &dyn Fn(&str) -> bool,
) -> Vec<ToolRound> {
    rounds(own_rows(events, channel_id))
        .iter()
        .filter_map(|calls| replayed_round(calls, resolves))
        .collect()
}

/// `channel_id`'s own rows among `events` its history replays, with the
/// content of its tool rows: rows it wrote (outbound; one that entered the
/// room inbound under its id is no call it made), told by their type and
/// source. A deleted one keeps its place with no content, whether the
/// history handed it whole or withheld its content, as a channel's history
/// does (RFC §7.5 rule 8, `visible_events`): it ends a round, as the
/// reference implementation's withheld row does, and so does an
/// instruction it kept (RFC §10.1.1), a row of no call. A row a turn holds
/// for the tool memory alone is none of its history's.
fn own_rows<'a>(
    events: &'a [RoomEvent],
    channel_id: &'a ChannelId,
) -> impl Iterator<Item = (&'a RoomEvent, Option<&'a ToolCallContent>)> {
    events.iter().filter_map(move |event| {
        let is_tool_row = matches!(
            event.r#type,
            EventType::ToolCallStart | EventType::ToolCallEnd | EventType::InstructionRead
        );
        let own = &event.source.channel_id == channel_id
            && event.source.direction == EventDirection::Outbound;
        let row = if let EventContent::ToolCall(row) = &event.content
            && !is_deleted(event)
        {
            Some(row)
        } else {
            None
        };
        (is_tool_row && own && !super::is_tool_memory_only(event)).then_some((event, row))
    })
}

/// The calls `rows` hold, round by round: a round is a run of start rows
/// of one turn, each call completed by the end row of the same turn and
/// id, the first of a round's calls under an id by the first such row. A
/// turn's rows all carry its `correlation_id`, as the reference
/// implementation pairs them: a provider may reuse an id from one turn to
/// the next, and two turns of a room may run at once. A row whose content
/// is withheld ends the round it stands in, and pairs with nothing.
fn rounds<'a>(
    rows: impl Iterator<Item = (&'a RoomEvent, Option<&'a ToolCallContent>)>,
) -> Vec<Vec<Call<'a>>> {
    let mut rounds: Vec<Vec<Call<'a>>> = Vec::new();
    let mut waiting = OpenCalls::new();
    let mut last_turn: Option<&str> = None;
    let mut opened = false;
    for (event, row) in rows {
        let turn = event.correlation_id.as_deref();
        let Some(row) = row else {
            opened = false;
            waiting.round_over(turn);
            continue;
        };
        let key = (turn, row.tool_id.as_str());
        if event.r#type == EventType::ToolCallStart {
            if !opened || turn != last_turn {
                rounds.push(Vec::new());
            }
            let round = rounds.len().saturating_sub(1);
            if let Some(calls) = rounds.last_mut() {
                waiting.hold(key, (round, calls.len()));
                calls.push(Call {
                    index: event.index,
                    turn,
                    start: row,
                    end: None,
                });
            }
            (last_turn, opened) = (turn, true);
            continue;
        }
        opened = false;
        waiting.round_over(turn);
        let Some((round, position)) = waiting.take(key) else {
            continue;
        };
        if let Some(call) = rounds
            .get_mut(round)
            .and_then(|calls| calls.get_mut(position))
        {
            call.end = Some(row);
        }
    }
    rounds
}

/// Whether a request's earlier calls may name `name`: what `OpenAI` and
/// Anthropic take, `[A-Za-z0-9_-]{1,128}`.
fn vendors_take(name: &str) -> bool {
    (1..=128).contains(&name.len())
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

/// Whether `call` goes back in a later request: it ended with a result the
/// model read, not cancelled before it answered, whose turn was cut or
/// stopped first, and not refused under a name vendors do not take. Only a
/// call the model invented carries such a name (a declared tool's is checked
/// before the request), and sent back it would make every later request of
/// the room a 400. An end stored without an outcome counts as refused here.
fn answered(call: &Call<'_>) -> bool {
    let Some(end) = call.end else {
        return false;
    };
    match end.outcome {
        Some(ToolCallOutcome::Cancelled) => false,
        None | Some(ToolCallOutcome::Refused) => vendors_take(&call.start.tool_name),
        Some(_) => true,
    }
}

/// `calls`, one round's, as it reads: the calls its turn ended and the model
/// read the result of, each under the id the model gave it, as the turn sent
/// it; none when none is.
fn replayed_round(calls: &[Call<'_>], resolves: &dyn Fn(&str) -> bool) -> Option<ToolRound> {
    let shown: Vec<&Call<'_>> = calls.iter().filter(|call| answered(call)).collect();
    let first = shown.first()?;
    let made = shown
        .iter()
        .map(|call| {
            // The arguments the model wrote: the start row's, with its intent
            // beside them, which the loop writes back among them (RFC §6.4).
            let arguments = call.start.arguments.clone();
            let mut made = ToolCall::new(
                call.start.tool_id.clone(),
                call.start.tool_name.clone(),
                arguments,
            );
            made.intent.clone_from(&call.start.intent);
            made
        })
        .collect();
    let mut messages = vec![Message::assistant_calls("", made)];
    messages.extend(shown.iter().map(|call| {
        let result = call
            .end
            .map_or(Cow::Borrowed(""), |end| shown_result(&end.result, resolves));
        let mut message = Message::tool_result(call.start.tool_id.clone(), result);
        // A failed call reads as one, as its round read it (RFC §6.4).
        message.is_error = call
            .end
            .is_some_and(|end| end.status == ToolCallStatus::Failed);
        message.outcome = call.end.and_then(|end| end.outcome);
        message
    }));
    Some(ToolRound {
        index: first.index,
        turn: first.turn.map(str::to_owned),
        messages,
    })
}

/// What a replayed call's `result` reads as: the result the model read; a
/// placeholder whose result the room no longer stores (`resolves`), as its
/// size alone, never as an id nothing resolves (RFC §6.4, Python's
/// `_shown_result`).
fn shown_result<'a>(result: &'a Value, resolves: &dyn Fn(&str) -> bool) -> Cow<'a, str> {
    let text = result_text(result);
    if evicted(result) && !resolves(&text) {
        return Cow::Owned(format!("{}, {NOT_SHOWN}.", placeholder_size(&text)));
    }
    text
}

/// Whether a stored result is the placeholder an oversized one became,
/// wherever it sits in a list of parts (an image may come first): Python's
/// `_evicted`.
fn evicted(result: &Value) -> bool {
    match result {
        Value::String(text) => text.starts_with(EVICTION_PLACEHOLDER_PREFIX),
        Value::Array(parts) => parts.iter().any(|part| {
            part.get("text")
                .and_then(Value::as_str)
                .is_some_and(|text| text.starts_with(EVICTION_PLACEHOLDER_PREFIX))
        }),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::Object(_) => false,
    }
}

/// The size head of the placeholder in `text`, `Result too large (N
/// tokens)`, without the stored id: Python's `eviction_placeholder_size`.
fn placeholder_size(text: &str) -> String {
    let start = text.find(EVICTION_PLACEHOLDER_PREFIX).unwrap_or(0);
    let tail = text.get(start..).unwrap_or(text);
    let head = tail.split_once(')').map_or(tail, |(head, _)| head);
    format!("{head})")
}

/// A stored result's text, borrowed when the row holds it as text: a list
/// of parts read as its texts joined, a part other than text named; `null`
/// reads as nothing; another value as its JSON.
fn result_text(result: &Value) -> Cow<'_, str> {
    match result {
        Value::String(text) => Cow::Borrowed(text),
        Value::Null => Cow::Borrowed(""),
        Value::Array(parts) => Cow::Owned(
            parts
                .iter()
                .map(|part| {
                    part.get("text")
                        .and_then(Value::as_str)
                        .unwrap_or(NON_TEXT_PART)
                })
                .collect::<Vec<_>>()
                .join(" "),
        ),
        Value::Bool(_) | Value::Number(_) | Value::Object(_) => Cow::Owned(result.to_string()),
    }
}

/// Whether `c` separates words as Python's `str.split()` splits them:
/// Unicode's white space, and the four information separators.
pub(super) fn is_python_space(c: char) -> bool {
    c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c)
}

#[cfg(test)]
mod tests;

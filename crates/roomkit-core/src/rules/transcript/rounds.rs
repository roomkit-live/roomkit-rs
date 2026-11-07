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
//! before it answered, whose result no model read. Nothing shortens a round
//! from turn to turn: a compaction does, when a provider refuses the context
//! as too long (RFC §6.4 "A turn compacted").

use std::borrow::Cow;
use std::collections::{BTreeMap, VecDeque};

use serde_json::Value;

use super::Message;
use crate::model::{
    ChannelId, EventContent, EventDirection, EventType, RoomEvent, ToolCall, ToolCallContent,
    ToolCallOutcome, ToolCallStatus,
};
use crate::rules::edits::is_deleted;

/// What a part of a stored result other than text reads as.
const NON_TEXT_PART: &str = "[non-text part]";

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
/// it was sent, but one cancelled before it answered.
pub(super) fn tool_rounds(events: &[RoomEvent], channel_id: &ChannelId) -> Vec<ToolRound> {
    rounds(own_rows(events, channel_id))
        .iter()
        .filter_map(|calls| replayed_round(calls))
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

/// A turn and a call's id in it.
type CallKey<'a> = (Option<&'a str>, &'a str);

/// A call waiting for its end row: the round of its turn it came in, and
/// where [`rounds`] holds it.
#[derive(Clone, Copy)]
struct Held {
    of_turn: u32,
    round: usize,
    position: usize,
}

/// The calls waiting for their end rows, first in, first out per turn and
/// id. A round of a turn is over at that turn's first row that is no start,
/// whatever another turn wrote among its starts. A call an earlier round
/// left without its end stays so: the next end row under its id is a later
/// call's.
#[derive(Default)]
struct Waiting<'a> {
    calls: BTreeMap<CallKey<'a>, VecDeque<Held>>,
    rounds: BTreeMap<Option<&'a str>, u32>,
}

impl<'a> Waiting<'a> {
    /// The call at `position` of `round` waits behind its round's calls
    /// under its id.
    fn hold(&mut self, key: CallKey<'a>, round: usize, position: usize) {
        let of_turn = self.rounds.get(&key.0).copied().unwrap_or(0);
        let calls = self.calls.entry(key).or_default();
        if calls.back().is_some_and(|held| held.of_turn != of_turn) {
            calls.clear();
        }
        calls.push_back(Held {
            of_turn,
            round,
            position,
        });
    }

    /// A row of `turn` that is no start was read: its round is over.
    fn round_over(&mut self, turn: Option<&'a str>) {
        let number = self.rounds.entry(turn).or_default();
        *number = number.saturating_add(1);
    }

    /// The call an end row under `key` closes, if any.
    fn take(&mut self, key: CallKey<'a>) -> Option<Held> {
        self.calls.get_mut(&key).and_then(VecDeque::pop_front)
    }
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
    let mut waiting = Waiting::default();
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
                waiting.hold(key, round, calls.len());
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
        let Some(held) = waiting.take(key) else {
            continue;
        };
        if let Some(call) = rounds
            .get_mut(held.round)
            .and_then(|calls| calls.get_mut(held.position))
        {
            call.end = Some(row);
        }
    }
    rounds
}

/// Whether `call` ended with a result the model read: not one cancelled
/// before it answered, whose turn was cut or stopped first. An end stored
/// without an outcome ended served or failed.
fn answered(call: &Call<'_>) -> bool {
    call.end
        .is_some_and(|end| end.outcome != Some(ToolCallOutcome::Cancelled))
}

/// `calls`, one round's, as it reads: the calls its turn ended and the model
/// read the result of, each under the id the model gave it, as the turn sent
/// it; none when none is.
fn replayed_round(calls: &[Call<'_>]) -> Option<ToolRound> {
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
            .map_or(Cow::Borrowed(""), |end| result_text(&end.result));
        let mut message = Message::tool_result(call.start.tool_id.clone(), result);
        // A failed call reads as one, as its round read it (RFC §6.4).
        message.is_error = call
            .end
            .is_some_and(|end| end.status == ToolCallStatus::Failed);
        message
    }));
    Some(ToolRound {
        index: first.index,
        turn: first.turn.map(str::to_owned),
        messages,
    })
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

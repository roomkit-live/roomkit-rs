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
//! A round replays what the model was given: the arguments it wrote (the
//! start row's, never a `BEFORE_TOOL_USE` rewrite, which may hold the very
//! value a hook kept from it) and the result it read (the end row's, an
//! `ON_TOOL_CALL` rewrite included). The calls replayed are the answers a
//! tool gave, served, failed or withheld by a hook; never a refusal, a call
//! nothing served or one cancelled, nor a call to a tool the caller says
//! is no work of the agent's (its skills' tools). Bounded as the reference
//! implementation bounds them: the last 8 calls, the 3 most recent with
//! their result whole up to 6,000 characters, the older ones a preview.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use super::Message;
use crate::model::{
    ChannelId, EventContent, EventDirection, EventId, EventType, RoomEvent, ToolCall,
    ToolCallContent, ToolCallOutcome,
};
use crate::rules::edits::is_deleted;

/// The calls a turn's history replays, the channel's most recent.
const REPLAYED_CALLS: usize = 8;

/// The most recent replayed calls whose result is replayed whole, up to
/// [`KEPT_CHARS`]; an older one is replayed as a preview.
const WHOLE_RESULTS: usize = 3;

/// How much of a recent result is replayed whole, in characters: the
/// reference implementation's 6,000, which its eviction may lower; nothing
/// is evicted here (RFC §21.5 is not built).
const KEPT_CHARS: usize = 6000;

/// How much of an older result its preview shows, in characters.
const PREVIEW_CHARS: usize = 120;

/// What a cut result says of the rest.
const NOT_SHOWN: &str = "the rest is not shown here: call the tool again for it";

/// What a part of a stored result other than text reads as.
const NON_TEXT_PART: &str = "[non-text part]";

/// One round of an earlier turn, as the history replays it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ToolRound {
    /// The timeline index of its first replayed call's start row.
    pub(super) index: u64,
    /// The assistant's message making its calls, then a tool message per
    /// result, in call order.
    pub(super) messages: Vec<Message>,
}

/// A call the channel's rows hold: its start row, and its end row once
/// paired.
struct Call<'a> {
    index: u64,
    start: &'a ToolCallContent,
    end: Option<&'a ToolCallContent>,
}

/// The rounds of `channel_id`'s earlier tool calls that `events`, a room's
/// timeline oldest first, hold, as its history replays them: its own rows
/// only (written by it, outbound, none deleted), `replayed` saying whether
/// a call to a tool is work the agent did.
pub(super) fn tool_rounds(
    events: &[RoomEvent],
    channel_id: &ChannelId,
    replayed: &dyn Fn(&str) -> bool,
) -> Vec<ToolRound> {
    let rounds = rounds(own_rows(events, channel_id));
    // Each call kept, by its round and its place in it.
    let kept: Vec<((usize, usize), &Call<'_>)> = rounds
        .iter()
        .enumerate()
        .flat_map(|(round, calls)| {
            let placed = calls.iter().enumerate();
            placed.map(move |(position, call)| ((round, position), call))
        })
        .filter(|(_, call)| is_kept(call, replayed))
        .collect();
    let kept = kept
        .get(kept.len().saturating_sub(REPLAYED_CALLS)..)
        .unwrap_or_default();
    let whole_from = kept.len().saturating_sub(WHOLE_RESULTS);
    let ids = unique_ids(kept.iter().map(|(_, call)| *call));
    let chosen: Chosen<'_> = kept
        .iter()
        .zip(&ids)
        .enumerate()
        .map(|(at, ((place, _), id))| (*place, (id.as_str(), at >= whole_from)))
        .collect();
    rounds
        .iter()
        .enumerate()
        .filter_map(|(round, calls)| replayed_round(round, calls, &chosen))
        .collect()
}

/// The calls replayed, by their round and their place in it: each one's id
/// and whether its result is replayed whole.
type Chosen<'a> = BTreeMap<(usize, usize), (&'a str, bool)>;

/// `channel_id`'s tool rows among `events`, with their content: rows it
/// wrote (outbound; one that entered the room inbound under its id is no
/// call it made), none deleted, as from any history (RFC §7.5 rule 8).
fn own_rows<'a>(
    events: &'a [RoomEvent],
    channel_id: &'a ChannelId,
) -> impl Iterator<Item = (&'a RoomEvent, &'a ToolCallContent)> {
    events.iter().filter_map(move |event| {
        let EventContent::ToolCall(row) = &event.content else {
            return None;
        };
        let is_tool_row = matches!(
            event.event_type,
            EventType::ToolCallStart | EventType::ToolCallEnd
        );
        let own = &event.source.channel_id == channel_id
            && event.source.direction == EventDirection::Outbound;
        (is_tool_row && own && !is_deleted(event)).then_some((event, row))
    })
}

/// The calls `rows` hold, round by round: a round is a run of start rows
/// of one turn, each call completed by the end row of the same turn and
/// id. A turn's rows all answer its trigger (`responds_to`): a provider
/// may reuse an id from one turn to the next.
fn rounds<'a>(
    rows: impl Iterator<Item = (&'a RoomEvent, &'a ToolCallContent)>,
) -> Vec<Vec<Call<'a>>> {
    let mut rounds: Vec<Vec<Call<'a>>> = Vec::new();
    let mut started: BTreeMap<(Option<&'a EventId>, &'a str), (usize, usize)> = BTreeMap::new();
    let mut last_turn: Option<&EventId> = None;
    let mut opened = false;
    for (event, row) in rows {
        let turn = event.responds_to.as_ref();
        let key = (turn, row.tool_id.as_str());
        if event.event_type == EventType::ToolCallStart {
            if !opened || turn != last_turn {
                rounds.push(Vec::new());
            }
            let round = rounds.len().saturating_sub(1);
            if let Some(calls) = rounds.last_mut() {
                started.insert(key, (round, calls.len()));
                calls.push(Call {
                    index: event.index,
                    start: row,
                    end: None,
                });
            }
            (last_turn, opened) = (turn, true);
            continue;
        }
        opened = false;
        let Some((round, position)) = started.remove(&key) else {
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

/// Whether `call` is replayed: it ended, with an answer the tool gave
/// (served, failed, blocked; an end stored without an outcome ended served
/// or failed), and its tool is work the agent did.
fn is_kept(call: &Call<'_>, replayed: &dyn Fn(&str) -> bool) -> bool {
    let Some(end) = call.end else {
        return false;
    };
    let answered = match end.outcome {
        None
        | Some(ToolCallOutcome::Served | ToolCallOutcome::Failed | ToolCallOutcome::Blocked) => {
            true
        }
        Some(ToolCallOutcome::Refused | ToolCallOutcome::Unserved | ToolCallOutcome::Cancelled) => {
            false
        }
    };
    answered && replayed(&call.start.tool_name)
}

/// Each of `calls`' ids, one per call: a provider may reuse an id from one
/// turn to the next, and a chat API pairs a result with its call by id. A
/// call with no id, or another's, takes its own with its start's index.
fn unique_ids<'a>(calls: impl Iterator<Item = &'a Call<'a>>) -> Vec<String> {
    let mut seen: BTreeSet<String> = BTreeSet::new();
    calls
        .map(|call| {
            let given = call.start.tool_id.as_str();
            let id = if given.is_empty() || seen.contains(given) {
                let stem = if given.is_empty() { "call" } else { given };
                format!("{stem}_{}", call.index)
            } else {
                given.to_owned()
            };
            seen.insert(id.clone());
            id
        })
        .collect()
}

/// The round numbered `round`, `calls`, as it reads: the `chosen` of its
/// calls alone, each with its id, its result whole or not; none when none
/// is chosen.
fn replayed_round(round: usize, calls: &[Call<'_>], chosen: &Chosen<'_>) -> Option<ToolRound> {
    let shown: Vec<(&Call<'_>, &str, bool)> = calls
        .iter()
        .enumerate()
        .filter_map(|(position, call)| {
            let (id, whole) = chosen.get(&(round, position))?;
            Some((call, *id, *whole))
        })
        .collect();
    let index = shown.first()?.0.index;
    let made = shown
        .iter()
        .map(|(call, id, _)| {
            // The arguments the model wrote: the start row's.
            let arguments = call.start.arguments.clone();
            ToolCall::new(*id, call.start.tool_name.clone(), arguments)
        })
        .collect();
    let mut messages = vec![Message::assistant_calls("", made)];
    messages.extend(shown.iter().map(|(call, id, whole)| {
        let result = call
            .end
            .map(|end| result_text(&end.result))
            .unwrap_or_default();
        Message::tool_result(*id, shown_result(&result, *whole))
    }));
    Some(ToolRound { index, messages })
}

/// A stored result's text: a list of parts read as its texts joined, a
/// part other than text named; `null` reads as nothing.
fn result_text(result: &Value) -> String {
    match result {
        Value::String(text) => text.clone(),
        Value::Null => String::new(),
        Value::Array(parts) => parts
            .iter()
            .map(|part| {
                part.get("text")
                    .and_then(Value::as_str)
                    .unwrap_or(NON_TEXT_PART)
            })
            .collect::<Vec<_>>()
            .join(" "),
        Value::Bool(_) | Value::Number(_) | Value::Object(_) => result.to_string(),
    }
}

/// What a replayed result reads as: whole up to [`KEPT_CHARS`] for one of
/// the most recent, cut there saying so; an older one's preview, its
/// spacing collapsed, cut at [`PREVIEW_CHARS`] saying so.
fn shown_result(text: &str, whole: bool) -> String {
    if !whole {
        let flat = text
            .split(is_python_space)
            .filter(|word| !word.is_empty())
            .collect::<Vec<_>>()
            .join(" ");
        if flat.chars().count() <= PREVIEW_CHARS {
            return flat;
        }
        return format!("{}… [{NOT_SHOWN}]", head(&flat, PREVIEW_CHARS));
    }
    let length = text.chars().count();
    if length <= KEPT_CHARS {
        return text.to_owned();
    }
    format!(
        "{}\n[first {KEPT_CHARS} of {length} characters; {NOT_SHOWN}]",
        head(text, KEPT_CHARS)
    )
}

/// The first `count` characters of `text`.
fn head(text: &str, count: usize) -> &str {
    text.char_indices()
        .nth(count)
        .and_then(|(at, _)| text.get(..at))
        .unwrap_or(text)
}

/// Whether `c` separates words as the reference implementation's preview
/// splits them (Python's `str.split()`): Unicode's white space, and the
/// four information separators.
fn is_python_space(c: char) -> bool {
    c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c)
}

#[cfg(test)]
mod tests;

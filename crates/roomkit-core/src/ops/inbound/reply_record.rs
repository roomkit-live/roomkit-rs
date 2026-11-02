//! What an agent's reply leaves its caller's record (RFC §6.4): every key
//! its turn's record holds but how the turn ended, which the caller reads
//! under `turns`, by the agent's channel. Python's `record_buffered_reply`
//! (`core/mixins/lane_execution.py`), `merge_channel_record`
//! (`models/response_metadata.py`) and `reply_turn_entry`
//! (`core/event_router.py`).

use serde_json::Value;

use crate::model::{ChannelId, EventType, Metadata, RoomEvent};

/// The key of a caller's record under which each turn's end is.
const TURNS: &str = "turns";
/// How a turn's loop ended, in its record.
const LOOP_END_REASON: &str = "loop_end_reason";
/// What a turn spent, in its record.
const AI_USAGE: &str = "ai_usage";

/// Merges the reply of the agent `channel_id`, its turn's record `reply`
/// and the `responses` it returned, into `record`, its caller's: every
/// key of `reply` but the turn's own (`turns`, `loop_end_reason`,
/// `ai_usage`); a `buffered` reply's end and usage then under `turns`, by
/// its channel, read off `reply` or else its last message. A stream's end
/// goes there once read.
pub(super) fn record_reply(
    record: &mut Metadata,
    channel_id: &ChannelId,
    (reply, responses): (&Metadata, &[RoomEvent]),
    buffered: bool,
) {
    let kept = reply
        .iter()
        .filter(|(key, _)| ![TURNS, LOOP_END_REASON, AI_USAGE].contains(&key.as_str()))
        .map(|(key, value)| (key.clone(), value.clone()));
    record.extend(kept);
    if !buffered {
        return;
    }
    let Some(entry) = reply_turn_entry(reply, responses) else {
        return;
    };
    let turns = record
        .entry(TURNS.to_owned())
        .or_insert_with(|| Value::Object(Metadata::new()));
    if !turns.is_object() {
        *turns = Value::Object(Metadata::new());
    }
    if let Value::Object(turns) = turns {
        turns.insert(channel_id.to_string(), Value::Object(entry));
    }
}

/// A buffered reply's entry under its caller's `turns`: the end the first
/// of its record `reply` and its messages' metadata, last first, names,
/// with that record's usage; `None` when none names an end (Python's
/// `turn_summary`).
fn reply_turn_entry(reply: &Metadata, responses: &[RoomEvent]) -> Option<Metadata> {
    let messages = responses
        .iter()
        .rev()
        .filter(|event| event.r#type == EventType::Message)
        .map(|event| &event.metadata);
    std::iter::once(reply).chain(messages).find_map(|record| {
        let end = record.get(LOOP_END_REASON).filter(|end| !end.is_null())?;
        let mut entry = Metadata::new();
        entry.insert(LOOP_END_REASON.to_owned(), end.clone());
        if let Some(usage) = record.get(AI_USAGE).filter(|usage| !usage.is_null()) {
            entry.insert(AI_USAGE.to_owned(), usage.clone());
        }
        Some(entry)
    })
}

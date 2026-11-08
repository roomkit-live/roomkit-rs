//! A round's tool calls as the room's subscribers read them (RFC §6.4,
//! §8.4): `TOOL_CALL_START` with each call once the round announced them,
//! `TOOL_CALL_END` with each result and status once they ended, and an
//! aborted end for each when the round is cut while they run, so that every
//! start has its end. Python's `_publish_tool_event`, `_started_calls`,
//! `_ended_calls` and `_aborted_results`.
//!
//! A room's realtime channel has no visibility gate: a turn whose trigger
//! scopes its answers publishes nothing, and the kit publishes only while
//! the agent's binding lets its answers reach the room.

use roomkit::TurnHost;
use roomkit::model::{ChannelId, Metadata, RoomId, ToolCall};
use roomkit::realtime::{EphemeralEvent, EphemeralEventType};
use serde_json::{Value, json};

/// The characters of a result a `TOOL_CALL_END` previews: Python's
/// `_RESULT_PREVIEW`.
const RESULT_PREVIEW: usize = 500;

/// What the model reads of a call cut while it ran: Python's
/// `_aborted_results`.
const ABORTED: &str = "{\"error\": \"Tool call aborted\"}";

/// How one call of a round ended, as its `TOOL_CALL_END` reads it.
pub(super) struct Ended {
    pub(super) call: ToolCall,
    /// The result the model reads.
    pub(super) result: String,
    /// Whether the model reads it as a failure.
    pub(super) failed: bool,
}

impl Ended {
    /// `call` cut while it ran, as Python's `_aborted_results` ends it.
    fn aborted(call: ToolCall) -> Self {
        Self {
            call,
            result: ABORTED.to_owned(),
            failed: true,
        }
    }
}

/// Who reads a round's tool calls: the room's subscribers, through the
/// kit that runs the turn.
#[derive(Clone)]
pub(super) struct ToolEvents {
    turn: TurnHost,
    room_id: RoomId,
    channel_id: ChannelId,
    round: u32,
}

impl ToolEvents {
    /// Round `round`'s publisher, none outside a kit or for a turn whose
    /// trigger scopes its answers (`answers_room` false, RFC §8.4).
    pub(super) fn new(
        turn: Option<&TurnHost>,
        (room_id, channel_id): (&RoomId, &ChannelId),
        round: u32,
        answers_room: bool,
    ) -> Option<Self> {
        let turn = turn.filter(|_| answers_room)?;
        Some(Self {
            turn: turn.clone(),
            room_id: room_id.clone(),
            channel_id: channel_id.clone(),
            round,
        })
    }

    /// `TOOL_CALL_START`: each of `calls` with its id, its tool, its
    /// arguments, and its intent when it has one.
    pub(super) fn started<'a>(&self, calls: impl IntoIterator<Item = &'a ToolCall>) {
        let calls = calls
            .into_iter()
            .map(|call| {
                with_intent(
                    json!({
                        "id": call.id,
                        "name": call.name,
                        "arguments": Value::Object(call.arguments.clone()),
                    }),
                    call,
                )
            })
            .collect();
        self.publish(EphemeralEventType::ToolCallStart, calls, None);
    }

    /// `TOOL_CALL_END`: each of `ends` with its result's first
    /// [`RESULT_PREVIEW`] characters and its status, and its intent when its
    /// call has one; `duration_ms` the round's, once its calls ran.
    fn ended(&self, ends: &[Ended], duration_ms: Option<u64>) {
        let calls = ends
            .iter()
            .map(|end| {
                let preview: String = end.result.chars().take(RESULT_PREVIEW).collect();
                let status = if end.failed { "failed" } else { "completed" };
                with_intent(
                    json!({
                        "id": end.call.id,
                        "name": end.call.name,
                        "result": preview,
                        "status": status,
                    }),
                    &end.call,
                )
            })
            .collect();
        self.publish(EphemeralEventType::ToolCallEnd, calls, duration_ms);
    }

    /// Queues `event_type` with `calls`, the round and the channel.
    fn publish(&self, event_type: EphemeralEventType, calls: Vec<Value>, duration_ms: Option<u64>) {
        let mut data = Metadata::new();
        data.insert("tool_calls".to_owned(), Value::Array(calls));
        data.insert("round".to_owned(), Value::from(self.round));
        data.insert(
            "channel_id".to_owned(),
            Value::from(self.channel_id.to_string()),
        );
        if let Some(duration_ms) = duration_ms {
            data.insert("duration_ms".to_owned(), Value::from(duration_ms));
        }
        let event = EphemeralEvent {
            channel_id: Some(self.channel_id.clone()),
            data,
            ..EphemeralEvent::new(
                self.room_id.clone(),
                event_type,
                self.channel_id.to_string(),
            )
        };
        self.turn.publish_ephemeral(event);
    }
}

/// A round's calls from their start to their end rows handed to the
/// reader: `TOOL_CALL_START` once they are announced, `TOOL_CALL_END` once
/// the reader took every end, after the stored rows. Dropped before, the
/// round cut, it publishes the end each call's rows say: its own for the
/// ends the reader took, aborted for the others.
pub(super) struct Running {
    events: Option<ToolEvents>,
    calls: Vec<ToolCall>,
    /// How each call ended and the round's duration, once they ended.
    ended: Option<(Vec<Ended>, u64)>,
    /// How many of the ends the reader took.
    taken: usize,
}

impl Running {
    /// Publishes `calls`' start for `events`' subscribers, none without.
    pub(super) fn start<'a>(
        events: Option<ToolEvents>,
        calls: impl IntoIterator<Item = &'a ToolCall> + Clone,
    ) -> Self {
        let calls = match &events {
            Some(events) => {
                events.started(calls.clone());
                calls.into_iter().cloned().collect()
            }
            None => Vec::new(),
        };
        Self {
            events,
            calls,
            ended: None,
            taken: 0,
        }
    }

    /// Whether anyone reads the round's calls.
    pub(super) fn published(&self) -> bool {
        self.events.is_some()
    }

    /// The calls ended as `ends` say, the round having taken `duration_ms`.
    pub(super) fn calls_ended(&mut self, ends: Vec<Ended>, duration_ms: u64) {
        self.ended = Some((ends, duration_ms));
    }

    /// The reader took the next end.
    pub(super) fn took_end(&mut self) {
        self.taken += 1;
    }

    /// The reader took every end: their `TOOL_CALL_END`, with the round's
    /// duration.
    pub(super) fn handed(mut self) {
        if let (Some(events), Some((ends, duration_ms))) = (self.events.take(), &self.ended) {
            events.ended(ends, Some(*duration_ms));
        }
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        let Some(events) = self.events.take() else {
            return;
        };
        let calls = std::mem::take(&mut self.calls);
        let ends: Vec<Ended> = match self.ended.take() {
            None => calls.into_iter().map(Ended::aborted).collect(),
            Some((ended, _)) => {
                let taken = self.taken;
                let rest = calls.into_iter().skip(taken).map(Ended::aborted);
                ended.into_iter().take(taken).chain(rest).collect()
            }
        };
        events.ended(&ends, None);
    }
}

/// `fields` with the call's intent, only when it has one: Python's
/// `with_intent_field`.
fn with_intent(mut fields: Value, call: &ToolCall) -> Value {
    if let (Some(intent), Value::Object(object)) = (&call.intent, &mut fields) {
        object.insert("intent".to_owned(), Value::from(intent.clone()));
    }
    fields
}

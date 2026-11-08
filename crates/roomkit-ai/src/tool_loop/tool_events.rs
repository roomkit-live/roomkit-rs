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
pub(super) struct Ended<'a> {
    pub(super) call: &'a ToolCall,
    /// The result the model reads.
    pub(super) result: String,
    /// Whether the model reads it as a failure.
    pub(super) failed: bool,
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
    pub(super) fn ended(&self, ends: &[Ended<'_>], duration_ms: Option<u64>) {
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
                    end.call,
                )
            })
            .collect();
        self.publish(EphemeralEventType::ToolCallEnd, calls, duration_ms);
    }

    /// `TOOL_CALL_END` for each of `calls`, cut while they ran.
    pub(super) fn aborted(&self, calls: &[ToolCall]) {
        let ends: Vec<Ended<'_>> = calls
            .iter()
            .map(|call| Ended {
                call,
                result: ABORTED.to_owned(),
                failed: true,
            })
            .collect();
        self.ended(&ends, None);
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

/// A round's calls while they run: dropped before [`Running::ended`], the
/// round cut, it publishes their aborted end.
pub(super) struct Running<'a> {
    events: Option<&'a ToolEvents>,
    calls: Vec<ToolCall>,
}

impl<'a> Running<'a> {
    pub(super) fn new(events: Option<&'a ToolEvents>, calls: Vec<ToolCall>) -> Self {
        Self { events, calls }
    }

    /// The calls ended: nothing is published on drop any more.
    pub(super) fn ended(mut self) {
        self.events = None;
    }
}

impl Drop for Running<'_> {
    fn drop(&mut self) {
        if let Some(events) = self.events {
            events.aborted(&self.calls);
        }
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

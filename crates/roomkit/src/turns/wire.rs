//! The messages of a turn's link (RFC §10.6.3), each a record in the
//! serialized form of RFC §1.4, as the Python implementation writes them
//! (`roomkit.turns.wire`).
//!
//! Five families: the request, the frames (executor to owner), the control
//! messages (owner to executor), the calls with their results, and the
//! channel control outside any turn; and `room_call`, a host verb made
//! after its turn. A frame and a control message travel in an envelope that
//! numbers them per direction ([`super::pipe`]). The messages of features
//! 0.1 lacks are not modelled (`committed_event`, `channel_call`,
//! `room_publication`), but `ephemeral`, which a Python executor sends, is
//! read so that it can be dropped.

use std::collections::BTreeMap;

use roomkit_core::model::{
    ChannelBinding, ChannelId, EventContent, EventId, EventType, Metadata, Observation,
    ParticipantId, Room, RoomContext, RoomEvent, RoomId, Task, Timestamp, ToolCallOutcome,
    ToolCallStatus, Visibility,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::steering::SteeringDirective;

/// A tool the owner serves a turn (RFC §10.6.5): its definition as its
/// JSON, and whether its call waits by design.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct OwnerTool {
    pub(crate) tool: Value,
    #[serde(default)]
    pub(crate) waits_by_design: bool,
}

/// The room's previous turn of the channel (RFC §10.6.2).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PreviousTurn {
    pub(crate) turn_id: String,
    #[serde(default)]
    pub(crate) executor_id: Option<String>,
}

/// What the owner sends an executor to start one turn (RFC §10.6.3).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct TurnRequest {
    pub(crate) turn_id: String,
    pub(crate) room_id: RoomId,
    pub(crate) channel_id: ChannelId,
    pub(crate) event: RoomEvent,
    pub(crate) binding: ChannelBinding,
    pub(crate) context: RoomContext,
    #[serde(default)]
    pub(crate) strategy_turn: bool,
    #[serde(default)]
    pub(crate) notes: Vec<String>,
    #[serde(default)]
    pub(crate) in_place: bool,
    #[serde(default)]
    pub(crate) owner_tools: Vec<OwnerTool>,
    #[serde(default)]
    pub(crate) room_hooks: Vec<String>,
    #[serde(default)]
    pub(crate) previous_turn: Option<PreviousTurn>,
    #[serde(default)]
    pub(crate) trace: Option<BTreeMap<String, String>>,
}

/// A request without its events, the event it answers and the room's
/// history, which the in-memory backend hands over as they are (RFC
/// §10.6.1): what it serializes and parses.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct RequestRest {
    pub(crate) turn_id: String,
    pub(crate) room_id: RoomId,
    pub(crate) channel_id: ChannelId,
    pub(crate) binding: ChannelBinding,
    pub(crate) context: ContextRest,
    #[serde(default)]
    pub(crate) strategy_turn: bool,
    #[serde(default)]
    pub(crate) notes: Vec<String>,
    #[serde(default)]
    pub(crate) in_place: bool,
    #[serde(default)]
    pub(crate) owner_tools: Vec<OwnerTool>,
    #[serde(default)]
    pub(crate) room_hooks: Vec<String>,
    #[serde(default)]
    pub(crate) previous_turn: Option<PreviousTurn>,
    #[serde(default)]
    pub(crate) trace: Option<BTreeMap<String, String>>,
}

/// A room's context without its recent events.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct ContextRest {
    pub(crate) room: Room,
    #[serde(default)]
    pub(crate) bindings: Vec<ChannelBinding>,
    #[serde(default)]
    pub(crate) participants: Vec<roomkit_core::model::Participant>,
    #[serde(default)]
    pub(crate) author_register: Option<Metadata>,
}

/// An error as it crosses (RFC §10.6.8): its kind, its message, what
/// classifies it, and its causes in order.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct WireError {
    pub(crate) kind: String,
    pub(crate) message: String,
    #[serde(default)]
    pub(crate) args: Vec<Value>,
    #[serde(default)]
    pub(crate) attributes: Metadata,
    #[serde(default)]
    pub(crate) causes: Vec<WireError>,
    #[serde(default)]
    pub(crate) traceback: Option<String>,
    #[serde(default)]
    pub(crate) bases: Vec<String>,
}

/// A channel's output without its stream (RFC §10.6.3 `output`).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct WireOutput {
    pub(crate) responded: bool,
    pub(crate) response_events: Vec<RoomEvent>,
    pub(crate) response_metadata: Metadata,
    pub(crate) tasks: Vec<Task>,
    pub(crate) observations: Vec<Observation>,
    pub(crate) metadata_updates: Metadata,
    pub(crate) error: Option<WireError>,
}

/// A call's result as a tool-call end carries it: a JSON value, or the
/// content parts the Python implementation's tools may answer with.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum WireResult {
    Parts { parts: Vec<Value> },
    Value { value: Value },
}

/// A call's end, as `tool_ended` carries it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct ToolEnd {
    pub(crate) tool_name: String,
    pub(crate) tool_id: String,
    #[serde(default)]
    pub(crate) arguments: Metadata,
    pub(crate) result: WireResult,
    pub(crate) status: ToolCallStatus,
    #[serde(default)]
    pub(crate) duration_ms: u64,
    #[serde(default)]
    pub(crate) error: Option<String>,
    #[serde(default)]
    pub(crate) structured_content: Option<Metadata>,
    #[serde(default)]
    pub(crate) outcome: Option<ToolCallOutcome>,
    #[serde(default)]
    pub(crate) refused_but_ran: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) intent: Option<String>,
}

/// One item of a turn's stream (RFC §10.6.3 `item`), named by its `kind`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum Item {
    /// Text, or the text of deltas the channel yielded together, joined,
    /// with their number when more than one.
    Text {
        text: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        deltas: Option<u32>,
    },
    /// A reasoning delta.
    Thinking { thinking: String },
    /// A call's start.
    ToolStart {
        tool_name: String,
        tool_id: String,
        #[serde(default)]
        arguments: Metadata,
        #[serde(default)]
        ran_with: Option<Metadata>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        intent: Option<String>,
    },
    /// A call's end, naming the frame of its start.
    ToolEnd {
        #[serde(default)]
        start: Option<u64>,
        tool_name: String,
        tool_id: String,
        #[serde(default)]
        arguments: Metadata,
        result: WireResult,
        status: ToolCallStatus,
        #[serde(default)]
        duration_ms: u64,
        #[serde(default)]
        error: Option<String>,
        #[serde(default)]
        structured_content: Option<Metadata>,
        #[serde(default)]
        outcome: Option<ToolCallOutcome>,
        #[serde(default)]
        refused_but_ran: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        intent: Option<String>,
    },
    /// The loop goes on after a round that ended without a call.
    SegmentBreak,
    /// What a generation used, told after it (RFC §6.4).
    Usage {
        #[serde(default)]
        usage: Metadata,
        #[serde(default)]
        model: Option<String>,
        #[serde(default)]
        provider: Option<String>,
    },
    /// The loop's end: why, its rounds, its usage, the limits it ran under.
    LoopEnd {
        reason: String,
        #[serde(default)]
        rounds: u64,
        #[serde(default)]
        usage: Metadata,
        #[serde(default)]
        max_rounds: Option<u64>,
        #[serde(default)]
        timeout_seconds: Option<f64>,
        #[serde(default)]
        budget_tokens: Option<u64>,
        #[serde(default)]
        budget_usd: Option<f64>,
        #[serde(default)]
        last_usage: Option<Metadata>,
        #[serde(default)]
        usage_complete: Option<bool>,
        #[serde(default)]
        model: Option<String>,
        #[serde(default)]
        provider: Option<String>,
    },
    /// The stream's end.
    StreamEnd,
}

/// What a call's handler reads of the turn it serves (RFC §10.6.5, §21.4).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct CallContext {
    pub(crate) tool_call_id: String,
    pub(crate) room_id: String,
    pub(crate) channel_id: String,
    pub(crate) chain_depth: u32,
    pub(crate) actor_id: Option<String>,
    pub(crate) allowed_names: Option<Vec<String>>,
    pub(crate) requester: Option<String>,
}

/// A frame, executor to owner (RFC §10.6.3), carrying the turn by its
/// envelope.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum Frame {
    Accepted {
        executor_id: String,
    },
    Heartbeat,
    Output {
        output: WireOutput,
        #[serde(default)]
        streams: bool,
    },
    Item {
        item: Item,
    },
    ToolArgs {
        start: u64,
        arguments: Metadata,
    },
    ToolEnded {
        start: u64,
        end: ToolEnd,
    },
    Record {
        record: Metadata,
    },
    Error {
        error: WireError,
    },
    End,
    HostCall {
        call_id: String,
        verb: String,
        #[serde(default)]
        arguments: Metadata,
        #[serde(default)]
        context: CallContext,
    },
    CallCancel {
        call_id: String,
    },
    /// A call to a tool the owner serves the turn (RFC §10.6.5): its
    /// handler runs on the owner.
    ToolCall {
        call_id: String,
        name: String,
        #[serde(default)]
        arguments: Metadata,
        #[serde(default)]
        context: CallContext,
    },
    /// An ephemeral event a turn published, which no owner of 0.1 has a
    /// realtime backend to publish on.
    Ephemeral {
        channel: String,
        event: Value,
    },
    FrameworkEvent {
        event_type: String,
        #[serde(default)]
        room_id: Option<String>,
        #[serde(default)]
        channel_id: Option<String>,
        #[serde(default)]
        event_id: Option<String>,
        #[serde(default)]
        data: Metadata,
    },
    /// How many loops a `steer` reached; none for a channel without
    /// steering, whose owner then cuts the turn on a `Cancel` (RFC
    /// §10.6.8).
    SteerResult {
        steer_id: String,
        #[serde(default)]
        loops: Option<u64>,
    },
    /// A tool of the turn asked a person for input (RFC §10.6.5): sent
    /// before `ON_USER_INPUT_REQUIRED` fires, so the owner can answer it at
    /// once.
    InputRequest {
        request_id: String,
        tool_name: String,
        #[serde(default)]
        arguments: Metadata,
    },
    /// A request settled where its tool waits, by other than the owner's
    /// answer (an answer the handler took, a hook's block, its bound, a
    /// release): `reason` its status.
    InputClosed {
        request_id: String,
        reason: String,
    },
    /// Whether the owner's `input_answer` settled its request: one settled
    /// first where its tool waits did not take it.
    InputTaken {
        request_id: String,
        taken: bool,
    },
}

/// A control message, owner to executor, or the answer to a call (RFC
/// §10.6.3).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum Control {
    Pull {
        #[serde(default = "one")]
        count: u32,
        #[serde(default)]
        read: u64,
    },
    Close,
    Cancel {
        reason: String,
    },
    Heartbeat,
    HostResult {
        call_id: String,
        #[serde(default)]
        result: Value,
        #[serde(default)]
        error: Option<WireError>,
    },
    /// A steering directive for the turn's loop (RFC §21.3), answered by
    /// `steer_result`.
    Steer {
        steer_id: String,
        directive: SteeringDirective,
    },
    /// The owner's answer to one `tool_call`: `served` with the handler's
    /// answer as the model reads it, else its error.
    ToolResult {
        call_id: String,
        outcome: ToolOutcome,
        #[serde(default)]
        result: Option<Value>,
        #[serde(default)]
        error: Option<WireError>,
        #[serde(default)]
        structured_content: Option<Value>,
    },
    /// The outcome the owner recorded for an `input_request` (RFC
    /// §10.6.5): answered with its `value`, or rejected with its `reason`.
    InputAnswer {
        request_id: String,
        outcome: InputOutcome,
        #[serde(default)]
        value: Option<String>,
        #[serde(default)]
        reason: Option<String>,
    },
}

/// The outcome an `input_answer` carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum InputOutcome {
    /// Answered with a value.
    Answered,
    /// Rejected with a reason.
    Rejected,
}

/// How the owner's handler took a `tool_call` (RFC §10.6.5, §21.4).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ToolOutcome {
    /// It answered.
    Served,
    /// It refused the call: nothing ran.
    Refused,
    /// It ran and failed.
    Failed,
    /// Nothing on the owner serves it.
    Unserved,
}

fn one() -> u32 {
    1
}

/// A frame in its envelope: the turn it belongs to and its number, as the
/// cross-read reads it; the pipe writes its own.
#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct FrameEnvelope {
    pub(crate) turn_id: String,
    pub(crate) seq: u64,
    pub(crate) message: Frame,
}

/// A control message in its envelope.
#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct ControlEnvelope {
    pub(crate) turn_id: String,
    pub(crate) seq: u64,
    pub(crate) message: Control,
}

/// A message about a channel's per-room state, outside any turn (RFC
/// §10.6.3 channel control).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ChannelControl {
    RoomAttached {
        channel_id: ChannelId,
        room_id: RoomId,
        binding: Box<ChannelBinding>,
    },
    RoomDetached {
        channel_id: ChannelId,
        room_id: RoomId,
    },
}

/// A channel control message in its envelope.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct ChannelControlEnvelope {
    pub(crate) message: ChannelControl,
}

/// What an executor sends its owner outside a turn (RFC §10.6.3).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum OwnerMessage {
    /// A host verb made after its turn ended, answered by a `host_result`.
    RoomCall {
        call_id: String,
        channel_id: ChannelId,
        room_id: RoomId,
        executor_id: String,
        turn_id: String,
        #[serde(default)]
        chain_depth: u32,
        verb: String,
        #[serde(default)]
        arguments: Metadata,
    },
}

/// A message outside a turn in its envelope.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct OwnerMessageEnvelope {
    pub(crate) message: OwnerMessage,
}

/// The answer to a `room_call`: a `host_result` outside any turn's link.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct RoomCallResult {
    #[serde(rename = "type", default = "host_result_kind")]
    pub(crate) kind: String,
    pub(crate) call_id: String,
    #[serde(default)]
    pub(crate) result: Value,
    #[serde(default)]
    pub(crate) error: Option<WireError>,
}

/// The type a `host_result` is tagged with.
pub(crate) fn host_result_kind() -> String {
    "host_result".to_owned()
}

/// The arguments of `send_event`, `TurnHost`'s one verb (RFC §10.5,
/// §10.6.5), as the Python implementation's `SendEvent` record names them:
/// an argument the verb does not take is refused.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SendEvent {
    pub(crate) room_id: RoomId,
    pub(crate) channel_id: ChannelId,
    pub(crate) content: EventContent,
    #[serde(default = "message")]
    pub(crate) event_type: EventType,
    #[serde(default)]
    pub(crate) chain_depth: u32,
    #[serde(default)]
    pub(crate) participant_id: Option<ParticipantId>,
    #[serde(default)]
    pub(crate) metadata: Option<Metadata>,
    #[serde(default)]
    pub(crate) visibility: Visibility,
    #[serde(default)]
    pub(crate) provider: Option<String>,
    #[serde(default)]
    pub(crate) response_visibility: Option<Visibility>,
    #[serde(default)]
    pub(crate) created_at: Option<Timestamp>,
    #[serde(default)]
    pub(crate) parent_event_id: Option<EventId>,
    #[serde(default)]
    pub(crate) idempotency_key: Option<String>,
    #[serde(default)]
    pub(crate) addressed_to: Option<Vec<ChannelId>>,
    #[serde(default)]
    pub(crate) organization_id: Option<String>,
    #[serde(default)]
    pub(crate) standalone: bool,
}

fn message() -> EventType {
    EventType::Message
}

/// The verb `send_event` (RFC §10.6.5).
pub(crate) const SEND_EVENT: &str = "send_event";

/// The verb `post_status` (RFC §10.6.5, §19.8).
pub(crate) const POST_STATUS: &str = "post_status";

/// The verb `update_room_metadata` (RFC §10.6.5).
pub(crate) const UPDATE_ROOM_METADATA: &str = "update_room_metadata";

/// The verb `add_observation` (RFC §10.6.5, §5.11).
pub(crate) const ADD_OBSERVATION: &str = "add_observation";

/// The verb `delegate` (RFC §10.6.5, §23.3).
pub(crate) const DELEGATE: &str = "delegate";

/// The verb `wait_task` (RFC §10.6.5).
pub(crate) const WAIT_TASK: &str = "wait_task";

/// The verb `room_tasks` (RFC §10.6.5, §23.4).
pub(crate) const ROOM_TASKS: &str = "room_tasks";

/// The verbs a host still sends after its turn ended (RFC §10.6.5), for
/// the turn's room: Python's `BETWEEN_TURNS`.
pub(crate) const BETWEEN_TURNS: [&str; 4] = [
    SEND_EVENT,
    POST_STATUS,
    UPDATE_ROOM_METADATA,
    ADD_OBSERVATION,
];

/// `post_status`'s arguments, as the Python implementation's `PostStatus`
/// record names them: an entry on the kit's status bus.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PostStatus {
    pub(crate) agent_id: String,
    pub(crate) action: String,
    pub(crate) status: crate::orchestration::StatusLevel,
    #[serde(default)]
    pub(crate) detail: String,
    #[serde(default)]
    pub(crate) metadata: Option<Metadata>,
}

/// `update_room_metadata`'s arguments, as the Python implementation's
/// `UpdateRoomMetadata` record names them.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct UpdateRoomMetadata {
    pub(crate) room_id: RoomId,
    pub(crate) metadata: Metadata,
    #[serde(default)]
    pub(crate) organization_id: Option<String>,
}

/// `delegate`'s arguments, as the Python implementation's `Delegate`
/// record names them: the keywords of a background delegation that are
/// data.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Delegate {
    pub(crate) room_id: RoomId,
    pub(crate) agent_id: ChannelId,
    pub(crate) task: String,
    #[serde(default)]
    pub(crate) context: Option<Metadata>,
    #[serde(default)]
    pub(crate) share_channels: Option<Vec<ChannelId>>,
    #[serde(default)]
    pub(crate) notify: Option<ChannelId>,
    #[serde(default = "yes")]
    pub(crate) post_status: bool,
    #[serde(default)]
    pub(crate) brief: Option<String>,
    #[serde(default)]
    pub(crate) conversation: u64,
    #[serde(default = "relay")]
    pub(crate) hand_back: String,
}

fn yes() -> bool {
    true
}

fn relay() -> String {
    "relay".to_owned()
}

/// `wait_task`'s arguments: a task a host of the room delegated, and how
/// long to wait, in seconds.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WaitTask {
    pub(crate) task_id: String,
    #[serde(default)]
    pub(crate) timeout: Option<f64>,
}

/// `room_tasks`'s arguments: none.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RoomTasks {}

/// `add_observation`'s arguments, as the Python implementation's
/// `AddObservation` record names them.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AddObservation {
    pub(crate) observation: Observation,
}

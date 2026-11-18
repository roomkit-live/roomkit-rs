//! The messages of a turn's link (RFC §10.6.3), each a record in the
//! serialized form of RFC §1.4, as the Python implementation writes them
//! (`roomkit.turns.wire`).
//!
//! Five families: the request, the frames (executor to owner), the control
//! messages (owner to executor), the calls with their results, and the
//! channel control outside any turn; and `room_call`, a host verb made
//! after its turn. A frame and a control message travel in an envelope that
//! numbers them per direction (RFC §10.6.3). Channel control and
//! messages to the owner after a turn carry no turn-link sequence number.

#[cfg(any(test, feature = "redis"))]
mod carried_request;
mod input_request;
mod peer_errors;
pub use input_request::InputRequest;
pub(crate) use peer_errors::native_class;
#[cfg(feature = "redis")]
pub(crate) use peer_errors::peer_error_payload;

mod request;
#[cfg(feature = "redis")]
pub(crate) use request::RequestRest;
pub(crate) use request::{ContextRest, validate_rest};
pub use request::{OwnerTool, PreviousTurn, TurnRequest};
mod output;
pub(crate) use output::ErrorRecord as WireError;
pub use output::{ErrorRecord, Item, ToolEnd, WireLoopEnd, WireOutput, WireResult};
mod messages;
pub use messages::{CallContext, Control, Frame, InputOutcome, RecordChanges, ToolOutcome};

use roomkit_core::model::{
    ChannelBinding, ChannelId, EventContent, EventId, EventType, Metadata, Observation,
    ParticipantId, RoomEvent, RoomId, Timestamp, Visibility,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The enclosing wire enum writes the discriminator itself when it embeds a typed message.
fn message_fields<T: Serialize, S: serde::Serializer>(
    message: &T,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let mut value = serde_json::to_value(message).map_err(serde::ser::Error::custom)?;
    let fields = value
        .as_object_mut()
        .ok_or_else(|| serde::ser::Error::custom("a wire message must be an object"))?;
    fields.shift_remove("type");
    fields.serialize(serializer)
}

/// A message of one direction of a turn link. The sender borrows its ID and
/// message while encoding; the receiver owns the deserialized fields.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Envelope<M, Id = String> {
    /// Identifier shared by both ends of this turn.
    pub turn_id: Id,
    /// Sequence number within this turn and direction, starting at one.
    pub seq: u64,
    /// Message carried by this envelope.
    pub message: M,
}

/// A numbered frame from the executor (RFC §10.6.3).
pub type FrameEnvelope = Envelope<Frame>;
/// A numbered control message from the owner (RFC §10.6.3).
pub type ControlEnvelope = Envelope<Control>;

/// A message about a channel's per-room state, outside any turn (RFC
/// §10.6.3 channel control).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ChannelControl {
    /// Release the executor’s cached build of the channel.
    ChannelReleased {
        /// Channel whose work the message carries.
        channel_id: ChannelId,
    },
    /// Apply one named channel verb in its room.
    ChannelCall(#[serde(serialize_with = "message_fields")] Box<super::channel_calls::ChannelCall>),
    /// An owner attached the channel to a room.
    RoomAttached {
        /// Channel whose work the message carries.
        channel_id: ChannelId,
        /// Room in which the work takes place.
        room_id: RoomId,
        /// Organization of the room, when assigned.
        #[serde(default)]
        organization_id: Option<String>,
        /// Binding of the channel in the room.
        binding: Box<ChannelBinding>,
    },
    /// An owner detached the channel from a room.
    RoomDetached {
        /// Channel whose work the message carries.
        channel_id: ChannelId,
        /// Room in which the work takes place.
        room_id: RoomId,
        /// Organization of the room, when assigned.
        #[serde(default)]
        organization_id: Option<String>,
    },
    /// An event committed in the room that the channel's memory is handed
    /// (a discussion's, RFC §19.7.5 rule 3), answered by nothing.
    CommittedEvent {
        /// Channel whose work the message carries.
        channel_id: ChannelId,
        /// Room in which the work takes place.
        room_id: RoomId,
        /// Organization of the room, when assigned.
        #[serde(default)]
        organization_id: Option<String>,
        /// Room event carried by this message.
        event: Box<RoomEvent>,
        /// The room's hooks on executor triggers, as a turn's request names
        /// them.
        #[serde(default)]
        room_hooks: Vec<String>,
    },
}

/// A channel control message in its envelope.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ChannelControlEnvelope {
    /// Message carried by this envelope.
    pub message: ChannelControl,
}

/// What an executor sends its owner outside a turn (RFC §10.6.3).
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OwnerMessage {
    /// An executor requests an owner verb outside its turn.
    RoomCall(RoomCall),
    /// An executor reports an event after the end of its turn.
    RoomPublication(RoomPublication),
}

impl Serialize for OwnerMessage {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::RoomCall(call) => call.serialize(serializer),
            Self::RoomPublication(publication) => publication.serialize(serializer),
        }
    }
}

/// An event a channel asks its room owner to publish after the turn (RFC §10.6.3).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename = "room_publication")]
pub struct RoomPublication {
    /// Channel whose work the message carries.
    pub channel_id: ChannelId,
    /// Room in which the work takes place.
    pub room_id: RoomId,
    /// Executor that accepted the work.
    pub executor_id: String,
    /// Identifier shared by both ends of this turn.
    pub turn_id: String,
    /// Event the executor asks its owner to publish.
    pub frame: Box<PublicationFrame>,
}

/// A host verb made after its turn ended, answered by a `host_result`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename = "room_call")]
pub struct RoomCall {
    /// Identifier used to correlate the call and its answer.
    pub call_id: String,
    /// Channel whose work the message carries.
    pub channel_id: ChannelId,
    /// Room in which the work takes place.
    pub room_id: RoomId,
    /// Executor that accepted the work.
    pub executor_id: String,
    /// Identifier shared by both ends of this turn.
    pub turn_id: String,
    /// Depth of the response chain in the room.
    #[serde(default)]
    pub chain_depth: u32,
    /// Name of the requested operation.
    pub verb: String,
    /// Arguments supplied to the call.
    #[serde(default)]
    pub arguments: Metadata,
}

/// Only the two publication frames are valid outside a turn.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "Frame")]
pub struct PublicationFrame(Frame);

impl TryFrom<Frame> for PublicationFrame {
    type Error = &'static str;

    fn try_from(frame: Frame) -> Result<Self, Self::Error> {
        #[expect(
            clippy::wildcard_enum_match_arm,
            reason = "only these two publication kinds may cross outside a turn"
        )]
        match frame {
            Frame::Ephemeral { .. } | Frame::FrameworkEvent { .. } => Ok(Self(frame)),
            _ => Err("a room publication must carry an ephemeral or a framework event"),
        }
    }
}

impl From<PublicationFrame> for Frame {
    fn from(frame: PublicationFrame) -> Self {
        frame.0
    }
}

/// A message outside a turn in its envelope.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OwnerMessageEnvelope {
    /// Message carried by this envelope.
    pub message: OwnerMessage,
}

/// The answer to a `room_call`: a `host_result` outside any turn's link.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename = "host_result")]
pub struct HostResult {
    /// Identifier used to correlate the call and its answer.
    pub call_id: String,
    /// Result returned by the call.
    #[serde(default)]
    pub result: Value,
    /// Error reported by the sender, when the operation failed.
    #[serde(default)]
    pub error: Option<ErrorRecord>,
    /// Changes made to the response record while serving the call.
    #[serde(default)]
    pub record_changes: Option<RecordChanges>,
}

pub(crate) use HostResult as RoomCallResult;

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

/// The room camera note reader (RFC §10.6.5, §12.8.7).
pub(crate) const ROOM_VISION: &str = "room_vision";

/// The verb `room_context` (RFC §10.6.5, §10.6.6).
pub(crate) const ROOM_CONTEXT: &str = "room_context";

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

/// No arguments, as Python's `RoomVision`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RoomVision {}

/// `room_context`'s arguments: none.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReadRoomContext {}

/// `add_observation`'s arguments, as the Python implementation's
/// `AddObservation` record names them.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AddObservation {
    pub(crate) observation: Observation,
}

#[cfg(test)]
mod tests {
    use roomkit_core::model::{ChannelType, EventSource, TextContent};
    use serde_json::json;

    use super::*;

    // Python: tests/test_turns_backend.py::test_every_stream_item_crosses_as_a_new_equal_object
    // (InstructionReadMarker, roomkit/turns/_codec.py `instruction_read`).
    #[test]
    fn the_instruction_a_turn_read_crosses_as_python_writes_it() {
        let item = Item::InstructionRead {
            text: "Introduce yourself.".to_owned(),
            runtime_record: None,
        };
        let written = serde_json::to_value(&item).unwrap();
        assert_eq!(
            written,
            json!({"kind": "instruction_read", "text": "Introduce yourself."})
        );
        assert_eq!(serde_json::from_value::<Item>(written).unwrap(), item);
        // One the runtime wrote carries its runtime record (RMK-645).
        let handback = Item::InstructionRead {
            text: "[Your background workers completed.]".to_owned(),
            runtime_record: Some("handback".to_owned()),
        };
        let written = serde_json::to_value(&handback).unwrap();
        assert_eq!(
            written,
            json!({
                "kind": "instruction_read",
                "text": "[Your background workers completed.]",
                "runtime_record": "handback",
            })
        );
        assert_eq!(serde_json::from_value::<Item>(written).unwrap(), handback);
    }

    // Python: roomkit/turns/wire.py CommittedEvent, as ChannelControlEnvelope's
    // model_dump(mode="json") writes it (roomkit origin/main e9d11fda).
    #[test]
    fn a_committed_event_crosses_as_python_writes_it() {
        let room_id = RoomId::new("r1").unwrap();
        let ops = ChannelId::new("ops").unwrap();
        let event = RoomEvent::new(
            room_id.clone(),
            EventSource::new(ops, ChannelType::SMS),
            TextContent::new("hi"),
        );
        let envelope = ChannelControlEnvelope {
            message: ChannelControl::CommittedEvent {
                channel_id: ChannelId::new("a").unwrap(),
                room_id,
                organization_id: None,
                event: Box::new(event.clone()),
                room_hooks: Vec::new(),
            },
        };

        let mut written = serde_json::to_value(&envelope).unwrap();
        let sent = written["message"]
            .as_object_mut()
            .unwrap()
            .shift_remove("event")
            .unwrap();

        assert_eq!(
            written,
            json!({"message": {
                "type": "committed_event", "channel_id": "a", "room_id": "r1", "organization_id": null, "room_hooks": []
            }})
        );
        assert_eq!(sent, serde_json::to_value(&event).unwrap());
        // Python's form without the room's hooks reads back.
        let mut python = serde_json::to_value(&envelope).unwrap();
        python["message"]
            .as_object_mut()
            .unwrap()
            .shift_remove("room_hooks");
        let read: ChannelControlEnvelope = serde_json::from_value(python).unwrap();
        assert_eq!(read, envelope);
    }
}

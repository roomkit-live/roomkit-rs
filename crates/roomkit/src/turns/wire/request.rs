//! The owner’s request and the local representation of its non-event fields.
use roomkit_core::model::{
    ChannelBinding, ChannelId, Metadata, Room, RoomContext, RoomEvent, RoomId,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// A tool the owner serves a turn (RFC §10.6.5): its definition as its
/// JSON, whether its call waits by design, and whether its served call
/// ends the turn, a delegation's result tool (RFC §23.3).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OwnerTool {
    /// Tool definition advertised to the channel.
    pub tool: Value,
    /// Whether waiting for this tool is part of ordinary execution.
    #[serde(default)]
    pub waits_by_design: bool,
    /// Whether a successful call completes the turn.
    #[serde(default)]
    pub ends_turn: bool,
}

/// The room's previous turn of the channel (RFC §10.6.2).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreviousTurn {
    /// Identifier shared by both ends of this turn.
    pub turn_id: String,
    /// Executor that accepted the work.
    #[serde(default)]
    pub executor_id: Option<String>,
    #[serde(skip)]
    pub(crate) scopes: std::collections::VecDeque<(String, crate::turns::records::AnswerScope)>,
}

impl PreviousTurn {
    /// The last turn handed to an executor, with its executor ID when known.
    pub fn new(turn_id: impl Into<String>, executor_id: Option<String>) -> Self {
        Self {
            turn_id: turn_id.into(),
            executor_id,
            scopes: std::collections::VecDeque::new(),
        }
    }
}

/// What the owner sends an executor to start one turn (RFC §10.6.3).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TurnRequest {
    /// Identifier shared by both ends of this turn.
    pub turn_id: String,
    /// Room in which the work takes place.
    pub room_id: RoomId,
    /// Channel whose work the message carries.
    pub channel_id: ChannelId,
    /// Room event carried by this message.
    pub event: RoomEvent,
    /// Binding of the channel in the room.
    pub binding: ChannelBinding,
    /// Room context captured when the owner submitted the turn.
    pub context: RoomContext,
    /// Whether an owner-side strategy requested this generation.
    #[serde(default)]
    pub strategy_turn: bool,
    /// Instructions supplied by the owner for this turn.
    #[serde(default)]
    pub notes: Vec<String>,
    /// Whether the event retains its position in discussion history.
    #[serde(default)]
    pub in_place: bool,
    /// Tools the owner offers to serve for this turn.
    #[serde(default)]
    pub owner_tools: Vec<OwnerTool>,
    /// Names of room hooks the executor must define.
    #[serde(default)]
    pub room_hooks: Vec<String>,
    /// Previous turn that could have changed the channel’s state in this room.
    #[serde(default)]
    pub previous_turn: Option<PreviousTurn>,
    /// Trace context propagated across the transport.
    #[serde(default)]
    pub trace: Option<BTreeMap<String, String>>,
    /// Private channel history the executor needs, oldest event first.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub channel_rows: Vec<RoomEvent>,
    #[serde(skip)]
    pub(crate) handover: crate::turns::channel_calls::Handover,
}

impl TurnRequest {
    /// Start an ordinary turn with the channel binding and the snapshot its owner supplies (RFC §10.6.3).
    pub fn new(
        turn_id: impl Into<String>,
        room_id: RoomId,
        channel_id: ChannelId,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> Self {
        Self {
            turn_id: turn_id.into(),
            room_id,
            channel_id,
            event,
            binding,
            context,
            strategy_turn: false,
            notes: Vec::new(),
            in_place: false,
            owner_tools: Vec::new(),
            room_hooks: Vec::new(),
            previous_turn: None,
            trace: None,
            channel_rows: Vec::new(),
            handover: crate::turns::channel_calls::Handover::default(),
        }
    }

    /// Mark this attempt handed to an executor before waiting for acceptance.
    pub fn hand_over(&self) {
        self.handover.mark(true);
    }

    /// The selected executor refused this attempt without applying it.
    pub fn refused(&self) {
        self.handover.mark(false);
    }

    /// Whether an executor could hold the room state of the current attempt.
    pub fn handed(&self) -> bool {
        self.handover.handed()
    }
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
    pub(crate) history_start_index: Option<u64>,
    #[serde(default)]
    pub(crate) author_register: Option<Metadata>,
}

pub(crate) fn validate_rest(raw: &str) -> Result<RequestRest, String> {
    use crate::turns::pipe::MAX_MESSAGE_BYTES;
    if raw.len() > MAX_MESSAGE_BYTES {
        return Err(format!(
            "{} bytes, past the bound of {MAX_MESSAGE_BYTES}",
            raw.len()
        ));
    }
    serde_json::from_str(raw).map_err(|error| error.to_string())
}

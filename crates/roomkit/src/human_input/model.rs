//! A human-input request and what its hooks are handed (RFC §9.3, §10.6.5),
//! Python's `roomkit.models.pending_input`.

use roomkit_core::model::{ChannelType, Metadata, Timestamp};
use roomkit_core::ops::{Environment, SystemEnvironment};
use serde::{Deserialize, Serialize};

/// Where a human-input request stands: Python's `PendingInputStatus`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PendingInputStatus {
    /// Waiting for its answer.
    #[default]
    Pending,
    /// Answered.
    Resolved,
    /// Rejected: by a person, a hook's block, or the handler giving it up.
    Rejected,
    /// Nobody answered within its bound.
    TimedOut,
}

impl PendingInputStatus {
    /// The status as Python's enum writes its value.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Resolved => "resolved",
            Self::Rejected => "rejected",
            Self::TimedOut => "timed_out",
        }
    }
}

/// A human-input request, as its handler records it: Python's
/// `PendingInput`. It goes from `Pending` to `Resolved`, `Rejected` or
/// `TimedOut` once, first come first served (RFC §9.3).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PendingInput {
    /// The handler's id of the request, which answers it.
    pub pending_id: String,
    /// The tool that asks.
    pub tool_name: String,
    /// The tool's arguments: the questions, the options.
    pub arguments: Metadata,
    /// The room the call was made in, empty when unknown.
    pub room_id: String,
    /// The model's id of the call, empty when unknown.
    pub tool_call_id: String,
    /// The channel whose model made the call, empty when unknown.
    pub channel_id: String,
    /// Where the request stands.
    pub status: PendingInputStatus,
    /// The answer, once resolved.
    pub result: Option<String>,
    /// Why it was rejected, once rejected.
    pub reject_reason: Option<String>,
    /// Whether no one waits on it: its creator releases it
    /// ([`HumanInputHandler::create_detached`](super::HumanInputHandler::create_detached)).
    pub detached: bool,
    /// When it was made.
    pub created_at: Timestamp,
    /// The participant whose turn raised it, when the tool loop knew one.
    /// It names the turn without authenticating it.
    pub actor_id: Option<String>,
}

impl PendingInput {
    /// A request pending now, by the system clock, as Python's constructor
    /// defaults its instant.
    pub fn new(
        pending_id: impl Into<String>,
        tool_name: impl Into<String>,
        arguments: Metadata,
        room_id: impl Into<String>,
        tool_call_id: impl Into<String>,
        channel_id: impl Into<String>,
    ) -> Self {
        Self {
            pending_id: pending_id.into(),
            tool_name: tool_name.into(),
            arguments,
            room_id: room_id.into(),
            tool_call_id: tool_call_id.into(),
            channel_id: channel_id.into(),
            status: PendingInputStatus::Pending,
            result: None,
            reject_reason: None,
            detached: false,
            created_at: SystemEnvironment.now(),
            actor_id: None,
        }
    }
}

/// What the `ON_USER_INPUT_REQUIRED` hooks are handed (RFC §9.3): Python's
/// `PendingInputEvent`, the request a notification layer tells the person
/// of.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PendingInputEvent {
    /// The handler's id of the request, which answers it.
    pub pending_id: String,
    /// The tool that asks.
    pub tool_name: String,
    /// The tool's arguments: the questions, the options.
    pub arguments: Metadata,
    /// The room the call was made in.
    pub room_id: String,
    /// The model's id of the call.
    pub tool_call_id: String,
    /// The channel whose model made the call.
    pub channel_id: String,
    /// The type of that channel: the door the call came through.
    pub channel_type: ChannelType,
    /// When the request was made.
    pub timestamp: Timestamp,
    /// The participant whose turn raised the request, when the tool loop
    /// knew one.
    pub actor_id: Option<String>,
}

impl PendingInputEvent {
    /// The event of a request made now, by the system clock, as Python's
    /// constructor defaults its instant.
    pub fn new(
        pending_id: impl Into<String>,
        tool_name: impl Into<String>,
        arguments: Metadata,
        room_id: impl Into<String>,
        tool_call_id: impl Into<String>,
        channel_id: impl Into<String>,
        channel_type: ChannelType,
    ) -> Self {
        Self {
            pending_id: pending_id.into(),
            tool_name: tool_name.into(),
            arguments,
            room_id: room_id.into(),
            tool_call_id: tool_call_id.into(),
            channel_id: channel_id.into(),
            channel_type,
            timestamp: SystemEnvironment.now(),
            actor_id: None,
        }
    }
}

//! A room's conversation state (RFC §19.2): Python's
//! `roomkit.orchestration.state`. Which phase the conversation is in, which
//! agent handles it, and every transition so far, stored in the room's
//! metadata under `_conversation_state`.

use std::fmt;

use roomkit_core::model::{ChannelId, Metadata, Room, RoomId, Timestamp};
use roomkit_core::ops::{Environment, SystemEnvironment};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::RoomKitError;
use crate::store::{ConversationStore, PatchRoomMetadataOptions};

/// The room metadata key a conversation state is stored under (RFC §19.2).
const STATE_KEY: &str = "_conversation_state";

/// The built-in conversation phases. A phase is any string: routing and the
/// state do not restrict phases to these. A phase converts into the
/// state's text and compares with it (`state.phase == ConversationPhase::Intake`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConversationPhase {
    /// The conversation's start.
    Intake,
    /// The request is qualified.
    Qualification,
    /// An agent handles the request.
    Handling,
    /// The request is escalated.
    Escalation,
    /// The request is resolved.
    Resolution,
    /// After the resolution.
    Followup,
}

impl ConversationPhase {
    /// The phase as the state writes it.
    fn text(self) -> &'static str {
        match self {
            Self::Intake => "intake",
            Self::Qualification => "qualification",
            Self::Handling => "handling",
            Self::Escalation => "escalation",
            Self::Resolution => "resolution",
            Self::Followup => "followup",
        }
    }
}

impl fmt::Display for ConversationPhase {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.text())
    }
}

impl From<ConversationPhase> for String {
    fn from(phase: ConversationPhase) -> Self {
        phase.text().to_owned()
    }
}

impl PartialEq<ConversationPhase> for String {
    fn eq(&self, phase: &ConversationPhase) -> bool {
        self == phase.text()
    }
}

impl PartialEq<ConversationPhase> for &str {
    fn eq(&self, phase: &ConversationPhase) -> bool {
        *self == phase.text()
    }
}

impl PartialEq<String> for ConversationPhase {
    fn eq(&self, text: &String) -> bool {
        self.text() == text
    }
}

impl PartialEq<&str> for ConversationPhase {
    fn eq(&self, text: &&str) -> bool {
        self.text() == *text
    }
}

/// The record of one phase change (RFC §19.2).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PhaseTransition {
    /// The phase the conversation left.
    pub from_phase: String,
    /// The phase it entered.
    pub to_phase: String,
    /// The agent that handled it before, if any.
    #[serde(default)]
    pub from_agent: Option<ChannelId>,
    /// The agent that handles it after, if any.
    #[serde(default)]
    pub to_agent: Option<ChannelId>,
    /// Why it changed.
    #[serde(default)]
    pub reason: String,
    /// When it changed.
    #[serde(default = "now")]
    pub timestamp: Timestamp,
    /// Integrator-defined data.
    #[serde(default)]
    pub metadata: Metadata,
}

impl PhaseTransition {
    /// The change from `from_phase` to `to_phase` now, with Python's
    /// defaults for the rest.
    pub fn new(from_phase: impl Into<String>, to_phase: impl Into<String>) -> Self {
        Self {
            from_phase: from_phase.into(),
            to_phase: to_phase.into(),
            from_agent: None,
            to_agent: None,
            reason: String::new(),
            timestamp: now(),
            metadata: Metadata::new(),
        }
    }
}

/// A room's conversation progress (RFC §19.2): every field defaulted, so a
/// room without orchestration has none and reads the default.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ConversationState {
    /// The current phase, any string; `intake` by default.
    pub phase: String,
    /// The agent handling the conversation, if any.
    pub active_agent_id: Option<ChannelId>,
    /// The agent that handled it before the last transition, if any.
    pub previous_agent_id: Option<ChannelId>,
    /// How many transitions changed the agent.
    pub handoff_count: u32,
    /// When the current phase started.
    pub phase_started_at: Timestamp,
    /// Every transition, oldest first.
    pub phase_history: Vec<PhaseTransition>,
    /// Integrator-defined data, the room's `language` among it.
    pub context: Metadata,
    /// A handoff to a human holds the room for them: the router asks no agent
    /// until a transition gives it to one (RFC §19.6).
    pub human_hold: bool,
}

impl ConversationState {
    /// The default state, its phase started now.
    pub fn new() -> Self {
        Self {
            phase: ConversationPhase::Intake.into(),
            active_agent_id: None,
            previous_agent_id: None,
            handoff_count: 0,
            phase_started_at: now(),
            phase_history: Vec::new(),
            context: Metadata::new(),
            human_hold: false,
        }
    }

    /// A new state, the transition to `to_phase` recorded: `options.to_agent`
    /// becomes the active agent, the active one the previous, and a change
    /// of agent counts a handoff. This state is left as it is.
    #[must_use]
    pub fn transition(&self, to_phase: impl Into<String>, options: TransitionOptions) -> Self {
        let to_phase = to_phase.into();
        let record = PhaseTransition {
            from_agent: self.active_agent_id.clone(),
            to_agent: options.to_agent.clone(),
            reason: options.reason,
            metadata: options.metadata,
            ..PhaseTransition::new(self.phase.clone(), to_phase.clone())
        };
        let agent_changed = options.to_agent != self.active_agent_id;
        let human_hold = self.human_hold && options.to_agent.is_none();
        let mut phase_history = self.phase_history.clone();
        phase_history.push(record);
        Self {
            phase: to_phase,
            active_agent_id: options.to_agent,
            previous_agent_id: self.active_agent_id.clone(),
            handoff_count: self.handoff_count + u32::from(agent_changed),
            phase_started_at: now(),
            phase_history,
            context: self.context.clone(),
            human_hold,
        }
    }
}

impl Default for ConversationState {
    fn default() -> Self {
        Self::new()
    }
}

/// What [`ConversationState::transition`] takes beside the phase: Python's
/// `to_agent=None, reason="", metadata=None`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TransitionOptions {
    /// The agent that handles the conversation after it; `None`, none.
    pub to_agent: Option<ChannelId>,
    /// Why the phase changes.
    pub reason: String,
    /// The transition record's data.
    pub metadata: Metadata,
}

/// The conversation state `room`'s metadata holds, the default state when
/// it holds none.
///
/// # Errors
///
/// [`RoomKitError::Value`] for a stored state that is not one.
pub fn get_conversation_state(room: &Room) -> Result<ConversationState, RoomKitError> {
    match room.metadata.get(STATE_KEY) {
        None | Some(Value::Null) => Ok(ConversationState::new()),
        Some(stored) => ConversationState::deserialize(stored)
            .map_err(|error| RoomKitError::Value(format!("invalid conversation state: {error}"))),
    }
}

/// A copy of `room` holding `state` as its conversation state, its other
/// metadata kept. It writes nothing: [`save_conversation_state`] does.
pub fn set_conversation_state(room: &Room, state: &ConversationState) -> Room {
    let mut updated = room.clone();
    updated.metadata.insert(STATE_KEY.to_owned(), stored(state));
    updated
}

/// Stores `state` as `room_id`'s conversation state, writing that metadata
/// key alone: a full room write from a room read earlier would undo what
/// was written since, the room's register of authors among it (RFC §10.1
/// step 12).
///
/// # Errors
///
/// [`RoomKitError::RoomNotFound`] when the store lacks the room, and what
/// the store fails with.
pub async fn save_conversation_state<S: ConversationStore>(
    store: &S,
    room_id: &RoomId,
    state: &ConversationState,
) -> Result<(), RoomKitError> {
    let patch = Metadata::from_iter([(STATE_KEY.to_owned(), stored(state))]);
    let patched = store
        .patch_room_metadata(room_id, patch, PatchRoomMetadataOptions::default())
        .await?;
    match patched {
        Some(_) => Ok(()),
        None => Err(RoomKitError::RoomNotFound(room_id.clone())),
    }
}

/// `state` in the form the room's metadata holds it.
fn stored(state: &ConversationState) -> Value {
    serde_json::to_value(state).unwrap_or_default()
}

/// The instant a state or a transition is made at: the system clock, as
/// Python's `datetime.now(UTC)`.
fn now() -> Timestamp {
    SystemEnvironment.now()
}

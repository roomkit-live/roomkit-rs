//! What a host reads of a discussion: its speak queue, and each change of it
//! (RFC §19.7.5). Python's `orchestration/strategies/discussion/models.py`.

use std::collections::BTreeSet;

use roomkit_core::model::{ChannelId, EventId, RoomId};
use roomkit_core::rules::hook_registry::HookTrigger;
use serde::{Deserialize, Serialize};

use crate::hooks::TurnPayload;

/// A room's speak queue as the host reads it (RFC §19.7.5).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpeakQueue {
    /// The agent whose turn runs now, or `None` between turns.
    pub speaking: Option<ChannelId>,
    /// The agents owed a turn, in the order they get it.
    pub queue: Vec<ChannelId>,
    /// The agents that only listen (`listen_only`).
    pub listening: BTreeSet<ChannelId>,
    /// (agent, person): an agent that asked a person and waits for the
    /// answer.
    pub asked: Vec<(ChannelId, String)>,
    /// Whether the discussion waits for a person's message.
    pub waiting: bool,
    /// Whether the discussion is over: it gives no further turn.
    pub over: bool,
}

/// What changed in a room's speak queue, as `ON_SPEAK_QUEUE` reports it,
/// written as Python's `StrEnum` writes it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpeakQueueChange {
    /// An agent is owed a turn.
    Queued,
    /// An agent's turn starts.
    TurnGiven,
    /// An agent's turn ended.
    TurnEnded,
    /// An instruction's turn will not run.
    InstructionDropped,
    /// An agent only listens now.
    Listening,
    /// An agent takes turns again.
    TalkingAgain,
    /// The discussion waits for a person's message.
    Waiting,
    /// The discussion is over.
    Over,
}

/// Carried to `ON_SPEAK_QUEUE` hooks: one change of a room's speak queue.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpeakQueueEvent {
    /// The room.
    pub room_id: RoomId,
    /// The queue as it is once changed.
    pub queue: SpeakQueue,
    /// What changed.
    pub change: SpeakQueueChange,
    /// The agents the change is about (queued, given a turn, listening...).
    pub channel_ids: Vec<ChannelId>,
    /// The event behind the change, when one is (the message that queued,
    /// the event a turn answers).
    pub event_id: Option<EventId>,
}

impl TurnPayload for SpeakQueueEvent {
    const TRIGGER: HookTrigger = HookTrigger::OnSpeakQueue;
}

impl SpeakQueueEvent {
    /// A change of the queue of `room_id`, about no agent and no event yet,
    /// Python's defaults: `channel_ids=()`, `event_id=None`.
    #[must_use]
    pub fn new(room_id: RoomId, queue: SpeakQueue, change: SpeakQueueChange) -> Self {
        Self {
            room_id,
            queue,
            change,
            channel_ids: Vec::new(),
            event_id: None,
        }
    }
}

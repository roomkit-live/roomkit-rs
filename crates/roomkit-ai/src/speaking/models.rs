//! Public speaking carriers. Durations are numbers, not validated timers.

use roomkit::ChannelError;
use roomkit::model::Timestamp;
use serde::{Deserialize, Serialize};

use super::{SharedRoomEvent, Thought};
use crate::classifiers::error::invalid;

crate::shared::ordered::ordered_mapping!(
    /// The policy's measured judgments, retaining the supplied mapping.
    Judgments, f64
);
crate::shared::ordered::ordered_mapping!(
    /// Speaker labels by event id, in the order supplied by the caller.
    Speakers, String
);

/// Whether to answer, briefly offer to contribute, or stay silent.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpeakMode {
    /// Answer the turn.
    Speak,
    /// Offer a contribution in one short sentence.
    Offer,
    /// Run no answer turn.
    Silent,
}

/// Optional fields of [`SpeakDecision`], with Python's defaults.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SpeakDecisionOptions {
    /// Why the policy chose this mode.
    pub reason: String,
    /// The judgments it weighed.
    pub judgments: Judgments,
    /// Blocks added to the answer's notes.
    pub notes: Vec<String>,
    /// A silence the thought will not change now.
    pub r#final: bool,
}

/// Whether the agent speaks and what it reads if it does (RFC §6.4).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SpeakDecision {
    /// The chosen mode.
    pub mode: SpeakMode,
    /// Why, in a few words, for observers.
    pub reason: String,
    /// Measured judgments, shared with the mapping supplied at construction.
    pub judgments: Judgments,
    /// Note blocks for a speaking or offering turn.
    pub notes: Vec<String>,
    /// Whether this silence does not wait for a new thought.
    pub r#final: bool,
}

impl SpeakDecision {
    /// A decision, refusing a final mode other than silent.
    ///
    /// # Errors
    /// `ValueError` if a final decision is not silent.
    pub fn new(mode: SpeakMode, options: SpeakDecisionOptions) -> Result<Self, ChannelError> {
        if options.r#final && mode != SpeakMode::Silent {
            return Err(invalid("ValueError", "only a silent decision can be final"));
        }
        Ok(Self {
            mode,
            reason: options.reason,
            judgments: options.judgments,
            notes: options.notes,
            r#final: options.r#final,
        })
    }
}

impl From<SpeakMode> for SpeakDecision {
    fn from(mode: SpeakMode) -> Self {
        Self {
            mode,
            reason: String::new(),
            judgments: Judgments::new(),
            notes: Vec::new(),
            r#final: false,
        }
    }
}

/// An answer cut off by someone speaking. This is a carrier, not a voice backend.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CutReply {
    /// The text handed to speech when cut.
    pub text: String,
    /// How much played, without a new sign or integer restriction.
    pub played_ms: f64,
    /// When it was cut.
    pub at: Timestamp,
}

/// What a policy judges: the trigger, conversation, people and current thought.
#[derive(Clone, Debug, PartialEq)]
pub struct SpeakTurn {
    /// The same event retained by the caller.
    pub event: SharedRoomEvent,
    /// Messages preceding it, oldest first, without tool records.
    pub recent: Vec<SharedRoomEvent>,
    /// People taking part besides the agent, as the conversation labels them.
    pub people: Vec<String>,
    /// The agent's channel; empty by default.
    pub channel_id: String,
    /// Labels for the trigger and recent events, by event id.
    pub speakers: Speakers,
    /// The agent's current thought, when it has a thinker.
    pub thought: Option<Thought>,
    /// Its cut-off answer, if it has not spoken since.
    pub cut: Option<CutReply>,
}

impl SpeakTurn {
    /// A turn with Python's empty context defaults.
    pub fn new(event: impl Into<SharedRoomEvent>) -> Self {
        Self {
            event: event.into(),
            recent: Vec::new(),
            people: Vec::new(),
            channel_id: String::new(),
            speakers: Speakers::new(),
            thought: None,
            cut: None,
        }
    }

    /// Whether this event is one of the agent's own answers.
    pub fn by_agent(&self, event: &SharedRoomEvent) -> bool {
        !self.channel_id.is_empty()
            && event.snapshot().source.channel_id.as_str() == self.channel_id
    }
}

/// The decision reported for one channel's event.
#[derive(Clone, Debug, PartialEq)]
pub struct SpeakDecisionEvent {
    /// The room, allowing the empty identifier of the Python carrier.
    pub room_id: String,
    /// The channel, likewise unvalidated by this carrier.
    pub channel_id: String,
    /// The event decided on, shared with its caller.
    pub event: SharedRoomEvent,
    /// The decision, including its shared judgments.
    pub decision: SpeakDecision,
    /// The call's elapsed milliseconds, or its bound if it expired.
    pub duration_ms: f64,
    /// The policy was asked again after a thought came back.
    pub asked_again: bool,
    /// The thought it was asked on.
    pub thought: Option<Thought>,
    /// No answer runs on this late decision; observers may act on it.
    pub late: bool,
}

impl SpeakDecisionEvent {
    /// A first decision, with no duration or thought supplied.
    pub fn new(
        room_id: impl Into<String>,
        channel_id: impl Into<String>,
        event: impl Into<SharedRoomEvent>,
        decision: SpeakDecision,
    ) -> Self {
        Self {
            room_id: room_id.into(),
            channel_id: channel_id.into(),
            event: event.into(),
            decision,
            duration_ms: 0.0,
            asked_again: false,
            thought: None,
            late: false,
        }
    }
}

/// One channel's new thought in one room.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ThoughtEvent {
    /// The room identifier, without construction-time validation.
    pub room_id: String,
    /// The channel identifier, without construction-time validation.
    pub channel_id: String,
    /// The new thought.
    pub thought: Thought,
    /// The thought replaced, empty by default.
    pub previous: Thought,
    /// Absent when speaking emptied the thought without a thinker call.
    pub duration_ms: Option<f64>,
}

impl ThoughtEvent {
    /// A thought change without a previous thought or measured duration.
    pub fn new(
        room_id: impl Into<String>,
        channel_id: impl Into<String>,
        thought: Thought,
    ) -> Self {
        Self {
            room_id: room_id.into(),
            channel_id: channel_id.into(),
            thought,
            previous: Thought::default(),
            duration_ms: None,
        }
    }
}

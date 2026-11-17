//! What an agent thinks while listening, and when it takes the turn.

mod answer_only;

mod mock;
mod models;
mod policy;
mod thought;

use roomkit::model::RoomEvent;

pub use answer_only::AnswerOnly;
pub use mock::{MockSpeakPolicy, MockSpeakPolicyOptions, MockThinker, MockThinkerOptions};
pub use models::{
    CutReply, Judgments, SpeakDecision, SpeakDecisionEvent, SpeakDecisionOptions, SpeakMode,
    SpeakTurn, Speakers, ThoughtEvent,
};
pub use policy::{AlwaysSpeak, SpeakPolicy, SpeakingFuture, Thinker};
pub use thought::Thought;

crate::shared::shared_input!(
    /// An event retained by a speaking turn or decision observer. Clones keep
    /// the same local object; this does not edit the store's independent record.
    SharedRoomEvent, RoomEvent
);

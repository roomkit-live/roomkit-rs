//! The speaking and thinking interfaces, independent of a provider.

use std::future::Future;
use std::pin::Pin;

use roomkit::ChannelError;

use super::{SpeakDecision, SpeakMode, SpeakTurn, Thought};
use crate::SharedAiContext;

/// A speaking operation that may retain any host error and its cause.
pub type SpeakingFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, ChannelError>> + Send + 'a>>;

/// Decides on every event the agent would answer. Instructions bypass it.
pub trait SpeakPolicy: Send + Sync + 'static {
    /// The decision on this turn.
    fn decide<'a>(&'a self, turn: &'a SpeakTurn) -> SpeakingFuture<'a, SpeakDecision>;

    /// Forgets state when the channel joins or leaves a room; optional.
    ///
    /// # Errors
    /// Implementations may return an underlying state-cleanup error.
    fn forget_room(&self, _room_id: &str) -> Result<(), ChannelError> {
        Ok(())
    }

    /// Releases owned resources. A channel does not close its supplied policy.
    fn close(&self) -> SpeakingFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

/// Rewrites the agent's thought while it listens, without producing a response.
pub trait Thinker: Send + Sync + 'static {
    /// Thinks on the exact context selected by the generation hook.
    fn think<'a>(
        &'a self,
        previous: &'a Thought,
        context: &'a SharedAiContext,
    ) -> SpeakingFuture<'a, Thought>;

    /// Releases owned resources. A channel cancels its work, not this resource.
    fn close(&self) -> SpeakingFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

/// Always answers, reporting `always` as the decision's reason.
#[derive(Clone, Copy, Debug, Default)]
pub struct AlwaysSpeak;

impl SpeakPolicy for AlwaysSpeak {
    fn decide<'a>(&'a self, _turn: &'a SpeakTurn) -> SpeakingFuture<'a, SpeakDecision> {
        Box::pin(async {
            Ok(SpeakDecision {
                reason: "always".into(),
                ..SpeakMode::Speak.into()
            })
        })
    }
}

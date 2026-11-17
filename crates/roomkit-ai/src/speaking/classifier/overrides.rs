//! Local composition of the policy's public rules, with a lazy standard rule.

use super::ClassifierSpeakPolicy;
use crate::classifiers::{Answers, Questions, SharedState};
use crate::speaking::{SpeakDecision, SpeakTurn};
use roomkit::ChannelError;
use std::fmt;
use std::sync::Arc;

/// A state override and the same policy's standard implementation.
pub type SpeakStateRule = Arc<
    dyn Fn(
            &ClassifierSpeakPolicy,
            &SpeakTurn,
            &dyn Fn(&SpeakTurn) -> Result<SharedState, ChannelError>,
        ) -> Result<SharedState, ChannelError>
        + Send
        + Sync,
>;

/// A questions override and the standard implementation, without redispatch.
pub type SpeakQuestionsRule = Arc<
    dyn Fn(
            &ClassifierSpeakPolicy,
            &SpeakTurn,
            &dyn Fn(&SpeakTurn) -> Result<Questions, ChannelError>,
        ) -> Result<Questions, ChannelError>
        + Send
        + Sync,
>;

/// A decision override. Calling its standard rule is optional and synchronous.
pub type SpeakDecisionRule = Arc<
    dyn Fn(
            &ClassifierSpeakPolicy,
            &SpeakTurn,
            &Answers,
            &dyn Fn(&SpeakTurn, &Answers) -> Result<SpeakDecision, ChannelError>,
        ) -> Result<SpeakDecision, ChannelError>
        + Send
        + Sync,
>;

/// Optional overrides; no standard rule runs before its callback asks for it.
#[derive(Clone, Default)]
pub struct ClassifierSpeakOverrides {
    /// Replaces or extends the state built for classification.
    pub state: Option<SpeakStateRule>,
    /// Replaces or extends the questions for classification.
    pub questions: Option<SpeakQuestionsRule>,
    /// Replaces or extends decision composition and its listening-state effects.
    pub decision: Option<SpeakDecisionRule>,
}

impl fmt::Debug for ClassifierSpeakOverrides {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClassifierSpeakOverrides")
            .field("state", &self.state.is_some())
            .field("questions", &self.questions.is_some())
            .field("decision", &self.decision.is_some())
            .finish()
    }
}

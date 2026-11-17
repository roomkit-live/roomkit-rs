//! A classifier supplies narrow judgments; this policy keeps the room's state.

mod decisions;
mod inputs;
mod overrides;
mod words;

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use roomkit::ChannelError;
use serde_json::Value;

use super::{SpeakDecision, SpeakMode, SpeakPolicy, SpeakTurn, SpeakingFuture};
use crate::classifiers::error::invalid;
use crate::classifiers::{Answers, ChoiceOptions, Classifier, Questions, SharedState};
use crate::shared::Shared;

pub use overrides::{
    ClassifierSpeakOverrides, SpeakDecisionRule, SpeakQuestionsRule, SpeakStateRule,
};

/// Optional classifier policy arguments, with Python's defaults.
#[derive(Clone, Debug)]
pub struct ClassifierSpeakPolicyOptions {
    /// The agent's role, when it helps judge whether a turn is for it.
    pub agent_role: String,
    /// Questions replacing the standard ones by name, or added to them.
    pub questions: Option<Questions>,
    /// Languages and their note blocks, in the supplied order.
    pub languages: Option<ChoiceOptions>,
    /// Recent text turns read; negative values retain Python's slice semantics.
    pub history: i64,
    /// Required certainty for an unasked contribution, in (0, 1].
    pub proactivity: f64,
    /// Optional compositions of the three public preparation/decision methods.
    pub overrides: ClassifierSpeakOverrides,
}

impl Default for ClassifierSpeakPolicyOptions {
    fn default() -> Self {
        Self {
            agent_role: String::new(),
            questions: None,
            languages: None,
            history: 6,
            proactivity: 0.5,
            overrides: ClassifierSpeakOverrides::default(),
        }
    }
}

/// Decides whether to speak, retaining an explicit listening state per room.
/// The supplied classifier remains its caller's resource and is not closed here.
pub struct ClassifierSpeakPolicy {
    classifier: Arc<dyn Classifier>,
    agent_name: String,
    options: ClassifierSpeakPolicyOptions,
    questions: Questions,
    languages: ChoiceOptions,
    rooms: Shared<HashMap<String, Value>>,
}

impl fmt::Debug for ClassifierSpeakPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClassifierSpeakPolicy")
            .field("agent_name", &self.agent_name)
            .finish_non_exhaustive()
    }
}

impl ClassifierSpeakPolicy {
    /// Builds a policy, copying the outer question and language mappings.
    /// Nested option handles inside supplied questions stay shared.
    ///
    /// # Errors
    /// `ValueError` for proactivity outside (0, 1] or the reserved language `other`.
    pub fn new(
        classifier: Arc<dyn Classifier>,
        agent_name: impl Into<String>,
        options: ClassifierSpeakPolicyOptions,
    ) -> Result<Self, ChannelError> {
        if !(options.proactivity > 0.0 && options.proactivity <= 1.0) {
            return Err(invalid("ValueError", "proactivity must be in (0, 1]"));
        }
        let languages = options
            .languages
            .as_ref()
            .map_or_else(ChoiceOptions::new, |map| map.iter().collect());
        if languages.get("other").is_some() {
            return Err(invalid(
                "ValueError",
                "'other' is kept for a language not listed",
            ));
        }
        let questions = words::base();
        if let Some(added) = &options.questions {
            for (name, question) in added {
                questions.insert(name, question);
            }
        }
        Ok(Self {
            classifier,
            agent_name: agent_name.into(),
            options,
            questions,
            languages,
            rooms: Shared::new(HashMap::new()),
        })
    }

    /// What the classifier reads, through the supplied state override if any.
    ///
    /// # Errors
    /// Returns any error raised by the application's state rule.
    pub fn state(&self, turn: &SpeakTurn) -> Result<SharedState, ChannelError> {
        self.options.overrides.state.as_ref().map_or_else(
            || Ok(self.standard_state(turn)),
            |rule| rule(self, turn, &|turn| Ok(self.standard_state(turn))),
        )
    }

    /// The questions asked on this turn, through its optional override.
    ///
    /// # Errors
    /// Returns any error raised by the application's questions rule.
    pub fn questions(&self, turn: &SpeakTurn) -> Result<Questions, ChannelError> {
        self.options.overrides.questions.as_ref().map_or_else(
            || Ok(self.standard_questions(turn)),
            |rule| rule(self, turn, &|turn| Ok(self.standard_questions(turn))),
        )
    }

    /// Composes answers and updates listening state, unless replaced by its rule.
    ///
    /// # Errors
    /// Returns any error raised by the application's decision rule.
    pub fn decision(
        &self,
        turn: &SpeakTurn,
        answers: &Answers,
    ) -> Result<SpeakDecision, ChannelError> {
        self.options.overrides.decision.as_ref().map_or_else(
            || Ok(self.standard_decision(turn, answers)),
            |rule| {
                rule(self, turn, answers, &|turn, answers| {
                    Ok(self.standard_decision(turn, answers))
                })
            },
        )
    }

    fn listening(&self, turn: &SpeakTurn) -> Option<Value> {
        let room_id = turn.event.snapshot().room_id;
        self.rooms
            .update(|rooms| rooms.get(room_id.as_str()).cloned())
    }

    async fn classify(&self, turn: &SpeakTurn) -> Result<Answers, ChannelError> {
        let state = self.state(turn)?;
        let questions = self.questions(turn)?;
        self.classifier.classify(&state, &questions).await
    }
}

impl SpeakPolicy for ClassifierSpeakPolicy {
    fn decide<'a>(&'a self, turn: &'a SpeakTurn) -> SpeakingFuture<'a, SpeakDecision> {
        Box::pin(async move {
            let listening = self.listening(turn).is_some();
            if inputs::text(&turn.event).is_empty() {
                return Ok(if listening {
                    decisions::listening_silence()
                } else {
                    SpeakDecision {
                        reason: "nothing to judge".into(),
                        ..SpeakMode::Speak.into()
                    }
                });
            }
            let answers = match self.classify(turn).await {
                Ok(answers) => answers,
                Err(_) if listening => {
                    tracing::warn!(room_id = %turn.event.snapshot().room_id, "Classifier failed in listening room");
                    return Ok(decisions::listening_silence());
                }
                Err(error) => return Err(error),
            };
            self.decision(turn, &answers)
        })
    }

    fn forget_room(&self, room_id: &str) -> Result<(), ChannelError> {
        self.rooms.update(|rooms| rooms.remove(room_id));
        Ok(())
    }
}

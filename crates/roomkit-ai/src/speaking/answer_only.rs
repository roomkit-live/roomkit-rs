//! A policy that hears everyone and answers only configured people (RFC §6.4).

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use roomkit::ChannelError;
use roomkit_core::rules::discussion::people::label_name;
use roomkit_core::rules::{casefold, person_name};

use super::{SpeakDecision, SpeakMode, SpeakPolicy, SpeakTurn, SpeakingFuture};
use crate::classifiers::error::invalid;

const LISTENED_TO: &str = "only listened to";

fn key(name: &str) -> String {
    casefold(&person_name(label_name(name)))
}

/// Answers only the named people through another policy. Name matching is
/// not authorization: a participant may choose any display name.
#[derive(Clone)]
pub struct AnswerOnly {
    policy: Arc<dyn SpeakPolicy>,
    people: BTreeSet<String>,
}

impl fmt::Debug for AnswerOnly {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AnswerOnly")
            .field("people", &self.people)
            .finish_non_exhaustive()
    }
}

impl AnswerOnly {
    /// Wraps a policy for at least one nonempty normalized name. A string
    /// alone is not an iterator of names; pass a collection of strings.
    ///
    /// # Errors
    /// `ValueError` when no supplied name remains after normalization.
    pub fn new<S: AsRef<str>>(
        policy: Arc<dyn SpeakPolicy>,
        people: impl IntoIterator<Item = S>,
    ) -> Result<Self, ChannelError> {
        let people: BTreeSet<String> = people
            .into_iter()
            .map(|name| key(name.as_ref()))
            .filter(|name| !name.is_empty())
            .collect();
        if people.is_empty() {
            return Err(invalid(
                "ValueError",
                "AnswerOnly needs at least one person to answer",
            ));
        }
        Ok(Self { policy, people })
    }

    /// Whether this optional room label names a person the agent answers.
    pub fn answers(&self, name: Option<&str>) -> bool {
        name.is_some_and(|name| self.people.contains(&key(name)))
    }

    fn judged_with(&self, turn: &SpeakTurn, speaker: &str) -> Vec<String> {
        let mut people: Vec<String> = turn
            .people
            .iter()
            .filter(|name| self.answers(Some(name)))
            .cloned()
            .collect();
        if !people.iter().any(|name| key(name) == key(speaker)) {
            people.push(speaker.to_owned());
        }
        people
    }
}

impl SpeakPolicy for AnswerOnly {
    fn decide<'a>(&'a self, turn: &'a SpeakTurn) -> SpeakingFuture<'a, SpeakDecision> {
        Box::pin(async move {
            let speaker = turn.speakers.get(turn.event.snapshot().id.as_str());
            let Some(speaker) = speaker.filter(|name| self.answers(Some(name))) else {
                return Ok(SpeakDecision {
                    reason: LISTENED_TO.into(),
                    r#final: true,
                    ..SpeakMode::Silent.into()
                });
            };
            let judged = SpeakTurn {
                people: self.judged_with(turn, &speaker),
                ..turn.clone()
            };
            self.policy.decide(&judged).await
        })
    }

    fn forget_room(&self, room_id: &str) -> Result<(), ChannelError> {
        self.policy.forget_room(room_id)
    }

    fn close(&self) -> SpeakingFuture<'_, ()> {
        self.policy.close()
    }
}

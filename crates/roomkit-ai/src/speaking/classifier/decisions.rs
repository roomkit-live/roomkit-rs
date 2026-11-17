//! Listening is remembered per room; the answers say how this turn was asked.

use serde_json::json;
use std::sync::OnceLock;

use super::{ClassifierSpeakPolicy, inputs, words};
use crate::classifiers::{Answer, Answers};
use crate::server_instructions::bounded_text;
use crate::speaking::compose::greater;
use crate::speaking::{
    ComposeOptions, Judgments, SpeakDecision, SpeakMode, SpeakTurn, compose, judgments_of,
};

impl ClassifierSpeakPolicy {
    pub(super) fn standard_decision(&self, turn: &SpeakTurn, answers: &Answers) -> SpeakDecision {
        if self.judged_listening(turn, answers) {
            return self.while_listening(turn, answers);
        }
        let judgments = judgments_of(answers);
        if judgments.get("listen_request").unwrap_or(0.0) >= 0.5 {
            let event = turn.event.snapshot();
            let speaker = turn
                .speakers
                .get(event.id.as_str())
                .unwrap_or_else(|| "someone".into());
            let request =
                json!({"speaker": speaker, "text": bounded_text(&inputs::text(&turn.event), 200)});
            self.rooms
                .update(|rooms| rooms.insert(event.room_id.to_string(), request));
            return decision(
                SpeakMode::Silent,
                "asked to listen",
                judgments,
                vec![],
                true,
            );
        }
        self.composed(turn, answers)
    }

    fn judged_listening(&self, turn: &SpeakTurn, answers: &Answers) -> bool {
        if answers.get("listen_request").is_some() {
            return false;
        }
        answers.get("asked_me").is_some()
            || answers.get("lift").is_some()
            || self.listening(turn).is_some()
    }

    fn while_listening(&self, turn: &SpeakTurn, answers: &Answers) -> SpeakDecision {
        let judgments = judgments_of(answers);
        judgments.insert("listening", 1.0);
        if judgments.get("lift").unwrap_or(0.0) >= 0.5 {
            let room = turn.event.snapshot().room_id;
            self.rooms.update(|rooms| rooms.remove(room.as_str()));
            return self.composed(turn, answers);
        }
        if judgments.get("directness").unwrap_or(0.0) >= 2.0
            && judgments.get("asked_me").unwrap_or(0.0) >= 0.5
            && !held(&judgments)
        {
            return decision(
                SpeakMode::Speak,
                "asked while listening",
                judgments,
                self.language_notes(answers),
                false,
            );
        }
        if self.wants_the_floor(turn, &judgments) {
            return decision(
                SpeakMode::Offer,
                "listening, has something to add",
                judgments,
                vec![],
                false,
            );
        }
        decision(SpeakMode::Silent, "listening", judgments, vec![], true)
    }

    fn wants_the_floor(&self, turn: &SpeakTurn, judgments: &Judgments) -> bool {
        let Some(thought) = &turn.thought else {
            return false;
        };
        if thought.want_to_say.is_empty() || held(judgments) {
            return false;
        }
        if !thought.urgent && judgments.get("waiting").unwrap_or(0.0) >= 0.5 {
            return false;
        }
        greater(
            judgments.get("answers").unwrap_or(0.0),
            judgments.get("corrects").unwrap_or(0.0),
        ) >= self.options.proactivity
    }

    fn composed(&self, turn: &SpeakTurn, answers: &Answers) -> SpeakDecision {
        let judgments = judgments_of(answers);
        let (mut mode, mut reason) = compose(
            &judgments,
            ComposeOptions {
                alone: turn.people.len() == 1,
                proactivity: self.options.proactivity,
                urgent: turn
                    .thought
                    .as_ref()
                    .is_some_and(|thought| thought.urgent && !thought.want_to_say.is_empty()),
            },
        );
        let mut notes = self.language_notes(answers);
        if let Some(cut) = &turn.cut {
            let free = judgments.get("resume").unwrap_or(0.0) >= 0.4;
            let held = ["hush", "unfinished"]
                .iter()
                .any(|name| judgments.get(name).unwrap_or(0.0) >= 0.5);
            if free && !held {
                mode = SpeakMode::Speak;
                reason = "resume after cut".into();
                notes.insert(
                    0,
                    words::RESUME_NOTE
                        .replace("{seconds:.0f}", &format!("{:.0}", cut.played_ms / 1000.0)),
                );
            }
        }
        decision(mode, &reason, judgments, notes, false)
    }

    fn language_notes(&self, answers: &Answers) -> Vec<String> {
        let Some(Answer::Choice(answer)) = answers.get("language") else {
            return vec![];
        };
        self.languages.get(&answer.choice).into_iter().collect()
    }
}

fn held(judgments: &Judgments) -> bool {
    ["unfinished", "deferred", "hush"]
        .iter()
        .any(|name| judgments.get(name).unwrap_or(0.0) >= 0.5)
}

fn decision(
    mode: SpeakMode,
    reason: &str,
    judgments: Judgments,
    notes: Vec<String>,
    r#final: bool,
) -> SpeakDecision {
    SpeakDecision {
        mode,
        reason: reason.into(),
        judgments,
        notes,
        r#final,
    }
}

pub(super) fn listening_silence() -> SpeakDecision {
    static SILENCE: OnceLock<SpeakDecision> = OnceLock::new();
    SILENCE
        .get_or_init(|| {
            decision(
                SpeakMode::Silent,
                "listening",
                [("listening", 1.0)].into_iter().collect(),
                vec![],
                true,
            )
        })
        .clone()
}

//! The state and typed questions read by a classifier, without calling it.

use serde_json::{Map, Value, json};
use std::cmp::Ordering;

use super::{ClassifierSpeakPolicy, words};
use crate::classifiers::{ChoiceOptions, ChoiceQuestion, Question, Questions, SharedState};
use crate::speaking::{SharedRoomEvent, SpeakTurn};

impl ClassifierSpeakPolicy {
    pub(super) fn standard_questions(&self, turn: &SpeakTurn) -> Questions {
        let asked: Questions = self
            .questions
            .iter()
            .filter(|(name, _)| !["listen_request", "asked_me", "lift"].contains(&name.as_str()))
            .collect();
        let listening = self.listening(turn).is_some();
        let names: &[&str] = if listening {
            &["asked_me", "lift"]
        } else {
            &["listen_request"]
        };
        for name in names {
            let question = self
                .questions
                .get(name)
                .unwrap_or_else(|| words::listening(name));
            asked.insert(*name, question);
        }
        if turn
            .thought
            .as_ref()
            .is_some_and(|thought| !thought.want_to_say.is_empty())
        {
            for (name, question) in &words::thought() {
                asked.insert(name, question);
            }
        }
        if turn.cut.is_some() {
            asked.insert("resume", words::resume());
        }
        if !self.languages.is_empty() {
            let options: ChoiceOptions = self
                .languages
                .iter()
                .map(|(name, _)| (name.clone(), name))
                .collect();
            options.insert(
                "other",
                "Another language, or nothing to tell it from.".into(),
            );
            asked.insert(
                "language",
                Question::Choice(ChoiceQuestion {
                    instructions: words::LANGUAGE_INSTRUCTIONS.into(),
                    options,
                }),
            );
        }
        asked
    }

    pub(super) fn standard_state(&self, turn: &SpeakTurn) -> SharedState {
        let mut agent = Map::from_iter([("name".into(), json!(self.agent_name))]);
        if !self.options.agent_role.is_empty() {
            agent.insert("role".into(), json!(self.options.agent_role));
        }
        if let Some(listening) = self.listening(turn) {
            agent.insert("listening_only".into(), listening);
        }
        let recent: Vec<_> = turn
            .recent
            .iter()
            .filter(|event| !text(event).is_empty())
            .collect();
        let count = usize::try_from(self.options.history.unsigned_abs()).unwrap_or(usize::MAX);
        let start = match self.options.history.cmp(&0) {
            Ordering::Greater => recent.len().saturating_sub(count),
            Ordering::Less => count.min(recent.len()),
            Ordering::Equal => recent.len(),
        };
        let said: Vec<_> = recent
            .iter()
            .skip(start)
            .map(|event| self.said(turn, event))
            .collect();
        let mut state = Map::from_iter([
            ("agent".into(), Value::Object(agent)),
            ("people".into(), json!(turn.people)),
            ("recent_turns".into(), json!(said)),
            ("last_turn".into(), self.said(turn, &turn.event)),
        ]);
        if let Some(thought) = &turn.thought {
            state.insert("assistant_thought".into(), json!(thought.as_state()));
        }
        if let Some(cut) = &turn.cut {
            let start = cut.text.chars().count().saturating_sub(600);
            // Decimal formatting rounds the binary float itself, as round(x, 1)
            // does; scaling and round() would change halfway cases.
            let rounded = format!("{:.1}", cut.played_ms / 1000.0)
                .parse::<f64>()
                .unwrap_or(cut.played_ms / 1000.0);
            state.insert("assistant_cut_off".into(), json!({"was_saying": cut.text.chars().skip(start).collect::<String>(), "heard_seconds": rounded}));
        }
        SharedState::new(Value::Object(state))
    }

    fn said(&self, turn: &SpeakTurn, event: &SharedRoomEvent) -> Value {
        let event_value = event.snapshot();
        let speaker = if turn.by_agent(event) {
            self.agent_name.clone()
        } else {
            turn.speakers
                .get(event_value.id.as_str())
                .unwrap_or_else(|| "someone".into())
        };
        json!({"speaker": speaker, "text": text(event)})
    }
}

pub(super) fn text(event: &SharedRoomEvent) -> String {
    crate::transcript_content::content_text(&event.snapshot().content).unwrap_or_default()
}

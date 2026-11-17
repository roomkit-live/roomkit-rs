//! A retained thought may ask again, but only the newest event owns that question.

use roomkit_ai::speaking::{SpeakingFuture, Thinker};
use roomkit_ai::{SharedAiContext, SpeakTurn, Thought};
use std::sync::Mutex;
use tokio::sync::Notify;

use super::*;

struct Held {
    thought: Thought,
    count: usize,
    calls: Mutex<Vec<(Thought, SharedAiContext)>>,
    entered: Notify,
    release: Notify,
    returned: Notify,
}

impl Held {
    fn new(wanted: bool, count: usize) -> Self {
        Self {
            thought: Thought {
                text: "A price.".into(),
                want_to_say: if wanted {
                    vec!["It costs twelve.".into()]
                } else {
                    vec![]
                },
                urgent: false,
            },
            count,
            calls: Mutex::default(),
            entered: Notify::new(),
            release: Notify::new(),
            returned: Notify::new(),
        }
    }
}

impl Thinker for Held {
    fn think<'a>(
        &'a self,
        previous: &'a Thought,
        context: &'a SharedAiContext,
    ) -> SpeakingFuture<'a, Thought> {
        Box::pin(async move {
            let number = {
                let mut calls = self.calls.lock().unwrap();
                calls.push((previous.clone(), context.clone()));
                calls.len()
            };
            if number == 1 {
                self.entered.notify_one();
                self.release.notified().await;
            }
            if number == self.count {
                self.returned.notify_one();
            }
            Ok(self.thought.clone())
        })
    }
}

async fn notified(notify: &Notify) {
    tokio::time::timeout(Duration::from_secs(3), notify.notified())
        .await
        .unwrap();
}

fn turn_text(turn: &SpeakTurn) -> String {
    let event = turn.event.snapshot();
    if let roomkit::model::EventContent::Text(text) = event.content {
        text.body
    } else {
        String::new()
    }
}

// Python: tests/test_speak_decision_hooks.py::test_only_the_latest_event_s_thought_is_decided_late
// Python: tests/test_speak_decision_hooks.py::test_a_thought_with_nothing_to_say_is_not_decided_late
#[tokio::test]
async fn only_the_latest_event_with_something_to_say_is_decided_late() {
    for wanted in [false, true] {
        let count = if wanted { 2 } else { 1 };
        let thinker = Arc::new(Held::new(wanted, count));
        let policy = Arc::new(MockSpeakPolicy::new(MockSpeakPolicyOptions {
            decisions: vec![SpeakMode::Silent.into()],
            ..MockSpeakPolicyOptions::default()
        }));
        let model = model(false);
        let channel = AiChannel::new(
            id("ai"),
            model.clone(),
            AiChannelOptions {
                speak_policy: Some(Arc::<MockSpeakPolicy>::clone(&policy)),
                thinker: Some(Arc::<Held>::clone(&thinker)),
                think_wait: Duration::ZERO,
                ..AiChannelOptions::default()
            },
        )
        .unwrap();
        let (first, binding, context) = input("First.");
        assert!(
            !channel
                .on_event(first, binding, context)
                .await
                .unwrap()
                .responded
        );
        notified(&thinker.entered).await;
        if wanted {
            for text in ["Superseded.", "Latest."] {
                let (event, binding, context) = input(text);
                assert!(
                    !channel
                        .on_event(event, binding, context)
                        .await
                        .unwrap()
                        .responded
                );
            }
        }
        thinker.release.notify_one();
        // On this current-thread runtime the direct policy is ready in the
        // same worker poll after think returns; no hook/context read intervenes.
        notified(&thinker.returned).await;
        let turns = policy.turns();
        let expected = if wanted {
            vec!["First.", "Superseded.", "Latest.", "Latest."]
        } else {
            vec!["First."]
        };
        assert_eq!(turns.iter().map(turn_text).collect::<Vec<_>>(), expected);
        if wanted {
            assert_eq!(turns.last().unwrap().thought, Some(thinker.thought.clone()));
        }
        let calls = thinker.calls.lock().unwrap().clone();
        assert_eq!(calls.len(), count);
        if wanted {
            assert_eq!(calls.last().unwrap().0, thinker.thought);
        }
        assert_eq!(model.calls(), Vec::new());
        channel.close().await;
    }
}

// Python: tests/test_thinker.py::test_a_late_thought_waits_for_the_next_turn
#[tokio::test]
async fn a_late_decision_starts_no_answer_but_the_next_event_reads_its_thought() {
    for streaming in [false, true] {
        let thinker = Arc::new(Held::new(true, 1));
        let policy = Arc::new(MockSpeakPolicy::new(MockSpeakPolicyOptions {
            decisions: vec![SpeakMode::Silent.into(), SpeakMode::Speak.into()],
            ..MockSpeakPolicyOptions::default()
        }));
        let model = model(streaming);
        let channel = AiChannel::new(
            id("ai"),
            model.clone(),
            AiChannelOptions {
                speak_policy: Some(Arc::<MockSpeakPolicy>::clone(&policy)),
                thinker: Some(Arc::<Held>::clone(&thinker)),
                think_wait: Duration::ZERO,
                ..AiChannelOptions::default()
            },
        )
        .unwrap();
        let (event, binding, context) = input("We are looking for the price.");
        assert!(
            !channel
                .on_event(event, binding, context)
                .await
                .unwrap()
                .responded
        );
        notified(&thinker.entered).await;
        thinker.release.notify_one();
        notified(&thinker.returned).await;
        assert_eq!(model.calls(), Vec::new());
        assert_eq!(policy.turns().len(), 2);
        let (event, binding, context) = input("Nova, any idea?");
        assert!(
            channel
                .on_event(event, binding, context)
                .await
                .unwrap()
                .responded
        );
        assert_eq!(
            policy.turns().last().unwrap().thought,
            Some(thinker.thought.clone())
        );
        let calls = model.calls();
        assert_eq!(calls.len(), 1);
        assert!(
            calls
                .first()
                .unwrap()
                .messages
                .last()
                .unwrap()
                .text()
                .contains("- “It costs twelve.”")
        );
        channel.close().await;
    }
}

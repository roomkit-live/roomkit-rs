use crate::{answers, script, turn};
use roomkit_ai::classifiers::{
    Answers, Classifier, ClassifierFuture, MockClassifier, MockClassifierOptions, Question,
    Questions, ScoreQuestion, SharedState,
};
use roomkit_ai::speaking::{
    ClassifierSpeakOverrides, ClassifierSpeakPolicy, ClassifierSpeakPolicyOptions, SpeakMode,
    SpeakPolicy,
};
use serde_json::json;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

type Seen = Arc<Mutex<Vec<String>>>;

#[expect(
    clippy::unwrap_used,
    reason = "a poisoned fixture recorder fails the test"
)]
fn record(seen: &Seen, value: &str) {
    seen.lock().unwrap().push(value.into());
}

struct RecordedClassifier {
    mock: MockClassifier,
    seen: Seen,
    state: Arc<Mutex<Option<SharedState>>>,
}
impl Classifier for RecordedClassifier {
    #[expect(
        clippy::unwrap_used,
        reason = "a poisoned fixture recorder fails the test"
    )]
    fn classify<'a>(
        &'a self,
        state: &'a SharedState,
        asked: &'a Questions,
    ) -> ClassifierFuture<'a, Answers> {
        Box::pin(async move {
            record(&self.seen, "classify");
            assert!(
                self.state
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .same_object(state)
            );
            self.mock.classify(state, asked).await
        })
    }
}

// Python: tests/test_classifier_policy_overrides.py::test_all_three_overrides_dispatch_in_order_and_can_delegate_to_the_same_policy.
#[tokio::test]
async fn all_three_overrides_dispatch_in_order_and_can_delegate_to_the_same_policy() {
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let kept = Arc::new(Mutex::new(None::<SharedState>));
    let extra = Question::Score(
        ScoreQuestion::new("How urgent?", vec!["later".into(), "now".into()]).unwrap(),
    );
    let rules = ClassifierSpeakOverrides {
        state: Some({
            let seen = Arc::clone(&seen);
            let kept = Arc::clone(&kept);
            Arc::new(move |_policy, turn, standard| {
                record(&seen, "state");
                let value = standard(turn)?;
                value.update(|state| state["application"] = json!("live"));
                *kept.lock().unwrap() = Some(value.clone());
                Ok(value)
            })
        }),
        questions: Some({
            let seen = Arc::clone(&seen);
            let extra = extra.clone();
            Arc::new(move |_policy, turn, standard| {
                record(&seen, "questions");
                let value = standard(turn)?;
                value.insert("urgency", extra.clone());
                Ok(value)
            })
        }),
        decision: Some({
            let seen = Arc::clone(&seen);
            Arc::new(move |_policy, turn, answers, standard| {
                record(&seen, "decision");
                let mut value = standard(turn, answers)?;
                assert_eq!(
                    (value.mode, value.reason.as_str()),
                    (SpeakMode::Silent, "not addressed")
                );
                assert_eq!(answers.score("urgency")?, 1.0);
                value.mode = SpeakMode::Speak;
                value.reason = "application decision".into();
                Ok(value)
            })
        }),
    };
    let classifier = Arc::new(RecordedClassifier {
        mock: MockClassifier::new(MockClassifierOptions {
            answers: vec![script(&[("urgency", 1.0)])],
            ..MockClassifierOptions::default()
        }),
        seen: Arc::clone(&seen),
        state: Arc::clone(&kept),
    });
    let policy = ClassifierSpeakPolicy::new(
        Arc::<RecordedClassifier>::clone(&classifier),
        "Assistant",
        ClassifierSpeakPolicyOptions {
            overrides: rules,
            ..ClassifierSpeakPolicyOptions::default()
        },
    )
    .unwrap();
    let turn = turn("A question.", "r1");
    let chosen = policy.decide(&turn).await.unwrap();
    assert_eq!(
        *seen.lock().unwrap(),
        ["state", "questions", "classify", "decision"]
    );
    assert_eq!(chosen.reason, "application decision");
    assert_eq!(
        kept.lock().unwrap().as_ref().unwrap().snapshot()["agent"],
        json!({"name": "Assistant"})
    );
    assert_eq!(
        classifier.mock.calls().first().unwrap().1.get("urgency"),
        Some(extra.clone())
    );
    seen.lock().unwrap().clear();
    assert_eq!(
        policy.state(&turn).unwrap().snapshot()["application"],
        "live"
    );
    assert_eq!(policy.questions(&turn).unwrap().get("urgency"), Some(extra));
    let direct: Answers = [(
        "urgency",
        roomkit_ai::classifiers::Answer::Score(roomkit_ai::ScoreAnswer::new(1.0)),
    )]
    .into_iter()
    .collect();
    assert_eq!(
        policy.decision(&turn, &direct).unwrap().reason,
        "application decision"
    );
    assert_eq!(*seen.lock().unwrap(), ["state", "questions", "decision"]);
}

#[derive(Debug, thiserror::Error)]
#[error("{0} failed")]
struct Failed(&'static str);

// Python: tests/test_classifier_policy_overrides.py::test_override_errors_keep_the_classification_boundary.
// Python: tests/test_classifier_policy_overrides.py::test_a_turn_without_text_bypasses_all_three_overrides.
#[tokio::test]
async fn override_errors_keep_the_classification_boundary_and_empty_turns_bypass_them() {
    for listening in [false, true] {
        for method in 1..=3 {
            let failed = Arc::new(AtomicU8::new(0));
            let rules = ClassifierSpeakOverrides {
                state: Some({
                    let failed = Arc::clone(&failed);
                    Arc::new(move |_policy, turn, standard| {
                        if failed.load(Ordering::SeqCst) == 1 {
                            Err(Box::new(Failed("state")))
                        } else {
                            standard(turn)
                        }
                    })
                }),
                questions: Some({
                    let failed = Arc::clone(&failed);
                    Arc::new(move |_policy, turn, standard| {
                        if failed.load(Ordering::SeqCst) == 2 {
                            Err(Box::new(Failed("questions")))
                        } else {
                            standard(turn)
                        }
                    })
                }),
                decision: Some({
                    let failed = Arc::clone(&failed);
                    Arc::new(move |_policy, turn, answers, standard| {
                        if failed.load(Ordering::SeqCst) == 3 {
                            Err(Box::new(Failed("decision")))
                        } else {
                            standard(turn, answers)
                        }
                    })
                }),
            };
            let classifier = Arc::new(MockClassifier::new(MockClassifierOptions::default()));
            let policy = ClassifierSpeakPolicy::new(
                Arc::<MockClassifier>::clone(&classifier),
                "Assistant",
                ClassifierSpeakPolicyOptions {
                    overrides: rules,
                    ..ClassifierSpeakPolicyOptions::default()
                },
            )
            .unwrap();
            if listening {
                policy
                    .decision(
                        &turn("Only listen.", "r1"),
                        &answers(&[("listen_request", 1.0)]),
                    )
                    .unwrap();
            }
            failed.store(method, Ordering::SeqCst);
            let empty = policy.decide(&turn("", "r1")).await.unwrap();
            assert_eq!(
                (empty.mode, empty.reason.as_str(), empty.r#final),
                if listening {
                    (SpeakMode::Silent, "listening", true)
                } else {
                    (SpeakMode::Speak, "nothing to judge", false)
                }
            );
            assert_eq!(classifier.calls().len(), 0);
            let result = policy.decide(&turn("A question.", "r1")).await;
            if listening && method != 3 {
                let result = result.unwrap();
                assert_eq!(
                    (result.mode, result.reason.as_str(), result.r#final),
                    (SpeakMode::Silent, "listening", true)
                );
                assert_eq!(result.judgments.snapshot(), vec![("listening".into(), 1.0)]);
            } else {
                assert!(result.unwrap_err().downcast_ref::<Failed>().is_some());
            }
            assert_eq!(classifier.calls().len(), usize::from(method == 3));
        }
    }
}

// Python: tests/test_classifier_policy_overrides.py::test_overrides_can_replace_all_three_rules_without_running_the_standard.
#[tokio::test]
async fn the_three_standard_rules_do_not_run_unless_the_override_requests_them() {
    let custom = Arc::new(AtomicBool::new(true));
    let state = SharedState::new(json!({"custom": "state"}));
    let questions: Questions = [(
        "listen_request",
        Question::Score(ScoreQuestion::new("Listen?", vec!["no".into(), "yes".into()]).unwrap()),
    )]
    .into_iter()
    .collect();
    let rules = ClassifierSpeakOverrides {
        state: Some({
            let custom = Arc::clone(&custom);
            let state = state.clone();
            Arc::new(move |_policy, turn, standard| {
                if custom.load(Ordering::SeqCst) {
                    Ok(state.clone())
                } else {
                    standard(turn)
                }
            })
        }),
        questions: Some({
            let custom = Arc::clone(&custom);
            Arc::new(move |_policy, turn, standard| {
                if custom.load(Ordering::SeqCst) {
                    Ok(questions.clone())
                } else {
                    standard(turn)
                }
            })
        }),
        decision: Some(Arc::new(|_policy, _turn, answers, _standard| {
            assert_eq!(answers.score("listen_request")?, 1.0);
            Ok(roomkit_ai::SpeakDecision {
                reason: "custom rule".into(),
                ..SpeakMode::Offer.into()
            })
        })),
    };
    let (policy, classifier) = crate::policy(
        vec![script(&[("listen_request", 1.0)])],
        ClassifierSpeakPolicyOptions {
            overrides: rules,
            ..ClassifierSpeakPolicyOptions::default()
        },
    );
    let turn = turn("A question.", "r1");
    let chosen = policy.decide(&turn).await.unwrap();
    assert_eq!(
        (chosen.mode, chosen.reason.as_str()),
        (SpeakMode::Offer, "custom rule")
    );
    let calls = classifier.calls();
    let (kept, asked) = calls.first().unwrap();
    assert!(kept.same_object(&state));
    assert_eq!(
        asked.iter().map(|(name, _)| name).collect::<Vec<_>>(),
        ["listen_request"]
    );
    custom.store(false, Ordering::SeqCst);
    assert!(
        policy.state(&turn).unwrap().snapshot()["agent"]
            .get("listening_only")
            .is_none()
    );
    assert!(
        policy
            .questions(&turn)
            .unwrap()
            .get("listen_request")
            .is_some()
    );
}

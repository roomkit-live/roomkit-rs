use crate::{answers, policy, script, turn};
use roomkit::model::Timestamp;
use roomkit_ai::classifiers::{ClassifierError, MockClassifier, MockClassifierOptions};
use roomkit_ai::speaking::{
    ClassifierSpeakPolicy, ClassifierSpeakPolicyOptions, CutReply, SpeakMode, SpeakPolicy, Thought,
};
use serde_json::json;
use std::sync::Arc;

// Python: tests/test_speak_listening.py::test_a_request_to_only_listen_starts_the_state_with_a_final_silence.
// Python: tests/test_speak_listening.py::test_a_question_put_to_the_agent_is_answered_and_the_room_goes_on_listening.
// Python: tests/test_speak_listening.py::test_a_turn_that_lets_the_agent_talk_again_opens_the_room.
#[tokio::test]
async fn listening_survives_an_answer_until_a_turn_lifts_it() {
    let (policy, classifier) = policy(
        vec![
            script(&[("listen_request", 0.9)]),
            script(&[("directness", 3.0), ("asked_me", 0.9)]),
            script(&[("directness", 0.4), ("request", 0.9), ("asked_me", 0.3)]),
            script(&[("directness", 3.0), ("lift", 0.9), ("request", 0.9)]),
            script(&[("directness", 0.0)]),
        ],
        ClassifierSpeakPolicyOptions::default(),
    );
    for (body, mode, reason, final_) in [
        ("Only listen.", SpeakMode::Silent, "asked to listen", true),
        (
            "What do you think?",
            SpeakMode::Speak,
            "asked while listening",
            false,
        ),
        ("Where is my coat?", SpeakMode::Silent, "listening", true),
        (
            "Talk again: what time is it?",
            SpeakMode::Speak,
            "addressed",
            false,
        ),
        ("Thanks.", SpeakMode::Silent, "not addressed", false),
    ] {
        let result = policy.decide(&turn(body, "r1")).await.unwrap();
        assert_eq!(
            (result.mode, result.reason.as_str(), result.r#final),
            (mode, reason, final_)
        );
    }
    let calls = classifier.calls();
    assert_eq!(calls.len(), 5);
    for (at, (state, asked)) in calls.iter().enumerate() {
        let listening = (1..4).contains(&at);
        assert_eq!(
            state.snapshot()["agent"].get("listening_only").is_some(),
            listening
        );
        assert_eq!(asked.get("listen_request").is_none(), listening);
        assert_eq!(asked.get("asked_me").is_some(), listening);
        assert_eq!(asked.get("lift").is_some(), listening);
        if listening {
            assert_eq!(
                state.snapshot()["agent"]["listening_only"],
                json!({"speaker": "Alice", "text": "Only listen."})
            );
        }
    }
}

// Python: tests/test_speak_listening.py::test_the_state_is_per_room.
// Python: tests/test_speak_listening.py::test_the_request_kept_is_bounded_at_a_word.
// Python: tests/test_speak_listening.py::test_a_turn_is_decided_as_it_was_asked.
#[test]
fn rooms_are_independent_and_in_flight_answers_keep_how_they_were_asked() {
    let (policy, _) = policy(vec![], ClassifierSpeakPolicyOptions::default());
    let turn = turn(&"stay quiet ".repeat(200), "r1");
    policy
        .decision(&turn, &answers(&[("listen_request", 0.9)]))
        .unwrap();
    let state = policy.state(&turn).unwrap().snapshot();
    let request = state["agent"]["listening_only"]["text"].as_str().unwrap();
    assert!(request.chars().count() <= 200);
    assert!(request.ends_with("quiet…") || request.ends_with("stay…"));
    assert!(
        policy
            .state(&crate::turn("Question?", "r2"))
            .unwrap()
            .snapshot()["agent"]
            .get("listening_only")
            .is_none()
    );
    let result = policy
        .decision(
            &turn,
            &answers(&[
                ("directness", 3.0),
                ("request", 0.9),
                ("listen_request", 0.0),
            ]),
        )
        .unwrap();
    assert_eq!(
        (result.mode, result.reason.as_str()),
        (SpeakMode::Speak, "addressed")
    );
    assert!(
        policy.state(&turn).unwrap().snapshot()["agent"]
            .get("listening_only")
            .is_some()
    );
    policy.forget_room("r1").unwrap();
    assert!(
        policy.state(&turn).unwrap().snapshot()["agent"]
            .get("listening_only")
            .is_none()
    );
    // A classifier call asked while listening stays so even if another call ended it.
    let result = policy
        .decision(&turn, &answers(&[("asked_me", 0.1)]))
        .unwrap();
    assert_eq!(
        (result.mode, result.reason.as_str(), result.r#final),
        (SpeakMode::Silent, "listening", true)
    );
}

// Python: tests/test_speak_listening.py::test_a_question_without_the_address_is_not_answered_while_listening.
// Python: tests/test_speak_listening.py::test_a_question_the_speaker_holds_back_is_not_answered_while_listening.
// Python: tests/test_speak_listening.py::test_while_listening_the_agent_asks_for_the_floor_only_with_what_answers.
#[test]
fn listening_answers_only_a_direct_question_and_offers_only_an_unheld_thought() {
    let (policy, _) = policy(vec![], ClassifierSpeakPolicyOptions::default());
    let mut turn = turn("A question?", "r1");
    policy
        .decision(&turn, &answers(&[("listen_request", 0.9)]))
        .unwrap();
    assert_eq!(
        policy
            .decision(&turn, &answers(&[("directness", 1.0), ("asked_me", 0.9)]))
            .unwrap()
            .mode,
        SpeakMode::Silent
    );
    for held in ["unfinished", "deferred", "hush"] {
        let result = policy
            .decision(
                &turn,
                &answers(&[("directness", 3.0), ("asked_me", 0.9), (held, 0.9)]),
            )
            .unwrap();
        assert_eq!((result.mode, result.r#final), (SpeakMode::Silent, true));
    }
    turn.thought = Some(Thought {
        text: "A price".into(),
        want_to_say: vec!["It costs 12.".into()],
        urgent: false,
    });
    let relevant = answers(&[("directness", 0.0), ("asked_me", 0.1), ("answers", 0.9)]);
    let result = policy.decision(&turn, &relevant).unwrap();
    assert_eq!((result.mode, result.r#final), (SpeakMode::Offer, false));
    for held in ["unfinished", "deferred", "hush", "waiting"] {
        let values = answers(&[
            ("directness", 0.0),
            ("asked_me", 0.1),
            ("answers", 0.9),
            (held, 0.9),
        ]);
        let result = policy.decision(&turn, &values).unwrap();
        assert_eq!((result.mode, result.r#final), (SpeakMode::Silent, true));
    }
    let result = policy
        .decision(&turn, &answers(&[("asked_me", 0.1), ("answers", 0.2)]))
        .unwrap();
    assert_eq!((result.mode, result.r#final), (SpeakMode::Silent, true));
}

// Python: tests/test_speak_listening.py::test_a_listening_room_resumes_no_cut_answer.
// Python: tests/test_cut_answer.py::test_wanting_the_turn_the_turn_decides.
// Python: tests/test_cut_answer.py::test_asked_for_quiet_it_does_not_resume.
// Python: tests/test_cut_answer.py::test_without_a_cut_no_resume_is_asked.
#[test]
fn a_cut_does_not_override_listening_or_a_turn_that_is_held() {
    let (policy, _) = policy(vec![], ClassifierSpeakPolicyOptions::default());
    let mut turn = turn("Okay.", "r1");
    assert!(policy.questions(&turn).unwrap().get("resume").is_none());
    turn.cut = Some(CutReply {
        text: "It will rain".into(),
        played_ms: 800.0,
        at: Timestamp::from_unix_millis(0).unwrap(),
    });
    let result = policy
        .decision(&turn, &answers(&[("resume", 0.2)]))
        .unwrap();
    assert_eq!(
        (result.mode, result.reason.as_str()),
        (SpeakMode::Silent, "not addressed")
    );
    let result = policy
        .decision(&turn, &answers(&[("resume", 0.9), ("hush", 0.9)]))
        .unwrap();
    assert_eq!(
        (result.mode, result.reason.as_str()),
        (SpeakMode::Silent, "asked to keep quiet")
    );
    policy
        .decision(&turn, &answers(&[("listen_request", 0.9)]))
        .unwrap();
    let result = policy
        .decision(&turn, &answers(&[("resume", 0.9)]))
        .unwrap();
    assert_eq!(
        (result.mode, result.reason.as_str()),
        (SpeakMode::Silent, "listening")
    );
    assert_eq!(result.notes, Vec::<String>::new());
}

// Python: tests/test_speak_listening.py::test_a_turn_a_listening_room_cannot_judge_stays_silent.
// Python: tests/test_speak_listening.py::test_an_open_room_still_falls_back_on_a_failing_classifier.
#[tokio::test]
async fn an_unreadable_turn_stays_silent_only_in_a_listening_room() {
    for listening in [false, true] {
        let classifier = Arc::new(MockClassifier::new(MockClassifierOptions {
            error: Some(Arc::new(ClassifierError::new("down"))),
            ..MockClassifierOptions::default()
        }));
        let policy = ClassifierSpeakPolicy::new(
            Arc::<MockClassifier>::clone(&classifier),
            "Assistant",
            ClassifierSpeakPolicyOptions::default(),
        )
        .unwrap();
        if listening {
            policy
                .decision(
                    &turn("Only listen.", "r1"),
                    &answers(&[("listen_request", 0.9)]),
                )
                .unwrap();
        }
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
        let failed = policy.decide(&turn("What time?", "r1")).await;
        if listening {
            let result = failed.unwrap();
            assert_eq!(
                (result.mode, result.reason.as_str(), result.r#final),
                (SpeakMode::Silent, "listening", true)
            );
        } else {
            assert_eq!(failed.unwrap_err().to_string(), "down");
        }
        assert_eq!(classifier.calls().len(), 1);
    }
}

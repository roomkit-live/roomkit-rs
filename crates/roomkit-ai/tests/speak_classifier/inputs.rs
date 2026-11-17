use crate::{policy, script, turn};
use roomkit::ClassedError;
use roomkit::model::{ChannelId, Timestamp};
use roomkit_ai::classifiers::{
    ChoiceOptions, MockClassifier, MockClassifierOptions, Question, Questions, ScriptedAnswer,
    YesNoQuestion,
};
use roomkit_ai::speaking::{
    ClassifierSpeakPolicy, ClassifierSpeakPolicyOptions, CutReply, SpeakMode, SpeakPolicy, Thought,
};
use serde_json::json;
use std::sync::Arc;

// Python: tests/test_speak_classifier.py::test_the_classifier_reads_the_turns_by_speaker_the_agent_under_its_name.
#[tokio::test]
async fn the_classifier_reads_the_turns_by_speaker_the_agent_under_its_name() {
    let (policy, classifier) = policy(
        vec![script(&[("directness", 3.0)])],
        ClassifierSpeakPolicyOptions {
            agent_role: "meeting assistant".into(),
            ..ClassifierSpeakPolicyOptions::default()
        },
    );
    let mut turn = turn("Can you sum up?", "r1");
    for (body, name, channel) in [
        ("The budget", Some("Bob"), "sms"),
        ("Shall I sum up?", None, "ai"),
        ("A picture", None, "sms"),
    ] {
        let earlier = crate::turn(body, "r1").event;
        earlier.update(|event| event.source.channel_id = ChannelId::new(channel).unwrap());
        if let Some(name) = name {
            turn.speakers
                .insert(earlier.snapshot().id.as_str(), name.into());
        }
        turn.recent.push(earlier);
    }
    let decision = policy.decide(&turn).await.unwrap();
    assert_eq!(
        (decision.mode, decision.reason.as_str()),
        (SpeakMode::Speak, "addressed")
    );
    let calls = classifier.calls();
    let (state, asked) = calls.first().unwrap();
    assert_eq!(
        state.snapshot(),
        json!({"agent": {"name": "Assistant", "role": "meeting assistant"}, "people": ["Alice", "Bob"], "recent_turns": [ {"speaker": "Bob", "text": "The budget"}, {"speaker": "Assistant", "text": "Shall I sum up?"}, {"speaker": "someone", "text": "A picture"} ], "last_turn": {"speaker": "Alice", "text": "Can you sum up?"}})
    );
    assert_eq!(
        asked.iter().map(|(name, _)| name).collect::<Vec<_>>(),
        [
            "directness",
            "deferred",
            "unfinished",
            "hush",
            "request",
            "answered",
            "listen_request"
        ]
    );
    assert_eq!(decision.judgments.get("directness"), Some(3.0));
    assert_eq!(decision.notes, Vec::<String>::new());
}

// Python: tests/test_speak_classifier.py::test_history_bounds_the_turns_read.
// Python: tests/test_speaking_public_models.py::test_classifier_history_preserves_signed_slice_semantics.
#[test]
fn history_keeps_the_text_turns_selected_by_the_signed_slice() {
    for (history, expected) in [
        (2, vec!["turn 3", "turn 4"]),
        (0, vec![]),
        (-2, vec!["turn 2", "turn 3", "turn 4"]),
    ] {
        let (policy, _) = policy(
            vec![],
            ClassifierSpeakPolicyOptions {
                history,
                ..ClassifierSpeakPolicyOptions::default()
            },
        );
        let mut turn = turn("and then", "r1");
        turn.recent = (0..5)
            .map(|i| crate::turn(&format!("turn {i}"), "r1").event)
            .collect();
        turn.recent.insert(1, crate::turn("", "r1").event);
        let state = policy.state(&turn).unwrap().snapshot();
        assert_eq!(state["agent"], json!({"name": "Assistant"}));
        assert_eq!(
            state["recent_turns"]
                .as_array()
                .unwrap()
                .iter()
                .map(|row| row["text"].as_str().unwrap())
                .collect::<Vec<_>>(),
            expected
        );
    }
}

// Python: tests/test_speak_classifier.py::test_the_language_is_judged_and_named_in_the_notes.
// Python: tests/test_speak_classifier.py::test_another_language_adds_no_note.
// Python: tests/test_speak_classifier.py::test_a_question_is_replaced_by_name_and_compose_overridden (question replacement).
#[tokio::test]
async fn questions_languages_and_their_notes_keep_the_supplied_order() {
    let languages: ChoiceOptions = [
        ("English", "Answer in English.".into()),
        ("German", "Answer in German.".into()),
    ]
    .into_iter()
    .collect();
    let questions: Questions = [("hush", Question::YesNo(YesNoQuestion::new("Shh?")))]
        .into_iter()
        .collect();
    let scripted = script(&[("directness", 3.0)]);
    scripted.insert("language", ScriptedAnswer::Text("German".into()));
    let (policy, classifier) = policy(
        vec![scripted.clone()],
        ClassifierSpeakPolicyOptions {
            languages: Some(languages.clone()),
            questions: Some(questions.clone()),
            ..ClassifierSpeakPolicyOptions::default()
        },
    );
    languages.remove("German");
    questions.remove("hush");
    let turn = turn("Hello?", "r1");
    let decision = policy.decide(&turn).await.unwrap();
    assert_eq!(decision.notes, ["Answer in German."]);
    assert_eq!(decision.judgments.get("language=German"), Some(1.0));
    let calls = classifier.calls();
    let asked = &calls.first().unwrap().1;
    assert_eq!(
        asked.get("hush"),
        Some(Question::YesNo(YesNoQuestion::new("Shh?")))
    );
    let Some(Question::Choice(language)) = asked.get("language") else {
        panic!("a language question is required");
    };
    assert_eq!(
        language
            .options
            .iter()
            .map(|(name, _)| name)
            .collect::<Vec<_>>(),
        ["English", "German", "other"]
    );
    scripted.insert("language", ScriptedAnswer::Text("other".into()));
    assert_eq!(
        policy.decide(&turn).await.unwrap().notes,
        Vec::<String>::new()
    );
}

// Python: tests/test_speak_classifier.py::test_other_is_not_a_language_name.
// Python: tests/test_speaking_public_models.py::test_classifier_proactivity_keeps_its_validation_boundary.
#[test]
fn invalid_configuration_is_refused_before_classification() {
    for options in [
        ClassifierSpeakPolicyOptions {
            languages: Some([("other", "x".into())].into_iter().collect()),
            ..ClassifierSpeakPolicyOptions::default()
        },
        ClassifierSpeakPolicyOptions {
            proactivity: 0.0,
            ..ClassifierSpeakPolicyOptions::default()
        },
        ClassifierSpeakPolicyOptions {
            proactivity: -0.1,
            ..ClassifierSpeakPolicyOptions::default()
        },
        ClassifierSpeakPolicyOptions {
            proactivity: 1.1,
            ..ClassifierSpeakPolicyOptions::default()
        },
        ClassifierSpeakPolicyOptions {
            proactivity: f64::NAN,
            ..ClassifierSpeakPolicyOptions::default()
        },
    ] {
        let error = ClassifierSpeakPolicy::new(
            Arc::new(MockClassifier::new(MockClassifierOptions::default())),
            "Assistant",
            options,
        )
        .unwrap_err();
        assert_eq!(
            error.downcast_ref::<ClassedError>().unwrap().class(),
            "ValueError"
        );
    }
}

// Python: tests/test_speak_classifier.py::test_one_person_makes_any_request_the_agents.
#[tokio::test]
async fn one_person_makes_any_request_the_agents() {
    let (policy, _) = policy(
        vec![script(&[("directness", 0.3), ("request", 0.9)])],
        ClassifierSpeakPolicyOptions::default(),
    );
    let mut turn = turn("What time is it?", "r1");
    turn.people = vec!["Alice".into()];
    assert_eq!(policy.decide(&turn).await.unwrap().mode, SpeakMode::Speak);
}

// Python: tests/test_speak_listening.py::test_a_listening_question_is_replaced_by_name.
// Python: tests/test_speak_listening.py::test_asked_me_and_lift_are_replaced_by_name.
#[tokio::test]
async fn every_listening_question_can_be_replaced_by_name() {
    let question = Question::YesNo(YesNoQuestion::new("Does this say pineapple?"));
    let custom: Questions = ["listen_request", "asked_me", "lift"]
        .into_iter()
        .map(|name| (name, question.clone()))
        .collect();
    let (policy, classifier) = policy(
        vec![script(&[("listen_request", 0.9)]), script(&[])],
        ClassifierSpeakPolicyOptions {
            questions: Some(custom),
            ..ClassifierSpeakPolicyOptions::default()
        },
    );
    policy.decide(&turn("Pineapple.", "r1")).await.unwrap();
    policy.decide(&turn("Pineapple?", "r1")).await.unwrap();
    let calls = classifier.calls();
    assert_eq!(
        calls.first().unwrap().1.get("listen_request"),
        Some(question.clone())
    );
    for name in ["asked_me", "lift"] {
        assert_eq!(calls.last().unwrap().1.get(name), Some(question.clone()));
    }
}

// Python: tests/test_cut_answer.py::test_free_to_go_on_the_agent_resumes.
#[tokio::test]
async fn the_thought_and_cut_reply_are_read_and_resumed_with_the_exact_note() {
    let (policy, classifier) = policy(
        vec![script(&[("resume", 0.9)])],
        ClassifierSpeakPolicyOptions::default(),
    );
    let mut turn = turn("Go on.", "r1");
    turn.thought = Some(Thought {
        text: "A price".into(),
        want_to_say: vec!["It costs 12.".into()],
        urgent: false,
    });
    turn.cut = Some(CutReply {
        text: "é".repeat(620),
        played_ms: 3200.0,
        at: Timestamp::from_unix_millis(0).unwrap(),
    });
    let decision = policy.decide(&turn).await.unwrap();
    assert_eq!(
        (decision.mode, decision.reason.as_str()),
        (SpeakMode::Speak, "resume after cut")
    );
    assert_eq!(
        decision.notes,
        [
            "You were cut off after about 3 s of your last answer. If what you were saying still matters, go on from where you were cut: a short link back (\"so, as I was saying\"), then only what was left. Do not repeat what was heard, do not apologise."
        ]
    );
    let calls = classifier.calls();
    let (state, asked) = calls.first().unwrap();
    assert_eq!(
        state.snapshot()["assistant_cut_off"],
        json!({"was_saying": "é".repeat(600), "heard_seconds": 3.2})
    );
    assert_eq!(
        state.snapshot()["assistant_thought"],
        json!({"thinking": "A price", "want_to_say": ["It costs 12."], "urgent": false})
    );
    for name in ["answers", "corrects", "waiting", "resume"] {
        assert!(asked.get(name).is_some());
    }
}

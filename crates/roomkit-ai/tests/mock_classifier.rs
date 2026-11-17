//! Python's scripted classifier: call records, defaults and live script mappings.

use roomkit::ClassedError;
use roomkit_ai::classifiers::{
    Answer, ChoiceAnswer, ChoiceOptions, ChoiceQuestion, Classifier, ClassifierError,
    MockClassifier, MockClassifierOptions, Question, Questions, ScoreQuestion, Script,
    ScriptedAnswer, SharedState, YesNoAnswer, YesNoQuestion,
};
use serde_json::json;
use std::sync::Arc;

#[expect(clippy::unwrap_used, reason = "fixed question shapes")]
fn questions() -> Questions {
    [
        ("addressed", Question::YesNo(YesNoQuestion::new("Asked?"))),
        (
            "language",
            Question::Choice(
                ChoiceQuestion::new(
                    "Language?",
                    [("fr", "French".into()), ("en", "English".into())]
                        .into_iter()
                        .collect(),
                )
                .unwrap(),
            ),
        ),
        (
            "urgency",
            Question::Score(
                ScoreQuestion::new("Urgent?", vec!["low".into(), "high".into()]).unwrap(),
            ),
        ),
    ]
    .into_iter()
    .collect()
}

fn mock(scripts: Vec<Script>) -> MockClassifier {
    MockClassifier::new(MockClassifierOptions {
        answers: scripts,
        ..MockClassifierOptions::default()
    })
}

// Python: tests/test_classifiers.py::test_mock_answers_from_its_script_and_records_calls.
// Python: tests/test_classifiers.py::test_mock_defaults_to_no_first_option_lowest_level.
#[tokio::test]
async fn mock_answers_from_its_script_and_records_calls_with_default_answers() {
    let state = SharedState::new(json!("It is urgent"));
    let questions = questions();
    let classifier = mock(vec![
        [
            ("addressed", ScriptedAnswer::Number(0.9)),
            ("language", ScriptedAnswer::Text("en".into())),
            ("urgency", ScriptedAnswer::Number(2.0)),
        ]
        .into_iter()
        .collect(),
    ]);
    let answers = classifier.classify(&state, &questions).await.unwrap();
    assert_eq!(answers.yes("addressed").unwrap(), 0.9);
    assert_eq!(answers.choice("language").unwrap(), "en");
    assert_eq!(answers.score("urgency").unwrap(), 2.0);
    assert_eq!(
        answers.get("language"),
        Some(Answer::Choice(ChoiceAnswer {
            choice: "en".into(),
            probabilities: [("fr", 0.0), ("en", 1.0)].into_iter().collect()
        }))
    );
    let calls = classifier.calls();
    assert_eq!(calls.len(), 1);
    assert!(calls.first().unwrap().0.same_object(&state));
    assert_eq!(calls.first().unwrap().1, questions);
    let default = mock(vec![]).classify(&state, &questions).await.unwrap();
    assert_eq!(
        (
            default.yes("addressed").unwrap(),
            default.choice("language").unwrap(),
            default.score("urgency").unwrap()
        ),
        (0.0, "fr".into(), 0.0)
    );
}

// Python: tests/test_speaking_public_models.py::test_mock_classifier_preserves_state_and_answers_but_copies_the_question_map.
// Python: tests/test_classifiers.py::test_mock_takes_a_scripted_answer_and_raises_its_error.
#[tokio::test]
async fn mock_keeps_typed_answers_and_live_inputs_but_copies_the_question_map() {
    let state = SharedState::new(json!({"text": "Hi"}));
    let question_options: ChoiceOptions = [("z", "First".into()), ("a", "Second".into())]
        .into_iter()
        .collect();
    let questions: Questions = [(
        "ready",
        Question::Choice(ChoiceQuestion::new("Ready?", question_options.clone()).unwrap()),
    )]
    .into_iter()
    .collect();
    // A typed answer bypasses even a question of another kind.
    let scripted = Answer::YesNo(YesNoAnswer { probability: 0.7 });
    let classifier = mock(vec![
        [("ready", ScriptedAnswer::Answer(scripted.clone()))]
            .into_iter()
            .collect(),
    ]);
    let answer = classifier.classify(&state, &questions).await.unwrap();
    assert_eq!(answer.get("ready"), Some(scripted));
    questions.remove("ready");
    state.update(|state| state["text"] = json!("Changed"));
    question_options.insert("b", "Later".into());
    let calls = classifier.calls();
    let (kept, asked) = calls.first().unwrap();
    assert!(kept.same_object(&state));
    assert_eq!(kept.snapshot()["text"], "Changed");
    let Some(Question::Choice(question)) = asked.get("ready") else {
        panic!("the recorded question must remain");
    };
    assert_eq!(question.options.get("b").as_deref(), Some("Later"));
}

// Python: tests/test_speaking_public_models.py::test_mock_classifier_keeps_the_supplied_script_mapping_live.
// Python: tests/test_speaking_public_models.py::test_mock_classifier_copies_the_script_sequence_but_keeps_each_mapping_live.
// Python: tests/test_speaking_public_models.py::test_mock_classifier_uses_insertion_order_and_repeats_the_last_script.
#[tokio::test]
async fn script_entries_stay_live_and_the_last_script_repeats() {
    let first: Script = [("addressed", ScriptedAnswer::Number(0.25))]
        .into_iter()
        .collect();
    let last: Script = [("addressed", ScriptedAnswer::Number(0.5))]
        .into_iter()
        .collect();
    let mut scripts = vec![first.clone(), last.clone()];
    let classifier = mock(scripts.clone());
    scripts.reverse();
    scripts.push(Script::new());
    let state = SharedState::new(json!([]));
    let asked = questions();
    assert_eq!(
        classifier
            .classify(&state, &asked)
            .await
            .unwrap()
            .yes("addressed")
            .unwrap(),
        0.25
    );
    last.insert("addressed", ScriptedAnswer::Number(0.75));
    assert_eq!(
        classifier
            .classify(&state, &asked)
            .await
            .unwrap()
            .yes("addressed")
            .unwrap(),
        0.75
    );
    scripts.clear();
    assert_eq!(
        classifier
            .classify(&state, &asked)
            .await
            .unwrap()
            .yes("addressed")
            .unwrap(),
        0.75
    );
    last.remove("addressed");
    assert_eq!(
        classifier
            .classify(&state, &asked)
            .await
            .unwrap()
            .yes("addressed")
            .unwrap(),
        0.0
    );
    let choices: Questions = [(
        "pick",
        Question::Choice(
            ChoiceQuestion::new(
                "Pick",
                [("z", "First".into()), ("a", "Second".into())]
                    .into_iter()
                    .collect(),
            )
            .unwrap(),
        ),
    )]
    .into_iter()
    .collect();
    assert_eq!(
        mock(vec![])
            .classify(&state, &choices)
            .await
            .unwrap()
            .choice("pick")
            .unwrap(),
        "z"
    );
    classifier.close().await.unwrap();
}

// Python: tests/test_classifiers.py::test_mock_takes_a_scripted_answer_and_raises_its_error.
#[tokio::test]
async fn errors_keep_the_original_cause_and_every_call_is_recorded() {
    let state = SharedState::new(json!("x"));
    let asked = questions();
    let original = Arc::new(ClassifierError::new("down"));
    let failed = MockClassifier::new(MockClassifierOptions {
        error: Some(Arc::<ClassifierError>::clone(&original)),
        ..MockClassifierOptions::default()
    });
    let error = failed.classify(&state, &asked).await.unwrap_err();
    assert!(std::ptr::eq(
        error
            .source()
            .unwrap()
            .downcast_ref::<ClassifierError>()
            .unwrap(),
        original.as_ref()
    ));
    assert_eq!(failed.calls().len(), 1);
    let wrong = mock(vec![
        [("language", ScriptedAnswer::Text("de".into()))]
            .into_iter()
            .collect(),
    ]);
    let error = wrong.classify(&state, &asked).await.unwrap_err();
    assert!(error.downcast_ref::<ClassifierError>().is_some());
    assert_eq!(error.to_string(), "'de' is not an option of 'language'");
    assert_eq!(wrong.calls().len(), 1);
}

// Python: tests/test_speaking_public_models.py::test_mock_classifier_converts_numeric_text_when_the_question_is_asked.
// Python: tests/test_speaking_public_models.py::test_mock_classifier_keeps_numeric_text_errors_and_records_the_call.
#[tokio::test]
async fn numeric_text_is_converted_at_call_time_and_bad_values_keep_value_error() {
    let state = SharedState::new(json!({}));
    let asked = questions();
    for text in [" 0.25 ", "٠.٢٥", "０.２５", "+2_5e-2"] {
        let script: Script = [
            ("addressed", ScriptedAnswer::Text(text.into())),
            ("urgency", ScriptedAnswer::Text(text.into())),
        ]
        .into_iter()
        .collect();
        let result = mock(vec![script]).classify(&state, &asked).await.unwrap();
        assert_eq!(result.yes("addressed").unwrap(), 0.25);
        assert_eq!(result.score("urgency").unwrap(), 0.25);
    }
    for text in ["_1", "1__0", "1_", "1_e0", "ready"] {
        let classifier = mock(vec![
            [("addressed", ScriptedAnswer::Text(text.into()))]
                .into_iter()
                .collect(),
        ]);
        let error = classifier.classify(&state, &asked).await.unwrap_err();
        assert_eq!(
            error.downcast_ref::<ClassedError>().unwrap().class(),
            "ValueError"
        );
        assert_eq!(
            error.to_string(),
            format!("could not convert string to float: '{text}'")
        );
        assert_eq!(classifier.calls().len(), 1);
    }
}

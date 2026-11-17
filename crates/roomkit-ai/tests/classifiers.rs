//! Python: `tests/test_classifiers.py` and `test_speaking_public_models.py`.

use roomkit::ClassedError;
use roomkit_ai::classifiers::{
    Answer, Answers, ChoiceAnswer, ChoiceOptions, ChoiceQuestion, Probabilities, Question,
    Questions, ScoreAnswer, ScoreQuestion, YesNoAnswer, YesNoQuestion, check_answers,
};

#[expect(clippy::unwrap_used, reason = "fixed valid question fixtures")]
fn questions() -> Questions {
    Questions::from_iter([
        (
            "addressed",
            Question::YesNo(YesNoQuestion::new("Is the last message addressed to Nova?")),
        ),
        (
            "language",
            Question::Choice(
                ChoiceQuestion::new(
                    "Which language?",
                    ChoiceOptions::from_iter([("fr", "French".into()), ("en", "English".into())]),
                )
                .unwrap(),
            ),
        ),
        (
            "urgency",
            Question::Score(
                ScoreQuestion::new(
                    "How urgent?",
                    vec!["can wait".into(), "soon".into(), "now".into()],
                )
                .unwrap(),
            ),
        ),
    ])
}

// Python: test_classifiers.py::test_a_choice_needs_two_options_and_a_score_two_levels.
#[test]
fn a_choice_needs_two_options_and_a_score_two_levels() {
    let error = ChoiceQuestion::new(
        "Which?",
        ChoiceOptions::from_iter([("only", "the one".into())]),
    )
    .unwrap_err();
    assert_eq!(
        error.downcast_ref::<ClassedError>().unwrap().class(),
        "ValueError"
    );
    assert_eq!(
        error.to_string(),
        "a ChoiceQuestion needs two options at least"
    );
    let error = ScoreQuestion::new("How much?", vec!["all".into()]).unwrap_err();
    assert_eq!(
        error.downcast_ref::<ClassedError>().unwrap().class(),
        "ValueError"
    );
    assert_eq!(
        error.to_string(),
        "a ScoreQuestion needs two levels at least"
    );
}

// Python: test_classifiers.py::test_answers_read_typed.
#[test]
fn answers_read_typed() {
    let answers = Answers::from_iter([
        ("addressed", Answer::YesNo(YesNoAnswer { probability: 0.8 })),
        ("language", Answer::Choice(ChoiceAnswer::new("fr"))),
        (
            "urgency",
            Answer::Score(ScoreAnswer {
                score: 1.4,
                probabilities: vec![0.1, 0.4, 0.5],
            }),
        ),
    ]);
    assert_eq!(answers.yes("addressed").unwrap(), 0.8);
    assert_eq!(answers.choice("language").unwrap(), "fr");
    assert_eq!(answers.score("urgency").unwrap(), 1.4);
    let wrong = answers.yes("language").unwrap_err();
    assert_eq!(
        wrong.downcast_ref::<ClassedError>().unwrap().class(),
        "TypeError"
    );
    assert_eq!(
        wrong.to_string(),
        "'language' was answered as ChoiceAnswer, not YesNoAnswer"
    );
    let missing = answers.yes("unknown").unwrap_err();
    assert_eq!(
        missing.downcast_ref::<ClassedError>().unwrap().class(),
        "KeyError"
    );
    assert_eq!(missing.to_string(), "'unknown'");
}

// Python: test_classifiers.py::test_check_answers_wants_every_question_with_its_kind.
// Python: test_speaking_public_models.py::test_check_answers_preserves_extra_entries_and_typed_reads_keep_their_errors.
#[test]
fn check_answers_wants_every_question_with_its_kind_and_keeps_extras() {
    let answers = Answers::from_iter([
        ("addressed", Answer::YesNo(YesNoAnswer { probability: 1.0 })),
        ("language", Answer::Choice(ChoiceAnswer::new("en"))),
        ("urgency", Answer::Score(ScoreAnswer::new(0.0))),
        ("extra", Answer::Score(ScoreAnswer::new(2.0))),
    ]);
    let questions = questions();
    assert!(std::ptr::eq(
        check_answers(&questions, &answers).unwrap(),
        std::ptr::from_ref(&answers)
    ));
    assert_eq!(answers.score("extra").unwrap(), 2.0);
    answers.remove("urgency");
    assert_eq!(
        check_answers(&questions, &answers).unwrap_err().to_string(),
        "no ScoreAnswer for 'urgency'"
    );
    answers.insert("addressed", Answer::Score(ScoreAnswer::new(1.0)));
    assert_eq!(
        check_answers(&questions, &answers).unwrap_err().to_string(),
        "no YesNoAnswer for 'addressed'"
    );
}

// Python: test_speaking_public_models.py::test_question_and_answer_maps_keep_order_and_the_supplied_aliases.
#[test]
fn question_and_answer_maps_keep_order_and_the_supplied_aliases() {
    let options = ChoiceOptions::from_iter([("z", "First".into()), ("a", "Second".into())]);
    let question = ChoiceQuestion::new("Which?", options.clone()).unwrap();
    options.insert("z", "Changed".into());
    assert_eq!(
        question.options.snapshot(),
        [
            ("z".into(), "Changed".into()),
            ("a".into(), "Second".into())
        ]
    );
    let probabilities = Probabilities::from_iter([("z", 0.8), ("a", 0.2)]);
    let answer = ChoiceAnswer {
        choice: "z".into(),
        probabilities: probabilities.clone(),
    };
    probabilities.insert("z", 0.9);
    assert_eq!(answer.probabilities.get("z"), Some(0.9));
    assert_eq!(
        serde_json::to_string(&answer.probabilities).unwrap(),
        "{\"z\":0.9,\"a\":0.2}"
    );
    assert_eq!(YesNoQuestion::new("Ready?").yes, None);
    assert_eq!(YesNoQuestion::new("Ready?").no, None);
    assert_eq!(
        ChoiceAnswer::new("z").probabilities.snapshot(),
        Vec::<(String, f64)>::new()
    );
    assert_eq!(ScoreAnswer::new(0.5).probabilities, Vec::<f64>::new());
}

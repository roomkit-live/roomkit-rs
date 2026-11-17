//! Python's public composition rules, independent of a classifier service.

use roomkit_ai::classifiers::{
    Answer, Answers, ChoiceAnswer, Probabilities, ScoreAnswer, YesNoAnswer,
};
use roomkit_ai::speaking::{
    AddressedOptions, ComposeOptions, Judgments, SpeakMode, addressed, compose, judgments_of,
};

type CompositionCase<'a> = (&'a [(&'a str, f64)], bool, SpeakMode, &'a str);

fn judgments(items: &[(&str, f64)]) -> Judgments {
    items
        .iter()
        .map(|(name, value)| ((*name).to_owned(), *value))
        .collect()
}

// Python: tests/test_speak_classifier.py::test_compose.
#[test]
fn compose_keeps_the_judgments_in_order() {
    use SpeakMode::{Offer, Silent, Speak};
    let cases: &[CompositionCase<'_>] = &[
        (
            &[("directness", 3.0), ("unfinished", 0.9)],
            false,
            Silent,
            "not finished",
        ),
        (
            &[("directness", 3.0), ("deferred", 0.8)],
            false,
            Silent,
            "postponed",
        ),
        (
            &[("directness", 3.0), ("hush", 0.7)],
            false,
            Silent,
            "asked to keep quiet",
        ),
        (&[("answered", 0.8)], false, Speak, "answers its question"),
        (&[("directness", 1.5)], false, Speak, "addressed"),
        (
            &[("directness", 0.8), ("request", 0.7)],
            false,
            Speak,
            "addressed",
        ),
        (
            &[("directness", 0.2), ("request", 0.9)],
            true,
            Speak,
            "addressed",
        ),
        (
            &[("directness", 0.2), ("request", 0.9)],
            false,
            Silent,
            "not addressed",
        ),
        (
            &[("directness", 0.9), ("request", 0.2)],
            false,
            Offer,
            "wondered about",
        ),
        (&[("directness", 0.6)], false, Silent, "not addressed"),
        (&[], false, Silent, "not addressed"),
    ];
    for (values, alone, mode, reason) in cases {
        assert_eq!(
            compose(
                &judgments(values),
                ComposeOptions {
                    alone: *alone,
                    ..ComposeOptions::default()
                }
            ),
            (*mode, (*reason).to_owned())
        );
    }
}

// Python: tests/test_speak_classifier.py::test_addressed_bands.
#[test]
fn addressed_bands() {
    let ordinary = AddressedOptions::default();
    assert!(addressed(1.5, 0.0, ordinary));
    assert!(!addressed(1.4, 0.4, ordinary));
    assert!(addressed(0.75, 0.5, ordinary));
    assert!(addressed(0.0, 0.5, AddressedOptions { alone: true }));
    assert!(!addressed(0.0, 0.5, ordinary));
}

// Python: tests/test_speak_classifier.py::test_judgments_of_reads_every_kind.
#[test]
fn judgments_of_reads_every_kind_in_the_supplied_order() {
    let answers = Answers::new();
    answers.insert("hush", Answer::YesNo(YesNoAnswer { probability: 0.2 }));
    answers.insert(
        "directness",
        Answer::Score(ScoreAnswer {
            score: 2.4,
            probabilities: vec![],
        }),
    );
    answers.insert(
        "language",
        Answer::Choice(ChoiceAnswer {
            choice: "French".into(),
            probabilities: [("French", 0.97), ("other", 0.03)].into_iter().collect(),
        }),
    );
    assert_eq!(
        judgments_of(&answers).snapshot(),
        vec![
            ("hush".into(), 0.2),
            ("directness".into(), 2.4),
            ("language=French".into(), 0.97)
        ]
    );
    answers.insert(
        "language",
        Answer::Choice(ChoiceAnswer {
            choice: "German".into(),
            probabilities: Probabilities::new(),
        }),
    );
    assert_eq!(judgments_of(&answers).get("language=German"), Some(1.0));
}

// Python: tests/test_speak_classifier.py::test_a_question_put_to_someone_else_is_theirs_to_answer_first.
#[test]
fn a_question_put_to_someone_else_is_theirs_to_answer_first() {
    for (directness, waiting, urgent, mode, reason) in [
        (0.0, 0.8, false, SpeakMode::Silent, "theirs to answer"),
        (0.0, 0.2, false, SpeakMode::Offer, "has something to add"),
        (0.0, 0.8, true, SpeakMode::Offer, "urgent"),
        (1.0, 0.8, false, SpeakMode::Offer, "wondered about"),
    ] {
        assert_eq!(
            compose(
                &judgments(&[
                    ("answers", 0.9),
                    ("waiting", waiting),
                    ("directness", directness)
                ]),
                ComposeOptions {
                    urgent,
                    ..ComposeOptions::default()
                }
            ),
            (mode, reason.to_owned())
        );
    }
}

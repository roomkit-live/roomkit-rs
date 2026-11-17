//! The narrow judgments are composed in order, without another model call.

use super::{Judgments, SpeakMode};
use crate::classifiers::{Answer, Answers};

/// Context for composing the measured judgments.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ComposeOptions {
    /// One person is talking with the agent.
    pub alone: bool,
    /// How sure an unasked contribution must be; defaults to 0.5.
    pub proactivity: f64,
    /// What the agent has to say cannot wait.
    pub urgent: bool,
}

impl Default for ComposeOptions {
    fn default() -> Self {
        Self {
            alone: false,
            proactivity: 0.5,
            urgent: false,
        }
    }
}

/// Whether the agent is the person's only listener.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AddressedOptions {
    /// Any request is for the agent when it is the only listener.
    pub alone: bool,
}

/// The mode and reason from the judgments, with missing judgments reading zero.
#[must_use]
pub fn compose(judgments: &Judgments, options: ComposeOptions) -> (SpeakMode, String) {
    let values = judgments.snapshot();
    let get = |name: &str| {
        values
            .iter()
            .find(|(n, _)| n == name)
            .map_or(0.0, |(_, v)| *v)
    };
    for (name, reason) in [
        ("unfinished", "not finished"),
        ("deferred", "postponed"),
        ("hush", "asked to keep quiet"),
    ] {
        if get(name) >= 0.5 {
            return (SpeakMode::Silent, reason.into());
        }
    }
    if get("answered") >= 0.5 {
        return (SpeakMode::Speak, "answers its question".into());
    }
    if addressed(
        get("directness"),
        get("request"),
        AddressedOptions {
            alone: options.alone,
        },
    ) {
        return (SpeakMode::Speak, "addressed".into());
    }
    let knows = greater(get("answers"), get("corrects"));
    let theirs = !options.urgent && get("waiting") >= 0.5;
    if get("directness") >= 0.75 {
        return if knows >= 0.5 && !theirs {
            (SpeakMode::Speak, "knows the answer".into())
        } else {
            (SpeakMode::Offer, "wondered about".into())
        };
    }
    let threshold = if options.urgent {
        options.proactivity / 2.0
    } else {
        options.proactivity
    };
    if knows < threshold || knows.is_nan() || threshold.is_nan() {
        return (SpeakMode::Silent, "not addressed".into());
    }
    if theirs {
        return (SpeakMode::Silent, "theirs to answer".into());
    }
    (
        SpeakMode::Offer,
        if options.urgent {
            "urgent"
        } else {
            "has something to add"
        }
        .into(),
    )
}

/// Whether directness and the request put this turn to the agent.
#[must_use]
pub fn addressed(directness: f64, request: f64, options: AddressedOptions) -> bool {
    (request >= 0.5 && (options.alone || directness >= 0.75)) || directness >= 1.5
}

/// Reads every answer in order; choices use `name=choice` as their judgment key.
#[must_use]
pub fn judgments_of(answers: &Answers) -> Judgments {
    answers
        .iter()
        .map(|(name, answer)| match answer {
            Answer::YesNo(answer) => (name, answer.probability),
            Answer::Score(answer) => (name, answer.score),
            Answer::Choice(answer) => (
                format!("{name}={}", answer.choice),
                answer.probabilities.get(&answer.choice).unwrap_or(1.0),
            ),
        })
        .collect()
}

// Python's max keeps its first value when comparison with the second is false,
// including NaN. These measured carriers do not add numeric validation.
pub(super) fn greater(first: f64, second: f64) -> f64 {
    if second > first { second } else { first }
}

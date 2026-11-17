//! Typed answer reads and complete-result validation (RFC §6.8).

use roomkit::ChannelError;
use roomkit_core::rules::python_repr;
use serde::{Deserialize, Serialize};

use super::error::invalid;
use super::{ClassifierError, Question, Questions};

crate::shared::ordered::ordered_mapping!(
    /// Probabilities by named choice, keeping the supplied order and sharing.
    Probabilities, f64
);

/// The probability of yes. The carrier does not validate its numeric range.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct YesNoAnswer {
    /// The reported probability.
    pub probability: f64,
}

/// The most probable named option and the optional full distribution.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ChoiceAnswer {
    /// The chosen name.
    pub choice: String,
    /// The supplied mapping; empty by default.
    pub probabilities: Probabilities,
}

impl ChoiceAnswer {
    /// An answer without a distribution.
    pub fn new(choice: impl Into<String>) -> Self {
        Self {
            choice: choice.into(),
            probabilities: Probabilities::new(),
        }
    }
}

/// The expected level, zero for the first and fractional between levels.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScoreAnswer {
    /// The expected level, without range validation in this carrier.
    pub score: f64,
    /// Each level's probability, in order; empty by default.
    pub probabilities: Vec<f64>,
}

impl ScoreAnswer {
    /// An answer without a distribution.
    pub fn new(score: f64) -> Self {
        Self {
            score,
            probabilities: Vec::new(),
        }
    }
}

/// An answer retaining the kind supplied by the classifier.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Answer {
    /// A yes/no probability.
    YesNo(YesNoAnswer),
    /// A named choice.
    Choice(ChoiceAnswer),
    /// An expected scale level.
    Score(ScoreAnswer),
}

impl Answer {
    fn kind(&self) -> &'static str {
        match self {
            Self::YesNo(_) => "YesNoAnswer",
            Self::Choice(_) => "ChoiceAnswer",
            Self::Score(_) => "ScoreAnswer",
        }
    }
}

crate::shared::ordered::ordered_mapping!(
    /// Answers by question name, with typed reads. Clones share this mapping.
    Answers, Answer
);

impl Answers {
    /// Reads the probability of yes; missing and mistyped answers are distinct errors.
    ///
    /// # Errors
    /// `KeyError` if absent, `TypeError` if it is not a yes/no answer.
    pub fn yes(&self, name: &str) -> Result<f64, ChannelError> {
        match self.typed(name, "YesNoAnswer")? {
            Answer::YesNo(answer) => Ok(answer.probability),
            Answer::Choice(_) | Answer::Score(_) => unreachable!("typed checked the answer kind"),
        }
    }

    /// Reads the chosen option.
    ///
    /// # Errors
    /// `KeyError` if absent, `TypeError` if it is not a choice answer.
    pub fn choice(&self, name: &str) -> Result<String, ChannelError> {
        match self.typed(name, "ChoiceAnswer")? {
            Answer::Choice(answer) => Ok(answer.choice),
            Answer::YesNo(_) | Answer::Score(_) => unreachable!("typed checked the answer kind"),
        }
    }

    /// Reads the expected level.
    ///
    /// # Errors
    /// `KeyError` if absent, `TypeError` if it is not a score answer.
    pub fn score(&self, name: &str) -> Result<f64, ChannelError> {
        match self.typed(name, "ScoreAnswer")? {
            Answer::Score(answer) => Ok(answer.score),
            Answer::YesNo(_) | Answer::Choice(_) => unreachable!("typed checked the answer kind"),
        }
    }

    fn typed(&self, name: &str, kind: &str) -> Result<Answer, ChannelError> {
        let answer = self
            .get(name)
            .ok_or_else(|| invalid("KeyError", python_repr(name)))?;
        if answer.kind() != kind {
            return Err(invalid(
                "TypeError",
                format!(
                    "{} was answered as {}, not {kind}",
                    python_repr(name),
                    answer.kind()
                ),
            ));
        }
        Ok(answer)
    }
}

/// Returns the same answers once every question has its matching kind.
/// Extra entries remain intact; missing or mistyped answers are classifier failures.
///
/// # Errors
/// A `ClassifierError` names the first missing or mistyped answer.
pub fn check_answers<'a>(
    questions: &Questions,
    answers: &'a Answers,
) -> Result<&'a Answers, ClassifierError> {
    for (name, question) in questions {
        let kind = match question {
            Question::Choice(_) => "ChoiceAnswer",
            Question::Score(_) => "ScoreAnswer",
            Question::YesNo(_) => "YesNoAnswer",
        };
        if answers.get(&name).as_ref().map(Answer::kind) != Some(kind) {
            return Err(ClassifierError::new(format!(
                "no {kind} for {}",
                python_repr(&name)
            )));
        }
    }
    Ok(answers)
}

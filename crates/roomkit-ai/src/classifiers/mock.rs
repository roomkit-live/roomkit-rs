//! Scripted answers, interpreted when their typed questions are asked.

use std::error::Error;
use std::sync::Arc;

use roomkit::ChannelError;
use roomkit_core::{model::python_float, rules::python_repr};
use serde::{Deserialize, Serialize};

use super::error::invalid;
use super::{
    Answer, Answers, ChoiceAnswer, ChoiceQuestion, Classifier, ClassifierError, ClassifierFuture,
    Question, Questions, ScoreAnswer, SharedState, YesNoAnswer,
};
use crate::shared::{Shared, failure};

/// A typed answer, or a scalar interpreted against the question at call time.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ScriptedAnswer {
    /// A typed answer, returned without rechecking its kind.
    Answer(Answer),
    /// A yes/no probability or expected level; a choice uses its numeric text.
    Number(f64),
    /// A choice name or the numeric text for a probability or score.
    Text(String),
}

crate::shared::ordered::ordered_mapping!(
    /// A live mock script: its cloned handles retain later entry edits.
    Script, ScriptedAnswer
);

/// Scripts in call order and an optional error, empty by default.
#[derive(Clone, Debug, Default)]
pub struct MockClassifierOptions {
    /// The last script repeats; no scripts means default answers.
    pub answers: Vec<Script>,
    /// Raised after recording the call, with this original cause retained.
    pub error: Option<Arc<dyn Error + Send + Sync>>,
}

/// Records the state handle and a copy of each outer question mapping.
#[derive(Clone, Debug)]
pub struct MockClassifier {
    options: Arc<MockClassifierOptions>,
    calls: Shared<Vec<(SharedState, Questions)>>,
}

impl MockClassifier {
    /// Takes the script sequence, retaining each script mapping's live handle.
    pub fn new(options: MockClassifierOptions) -> Self {
        Self {
            options: Arc::new(options),
            calls: Shared::new(vec![]),
        }
    }

    /// The recorded calls; cloned states keep the same objects.
    pub fn calls(&self) -> Vec<(SharedState, Questions)> {
        self.calls.snapshot()
    }
}

impl Classifier for MockClassifier {
    fn classify<'a>(
        &'a self,
        state: &'a SharedState,
        questions: &'a Questions,
    ) -> ClassifierFuture<'a, Answers> {
        Box::pin(async move {
            let count = self.calls.update(|calls| {
                calls.push((state.clone(), questions.iter().collect()));
                calls.len()
            });
            if let Some(error) = &self.options.error {
                return Err(failure::retained(error));
            }
            let script = self
                .options
                .answers
                .get(count.saturating_sub(1))
                .or_else(|| self.options.answers.last())
                .cloned()
                .unwrap_or_default();
            questions
                .iter()
                .map(|(name, question)| {
                    answer(&script, &name, &question).map(|value| (name, value))
                })
                .collect()
        })
    }
}

fn answer(script: &Script, name: &str, question: &Question) -> Result<Answer, ChannelError> {
    let scripted = script.get(name);
    if let Some(ScriptedAnswer::Answer(answer)) = scripted {
        return Ok(answer);
    }
    match question {
        Question::Choice(question) => choice(scripted.as_ref(), name, question),
        Question::Score(_) => Ok(Answer::Score(ScoreAnswer::new(number(scripted.as_ref())?))),
        Question::YesNo(_) => Ok(Answer::YesNo(YesNoAnswer {
            probability: number(scripted.as_ref())?,
        })),
    }
}

fn choice(
    scripted: Option<&ScriptedAnswer>,
    name: &str,
    question: &ChoiceQuestion,
) -> Result<Answer, ChannelError> {
    let choice = match scripted {
        Some(ScriptedAnswer::Text(text)) => text.clone(),
        Some(ScriptedAnswer::Number(value)) => python_float(*value),
        None => question
            .options
            .iter()
            .next()
            .map(|(name, _)| name)
            .ok_or_else(|| invalid("RuntimeError", "coroutine raised StopIteration"))?,
        Some(ScriptedAnswer::Answer(_)) => {
            unreachable!("typed answers return before choice conversion")
        }
    };
    if question.options.get(&choice).is_none() {
        return Err(Box::new(ClassifierError::new(format!(
            "{} is not an option of {}",
            python_repr(&choice),
            python_repr(name)
        ))));
    }
    let probabilities = question
        .options
        .iter()
        .map(|(name, _)| {
            let probability = f64::from(name == choice);
            (name, probability)
        })
        .collect();
    Ok(Answer::Choice(ChoiceAnswer {
        choice,
        probabilities,
    }))
}

fn number(scripted: Option<&ScriptedAnswer>) -> Result<f64, ChannelError> {
    match scripted {
        None => Ok(0.0),
        Some(ScriptedAnswer::Number(value)) => Ok(*value),
        Some(ScriptedAnswer::Text(text)) => float_text(text).ok_or_else(|| {
            invalid(
                "ValueError",
                format!("could not convert string to float: {}", python_repr(text)),
            )
        }),
        Some(ScriptedAnswer::Answer(_)) => {
            unreachable!("typed answers return before numeric conversion")
        }
    }
}

fn float_text(text: &str) -> Option<f64> {
    let ascii = crate::decimal_digits::ascii(text);
    let text = ascii.trim();
    let bytes = text.as_bytes();
    for (at, byte) in bytes.iter().enumerate() {
        if *byte == b'_'
            && !(at
                .checked_sub(1)
                .and_then(|at| bytes.get(at))
                .is_some_and(u8::is_ascii_digit)
                && bytes.get(at + 1).is_some_and(u8::is_ascii_digit))
        {
            return None;
        }
    }
    text.replace('_', "").parse().ok()
}

//! Typed questions and answers used by speaking policies.

mod answers;
pub(crate) mod error;
mod mock;
mod questions;

use std::future::Future;
use std::pin::Pin;

use roomkit::ChannelError;
use serde_json::Value;

pub use answers::{
    Answer, Answers, ChoiceAnswer, Probabilities, ScoreAnswer, YesNoAnswer, check_answers,
};
pub use error::ClassifierError;
pub use mock::{MockClassifier, MockClassifierOptions, Script, ScriptedAnswer};
pub use questions::{
    ChoiceOptions, ChoiceQuestion, Question, Questions, ScoreQuestion, YesNoQuestion,
};

crate::shared::shared_input!(
    /// The classifier's JSON-compatible state. A mock retains this object,
    /// including edits made after its call, rather than taking a snapshot.
    SharedState, Value
);

/// A classifier's bounded operation, retaining arbitrary host errors.
pub type ClassifierFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ChannelError>> + Send + 'a>>;

/// Answers every supplied typed question about the same state (RFC §6.8).
pub trait Classifier: Send + Sync + 'static {
    /// The answers under their question names.
    fn classify<'a>(
        &'a self,
        state: &'a SharedState,
        questions: &'a Questions,
    ) -> ClassifierFuture<'a, Answers>;

    /// Releases resources owned by this classifier; optional for implementations.
    fn close(&self) -> ClassifierFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

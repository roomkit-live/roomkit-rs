//! Application-facing errors for text tool calls and delegated turns.

use std::error::Error;
use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use roomkit_core::rules::python_repr;
mod record;
mod worker;
pub(crate) use record::text_record;
pub(crate) use worker::WorkerCause;

/// A tool call refused by the channel before the tool ran (RFC §9.3).
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{display}")]
pub struct ChannelRefusalError {
    /// The words the model may read.
    pub message: String,
    display: String,
}

impl ChannelRefusalError {
    /// Construct a channel refusal carrying the given words.
    pub fn new(message: impl Into<String>) -> Self {
        let message = message.into();
        Self {
            display: message.clone(),
            message,
        }
    }
}

/// An elapsed tool-call deadline, as reported to observers (RFC §21.6).
#[derive(Clone, Debug, thiserror::Error)]
#[error("{display}")]
pub struct ToolTimeoutError {
    /// The name of the tool that missed the deadline.
    pub name: String,
    /// The bound in seconds, as supplied.
    pub timeout: f64,
    display: String,
}

impl ToolTimeoutError {
    /// Construct the diagnostic with Python's quoted name and six significant digits.
    pub fn new(name: impl Into<String>, timeout: f64) -> Self {
        let name = name.into();
        let display = format!(
            "tool {} did not answer within {} s",
            python_repr(&name),
            general(timeout)
        );
        Self {
            name,
            timeout,
            display,
        }
    }
}

/// A turn that ended before producing its answer (RFC §6.4).
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct TurnCutShortError {
    message: String,
    /// Its open-ended protocol or application reason, when known.
    pub reason: Option<String>,
}

impl TurnCutShortError {
    /// Construct a turn cut with the caller's message and optional reason.
    pub fn new(message: impl Into<String>, reason: Option<String>) -> Self {
        Self {
            message: message.into(),
            reason,
        }
    }
}

/// A delegated worker's turn ended before its answer (RFC §23.3).
/// Borrow its underlying [`TurnCutShortError`] to handle the entire family.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{turn}")]
pub struct TaskCutShortError {
    turn: TurnCutShortError,
    /// The worker's last narration, when present.
    pub narration: Option<String>,
}

impl TaskCutShortError {
    /// Construct a cut using the same text as a failed delegated task.
    pub fn new(reason: impl Into<String>, narration: Option<String>) -> Self {
        let reason = reason.into();
        Self {
            turn: TurnCutShortError::new(Self::message_for(&reason), Some(reason)),
            narration,
        }
    }

    /// The message for a worker whose turn ended for this open-ended reason.
    pub fn message_for(reason: &str) -> String {
        format!("The worker's turn ended {reason} before its answer")
    }
}

impl AsRef<TurnCutShortError> for TaskCutShortError {
    fn as_ref(&self) -> &TurnCutShortError {
        &self.turn
    }
}

impl Deref for TaskCutShortError {
    type Target = TurnCutShortError;

    fn deref(&self) -> &Self::Target {
        &self.turn
    }
}

/// A worker turn failed while running and kept its cause and narration (RFC §23.3).
#[derive(Clone, Debug)]
pub struct TaskTurnFailedError {
    error: Arc<dyn Error + Send + Sync>,
    display: String,
    /// The worker's reason for ending its turn.
    pub reason: String,
    /// The last narration, when present.
    pub narration: Option<String>,
}

impl TaskTurnFailedError {
    /// Keep the original shared error as the source and capture its current message.
    pub fn new(
        error: Arc<dyn Error + Send + Sync>,
        reason: impl Into<String>,
        narration: Option<String>,
    ) -> Self {
        Self {
            display: error.to_string(),
            error,
            reason: reason.into(),
            narration,
        }
    }
}

impl fmt::Display for TaskTurnFailedError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.display)
    }
}

impl Error for TaskTurnFailedError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.error.as_ref())
    }
}

/// `value` as Python's `format(value, "g")` writes it: six significant
/// digits, trailing zeros dropped, in exponent form below 1e-4 or from
/// 1e6.
pub(crate) fn general(value: f64) -> String {
    if value.is_nan() {
        return "nan".to_owned();
    }
    if value == 0.0 || value.is_infinite() {
        return value.to_string();
    }
    let scientific = format!("{value:.5e}");
    let (mantissa, exponent) = scientific.split_once('e').unwrap_or((&scientific, "0"));
    let exponent: i32 = exponent.parse().unwrap_or_default();
    if (-4..6).contains(&exponent) {
        let decimals = usize::try_from(5 - exponent).unwrap_or_default();
        return without_trailing_zeros(&format!("{value:.decimals$}")).to_owned();
    }
    let sign = if exponent < 0 { '-' } else { '+' };
    format!(
        "{}e{sign}{:02}",
        without_trailing_zeros(mantissa),
        exponent.unsigned_abs()
    )
}

/// A decimal's text without the zeros ending its fraction, nor a point
/// left alone.
fn without_trailing_zeros(text: &str) -> &str {
    if text.contains('.') {
        text.trim_end_matches('0').trim_end_matches('.')
    } else {
        text
    }
}

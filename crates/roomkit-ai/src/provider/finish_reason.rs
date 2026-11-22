//! Why a generation stopped, in the one vocabulary of the provider contract
//! (RFC §6.7): Python's `roomkit.providers.ai.FinishReason`.
//!
//! Each vendor reports how a response ended in its own words. A provider
//! maps them onto this closed set in its adapter, so the core reads one
//! vocabulary and knows no vendor's word.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Why a generation stopped (RFC §6.7). A provider maps its vendor's words
/// onto these, and ends a generation on `None` when its vendor gave no
/// reason or one the provider does not know.
///
/// It reads and writes as its word:
///
/// ```
/// use roomkit_ai::FinishReason;
///
/// let reason: FinishReason = serde_json::from_str(r#""malformed_call""#).unwrap();
/// assert_eq!(reason, FinishReason::MalformedCall);
/// assert!(serde_json::from_str::<FinishReason>(r#""end_turn""#).is_err());
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum FinishReason {
    /// The model ended its answer itself.
    Stop,
    /// The model stopped to call tools.
    ToolCalls,
    /// The response ran out of room: its output cap, or its context window
    /// filling up mid-answer.
    Length,
    /// A filter or the provider's safeguards withheld or stopped the answer.
    ContentFilter,
    /// The model refused to answer.
    Refusal,
    /// The model called a tool, and the provider could not parse the call,
    /// so it handed none over.
    MalformedCall,
    /// The model called a tool the request did not enable, and the provider
    /// handed no call over.
    UnexpectedCall,
    /// The generation failed before its end.
    Error,
}

impl FinishReason {
    /// The word, as Python's `value` spells it.
    #[doc(hidden)]
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Stop => "stop",
            Self::ToolCalls => "tool_calls",
            Self::Length => "length",
            Self::ContentFilter => "content_filter",
            Self::Refusal => "refusal",
            Self::MalformedCall => "malformed_call",
            Self::UnexpectedCall => "unexpected_call",
            Self::Error => "error",
        }
    }
}

impl fmt::Display for FinishReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

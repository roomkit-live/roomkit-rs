//! Public text-provider decisions on call arguments, as received from the wire.

use serde_json::Value;

use super::finish_reason::FinishReason;
use super::response::{cuts_calls, read_arguments};

// Each function has its own options type, following the public API convention.
// All three interpret the same position within a round of tool calls.
macro_rules! position_options {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub struct $name {
            /// Whether this is the last call in the provider response.
            pub last: bool,
        }

        impl Default for $name {
            fn default() -> Self {
                Self { last: true }
            }
        }
    };
}

position_options!(
    CallCutOptions,
    "Position of the call checked by [`call_cut`]."
);
position_options!(
    CallPartialOptions,
    "Position of the call checked by [`call_partial`]."
);
position_options!(
    CallGarbledOptions,
    "Position of the call checked by [`call_garbled`]."
);

/// Whether the raw arguments cannot be read as an object. A populated
/// mapping, no arguments, blank text and JSON `null` are readable.
pub fn unreadable_arguments(raw: &Value) -> bool {
    match raw {
        Value::Object(_) | Value::Null => false,
        Value::String(text) => read_arguments(text).is_none(),
        Value::Bool(_) | Value::Number(_) | Value::Array(_) => true,
    }
}

/// Whether the model's last call failed to arrive whole under a cut.
pub fn call_cut(raw: &Value, finish_reason: Option<FinishReason>, options: CallCutOptions) -> bool {
    ArgumentStatus::of(raw, finish_reason, options.last).cut
}

/// Whether the call must be refused.
pub fn call_partial(
    raw: &Value,
    finish_reason: Option<FinishReason>,
    options: CallPartialOptions,
) -> bool {
    ArgumentStatus::of(raw, finish_reason, options.last).partial
}

/// Whether the provider supplied unreadable arguments without a cut.
pub fn call_garbled(
    raw: &Value,
    finish_reason: Option<FinishReason>,
    options: CallGarbledOptions,
) -> bool {
    ArgumentStatus::of(raw, finish_reason, options.last).garbled
}

/// Python's `str.strip()` also treats the four ASCII separators as space.
pub(super) fn blank(text: &str) -> bool {
    text.chars()
        .all(|c| c.is_whitespace() || matches!(c, '\u{1c}'..='\u{1f}'))
}

pub(super) struct ArgumentStatus {
    pub(super) partial: bool,
    pub(super) garbled: bool,
    cut: bool,
}

impl ArgumentStatus {
    fn of(raw: &Value, finish_reason: Option<FinishReason>, last: bool) -> Self {
        let text_arrived = raw.as_str().is_some_and(|text| !blank(text));
        Self::from_read(unreadable_arguments(raw), text_arrived, finish_reason, last)
    }

    pub(super) fn from_read(
        unreadable: bool,
        text_arrived: bool,
        finish_reason: Option<FinishReason>,
        last: bool,
    ) -> Self {
        let cut = last && cuts_calls(finish_reason) && (unreadable || !text_arrived);
        Self {
            partial: unreadable || cut,
            garbled: unreadable && !cut,
            cut,
        }
    }
}

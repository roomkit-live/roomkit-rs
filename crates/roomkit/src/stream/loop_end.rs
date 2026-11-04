//! How an intelligence channel's streamed response ended (RFC §6.4 "The
//! turn's record"): Python's `roomkit.models.LoopEndMarker`, and the record
//! the kit writes from it on the turn's last message, else its last row.

use std::fmt;
use std::time::Duration;

use roomkit_core::model::Metadata;
use serde_json::Value;

/// The record's key naming why the loop ended.
const LOOP_END_REASON: &str = "loop_end_reason";
/// The record's key holding what the turn's generations used.
const AI_USAGE: &str = "ai_usage";
/// The record's key holding what the turn's last generation used.
const AI_LAST_USAGE: &str = "ai_last_usage";
/// The record's key saying whether every generation reported its counters.
const AI_USAGE_COMPLETE: &str = "ai_usage_complete";
/// The record's key naming the model the turn ran on.
const AI_MODEL: &str = "ai_model";
/// The record's key naming the provider the turn ran on.
const AI_PROVIDER: &str = "ai_provider";

/// Why a turn's loop ended (RFC §6.4, Appendix A.9): Python's
/// `LoopEndReason`, each variant its value in `snake_case`, which the
/// record writes as `loop_end_reason`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum LoopEndReason {
    /// The model answered.
    Completed,
    /// The model still called past the round bound.
    MaxRounds,
    /// The loop's deadline passed.
    Timeout,
    /// The turn reached its token or cost budget at a round boundary.
    BudgetExceeded,
    /// A round was cut at the output cap or the context window.
    Truncated,
    /// After a round of calls, the model answered nothing.
    EmptyResponse,
    /// The channel's continuation policy still found the answer unfinished
    /// once its tries had run out.
    Unfinished,
    /// The loop was stopped before its end.
    ForceStopped,
    /// The turn was cancelled.
    Cancelled,
    /// The provider failed after a round.
    Error,
}

impl LoopEndReason {
    /// Every reason, in Python's order.
    const ALL: [Self; 10] = [
        Self::Completed,
        Self::MaxRounds,
        Self::Timeout,
        Self::BudgetExceeded,
        Self::Truncated,
        Self::EmptyResponse,
        Self::Unfinished,
        Self::ForceStopped,
        Self::Cancelled,
        Self::Error,
    ];

    /// The reason as Python writes it.
    fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::MaxRounds => "max_rounds",
            Self::Timeout => "timeout",
            Self::BudgetExceeded => "budget_exceeded",
            Self::Truncated => "truncated",
            Self::EmptyResponse => "empty_response",
            Self::Unfinished => "unfinished",
            Self::ForceStopped => "force_stopped",
            Self::Cancelled => "cancelled",
            Self::Error => "error",
        }
    }

    /// The reason Python writes as `value`, if it is one.
    pub(crate) fn of(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|reason| reason.as_str() == value)
    }
}

impl fmt::Display for LoopEndReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// What a generation of a turn used, and what it ran on, told after it, in
/// its order among the text and the rows (RFC §6.4): Python's
/// `roomkit.models.UsageMarker(usage={}, model=None, provider=None)`.
/// `usage` is that generation's alone, empty when it reported no counters;
/// the store holds none of it, and a generation in flight has told nothing.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct UsageMarker {
    /// What the generation used, as its provider reported it.
    pub usage: Metadata,
    /// The model that served it.
    pub model: Option<String>,
    /// The provider that served it, a fallback when it did.
    pub provider: Option<String>,
}

/// The last item of an intelligence channel's streamed response, saying
/// why its loop ended (RFC §6.4): Python's `LoopEndMarker(reason, rounds=0,
/// usage={}, max_rounds=None, timeout_seconds=None, budget_tokens=None,
/// budget_usd=None, last_usage=None, usage_complete=None, model=None,
/// provider=None)`. With the reason, `usage` is the turn's record, which
/// the kit writes on the turn's last message, else its last row: `loop_end_reason` and
/// `ai_usage`, then `ai_last_usage`, `ai_usage_complete`, `ai_model` and
/// `ai_provider` when they are set, the first two together, when
/// `usage_complete` is.
#[derive(Clone, Debug, PartialEq)]
pub struct LoopEndMarker {
    /// Why the loop ended.
    pub reason: LoopEndReason,
    /// How many rounds of tool calls ran before it ended.
    pub rounds: u32,
    /// What the turn's generations used, summed over every round.
    pub usage: Metadata,
    /// The round bound the turn ran under, if any.
    pub max_rounds: Option<u32>,
    /// The deadline the turn ran under, if any.
    pub timeout: Option<Duration>,
    /// The token budget the turn ran under, if any.
    pub budget_tokens: Option<u64>,
    /// The cost budget the turn ran under, in US dollars, if any.
    pub budget_usd: Option<f64>,
    /// What the turn's last generation used, empty when it reported no
    /// counters, `None` when it failed.
    pub last_usage: Option<Metadata>,
    /// Whether every generation of the turn reported its counters; `None`
    /// when the channel does not say.
    pub usage_complete: Option<bool>,
    /// The model the turn's last generation ran on, a fallback's when it
    /// served it.
    pub model: Option<String>,
    /// The provider the turn's last generation ran on.
    pub provider: Option<String>,
}

impl LoopEndMarker {
    /// The end of a loop for `reason`, with Python's defaults: no round, no
    /// usage, no limit.
    pub fn new(reason: LoopEndReason) -> Self {
        Self {
            reason,
            rounds: 0,
            usage: Metadata::new(),
            max_rounds: None,
            timeout: None,
            budget_tokens: None,
            budget_usd: None,
            last_usage: None,
            usage_complete: None,
            model: None,
            provider: None,
        }
    }
}

impl LoopEndMarker {
    /// The turn's record the marker gives (RFC §6.4 "The turn's record"),
    /// which the kit writes on the turn's last message, else its last row: Python's
    /// `loop_end_reason` and `ai_usage`, then the keys its other fields
    /// set. Outside the public API (decision 16): roomkit-ai writes
    /// it on a turn it answers whole.
    #[doc(hidden)]
    pub fn record(&self) -> Metadata {
        let mut record = Metadata::new();
        record.insert(LOOP_END_REASON.to_owned(), self.reason.to_string().into());
        record.insert(AI_USAGE.to_owned(), Value::Object(self.usage.clone()));
        if let Some(complete) = self.usage_complete {
            let last = self.last_usage.clone().map_or(Value::Null, Value::Object);
            record.insert(AI_LAST_USAGE.to_owned(), last);
            record.insert(AI_USAGE_COMPLETE.to_owned(), complete.into());
        }
        if let Some(model) = &self.model {
            record.insert(AI_MODEL.to_owned(), model.clone().into());
        }
        if let Some(provider) = &self.provider {
            record.insert(AI_PROVIDER.to_owned(), provider.clone().into());
        }
        record
    }
}

/// Adds a generation's integer counters to a turn's running `total`, cache
/// reads and writes with its input and output: Python's `add_usage`. Outside
/// the public API (decision 16): roomkit-ai counts with it.
#[doc(hidden)]
pub fn add_usage(total: &mut Metadata, used: &Metadata) {
    for (name, used) in used {
        let Some(used) = used.as_i64() else {
            continue;
        };
        let sum = total
            .get(name)
            .and_then(Value::as_i64)
            .unwrap_or(0)
            .saturating_add(used);
        total.insert(name.clone(), Value::from(sum));
    }
}

/// Whether `used` holds a generation's input and output counters: Python's
/// `reports_counters`. Outside the public API (decision 16): roomkit-ai
/// counts with it.
#[doc(hidden)]
pub fn reports_counters(used: &Metadata) -> bool {
    ["input_tokens", "output_tokens"]
        .iter()
        .all(|key| used.get(*key).and_then(Value::as_u64).is_some())
}

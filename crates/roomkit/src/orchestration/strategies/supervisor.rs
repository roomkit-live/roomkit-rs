//! What the supervisor strategy shares with the loop: Python's
//! `roomkit.orchestration.strategies.supervisor`, its `WorkerStrategy`.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::error::RoomKitError;

/// How workers are run when delegated to (RFC §19.7.3, §19.7.4): one after
/// the other, or all at once. Python's `WorkerStrategy`, written
/// `sequential` and `parallel`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerStrategy {
    /// One after the other, each handed what the ones before it said.
    Sequential,
    /// All at once, on the same input.
    Parallel,
}

impl WorkerStrategy {
    /// The strategy as it is written: Python's `WorkerStrategy.value`.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Sequential => "sequential",
            Self::Parallel => "parallel",
        }
    }
}

impl fmt::Display for WorkerStrategy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Python's `WorkerStrategy(value)`: the strategy a text names, a
/// [`RoomKitError::Value`] (Python's `ValueError`) for any other text.
impl FromStr for WorkerStrategy {
    type Err = RoomKitError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        [Self::Sequential, Self::Parallel]
            .into_iter()
            .find(|strategy| strategy.as_str() == value)
            .ok_or_else(|| RoomKitError::Value(format!("'{value}' is not a valid WorkerStrategy")))
    }
}

#[cfg(test)]
mod tests {
    use super::WorkerStrategy;

    // Python: tests/orchestration/test_loop_strategy.py::TestLoopConstructor::test_strategy_parsed
    #[test]
    fn a_worker_strategy_is_read_from_its_value() {
        assert_eq!(
            "parallel".parse::<WorkerStrategy>().ok(),
            Some(WorkerStrategy::Parallel)
        );
        assert_eq!(
            "sequential".parse::<WorkerStrategy>().ok(),
            Some(WorkerStrategy::Sequential)
        );
        assert!("fan-out".parse::<WorkerStrategy>().is_err());
        assert_eq!(WorkerStrategy::Parallel.to_string(), "parallel");
    }
}

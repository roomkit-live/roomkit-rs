//! The supervisor strategy (RFC §19.7.3): Python's
//! `roomkit.orchestration.strategies.supervisor`. A supervisor agent
//! answers the room and delegates tasks to worker agents, which run in
//! child rooms (§23.3), registered on the kit and never attached to the
//! room; and the `WorkerStrategy` it shares with the loop.

mod per_worker;
mod prompts;
mod results;
pub(crate) mod strategy;
mod supervised;
mod team;

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

#[doc(inline)]
pub use self::strategy::Supervisor;
use self::strategy::Team;
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
    use roomkit_core::model::ChannelId;

    use super::strategy::SupervisorOptions;
    use super::{Supervisor, WorkerStrategy};
    use crate::orchestration::Orchestration;
    use crate::store::InMemoryStore;

    fn id(name: &str) -> ChannelId {
        ChannelId::new(name).unwrap_or_else(|error| panic!("{error}"))
    }

    // Python: tests/test_supervisor.py::TestSupervisorConstructor::test_constructor_with_strategy
    // Python: tests/test_supervisor.py::TestSupervisorConstructor::test_constructor_parallel_strategy
    // (its defaults: tests/api_defaults.rs supervisor_options_defaults_as_python)
    #[test]
    fn a_supervisor_keeps_its_strategy_and_its_workers() {
        let options = SupervisorOptions {
            strategy: Some("parallel".parse().unwrap_or_else(|error| panic!("{error}"))),
            ..SupervisorOptions::default()
        };
        let team = Supervisor::new(id("sup"), vec![id("w1").into()], options)
            .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(team.options.strategy, Some(WorkerStrategy::Parallel));
        assert_eq!(team.workers().len(), 1);
    }

    // Python: tests/orchestration/test_strategy_supervisor.py::TestSupervisorAgents::test_agents_returns_only_supervisor
    #[test]
    fn its_agents_are_the_supervisor_alone() {
        let team = Supervisor::new(
            id("sup"),
            vec![id("w1").into(), id("w2").into()],
            SupervisorOptions::default(),
        )
        .unwrap_or_else(|error| panic!("{error}"));
        let agents = Orchestration::<InMemoryStore>::agents(&team);
        let ids: Vec<&str> = agents
            .iter()
            .map(|agent| agent.channel_id().as_str())
            .collect();
        assert_eq!(ids, ["sup"]);
    }

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

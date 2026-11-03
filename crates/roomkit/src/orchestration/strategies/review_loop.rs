//! The loop strategy (RFC §19.7.4): Python's `Loop`. An agent produces
//! output, reviewers evaluate it, and the cycle repeats until all reviewers
//! approve or the iterations run out. The framework drives the flow; the
//! agents only produce content.

use std::sync::Arc;

use roomkit_core::model::{
    ChannelBinding, ChannelOutput, ChannelType, RoomContext, RoomEvent, RoomId,
};
use roomkit_core::rules::edits::mutation;
use serde_json::json;

use super::loop_run::{LoopRun, run_loop};
use super::supervisor::WorkerStrategy;
use crate::error::RoomKitError;
use crate::kit::RoomKit;
use crate::kit::strategy_address;
use crate::orchestration::agents::{HeldAgent, held};
use crate::orchestration::worker_run::task_input;
use crate::orchestration::{
    ConversationState, Orchestration, StrategyAgent, StrategyFuture, save_conversation_state,
};
use crate::store::ConversationStore;
use crate::turn_runners::{Taken, TurnRunner};

/// What [`Loop::new`] takes beside the producer: Python's `Loop(agent,
/// reviewers=None, reviewer=None, max_iterations=3, *, strategy=None)`.
#[derive(Clone, Debug)]
pub struct LoopOptions {
    /// The reviewing agents, in the order they review.
    pub reviewers: Vec<StrategyAgent>,
    /// A single reviewer, the same as `reviewers` naming it alone.
    pub reviewer: Option<StrategyAgent>,
    /// The most produce-and-review cycles: 3 by default.
    pub max_iterations: u32,
    /// How several reviewers review: [`WorkerStrategy::Sequential`], each
    /// handed the feedback of those before it, or
    /// [`WorkerStrategy::Parallel`], all at once on the same output;
    /// sequential when `None`.
    pub strategy: Option<WorkerStrategy>,
}

impl Default for LoopOptions {
    fn default() -> Self {
        Self {
            reviewers: Vec::new(),
            reviewer: None,
            max_iterations: 3,
            strategy: None,
        }
    }
}

/// The loop strategy (RFC §19.7.4): Python's `roomkit.Loop`. The producer
/// answers the room: each message's text is its task, delegated to it in a
/// child room (RFC §23.3), then each reviewer reviews the output in a child
/// room of its own. All approving ends the loop; otherwise the feedback goes
/// back to the producer for a revision, up to `max_iterations` outputs. The
/// last output answers the message, saying whether it was approved, at which
/// iteration and how the loop stopped.
///
/// Its agents are each given live or named by the id of an agent the kit
/// holds. The install registers the reviewers on the kit without attaching
/// them to the room, takes the producer's turns in the room, and writes the
/// room's conversation state; elsewhere the producer answers as itself.
///
/// ```
/// use std::sync::Arc;
///
/// use roomkit::model::{ChannelId, RoomId};
/// use roomkit::orchestration::LoopOptions;
/// use roomkit::{Loop, RoomKit};
///
/// # async fn install(kit: &RoomKit) -> Result<(), roomkit::RoomKitError> {
/// let review = Loop::new(
///     ChannelId::new("writer")?,
///     LoopOptions {
///         reviewer: Some(ChannelId::new("editor")?.into()),
///         max_iterations: 2,
///         ..LoopOptions::default()
///     },
/// )?;
/// kit.install_strategy(&RoomId::new("r1")?, Arc::new(review), Default::default())
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct Loop {
    /// The producer, then the reviewers, as given.
    named: Vec<StrategyAgent>,
    max_iterations: u32,
    strategy: Option<WorkerStrategy>,
}

impl Loop {
    /// A loop of `agent`, the producer, and the reviewers `options` names.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::Value`], Python's `ValueError`, when `options` names
    /// both `reviewers` and `reviewer`, or no reviewer.
    pub fn new(
        agent: impl Into<StrategyAgent>,
        options: LoopOptions,
    ) -> Result<Self, RoomKitError> {
        let LoopOptions {
            reviewers,
            reviewer,
            max_iterations,
            strategy,
        } = options;
        let named = match (reviewer, reviewers.is_empty()) {
            (Some(_), false) => {
                return Err(RoomKitError::Value(
                    "Provide either 'reviewers' or 'reviewer', not both".to_owned(),
                ));
            }
            (Some(reviewer), true) => vec![reviewer],
            (None, false) => reviewers,
            (None, true) => {
                return Err(RoomKitError::Value(
                    "At least one reviewer is required".to_owned(),
                ));
            }
        };
        Ok(Self {
            named: std::iter::once(agent.into()).chain(named).collect(),
            max_iterations,
            strategy,
        })
    }

    /// The reviewers, as given.
    fn reviewers(&self) -> &[StrategyAgent] {
        self.named.get(1..).unwrap_or_default()
    }

    /// Wires the framework-driven loop in `room_id`: the reviewers
    /// registered, the producer's turns taken, the initial state saved.
    async fn install_in<S: ConversationStore>(
        &self,
        kit: &RoomKit<S>,
        room_id: &RoomId,
    ) -> Result<(), RoomKitError> {
        let owner = strategy_address::<S>(self);
        kit.claim_for(room_id, owner)?;
        let mut agents = self
            .named
            .iter()
            .map(|agent| held(kit, agent))
            .collect::<Result<Vec<HeldAgent>, _>>()?;
        let producer = agents.remove(0);
        // The reviewers are the kit's, never attached to the room.
        kit.register_strategy_agents(self.reviewers())?;
        let run = LoopRun {
            producer: producer.clone(),
            reviewers: agents,
            strategy: self.strategy,
            max_iterations: self.max_iterations,
        };
        // Kept by the room's owner, never on the shared producer (RFC
        // §10.6.1): the producer answers as itself in every other room.
        let turns: Arc<dyn TurnRunner<S>> = Arc::new(LoopTurns { run: Arc::new(run) });
        kit.inner
            .turn_runners
            .take(room_id, &producer.channel_id, turns, owner)?;
        let mut state = ConversationState::new();
        state.phase = producer.channel_id.to_string();
        state.active_agent_id = Some(producer.channel_id);
        state.context = serde_json::from_value(json!({
            "_loop_iteration": 0,
            "_loop_approved": false,
            "_loop_max_iterations": self.max_iterations,
        }))
        .unwrap_or_default();
        save_conversation_state(kit.store(), room_id, &state).await
    }
}

impl<S: ConversationStore> Orchestration<S> for Loop {
    /// The producer: it presents the results to the room.
    fn agents(&self) -> Vec<StrategyAgent> {
        self.named.first().cloned().into_iter().collect()
    }

    fn install<'call>(
        &'call self,
        kit: &'call RoomKit<S>,
        room_id: &'call RoomId,
    ) -> StrategyFuture<'call> {
        Box::pin(self.install_in(kit, room_id))
    }
}

/// One loop install's turns: the producer produces, the reviewers review.
struct LoopTurns {
    run: Arc<LoopRun>,
}

impl<S: ConversationStore> TurnRunner<S> for LoopTurns {
    fn take(
        &self,
        kit: RoomKit<S>,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> Taken {
        let producer = &self.run.producer.channel_id;
        if &event.source.channel_id == producer {
            return Taken::Answered(Box::pin(async { Ok(ChannelOutput::empty()) }));
        }
        // An agent's or the system's event, or an edit or a delete, which
        // is no task (RFC §10.3), or a message the loop's tasks cannot
        // carry: the producer's own channel reads it (RFC §19.7.4).
        let source = &event.source.channel_type;
        let from_agents = *source == ChannelType::AI || *source == ChannelType::SYSTEM;
        let changes = !matches!(mutation(&event), Ok(None));
        if from_agents || changes || task_input(&event).is_empty() {
            return Taken::Own(Box::new((event, binding, context)));
        }
        let run = Arc::clone(&self.run);
        let room_id = context.room.id;
        Taken::Answered(Box::pin(async move {
            run_loop(&kit, &room_id, &run, &event)
                .await
                .map_err(Into::into)
        }))
    }
}

/// The reviewers given, as a test reads them.
#[cfg(test)]
pub(super) fn reviewers_of(review: &Loop) -> Vec<String> {
    review
        .reviewers()
        .iter()
        .map(|agent| agent.channel_id().to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use roomkit_core::model::{
        ChannelBinding, ChannelId, ChannelType, EventContent, EventSource, Room, RoomContext,
        RoomEvent, RoomId, TextContent,
    };

    use super::{Loop, LoopOptions, reviewers_of};
    use crate::error::RoomKitError;
    use crate::kit::{RoomKit, RoomKitOptions};
    use crate::orchestration::Orchestration;
    use crate::orchestration::strategies::supervisor::WorkerStrategy;
    use crate::store::InMemoryStore;
    use crate::turn_runners::{Taken, TurnRunner};

    fn id(name: &str) -> ChannelId {
        ChannelId::new(name).unwrap_or_else(|error| panic!("{error}"))
    }

    fn named(reviewers: &[&str], reviewer: Option<&str>) -> LoopOptions {
        LoopOptions {
            reviewers: reviewers.iter().map(|name| id(name).into()).collect(),
            reviewer: reviewer.map(|name| id(name).into()),
            ..LoopOptions::default()
        }
    }

    // Python: tests/orchestration/test_loop_strategy.py::TestLoopConstructor::test_both_reviewer_and_reviewers_raises
    #[test]
    fn both_reviewer_and_reviewers_are_refused() {
        let refused = Loop::new(id("w"), named(&["r1"], Some("r2")));
        assert!(
            matches!(refused, Err(RoomKitError::Value(message)) if message.contains("not both"))
        );
    }

    // Python: tests/orchestration/test_loop_strategy.py::TestLoopConstructor::test_no_reviewer_raises
    #[test]
    fn no_reviewer_is_refused() {
        let refused = Loop::new(id("w"), LoopOptions::default());
        assert!(
            matches!(refused, Err(RoomKitError::Value(message)) if message.contains("At least one reviewer"))
        );
    }

    // Python: tests/orchestration/test_loop_strategy.py::TestLoopConstructor::test_single_reviewer_convenience
    // Python: tests/orchestration/test_loop_strategy.py::TestLoopConstructor::test_multiple_reviewers
    #[test]
    fn a_single_reviewer_is_the_list_of_it_alone() -> Result<(), RoomKitError> {
        assert_eq!(
            reviewers_of(&Loop::new(id("w"), named(&[], Some("r")))?),
            ["r"]
        );
        assert_eq!(
            reviewers_of(&Loop::new(id("w"), named(&["r1", "r2"], None))?),
            ["r1", "r2"]
        );
        Ok(())
    }

    // Python: tests/orchestration/test_loop_strategy.py::TestLoopConstructor::test_strategy_parsed
    #[test]
    fn the_strategy_is_kept() -> Result<(), RoomKitError> {
        let options = LoopOptions {
            strategy: Some(WorkerStrategy::Parallel),
            ..named(&[], Some("r"))
        };
        assert_eq!(
            Loop::new(id("w"), options)?.strategy,
            Some(WorkerStrategy::Parallel)
        );
        Ok(())
    }

    fn turns() -> super::LoopTurns {
        let producer = crate::orchestration::agents::tests::held_agent("writer");
        super::LoopTurns {
            run: std::sync::Arc::new(super::LoopRun {
                producer,
                reviewers: Vec::new(),
                strategy: None,
                max_iterations: 3,
            }),
        }
    }

    /// What the loop does with a message of `content` from `source`, of
    /// type `channel_type`, in room r1: its own answer, or the producer's
    /// own turn.
    fn taken(source: &str, channel_type: ChannelType, content: impl Into<EventContent>) -> Taken {
        let room = RoomId::new("r1").unwrap_or_else(|error| panic!("{error}"));
        let event = RoomEvent::new(
            room.clone(),
            EventSource::new(id(source), channel_type),
            content,
        );
        let binding = ChannelBinding::new(id("writer"), room.clone(), ChannelType::AI);
        let in_room = RoomContext::new(Room::new(room));
        let kit = RoomKit::new(RoomKitOptions::default());
        TurnRunner::<InMemoryStore>::take(&turns(), kit, event, binding, in_room)
    }

    // Python: tests/orchestration/test_loop_strategy.py::TestWrappedOnEvent::test_event_from_producer_returns_empty
    #[tokio::test]
    async fn the_producer_s_own_event_answers_nothing() -> Result<(), crate::channel::ChannelError>
    {
        let Taken::Answered(answer) = taken("writer", ChannelType::AI, TextContent::new("Mine."))
        else {
            panic!("the loop took the producer's own event to its turn");
        };
        assert!(!answer.await?.responded);
        Ok(())
    }

    // Python: tests/orchestration/test_loop_strategy.py::TestWrappedOnEvent::test_ai_event_goes_to_original
    // Python: tests/orchestration/test_loop_strategy.py::TestWrappedOnEvent::test_system_event_goes_to_original
    #[test]
    fn an_agent_s_or_the_system_s_event_is_the_producer_s_own_turn() {
        for (source, channel_type) in [
            ("other-ai", ChannelType::AI),
            ("system", ChannelType::SYSTEM),
        ] {
            let taken = taken(source, channel_type, TextContent::new("Hello"));
            assert!(matches!(taken, Taken::Own(..)), "{source}");
        }
        assert!(matches!(
            taken("sms", ChannelType::SMS, TextContent::new("")),
            Taken::Own(..)
        ));
        assert!(matches!(
            taken("sms", ChannelType::SMS, TextContent::new("Write it.")),
            Taken::Answered(_)
        ));
    }

    // Python: tests/orchestration/test_loop_strategy.py::TestLoopConstructor::test_agents_returns_producer
    // Python: tests/orchestration/test_strategy_loop.py::TestLoopAgents::test_agents_returns_only_producer
    #[test]
    fn its_agents_are_the_producer_alone() -> Result<(), RoomKitError> {
        let review = Loop::new(id("writer"), named(&["editor"], None))?;
        let agents = Orchestration::<InMemoryStore>::agents(&review);
        let ids: Vec<&str> = agents
            .iter()
            .map(|agent| agent.channel_id().as_str())
            .collect();
        assert_eq!(ids, ["writer"]);
        Ok(())
    }
}

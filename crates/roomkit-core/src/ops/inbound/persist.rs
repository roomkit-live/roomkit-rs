//! Step 15 of the inbound pipeline (RFC §10.1 step 15): once the cascade's
//! passes and sets are over, the tasks and observations every pass and set
//! collected are stored, the room metadata updates the channels returned
//! are applied (gap G-049), and each stored task's `ON_TASK_CREATED`
//! observers run. Then the operation ends.

use super::{Machine, Stage, report};
use std::collections::HashMap;

use crate::model::{Metadata, Observation, RoomContext, RoomEvent, RoomUpdates, Task, TaskId};
use crate::ops::Cx;
use crate::ops::protocol::{EffectId, Outcome, StoreRequest};

/// What the call's passes and sets collected to store at step 15: tasks,
/// observations and room metadata updates, and the room as the last pass or
/// set that filed tasks left it, which their `ON_TASK_CREATED` observers
/// read.
#[derive(Debug, Default)]
pub(super) struct SideEffects {
    tasks: Vec<Task>,
    observations: Vec<Observation>,
    /// The room metadata updates the channels returned (RFC §6.1).
    metadata_updates: Metadata,
    task_context: Option<Box<RoomContext>>,
    /// The event each task was made for, by the task: the one its
    /// `ON_TASK_CREATED` event carries the source and content of.
    made_for: HashMap<TaskId, RoomEvent>,
}

impl SideEffects {
    /// Keeps the tasks and observations a pass or a set collected for
    /// `made_for`, the event its step handled; the room as `context` holds
    /// it is the one their tasks' observers read, when it filed tasks and
    /// left a room.
    pub(super) fn collect(
        &mut self,
        tasks: Vec<Task>,
        observations: Vec<Observation>,
        context: Option<&RoomContext>,
        made_for: Option<&RoomEvent>,
    ) {
        if !tasks.is_empty()
            && let Some(context) = context
        {
            self.task_context = Some(Box::new(context.clone()));
        }
        if let Some(event) = made_for {
            for task in &tasks {
                self.made_for.insert(task.id.clone(), event.clone());
            }
        }
        self.tasks.extend(tasks);
        self.observations.extend(observations);
    }

    /// Keeps an observation the call makes itself.
    pub(super) fn observe(&mut self, observation: Observation) {
        self.observations.push(observation);
    }

    /// Keeps the room metadata updates a set's channels returned.
    pub(super) fn update_metadata(&mut self, updates: Metadata) {
        self.metadata_updates.extend(updates);
    }

    /// The tasks and observations to store, none once taken.
    fn take_stored(&mut self) -> Option<(Vec<Task>, Vec<Observation>)> {
        if self.tasks.is_empty() && self.observations.is_empty() {
            return None;
        }
        Some((
            std::mem::take(&mut self.tasks),
            std::mem::take(&mut self.observations),
        ))
    }

    /// The room metadata updates to write, none once taken.
    fn take_metadata(&mut self) -> Option<Metadata> {
        (!self.metadata_updates.is_empty()).then(|| std::mem::take(&mut self.metadata_updates))
    }

    /// The room the observers of the stored tasks read.
    fn task_context(&self) -> Option<&RoomContext> {
        self.task_context.as_deref()
    }
}

impl Machine {
    /// Stores the side effects every pass and set collected, and applies the
    /// room metadata updates the channels returned (RFC §10.1 step 15, gap
    /// G-049), then ends. The update names its keys only, so it needs no
    /// lock: it never overwrites the counters of an event committed
    /// meanwhile.
    pub(super) fn persist_side_effects(&mut self, cx: &mut Cx<'_>) {
        let side_effects = self.issue_side_effects(cx);
        let metadata = self.issue_metadata_updates(cx);
        self.await_persisted(cx, side_effects, metadata);
    }

    /// Asks for the collected tasks and observations to be stored, if any.
    fn issue_side_effects(&mut self, cx: &mut Cx<'_>) -> Option<(EffectId, Vec<Task>)> {
        let room_id = self.call.room_id.clone()?;
        let (tasks, observations) = self.side_effects.take_stored()?;
        let effect = cx.io.issue(StoreRequest::PersistSideEffects {
            room_id,
            tasks: tasks.clone(),
            observations,
        });
        Some((effect, tasks))
    }

    /// Asks for the collected room metadata updates to be written, if any.
    fn issue_metadata_updates(&mut self, cx: &mut Cx<'_>) -> Option<EffectId> {
        let room_id = self.call.room_id.clone()?;
        let metadata = self.side_effects.take_metadata()?;
        Some(cx.io.issue(StoreRequest::UpdateRoom {
            room_id,
            updates: RoomUpdates {
                updated_at: Some(cx.env.now()),
                metadata,
                ..RoomUpdates::default()
            },
        }))
    }

    /// Takes the answer of one of step 15's writes.
    pub(super) fn on_persisting(
        &mut self,
        cx: &mut Cx<'_>,
        side_effects: Option<(EffectId, Vec<Task>)>,
        metadata: Option<EffectId>,
        effect: EffectId,
        outcome: Outcome,
    ) {
        match (side_effects, metadata) {
            (Some((expected, tasks)), metadata) if expected == effect => {
                self.on_persisted(cx, outcome, &tasks);
                self.await_persisted(cx, None, metadata);
            }
            (side_effects, Some(expected)) if expected == effect => {
                if let Err(error) = outcome.into_stored() {
                    self.trigger.note_error(Some(error));
                }
                self.await_persisted(cx, side_effects, None);
            }
            (side_effects, metadata) => {
                debug_assert!(false, "inbound received {effect:?} while persisting");
                self.stage.put_back(Stage::Persisting {
                    side_effects,
                    metadata,
                });
            }
        }
    }

    /// Waits for step 15's outstanding writes, or ends once none is left.
    fn await_persisted(
        &mut self,
        cx: &mut Cx<'_>,
        side_effects: Option<(EffectId, Vec<Task>)>,
        metadata: Option<EffectId>,
    ) {
        if side_effects.is_none() && metadata.is_none() {
            let result = std::mem::take(&mut self.trigger.result);
            return self.end(cx, Ok(result));
        }
        self.stage.enter(Stage::Persisting {
            side_effects,
            metadata,
        });
    }

    /// The side effects are stored: each task's `ON_TASK_CREATED` observers
    /// run (RFC §10.1 step 15).
    fn on_persisted(&mut self, cx: &mut Cx<'_>, outcome: Outcome, tasks: &[Task]) {
        match outcome.into_persisted() {
            Ok(()) => {
                if let Some(context) = self.side_effects.task_context() {
                    report::tasks_created(cx, tasks, &self.side_effects.made_for, context);
                }
            }
            Err(error) => self.trigger.note_error(Some(error)),
        }
    }
}

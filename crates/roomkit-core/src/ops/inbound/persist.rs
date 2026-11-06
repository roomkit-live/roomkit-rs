//! Step 15 of the inbound pipeline (RFC §10.1 step 15). A delivery set,
//! once over, stores what it and its event's pass collected: the tasks and
//! observations its event's `BEFORE_BROADCAST` hooks gave and its targets
//! returned, and the room metadata updates its targets returned (gap
//! G-049), before its event's `AFTER_BROADCAST` hooks run (step 16): those
//! hooks read them stored, as Python's `_post_plan_effects` writes them
//! before it starts them. Each stored task's `ON_TASK_CREATED` observers
//! run once it is. What no set stores, a record's side effects, an event's
//! whose set delivered nothing, the call's own observations, is stored once
//! the cascade's passes and sets are over; then the operation ends.

use super::{Machine, Stage, report};
use std::collections::HashMap;

use crate::model::{
    ErrorRecord, Metadata, Observation, RoomContext, RoomEvent, RoomId, RoomUpdates, Task, TaskId,
};
use crate::ops::protocol::{EffectId, Outcome, StoreRequest};
use crate::ops::{Cx, Progress};

/// What the call's passes and sets left for the end of its cascade: the
/// tasks and observations no set stored, and the room as the last pass or
/// set that filed tasks left it, which their `ON_TASK_CREATED` observers
/// read.
#[derive(Debug, Default)]
pub(super) struct SideEffects {
    tasks: Vec<Task>,
    observations: Vec<Observation>,
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

    /// The room the observers of the stored tasks read.
    fn task_context(&self) -> Option<&RoomContext> {
        self.task_context.as_deref()
    }
}

/// What one delivery set stores once it is over, before its event's
/// `AFTER_BROADCAST` hooks run (RFC §10.1 steps 15 and 16).
#[derive(Debug, Default)]
pub(super) struct SetEffects {
    /// The tasks and observations its event's `BEFORE_BROADCAST` hooks
    /// gave, which its pass handed the set.
    hooks: (Vec<Task>, Vec<Observation>),
    /// The write of its tasks and observations under way, with its tasks.
    stored: Option<(EffectId, Vec<Task>)>,
    /// The write of its room metadata updates under way.
    updated: Option<EffectId>,
    /// What the writes leave the set's outcome: the tasks stored, whose
    /// `ON_TASK_CREATED` observers run, and the first write's failure.
    written: Written,
}

/// What a set's writes left: the tasks stored, and the first failure.
#[derive(Clone, Debug, Default, PartialEq)]
pub(super) struct Written {
    /// The tasks stored, whose `ON_TASK_CREATED` observers run.
    pub(super) tasks: Vec<Task>,
    /// The first write that failed, the call's error.
    pub(super) error: Option<ErrorRecord>,
}

impl SetEffects {
    /// The effects of a set whose event's hooks gave `tasks` and
    /// `observations`.
    pub(super) fn of_hooks(tasks: Vec<Task>, observations: Vec<Observation>) -> Self {
        Self {
            hooks: (tasks, observations),
            ..Self::default()
        }
    }

    /// What the event's hooks gave, taken: a set that delivered nothing
    /// stores none of it, and hands it to the end of the cascade.
    pub(super) fn take_hooks(&mut self) -> (Vec<Task>, Vec<Observation>) {
        std::mem::take(&mut self.hooks)
    }

    /// Asks for the hooks' tasks and observations then `tasks` and
    /// `observations`, the set's targets', to be stored in `room_id`, and
    /// `metadata` to be written on it, those there are; done at once with
    /// none (Python's `_persist_side_effects`).
    pub(super) fn write(
        &mut self,
        cx: &mut Cx<'_>,
        room_id: &RoomId,
        (tasks, observations): (Vec<Task>, Vec<Observation>),
        metadata: Metadata,
    ) -> Progress<Written> {
        let (mut all_tasks, mut all_observations) = self.take_hooks();
        all_tasks.extend(tasks);
        all_observations.extend(observations);
        if !all_tasks.is_empty() || !all_observations.is_empty() {
            let effect = cx.io.issue(StoreRequest::PersistSideEffects {
                room_id: room_id.clone(),
                tasks: all_tasks.clone(),
                observations: all_observations,
            });
            self.stored = Some((effect, all_tasks));
        }
        if !metadata.is_empty() {
            // Its keys only: it never overwrites the counters of an event
            // committed meanwhile, so it needs no lock.
            self.updated = Some(cx.io.issue(StoreRequest::UpdateRoom {
                room_id: room_id.clone(),
                updates: RoomUpdates {
                    updated_at: Some(cx.env.now()),
                    metadata,
                    ..RoomUpdates::default()
                },
            }));
        }
        self.progress()
    }

    /// Whether one of the writes waits for `effect`.
    pub(super) fn awaits(&self, effect: EffectId) -> bool {
        self.stored
            .as_ref()
            .is_some_and(|(stored, _)| *stored == effect)
            || self.updated == Some(effect)
    }

    /// Whether a write is under way.
    pub(super) fn writing(&self) -> bool {
        self.stored.is_some() || self.updated.is_some()
    }

    /// Takes the answer of one of the writes.
    pub(super) fn accept(&mut self, effect: EffectId, outcome: Outcome) -> Progress<Written> {
        if let Some((_, tasks)) = self.stored.take_if(|(stored, _)| *stored == effect) {
            match outcome.into_persisted() {
                Ok(()) => self.written.tasks = tasks,
                Err(error) => {
                    self.written.error.get_or_insert(error);
                }
            }
        } else if self.updated.take_if(|updated| *updated == effect).is_some()
            && let Err(error) = outcome.into_stored()
        {
            self.written.error.get_or_insert(error);
        }
        self.progress()
    }

    /// Done once no write is under way.
    fn progress(&mut self) -> Progress<Written> {
        if self.writing() {
            return Progress::Waiting;
        }
        Progress::Done(std::mem::take(&mut self.written))
    }
}

impl Machine {
    /// Stores what no set stored once the cascade's passes and sets are
    /// over (RFC §10.1 step 15), then ends.
    pub(super) fn persist_side_effects(&mut self, cx: &mut Cx<'_>) {
        let side_effects = self.issue_side_effects(cx);
        self.await_persisted(cx, side_effects);
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

    /// Takes the answer of step 15's write.
    pub(super) fn on_persisting(
        &mut self,
        cx: &mut Cx<'_>,
        side_effects: (EffectId, Vec<Task>),
        effect: EffectId,
        outcome: Outcome,
    ) {
        let (expected, tasks) = side_effects;
        if expected != effect {
            debug_assert!(false, "inbound received {effect:?} while persisting");
            self.stage.put_back(Stage::Persisting {
                side_effects: (expected, tasks),
            });
            return;
        }
        self.on_persisted(cx, outcome, &tasks);
        self.await_persisted(cx, None);
    }

    /// Waits for step 15's write, or ends once it is answered.
    fn await_persisted(&mut self, cx: &mut Cx<'_>, side_effects: Option<(EffectId, Vec<Task>)>) {
        let Some(side_effects) = side_effects else {
            let result = std::mem::take(&mut self.trigger.result);
            return self.end(cx, Ok(result));
        };
        self.stage.enter(Stage::Persisting { side_effects });
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

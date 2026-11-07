//! Step 15 of the inbound pipeline (RFC §10.1 step 15). A delivery set,
//! once over, stores what it and its event's pass collected: the tasks and
//! observations its event's `BEFORE_BROADCAST` hooks gave and its targets
//! returned, then the room metadata updates its targets returned (gap
//! G-049), under the room's lock (`metadata_write`), as Python's
//! `_persist_side_effects` writes them in that order, before its event's
//! `AFTER_BROADCAST` hooks run (step 16): those hooks read them stored, as
//! Python's `_post_plan_effects` writes them before it starts them. Each
//! stored task's `ON_TASK_CREATED` observers run once it is. What no set
//! stores, a record's side effects, an event's whose set delivered nothing,
//! the call's own observations, is stored once the cascade's passes and
//! sets are over; then the operation ends.
//!
//! A write the store fails is logged, at ERROR (RFC §15.2), and the call
//! goes on, a failed write never its error: each task and observation is
//! written on its own, those the store fails leaving the others written,
//! and only a task stored fires `ON_TASK_CREATED`, as Python's
//! `_persist_side_effects` and `_write_metadata_updates` do.

use super::metadata_write::MetadataWrite;
use super::{Machine, Stage, report};
use std::collections::HashMap;

use crate::model::{
    ErrorRecord, Metadata, Observation, RoomContext, RoomEvent, RoomId, Task, TaskId,
};
use crate::ops::protocol::{EffectId, KIT_CLOSED, Outcome, StoreRequest, Unpersisted};
use crate::ops::{Cx, Progress};

/// Of `tasks`, those a write of side effects answered by `answer` stored,
/// whose `ON_TASK_CREATED` observers run. Each task and observation it did
/// not write is logged at ERROR, as Python's `_persist_side_effects` logs
/// it. A write the driver refused whole, cut by the kit's close before it
/// started, is announced as storing none, and logged once, at DEBUG when
/// the close cut it.
pub(crate) fn stored_tasks(
    room_id: &RoomId,
    mut tasks: Vec<Task>,
    answer: Result<Vec<Unpersisted>, ErrorRecord>,
) -> Vec<Task> {
    let unpersisted = match answer {
        Ok(unpersisted) => unpersisted,
        Err(error) => {
            refused_whole(room_id, &error);
            return Vec::new();
        }
    };
    for failure in unpersisted {
        match failure {
            Unpersisted::Task { task_id, error } => {
                tracing::error!(
                    target: "roomkit::framework",
                    room_id = %room_id,
                    task_id = %task_id,
                    error_type = %error.error_type,
                    error = %error.message,
                    "Failed to persist task {task_id}"
                );
                if let Some(position) = tasks.iter().position(|task| task.id == task_id) {
                    tasks.remove(position);
                }
            }
            Unpersisted::Observation {
                observation_id,
                error,
            } => tracing::error!(
                target: "roomkit::framework",
                room_id = %room_id,
                observation_id = %observation_id,
                error_type = %error.error_type,
                error = %error.message,
                "Failed to persist observation {observation_id}"
            ),
        }
    }
    tasks
}

/// Logs a write of side effects the driver refused whole: expected once
/// the kit's close cut it.
fn refused_whole(room_id: &RoomId, error: &ErrorRecord) {
    if error.error_type == KIT_CLOSED {
        tracing::debug!(
            target: "roomkit::framework",
            room_id = %room_id,
            "the kit closed before the side effects were stored"
        );
        return;
    }
    tracing::error!(
        target: "roomkit::framework",
        room_id = %room_id,
        error_type = %error.error_type,
        error = %error.message,
        "Failed to persist the side effects of room {room_id}"
    );
}

/// The store's error for a room it does not hold, which Python's
/// `patch_room_metadata` answers with `None`, raising nothing.
const ROOM_NOT_FOUND: &str = "RoomNotFound";

/// Logs that the room metadata updates under `keys` were not written, as
/// Python's `_write_metadata_updates` does, the keys sorted: the delivery
/// set stands. A room gone meanwhile is no failure, Python's write of it
/// returning `None`, nor a write the kit's close cut before it started,
/// which Python's `except Exception` does not catch: both at DEBUG.
pub(crate) fn metadata_not_written(room_id: &RoomId, keys: &[String], error: &ErrorRecord) {
    if error.error_type == ROOM_NOT_FOUND || error.error_type == KIT_CLOSED {
        tracing::debug!(
            target: "roomkit::framework",
            room_id = %room_id,
            error_type = %error.error_type,
            "the room metadata updates were not written: the room is gone or the kit closed"
        );
        return;
    }
    let mut keys: Vec<&String> = keys.iter().collect();
    keys.sort();
    let keys = keys
        .iter()
        .map(|key| format!("'{key}'"))
        .collect::<Vec<_>>()
        .join(", ");
    tracing::error!(
        target: "roomkit::framework",
        room_id = %room_id,
        error_type = %error.error_type,
        error = %error.message,
        "Failed to write the room metadata updates [{keys}]"
    );
}

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
    /// Its room metadata updates, written once its tasks and observations
    /// are.
    metadata: Metadata,
    /// The write of its room metadata updates under way, under the room's
    /// lock.
    updated: Option<MetadataWrite>,
    /// What the writes leave the set's outcome: the tasks stored, whose
    /// `ON_TASK_CREATED` observers run.
    written: Written,
}

/// What a set's writes left: the tasks stored. A write that failed is
/// logged, never the call's error.
#[derive(Clone, Debug, Default, PartialEq)]
pub(super) struct Written {
    /// The tasks stored, whose `ON_TASK_CREATED` observers run.
    pub(super) tasks: Vec<Task>,
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
    /// `observations`, the set's targets', to be stored in `room_id`, then
    /// `metadata` to be written on it under the room's lock, those there
    /// are; done at once with none (Python's `_persist_side_effects`).
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
        self.metadata = metadata;
        if !all_tasks.is_empty() || !all_observations.is_empty() {
            let effect = cx.io.issue(StoreRequest::PersistSideEffects {
                room_id: room_id.clone(),
                tasks: all_tasks.clone(),
                observations: all_observations,
            });
            self.stored = Some((effect, all_tasks));
            return Progress::Waiting;
        }
        self.write_metadata(cx, room_id)
    }

    /// Writes the set's room metadata updates, if any, under the room's
    /// lock, as Python's `_write_metadata_updates` does once the tasks and
    /// observations are written.
    fn write_metadata(&mut self, cx: &mut Cx<'_>, room_id: &RoomId) -> Progress<Written> {
        if !self.metadata.is_empty() {
            let metadata = std::mem::take(&mut self.metadata);
            self.updated = Some(MetadataWrite::start(cx, room_id, metadata));
        }
        self.progress()
    }

    /// Whether one of the writes waits for `effect`.
    pub(super) fn awaits(&self, effect: EffectId) -> bool {
        self.stored
            .as_ref()
            .is_some_and(|(stored, _)| *stored == effect)
            || self
                .updated
                .as_ref()
                .is_some_and(|updated| updated.awaits(effect))
    }

    /// Whether a write is under way.
    pub(super) fn writing(&self) -> bool {
        self.stored.is_some() || self.updated.is_some()
    }

    /// Takes the answer of one of the writes, made in `room_id`; one that
    /// failed is logged. The tasks and observations written, the metadata
    /// updates are.
    pub(super) fn accept(
        &mut self,
        cx: &mut Cx<'_>,
        room_id: &RoomId,
        effect: EffectId,
        outcome: Outcome,
    ) -> Progress<Written> {
        if let Some((_, tasks)) = self.stored.take_if(|(stored, _)| *stored == effect) {
            self.written.tasks = stored_tasks(room_id, tasks, outcome.into_persisted());
            return self.write_metadata(cx, room_id);
        }
        if let Some(updated) = self.updated.as_mut()
            && updated.awaits(effect)
            && matches!(updated.accept(cx, outcome), Progress::Done(()))
        {
            self.updated = None;
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
        self.on_persisted(cx, outcome, tasks);
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

    /// The side effects are stored, but those the store failed, logged:
    /// each stored task's `ON_TASK_CREATED` observers run (RFC §10.1 step
    /// 15).
    fn on_persisted(&mut self, cx: &mut Cx<'_>, outcome: Outcome, tasks: Vec<Task>) {
        let Some(room_id) = self.call.room_id.as_ref() else {
            return;
        };
        let tasks = stored_tasks(room_id, tasks, outcome.into_persisted());
        if let Some(context) = self.side_effects.task_context() {
            report::tasks_created(cx, &tasks, &self.side_effects.made_for, context);
        }
    }
}

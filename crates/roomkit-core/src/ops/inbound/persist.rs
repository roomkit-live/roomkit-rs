//! Step 15 of the inbound pipeline (RFC §10.1 step 15). A delivery set,
//! once over, stores what it and its event's pass collected: the tasks and
//! observations its event's `BEFORE_BROADCAST` hooks gave and its targets
//! returned, then the room metadata updates its targets returned (gap
//! G-049), under the room's lock (`metadata_write`), as Python's
//! `_persist_side_effects` writes them in that order, before its event's
//! `AFTER_BROADCAST` hooks run (step 16): those hooks read them stored, as
//! Python's `_post_plan_effects` writes them before it starts them. Each
//! stored task's `ON_TASK_CREATED` observers run once it is. A set the
//! room's lane dropped before it ran stores what its event's hooks gave at
//! its drop (Python's `_store_dropped_side_effects`). A set reads its room
//! first, and writes none of it once the room it read is gone, deleted or
//! created again under its id (RFC §13.5), the log saying why (Python's
//! `_side_effects_room_gone`). An event with no set writes its hooks' side
//! effects at its decision, under the lock (`pass::hook_effects`): a record
//! stored blocked, a blocked instruction, and an instruction a discussion
//! took; and a discussion's stopped turn writes its observation with its
//! record.
//!
//! A write the store fails is logged, at ERROR (RFC §15.2), and the call
//! goes on, a failed write never its error: each task and observation is
//! written on its own, those the store fails leaving the others written,
//! and only a task stored fires `ON_TASK_CREATED`, as Python's
//! `_persist_side_effects` and `_write_metadata_updates` do.

use super::metadata_write::MetadataWrite;

use crate::model::{ErrorRecord, Metadata, Observation, RoomId, Task, Timestamp};
use crate::ops::protocol::{EffectId, KIT_CLOSED, Outcome, StoreRequest, Unpersisted};
use crate::ops::{Cx, Progress, RecentEvents};

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
    let keys = sorted_keys(keys.iter());
    tracing::error!(
        target: "roomkit::framework",
        room_id = %room_id,
        error_type = %error.error_type,
        error = %error.message,
        "Failed to write the room metadata updates {keys}"
    );
}

/// `keys`, sorted, as Python prints a sorted list of them.
fn sorted_keys<'a>(keys: impl Iterator<Item = &'a String>) -> String {
    let mut keys: Vec<&String> = keys.collect();
    keys.sort();
    let keys = keys
        .iter()
        .map(|key| format!("'{key}'"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{keys}]")
}

/// Logs that a set's side effects are not written, its room gone, deleted
/// or created again under its id, as Python's `_side_effects_room_gone`
/// logs it (RFC §13.5).
fn room_gone(room_id: &RoomId, checking: &Checking, metadata: &Metadata) {
    tracing::info!(
        target: "roomkit::framework",
        room_id = %room_id,
        "Room {room_id} is gone, deleted or created again: its delivery set's {} task(s), \
         {} observation(s) and metadata updates {} are not written",
        checking.tasks.len(),
        checking.observations.len(),
        sorted_keys(metadata.keys())
    );
}

/// Logs a read of a set's room, before its side effects, that failed: at
/// ERROR, as Python's `_side_effects_room_gone` logs it, but at DEBUG once
/// the kit's close cut it.
fn room_unread(room_id: &RoomId, error: &ErrorRecord) {
    if error.error_type == KIT_CLOSED {
        tracing::debug!(
            target: "roomkit::framework",
            room_id = %room_id,
            "the kit closed before the room was read for its side effects"
        );
        return;
    }
    tracing::error!(
        target: "roomkit::framework",
        room_id = %room_id,
        error_type = %error.error_type,
        error = %error.message,
        "Could not read room {room_id} before writing its side effects"
    );
}

/// What one delivery set stores once it is over, before its event's
/// `AFTER_BROADCAST` hooks run (RFC §10.1 steps 15 and 16).
#[derive(Debug, Default)]
pub(super) struct SetEffects {
    /// The tasks and observations its event's `BEFORE_BROADCAST` hooks
    /// gave, which its pass handed the set.
    hooks: (Vec<Task>, Vec<Observation>),
    /// The read of its room under way, before any write.
    checking: Option<Checking>,
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

/// The read of a set's room before its side effects are written, with the
/// tasks and observations they hold: the room the set read is the one
/// created at `created_at` (RFC §13.5).
#[derive(Debug)]
struct Checking {
    effect: EffectId,
    created_at: Timestamp,
    tasks: Vec<Task>,
    observations: Vec<Observation>,
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

    /// What the event's hooks gave, taken.
    pub(super) fn take_hooks(&mut self) -> (Vec<Task>, Vec<Observation>) {
        std::mem::take(&mut self.hooks)
    }

    /// Asks for the hooks' tasks and observations then `tasks` and
    /// `observations`, the set's targets', to be stored in `room_id`, then
    /// `metadata` to be written on it under the room's lock, those there
    /// are; done at once with none (Python's `_persist_side_effects`). The
    /// room is read first: the set's is the one created at `created_at`,
    /// and once it is gone, deleted or created again under its id, none is
    /// written and the log says why (RFC §13.5).
    pub(super) fn write(
        &mut self,
        cx: &mut Cx<'_>,
        room_id: &RoomId,
        created_at: Timestamp,
        (tasks, observations): (Vec<Task>, Vec<Observation>),
        metadata: Metadata,
    ) -> Progress<Written> {
        let (mut all_tasks, mut all_observations) = self.take_hooks();
        all_tasks.extend(tasks);
        all_observations.extend(observations);
        self.metadata = metadata;
        if all_tasks.is_empty() && all_observations.is_empty() && self.metadata.is_empty() {
            return self.progress();
        }
        let effect = cx.io.issue(StoreRequest::LoadContext {
            room_id: room_id.clone(),
            recent_events: RecentEvents::default(),
        });
        self.checking = Some(Checking {
            effect,
            created_at,
            tasks: all_tasks,
            observations: all_observations,
        });
        Progress::Waiting
    }

    /// Writes what `checking` holds once the room it read is the set's; a
    /// room gone takes none of it, the log saying why. A read that failed
    /// is logged, and the side effects are written as before the read.
    fn on_room_read(
        &mut self,
        cx: &mut Cx<'_>,
        room_id: &RoomId,
        checking: Checking,
        outcome: Outcome,
    ) -> Progress<Written> {
        match outcome.into_context() {
            Ok(Some(context)) if context.room.created_at == checking.created_at => {}
            Ok(_) => {
                let metadata = std::mem::take(&mut self.metadata);
                room_gone(room_id, &checking, &metadata);
                return self.progress();
            }
            Err(error) => room_unread(room_id, &error),
        }
        self.persist(cx, room_id, checking.tasks, checking.observations)
    }

    /// Asks for `tasks` and `observations` to be stored in `room_id`, then
    /// the metadata updates to be written, those there are.
    fn persist(
        &mut self,
        cx: &mut Cx<'_>,
        room_id: &RoomId,
        tasks: Vec<Task>,
        observations: Vec<Observation>,
    ) -> Progress<Written> {
        if tasks.is_empty() && observations.is_empty() {
            return self.write_metadata(cx, room_id);
        }
        let effect = cx.io.issue(StoreRequest::PersistSideEffects {
            room_id: room_id.clone(),
            tasks: tasks.clone(),
            observations,
        });
        self.stored = Some((effect, tasks));
        Progress::Waiting
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
        self.checking
            .as_ref()
            .is_some_and(|checking| checking.effect == effect)
            || self
                .stored
                .as_ref()
                .is_some_and(|(stored, _)| *stored == effect)
            || self
                .updated
                .as_ref()
                .is_some_and(|updated| updated.awaits(effect))
    }

    /// Whether a write is under way.
    pub(super) fn writing(&self) -> bool {
        self.checking.is_some() || self.stored.is_some() || self.updated.is_some()
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
        if let Some(checking) = self.checking.take_if(|checking| checking.effect == effect) {
            return self.on_room_read(cx, room_id, checking, outcome);
        }
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

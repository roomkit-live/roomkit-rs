//! The tasks the turns' hosts delegated, kept for their `wait_task`
//! (RFC §10.6.5): Python's `KitHost._tasks`, those running and, once
//! ended, until `ENDED_TASKS_KEPT` more ended after them.

use std::sync::{Mutex, PoisonError};

use roomkit_core::model::TaskStatus;

use super::delegated::DelegatedTask;

/// How many ended tasks the hosts keep for a `wait_task` that has not
/// come: Python's `ENDED_TASKS_KEPT`.
const ENDED_TASKS_KEPT: usize = 1024;

/// The tasks the room owner's hosts delegated, in the order delegated.
#[derive(Debug, Default)]
pub(crate) struct HostTasks {
    kept: Mutex<Vec<DelegatedTask>>,
}

impl HostTasks {
    /// Keeps `task`, letting go of the oldest ended ones past the bound.
    pub(crate) fn keep(&self, task: DelegatedTask) {
        let mut kept = self.kept.lock().unwrap_or_else(PoisonError::into_inner);
        kept.retain(|held| held.id != task.id);
        kept.push(task);
        let ended = kept.iter().filter(|held| has_ended(held)).count();
        let mut excess = ended.saturating_sub(ENDED_TASKS_KEPT);
        kept.retain(|held| {
            if excess > 0 && has_ended(held) {
                excess -= 1;
                return false;
            }
            true
        });
    }

    /// The task `task_id` a host of `room_id` delegated, if kept.
    pub(crate) fn of_room(&self, task_id: &str, room_id: &str) -> Option<DelegatedTask> {
        self.kept
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .find(|held| held.id == task_id && held.parent_room_id.as_str() == room_id)
            .cloned()
    }
}

fn has_ended(task: &DelegatedTask) -> bool {
    !matches!(task.status(), TaskStatus::Pending | TaskStatus::InProgress)
}

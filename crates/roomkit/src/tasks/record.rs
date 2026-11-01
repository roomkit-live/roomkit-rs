//! What the kit knows of the tasks it runs for each room (RFC §23.4), and
//! the cancels under way: Python's `tasks._room_tasks`, `RoomTaskRecord`
//! and `TaskCancel`.
//!
//! A room's `cancel_task` answers from the record, never from what the
//! status bus lists alone: a task running for the room and how it can end,
//! or the status a task the kit ran there ended with, even before the bus
//! shows that end.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use roomkit_core::model::RoomId;
use tokio::sync::watch;

/// How many ended tasks the record keeps, the latest to end, all rooms
/// together: Python's `ENDED_ROOM_TASKS_KEPT`.
pub(crate) const ENDED_ROOM_TASKS_KEPT: usize = 1024;

/// How a task of a room can end before its own end (RFC §23.4): Python's
/// `TaskReach`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum TaskReach {
    /// A background task the room's `cancel_task` cancels.
    Tool,
    /// A task a strategy runs without posting it, which the strategy
    /// follows.
    Strategy,
    /// An inline delegation, which ends with the turn that asked for it.
    Turn,
}

/// A task the kit runs for a room, and how it can end.
#[derive(Clone, Debug, PartialEq, Eq)]
struct RoomTask {
    room_id: RoomId,
    reach: TaskReach,
}

/// The tasks the kit runs for each room, by id: those running, with how
/// each can end, and the latest ended, with the status each ended with.
#[derive(Debug, Default)]
pub(crate) struct RoomTaskRecord {
    state: Mutex<Record>,
}

#[derive(Debug, Default)]
struct Record {
    /// The tasks running, in the order they started.
    running: Vec<(String, RoomTask)>,
    /// The latest ended, oldest first.
    ended: VecDeque<(String, RoomTask, String)>,
}

impl RoomTaskRecord {
    /// The tasks running, by id, in the order they started.
    #[cfg(test)]
    pub(crate) fn running(&self) -> Vec<String> {
        self.state()
            .running
            .iter()
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// `task_id` runs for `room_id` from now on, ending as `reach` says.
    pub(crate) fn start(&self, task_id: &str, room_id: &RoomId, reach: TaskReach) {
        let mut state = self.state();
        state.running.retain(|(id, _)| id != task_id);
        let task = RoomTask {
            room_id: room_id.clone(),
            reach,
        };
        state.running.push((task_id.to_owned(), task));
    }

    /// `task_id` ended with `status`: kept among the latest ended. A task
    /// not running is left as it stands.
    pub(crate) fn end(&self, task_id: &str, status: &str) {
        let mut state = self.state();
        let Some(at) = state.running.iter().position(|(id, _)| id == task_id) else {
            return;
        };
        let (id, task) = state.running.remove(at);
        state.ended.push_back((id, task, status.to_owned()));
        if state.ended.len() > ENDED_ROOM_TASKS_KEPT {
            state.ended.pop_front();
        }
    }

    /// The room `task_id` runs for; `None` for a task not running.
    pub(crate) fn room_of(&self, task_id: &str) -> Option<RoomId> {
        self.state()
            .running
            .iter()
            .find(|(id, _)| id == task_id)
            .map(|(_, task)| task.room_id.clone())
    }

    fn state(&self) -> MutexGuard<'_, Record> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// What a room's `cancel_task` reads of its tasks (RFC §23.4).
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "read by the agents' cancel_task, RMK-1144's task tools"
    )
)]
impl RoomTaskRecord {
    /// How `task_id`, running for `room_id`, can end; `None` for any other
    /// task: another room's, one ended, one the kit never ran.
    pub(crate) fn reach(&self, task_id: &str, room_id: &str) -> Option<TaskReach> {
        self.state()
            .running
            .iter()
            .find(|(id, task)| id == task_id && task.room_id.as_str() == room_id)
            .map(|(_, task)| task.reach)
    }

    /// The status `task_id`, run for `room_id`, ended with; `None` for any
    /// other task, or one ended before the latest kept.
    pub(crate) fn ended_with(&self, task_id: &str, room_id: &str) -> Option<String> {
        self.ended(task_id, room_id).map(|(_, status)| status)
    }

    /// How `task_id`, run for `room_id` and ended, could have ended before
    /// its own end; `None` for any other task.
    pub(crate) fn ended_reach(&self, task_id: &str, room_id: &str) -> Option<TaskReach> {
        self.ended(task_id, room_id).map(|(reach, _)| reach)
    }

    fn ended(&self, task_id: &str, room_id: &str) -> Option<(TaskReach, String)> {
        self.state()
            .ended
            .iter()
            .rev()
            .find(|(id, task, _)| id == task_id && task.room_id.as_str() == room_id)
            .map(|(_, task, status)| (task.reach, status.clone()))
    }
}

/// A cancel under way for one task: the channels whose calls asked for
/// it, none of which is handed the cancellation back, whether it cancelled
/// the task, which a call asking meanwhile is answered, and whether the
/// task's end has decided whom it hands its end back to (`sealed`): a call
/// asking after that is no longer one of its callers (RFC §23.4). Python's
/// `TaskCancel`.
#[derive(Debug)]
pub(crate) struct TaskCancel {
    callers: Mutex<HashSet<String>>,
    cancelled: watch::Sender<Option<bool>>,
    sealed: Mutex<bool>,
}

impl TaskCancel {
    fn new(channel_id: Option<&str>) -> Self {
        Self {
            callers: Mutex::new(channel_id.map(str::to_owned).into_iter().collect()),
            cancelled: watch::Sender::new(None),
            sealed: Mutex::new(false),
        }
    }

    /// `channel_id` asked for this cancel too.
    pub(crate) fn join(&self, channel_id: Option<&str>) {
        if let Some(channel_id) = channel_id {
            lock(&self.callers).insert(channel_id.to_owned());
        }
    }

    /// Whether the cancel cancelled the task, once it ended.
    pub(crate) async fn outcome(&self) -> bool {
        let mut cancelled = self.cancelled.subscribe();
        let outcome = cancelled.wait_for(Option::is_some).await;
        outcome.ok().and_then(|outcome| *outcome).unwrap_or(false)
    }

    /// Whether `channel_id` asked for this cancel; the cancel sealed, so a
    /// call asking after this is answered as for any ended task.
    pub(crate) fn seal_for(&self, channel_id: &str) -> bool {
        *lock(&self.sealed) = true;
        lock(&self.callers).contains(channel_id)
    }
}

/// The cancels under way, by task: Python's `RoomKit._task_cancels`.
#[derive(Debug, Default)]
pub(crate) struct TaskCancels {
    under_way: Mutex<HashMap<String, Arc<TaskCancel>>>,
}

impl TaskCancels {
    /// The cancel of `task_id` under way, if any.
    pub(crate) fn get(&self, task_id: &str) -> Option<Arc<TaskCancel>> {
        lock(&self.under_way).get(task_id).cloned()
    }

    /// The cancel of `task_id` under way, joined by `channel_id`, or a new
    /// one `channel_id` asked for first: `true` with it when it is new.
    pub(crate) fn join_or_begin(
        &self,
        task_id: &str,
        channel_id: Option<&str>,
    ) -> (Arc<TaskCancel>, bool) {
        let mut under_way = lock(&self.under_way);
        if let Some(cancel) = under_way.get(task_id) {
            cancel.join(channel_id);
            return (Arc::clone(cancel), false);
        }
        let cancel = Arc::new(TaskCancel::new(channel_id));
        under_way.insert(task_id.to_owned(), Arc::clone(&cancel));
        (cancel, true)
    }

    /// The cancel of `task_id` ended, `cancelled` or not: its waiters are
    /// answered, and a call from now on begins another.
    pub(crate) fn finish(&self, task_id: &str, cancel: &TaskCancel, cancelled: bool) {
        lock(&self.under_way).remove(task_id);
        cancel.cancelled.send_replace(Some(cancelled));
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn room(id: &str) -> RoomId {
        RoomId::new(id).unwrap()
    }

    // Python: tests/tasks/test_cancel_task.py::test_the_record_keeps_the_latest_ended_tasks_only
    #[test]
    fn the_record_keeps_the_latest_ended_tasks_only() {
        let record = RoomTaskRecord::default();
        for n in 0..=ENDED_ROOM_TASKS_KEPT {
            let task_id = format!("t{n}");
            record.start(&task_id, &room("r"), TaskReach::Tool);
            record.end(&task_id, "completed");
        }
        let last = format!("t{ENDED_ROOM_TASKS_KEPT}");
        assert_eq!(
            (
                record.ended_with("t0", "r"),
                record.ended_with("t1", "r").as_deref(),
                record.ended_with(&last, "r").as_deref(),
                record.running().len(),
            ),
            (None, Some("completed"), Some("completed"), 0)
        );
    }

    #[test]
    fn a_task_is_its_own_room_s_alone() {
        let record = RoomTaskRecord::default();
        record.start("t", &room("r"), TaskReach::Turn);
        assert_eq!(
            (record.reach("t", "r"), record.reach("t", "other")),
            (Some(TaskReach::Turn), None)
        );
        record.end("t", "failed");
        assert_eq!(
            (
                record.reach("t", "r"),
                record.ended_with("t", "other"),
                record.ended_reach("t", "r"),
            ),
            (None, None, Some(TaskReach::Turn))
        );
    }
}

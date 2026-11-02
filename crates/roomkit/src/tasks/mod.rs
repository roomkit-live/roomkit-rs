//! Task delegation (RFC §23): a delegated task's handle and its result,
//! and the runner its background work runs on, Python's `roomkit.tasks`.

// The tasks a kit runs in the background, tracked so that the kit's close
// can wait for them and cut them (decision 3 of step 2b, RFC §12.10.4):
// the inbound runs (RFC §10.1), whose caller waits for them or went on,
// with the tasks their turns run; the observer hooks that run after the
// fact (RFC §9.3); the store's writes; and the direct calls, a lifecycle
// change, a direct change of a stored event, `commit_event`, each run on
// a task of its own as an inbound call is.
//
// A task is tracked from its spawn until its future is dropped: at its end,
// aborted, or with the runtime. A task that spawns another one spawns it
// before it ends, so the kit is never seen idle between the two.
//
// Each task runs in a unit of the kit's work (`marks`). The units a
// `close()` is called from inside are spared (RFC §12.10.4): the close's
// waits pass over their tasks, and it cuts none of them.
//
// The close goes through phases, which `Tasks::phases` tells each run as
// they change: it starts, refusing every new call and cutting what has
// not started; its grace ends, cutting what is left; it seals the kit.

pub(crate) mod brief;
mod cut;
mod delegated;
mod held;
mod hosted;
pub(crate) mod json;
pub(crate) mod lines;
mod marks;
mod record;
pub(crate) mod result_tool;
mod runner;
pub(crate) mod text;

pub(crate) use self::cut::{TaskCut, TaskCuts};
pub use self::delegated::{DelegatedTask, DelegatedTaskOptions, DelegatedTaskResult, WaitOptions};
pub(crate) use self::held::{Claim, HeldTasks, spawn_apart, until_ended};
pub(crate) use self::hosted::HostTasks;
#[cfg(test)]
pub(crate) use self::record::ENDED_ROOM_TASKS_KEPT;
pub(crate) use self::record::{AGENT_REACH, RoomTaskRecord, TaskCancel, TaskCancels, TaskReach};
pub(crate) use self::result_tool::ResultTool;
pub use self::runner::{InMemoryTaskRunner, OnCompleteCallback, SubmitOptions, TaskRunner};
// For roomkit-ai's `TaskStatusTool` and `CancelTaskTool` alone, which
// serve the task tools the kit serves as an owner's tools.
#[doc(hidden)]
pub use crate::kit::background_tools::{
    TaskToolWindow, cancel_task_definition, task_status_definition,
};

use std::collections::{HashMap, HashSet};
use std::pin::pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use tokio::sync::{Notify, watch};
use tokio::task::{self, AbortHandle, JoinHandle};
use tokio::time::Instant;

pub(crate) use self::marks::{Marks, Unit};

/// How long the kit's close lets the work it cut unwind, a run reporting
/// what it came to, its commit read back, before it goes on (RFC
/// §12.10.4): the Python implementation's `CUT_UNWIND_S`.
pub(crate) const CUT_UNWIND: Duration = Duration::from_secs(1);

/// What a tracked piece of the kit's work is: what the close's log names
/// it by, and how the close cuts it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    /// An inbound run (RFC §10.1): its driver cuts it as the close tells it
    /// to, the run reporting what it came to; aborted only once its
    /// unwind ran out.
    Run,
    /// A direct call's run, on a task of its own: its driver cuts it as
    /// the close tells it to, as an inbound run's; aborted only once its
    /// unwind ran out.
    Call,
    /// A task of a turn's, its owner's or its executor's: aborted at the
    /// cut.
    Turn,
    /// An observer hook running after the fact (RFC §9.3): aborted at the
    /// cut.
    Observer,
    /// A write of the store a run asked for: aborted at the cut, so that
    /// nothing is written once `close()` returned.
    Write,
}

impl Kind {
    /// What the close's log names the work by.
    fn what(self) -> &'static str {
        match self {
            Self::Run => "an inbound run",
            Self::Call => "a call",
            Self::Turn => "a turn",
            Self::Observer => "an observer hook",
            Self::Write => "a store write",
        }
    }
}

/// Where the kit's close is, in the order it goes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Phase {
    /// No close began.
    #[default]
    Open,
    /// The close began: every new call is refused, and what has not started
    /// is cut, a delivery queued in a room's lane and a call waiting for its
    /// room's lock; the work running has its grace.
    Closing,
    /// The close's grace ran out: the work left is cut.
    Cut,
    /// The kit is sealed: no task is spawned any more, and the work a close
    /// spared stores and delivers nothing.
    Sealed,
}

/// The kit's background tasks.
#[derive(Debug)]
pub(crate) struct Tasks {
    state: Mutex<State>,
    /// The close's phase, told to the runs as it changes.
    phase: watch::Sender<Phase>,
    /// Woken whenever a task ends and a unit is spared.
    changed: Notify,
    /// The instant of the close's cut, once it came.
    cut_at: Mutex<Option<Instant>>,
    /// The cuts of the held tasks' works, which their turns' reads stop
    /// on (RFC §23.3).
    pub(crate) task_cuts: Arc<TaskCuts>,
}

impl Default for Tasks {
    fn default() -> Self {
        Self {
            state: Mutex::default(),
            phase: watch::Sender::new(Phase::Open),
            changed: Notify::new(),
            cut_at: Mutex::new(None),
            task_cuts: Arc::default(),
        }
    }
}

#[derive(Debug, Default)]
struct State {
    /// The key the next task is tracked under.
    next: u64,
    /// The tasks running, by their key.
    running: HashMap<u64, Running>,
    /// The units the callers of the kit's close run within (RFC §12.10.4).
    spared: HashSet<Unit>,
    /// The tasks the kit's close is called from: a turn run in one of them
    /// runs on whatever cuts it (RFC §12.10.4).
    acting: HashSet<task::Id>,
    /// The tasks the close aborted: ended by it.
    cut: HashSet<task::Id>,
}

/// A task running: its kind, the units it runs within, its own the
/// innermost, and the handle that aborts a task once its spawn returned
/// it.
#[derive(Debug)]
struct Running {
    kind: Kind,
    /// The units the work runs within: a unit the close spares among them
    /// spares it, the work of a run a close is called from among them.
    within: Arc<[Unit]>,
    handle: Option<AbortHandle>,
}

impl State {
    /// Whether the close spares `running`: a unit it runs within.
    fn spares(&self, running: &Running) -> bool {
        running.within.iter().any(|unit| self.spared.contains(unit))
    }

    /// Whether `marks` name a unit the close spares.
    fn spares_any(&self, marks: &Marks) -> bool {
        marks.units().iter().any(|unit| self.spared.contains(unit))
    }

    /// The tasks running that the close does not spare.
    fn unspared(&self) -> impl Iterator<Item = &Running> {
        self.running
            .values()
            .filter(move |running| !self.spares(running))
    }
}

/// The mark of a tracked task or call, carried by its future: dropped with
/// it, it untracks the work.
#[derive(Debug)]
pub(crate) struct Tracked {
    tasks: Arc<Tasks>,
    key: u64,
}

impl Tasks {
    /// Runs `future` as a task of `kind` the kit tracks, in a unit of its
    /// own within the units of the current code; `None`, nothing spawned,
    /// once the close's grace ran out, but for work the close spares, and
    /// once the kit is sealed.
    pub(crate) fn spawn<F>(self: &Arc<Self>, kind: Kind, future: F) -> Option<JoinHandle<F::Output>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let marks = Marks::current().with(Unit::new());
        self.spawn_in(kind, marks, future)
    }

    /// Runs `future` as [`spawn`](Self::spawn) does, within no work but
    /// its own: a task the current code fires and forgets, an observer,
    /// which nothing of the current code waits for, so that a close it
    /// calls spares that task alone.
    pub(crate) fn spawn_alone<F>(
        self: &Arc<Self>,
        kind: Kind,
        future: F,
    ) -> Option<JoinHandle<F::Output>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.spawn_in(kind, Marks::default().with(Unit::new()), future)
    }

    /// Runs `future` as [`spawn`](Self::spawn) does, in the unit the
    /// current code runs in: a task of a turn's, which the kit's close
    /// spares or cuts with the turn.
    pub(crate) fn spawn_joined<F>(
        self: &Arc<Self>,
        kind: Kind,
        future: F,
    ) -> Option<JoinHandle<F::Output>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let marks = Marks::current();
        match marks.innermost() {
            Some(_) => self.spawn_in(kind, marks, future),
            None => self.spawn(kind, future),
        }
    }

    fn spawn_in<F>(
        self: &Arc<Self>,
        kind: Kind,
        marks: Marks,
        future: F,
    ) -> Option<JoinHandle<F::Output>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let key = {
            let mut state = self.state();
            let phase = *self.phase.borrow();
            if phase == Phase::Sealed || (phase == Phase::Cut && !state.spares_any(&marks)) {
                return None;
            }
            track(&mut state, kind, marks.units().into())
        };
        let tracked = Tracked {
            tasks: Arc::clone(self),
            key,
        };
        // Spawned with the state unlocked: a runtime shutting down drops the
        // task's future at once, which untracks it.
        let task = tokio::spawn(marks.scoped(async move {
            let _tracked = tracked;
            future.await
        }));
        if let Some(running) = self.state().running.get_mut(&key) {
            running.handle = Some(task.abort_handle());
        }
        Some(task)
    }

    /// Tracks work of `kind` with no task while the guard lives, which no
    /// abort ends: the work a test makes ignore its cut.
    #[cfg(test)]
    pub(crate) fn track_unabortable(self: &Arc<Self>, kind: Kind) -> Tracked {
        let within = Marks::current().with(Unit::new()).units().into();
        let key = track(&mut self.state(), kind, within);
        Tracked {
            tasks: Arc::clone(self),
            key,
        }
    }

    /// Begins the kit's close: every new call is refused from now on, and
    /// the runs are told to cut what has not started.
    pub(crate) fn close(&self) {
        self.reach(Phase::Closing);
    }

    /// Moves the close on to `phase`, never back.
    fn reach(&self, phase: Phase) {
        self.phase.send_if_modified(|now| {
            let moved = *now < phase;
            if moved {
                *now = phase;
            }
            moved
        });
    }

    /// Whether the kit's close began.
    pub(crate) fn is_closing(&self) -> bool {
        *self.phase.borrow() >= Phase::Closing
    }

    /// Whether the kit is sealed.
    pub(crate) fn is_sealed(&self) -> bool {
        *self.phase.borrow() == Phase::Sealed
    }

    /// The close's phase, told as it changes.
    pub(crate) fn phases(&self) -> watch::Receiver<Phase> {
        self.phase.subscribe()
    }

    /// The close's grace ran out: the runs and the calls are told to cut
    /// what they still run, and the other tasks the close does not spare
    /// are aborted, the store's writes among them. A task spawned from now
    /// on, but one of a unit the close spares, is refused.
    pub(crate) fn cut(&self) {
        self.cut_at
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_or_insert_with(Instant::now);
        self.reach(Phase::Cut);
        self.abort_unspared(|kind| !matches!(kind, Kind::Run | Kind::Call));
    }

    /// The instant the close's grace ran out and it cut the work, once it
    /// did: the runs read their commits back within the unwind from then.
    pub(crate) fn cut_at(&self) -> Option<Instant> {
        *self.cut_at.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Aborts every task the close does not spare still running, the runs
    /// included: the unwind the cut gave them ran out.
    pub(crate) fn abort_left(&self) {
        self.abort_unspared(|_| true);
    }

    /// Aborts the turns' tasks the close does not spare still running: the
    /// unwind the turn backend's close gave them ran out.
    pub(crate) fn abort_left_turns(&self) {
        self.abort_unspared(|kind| kind == Kind::Turn);
    }

    fn abort_unspared(&self, aborts: impl Fn(Kind) -> bool) {
        let aborted: Vec<AbortHandle> = {
            let mut state = self.state();
            let handles: Vec<AbortHandle> = state
                .unspared()
                .filter(|running| aborts(running.kind))
                .filter_map(|running| running.handle.clone())
                .collect();
            state.cut.extend(handles.iter().map(AbortHandle::id));
            handles
        };
        for task in aborted {
            task.abort();
        }
    }

    /// Seals the kit: no task is spawned any more.
    pub(crate) fn seal(&self) {
        self.reach(Phase::Sealed);
    }

    /// Whether the task `id`, which was aborted, was ended by the kit's
    /// close.
    pub(crate) fn ended_by_close(&self, id: task::Id) -> bool {
        self.state().cut.contains(&id)
    }

    /// Whether the kit, sealed, refuses what work within `marks` asks: the
    /// work a close spared stores and delivers nothing once the kit is
    /// sealed (RFC §12.10.4).
    pub(crate) fn refuses(&self, marks: &Marks) -> bool {
        self.is_sealed() && self.state().spares_any(marks)
    }

    /// Whether the close spares work within `marks`.
    pub(crate) fn spares(&self, marks: &Marks) -> bool {
        self.state().spares_any(marks)
    }

    /// Spares the units `marks` name, and the task the current code runs
    /// in, from the kit's close (RFC §12.10.4): its waits pass over their
    /// tasks from now on, those under way woken to read so, and it cuts
    /// none of them.
    pub(crate) fn spare(&self, marks: &Marks) {
        let mut state = self.state();
        if let Some(id) = task::try_id() {
            state.acting.insert(id);
        }
        let before = state.spared.len();
        state.spared.extend(marks.units().iter().copied());
        if state.spared.len() == before {
            return;
        }
        drop(state);
        self.changed.notify_waiters();
    }

    /// Whether the current task is one a close is called from.
    pub(crate) fn acting(&self) -> bool {
        task::try_id().is_some_and(|id| self.state().acting.contains(&id))
    }

    /// Waits until no tracked task runs, spared or not.
    pub(crate) async fn idle(&self) {
        let none = Notify::new();
        self.wait_until(|state| state.running.is_empty(), || true, &none)
            .await;
    }

    /// Waits until no tracked task or call the close does not spare runs,
    /// and `also` holds, read again whenever a task ends, a unit is spared,
    /// or `also_changed` is told.
    pub(crate) async fn unspared_idle(&self, also: impl Fn() -> bool, also_changed: &Notify) {
        self.wait_until(
            |state| state.unspared().next().is_none(),
            also,
            also_changed,
        )
        .await;
    }

    /// Waits until no tracked task runs within `unit`: the work a section
    /// of the host's code started, a cut call's store write among it.
    pub(crate) async fn none_within(&self, unit: Unit) {
        let none = Notify::new();
        self.wait_until(
            |state| {
                state
                    .running
                    .values()
                    .all(|running| !running.within.contains(&unit))
            },
            || true,
            &none,
        )
        .await;
    }

    /// Waits until no task of a turn's the close does not spare runs: the
    /// turn backend's close waits for its turns alone.
    pub(crate) async fn unspared_turns_idle(&self) {
        let none = Notify::new();
        self.wait_until(
            |state| state.unspared().all(|running| running.kind != Kind::Turn),
            || true,
            &none,
        )
        .await;
    }

    async fn wait_until(
        &self,
        until: impl Fn(&State) -> bool,
        also: impl Fn() -> bool,
        also_changed: &Notify,
    ) {
        loop {
            // Listening before looking: a task that ends between the look
            // and the wait still wakes this one.
            let mut changed = pin!(self.changed.notified());
            changed.as_mut().enable();
            let mut other = pin!(also_changed.notified());
            other.as_mut().enable();
            if until(&self.state()) && also() {
                return;
            }
            tokio::select! {
                () = changed => {}
                () = other => {}
            }
        }
    }

    /// What the tasks and calls the close does not spare that still run
    /// are, each once, by the work they run.
    pub(crate) fn left(&self) -> Vec<&'static str> {
        let mut left: Vec<&'static str> = self
            .state()
            .unspared()
            .map(|running| running.kind.what())
            .collect();
        left.sort_unstable();
        left.dedup();
        left
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// How many tracked tasks run, for tests.
    #[cfg(test)]
    pub(crate) fn running(&self) -> usize {
        self.state().running.len()
    }
}

/// Tracks a piece of work of `kind` within the units `within`.
fn track(state: &mut State, kind: Kind, within: Arc<[Unit]>) -> u64 {
    let key = state.next;
    state.next = key.wrapping_add(1);
    let running = Running {
        kind,
        within,
        handle: None,
    };
    state.running.insert(key, running);
    key
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.tasks.state().running.remove(&self.key);
        self.tasks.changed.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::time::Duration;

    use super::*;

    /// How long a wait that should end may take before the test fails.
    const BOUND: Duration = Duration::from_secs(5);

    #[tokio::test]
    async fn a_kit_with_no_task_running_is_idle_at_once() {
        let tasks = Arc::new(Tasks::default());
        assert!(tokio::time::timeout(BOUND, tasks.idle()).await.is_ok());
    }

    #[tokio::test]
    async fn idle_waits_for_the_last_task_to_end() {
        let tasks = Arc::new(Tasks::default());
        let (go, wait) = tokio::sync::oneshot::channel::<()>();
        tasks
            .spawn(Kind::Observer, async move { drop(wait.await) })
            .unwrap();
        let early = tokio::time::timeout(Duration::from_millis(20), tasks.idle()).await;
        go.send(()).unwrap();
        let idle = tokio::time::timeout(BOUND, tasks.idle()).await;
        assert_eq!((early.is_err(), idle.is_ok()), (true, true));
    }

    /// RFC §12.10.4: the cut aborts the tasks the close does not spare, a
    /// write among them, but a run, which its driver cuts; the kit spawns
    /// none from then on.
    #[tokio::test]
    async fn the_cut_aborts_the_tasks_but_the_runs_and_spawns_none() {
        let tasks = Arc::new(Tasks::default());
        let write = tasks.spawn(Kind::Write, pending::<()>()).unwrap();
        let run = tasks.spawn(Kind::Run, pending::<()>()).unwrap();
        tasks.close();
        tasks.cut();
        let aborted = write.await.is_err_and(|error| error.is_cancelled());
        let refused = tasks.spawn(Kind::Observer, async {}).is_none();
        assert_eq!(
            (aborted, run.is_finished(), tasks.running(), refused),
            (true, false, 1, true)
        );
        run.abort();
    }

    /// RFC §12.10.4: the work a close is called from inside is spared: a
    /// wait of the close passes over it, woken as it is spared, and the cut
    /// aborts neither it nor a task of its unit.
    #[tokio::test]
    async fn a_spared_unit_is_neither_waited_for_nor_aborted() {
        let tasks = Arc::new(Tasks::default());
        let (marks_of, marked) = tokio::sync::oneshot::channel();
        let joined = Arc::clone(&tasks);
        let work = tasks
            .spawn(Kind::Turn, async move {
                let _helper = joined.spawn_joined(Kind::Turn, pending::<()>());
                drop(marks_of.send(Marks::current()));
                pending::<()>().await;
            })
            .unwrap();
        let marks = marked.await.unwrap();
        let waiting = tokio::spawn({
            let tasks = Arc::clone(&tasks);
            async move {
                let none = Notify::new();
                tasks.unspared_idle(|| true, &none).await;
            }
        });
        task::yield_now().await;
        let waited = !waiting.is_finished();
        tasks.spare(&marks);
        let woken = tokio::time::timeout(BOUND, waiting).await.is_ok();
        tasks.close();
        tasks.cut();
        tasks.abort_left();
        task::yield_now().await;
        assert_eq!(
            (waited, woken, work.is_finished(), tasks.running()),
            (true, true, false, 2)
        );
    }
}

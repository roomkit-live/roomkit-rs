//! The tasks a kit runs in the background, tracked so that the kit can wait
//! for them and end them (decision 3 of step 2b): the inbound runs (RFC
//! §10.1), whose caller waits for them or went on, answered at the decision
//! or gone, with the tasks their turns run and the observers their
//! deliveries fire; the other observer hooks that run after the fact (RFC
//! §9.3); and the store's writes, which run to their end. The kit's close
//! waits for each kind in turn.
//!
//! A task is tracked from its spawn until its future is dropped: at its end,
//! aborted, or with the runtime. A task that spawns another one spawns it
//! before it ends, so the kit is never seen idle between the two.
//!
//! Each task runs in a unit of the kit's work (`marks`). The units a
//! `close()` is called from inside are spared (RFC §12.10.4): the close's
//! waits pass over their tasks, and it aborts none of them.

mod marks;

use std::collections::{HashMap, HashSet};
use std::pin::pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tokio::sync::Notify;
use tokio::task::{self, AbortHandle, JoinHandle};

pub(crate) use self::marks::{Marks, Unit};

/// What a tracked task is, which the kit's close waits for in turn.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    /// An inbound run, its deliveries and its turns, or a task a turn runs:
    /// drained first, within the close's grace. An inbound run is the
    /// drain's until it is committed, whatever holds it, a hook or its own
    /// commit's write (RMK-1010 item 41).
    Work,
    /// An observer hook running after the fact (RFC §9.3) that no delivery
    /// fired, bounded by its hook's timeout: waited for once the work is
    /// drained or cut.
    Observer,
    /// A write of the store a run asked for, an inbound run's until the run
    /// went on from it: never aborted, so that a write decided runs to its
    /// end whatever becomes of its run (RFC §13.6), and waited for three
    /// times in the kit's close, as Python's decided writes: first and
    /// second until 5 seconds from the close's start, last until 5 seconds
    /// from the channels' close.
    Write,
}

/// The kit's background tasks.
#[derive(Debug, Default)]
pub(crate) struct Tasks {
    state: Mutex<State>,
    /// Woken whenever a task ends, a unit is spared, and the kit shuts
    /// down.
    changed: Notify,
}

/// Where the kit's close is, in the order it goes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
enum Phase {
    /// No close began.
    #[default]
    Open,
    /// The close began: the store's writes get their wait.
    Closing,
    /// The close drains the work under way: a delivery set takes no place
    /// in its room's lane any more.
    Draining,
    /// The close cut the work under way: a run ending now was ended by it.
    Cut,
    /// The kit is sealed, its close past its drain: no call starts an
    /// operation and no task is spawned any more, but a write.
    Sealed,
}

#[derive(Debug, Default)]
struct State {
    phase: Phase,
    /// The key the next task is tracked under.
    next: u64,
    /// The tasks running, by their key.
    running: HashMap<u64, Running>,
    /// The units the callers of the kit's close run within (RFC §12.10.4).
    spared: HashSet<Unit>,
    /// Counts each change of `spared`, so that a wait learns of one.
    spared_changes: u64,
    /// The tasks the close aborted while it drained, queued behind a spared
    /// unit's place in a room's lane: ended by the close as those it cuts
    /// later are.
    dropped: HashSet<task::Id>,
}

/// A task running: its kind, its unit, and the handle that aborts it once
/// its spawn returned it.
#[derive(Debug)]
struct Running {
    kind: Kind,
    unit: Unit,
    handle: Option<AbortHandle>,
}

impl State {
    /// Moves the close on to `phase`, never back.
    fn reach(&mut self, phase: Phase) {
        self.phase = self.phase.max(phase);
    }

    /// The tasks running that `counts` counts and the close does not
    /// spare.
    fn unspared(&self, counts: impl Fn(Kind) -> bool) -> impl Iterator<Item = &Running> {
        self.running
            .values()
            .filter(move |running| counts(running.kind) && !self.spared.contains(&running.unit))
    }
}

/// The mark of a tracked task, carried by its future: dropped with it, it
/// untracks the task.
struct Tracked {
    tasks: Arc<Tasks>,
    key: u64,
}

impl Tasks {
    /// Runs `future` as a task of `kind` the kit tracks, in a unit of its
    /// own within the units of the current code; `None`, nothing spawned,
    /// once the kit is sealed, but for a write of the store, which runs
    /// whatever the kit's state.
    pub(crate) fn spawn<F>(self: &Arc<Self>, kind: Kind, future: F) -> Option<JoinHandle<F::Output>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let unit = Unit::new();
        self.spawn_in(kind, unit, Marks::current().with(unit), future)
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
            Some(unit) => self.spawn_in(kind, unit, marks, future),
            None => self.spawn(kind, future),
        }
    }

    fn spawn_in<F>(
        self: &Arc<Self>,
        kind: Kind,
        unit: Unit,
        marks: Marks,
        future: F,
    ) -> Option<JoinHandle<F::Output>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let key = {
            let mut state = self.state();
            if state.phase == Phase::Sealed && kind != Kind::Write {
                return None;
            }
            let key = state.next;
            state.next = key.wrapping_add(1);
            let running = Running {
                kind,
                unit,
                handle: None,
            };
            state.running.insert(key, running);
            key
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
        let shut_down = {
            let mut state = self.state();
            if let Some(running) = state.running.get_mut(&key) {
                running.handle = Some(task.abort_handle());
            }
            state.phase == Phase::Sealed && !state.spared.contains(&unit)
        };
        if shut_down && kind != Kind::Write {
            task.abort();
        }
        Some(task)
    }

    /// Begins the kit's close: the tasks running go on, and the calls too,
    /// until the kit is sealed; `false` when the close began already.
    pub(crate) fn close(&self) -> bool {
        let mut state = self.state();
        let began = state.phase == Phase::Open;
        state.reach(Phase::Closing);
        began
    }

    /// Whether the kit's close began.
    pub(crate) fn is_closing(&self) -> bool {
        self.state().phase >= Phase::Closing
    }

    /// Marks the work under way drained by the kit's close: a delivery set
    /// takes no place in its room's lane from now on, as the Python
    /// implementation's lanes are sealed when they drain.
    pub(crate) fn drain(&self) {
        self.state().reach(Phase::Draining);
    }

    /// Whether the kit's close drains the work under way.
    pub(crate) fn is_draining(&self) -> bool {
        self.state().phase >= Phase::Draining
    }

    /// Whether the kit is sealed: its close is past its drain, and no call
    /// starts an operation any more.
    pub(crate) fn is_sealed(&self) -> bool {
        self.state().phase == Phase::Sealed
    }

    /// Marks the work under way cut by the kit's close, once its grace ran
    /// out.
    pub(crate) fn cut(&self) {
        self.state().reach(Phase::Cut);
    }

    /// Whether the kit's close cut the work of `unit`, a run's own: it cut
    /// the work under way, and spares not `unit`.
    pub(crate) fn cuts(&self, unit: Option<Unit>) -> bool {
        let state = self.state();
        state.phase >= Phase::Cut && unit.is_none_or(|unit| !state.spared.contains(&unit))
    }

    /// Whether the task `id`, which was aborted, was ended by the kit's
    /// close: aborted once the close cut the work under way, or dropped as
    /// it drained.
    pub(crate) fn ended_by_close(&self, id: task::Id) -> bool {
        let state = self.state();
        state.phase >= Phase::Cut || state.dropped.contains(&id)
    }

    /// Spares the units `marks` name from the kit's close (RFC
    /// §12.10.4): its waits pass over their tasks from now on, those under
    /// way woken to read so, and it aborts none of them.
    pub(crate) fn spare(&self, marks: &Marks) {
        let mut state = self.state();
        let before = state.spared.len();
        state.spared.extend(marks.units().iter().copied());
        if state.spared.len() == before {
            return;
        }
        state.spared_changes = state.spared_changes.wrapping_add(1);
        drop(state);
        self.changed.notify_waiters();
    }

    /// The units the kit's close spares now.
    pub(crate) fn spared(&self) -> HashSet<Unit> {
        self.state().spared.clone()
    }

    /// Counts the changes of the units spared, for
    /// [`idle_or_spared`](Self::idle_or_spared).
    pub(crate) fn spared_changes(&self) -> u64 {
        self.state().spared_changes
    }

    /// Waits until no tracked task runs, spared or not.
    pub(crate) async fn idle(&self) {
        self.wait_until(|state| state.running.is_empty()).await;
    }

    /// Waits until no tracked task of `kind` the close does not spare runs.
    pub(crate) async fn idle_of_kind(&self, kind: Kind) {
        self.wait_until(|state| state.unspared(|running| running == kind).next().is_none())
            .await;
    }

    /// Waits until no tracked task of `kind` in one of `units` the close
    /// does not spare runs.
    pub(crate) async fn idle_of_units(&self, kind: Kind, units: &HashSet<Unit>) {
        self.wait_until(|state| {
            state
                .unspared(|running| running == kind)
                .all(|running| !units.contains(&running.unit))
        })
        .await;
    }

    /// Waits until no tracked task of `kind` the close does not spare
    /// runs, `true`, or until the units spared changed since the count
    /// `seen` of [`spared_changes`](Self::spared_changes), `false`.
    pub(crate) async fn idle_or_spared(&self, kind: Kind, seen: u64) -> bool {
        let mut idle = false;
        self.wait_until(|state| {
            idle = state.unspared(|running| running == kind).next().is_none();
            idle || state.spared_changes != seen
        })
        .await;
        idle
    }

    async fn wait_until(&self, mut until: impl FnMut(&State) -> bool) {
        loop {
            // Listening before looking: a task that ends between the look
            // and the wait still wakes this one.
            let mut changed = pin!(self.changed.notified());
            changed.as_mut().enable();
            if until(&self.state()) {
                return;
            }
            changed.await;
        }
    }

    /// Aborts the tasks of `kind` running that the close does not spare,
    /// and waits until they are gone; tasks of the kind spawned meanwhile
    /// run.
    pub(crate) async fn abort(&self, kind: Kind) {
        let running = self.handles(|running| running == kind);
        for task in running {
            task.abort();
        }
        self.idle_of_kind(kind).await;
    }

    /// Aborts at once the work of `units` the close does not spare, as it
    /// drains: the runs queued behind a spared run's place in a room's
    /// lane, which it drops (RFC §12.10.4). Their callers read them ended
    /// by the close.
    pub(crate) fn drop_work_of(&self, units: &HashSet<Unit>) {
        let dropped: Vec<AbortHandle> = {
            let mut state = self.state();
            let handles: Vec<AbortHandle> = state
                .unspared(|kind| kind == Kind::Work)
                .filter(|running| units.contains(&running.unit))
                .filter_map(|running| running.handle.clone())
                .collect();
            state.dropped.extend(handles.iter().map(AbortHandle::id));
            handles
        };
        for task in dropped {
            task.abort();
        }
    }

    /// Seals the kit: spawns no task any more, aborts the tasks running but
    /// the store's writes and the ones the close spares, and waits until
    /// they are gone.
    pub(crate) async fn shut_down(&self) {
        // A task whose spawn has not returned its handle yet is aborted by
        // its spawn, which finds the kit shut down.
        self.state().reach(Phase::Sealed);
        let running = self.handles(|kind| kind != Kind::Write);
        self.changed.notify_waiters();
        for task in running {
            task.abort();
        }
        self.wait_until(|state| state.unspared(|kind| kind != Kind::Write).next().is_none())
            .await;
    }

    /// The handles of the tasks running that `counts` counts and the close
    /// does not spare.
    fn handles(&self, counts: impl Fn(Kind) -> bool) -> Vec<AbortHandle> {
        self.state()
            .unspared(counts)
            .filter_map(|running| running.handle.clone())
            .collect()
    }

    /// How many tasks of `kind` the close does not spare run.
    pub(crate) fn running_of(&self, kind: Kind) -> usize {
        self.state().unspared(|running| running == kind).count()
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
            .spawn(Kind::Work, async move { drop(wait.await) })
            .unwrap();
        let early = tokio::time::timeout(Duration::from_millis(20), tasks.idle()).await;
        go.send(()).unwrap();
        let idle = tokio::time::timeout(BOUND, tasks.idle()).await;
        assert_eq!((early.is_err(), idle.is_ok()), (true, true));
    }

    #[tokio::test]
    async fn a_shut_down_kit_aborts_its_tasks_and_spawns_none() {
        let tasks = Arc::new(Tasks::default());
        let task = tasks.spawn(Kind::Work, pending::<()>()).unwrap();
        tokio::time::timeout(BOUND, tasks.shut_down())
            .await
            .unwrap();
        let aborted = task.await.is_err_and(|error| error.is_cancelled());
        assert_eq!(
            (
                aborted,
                tasks.running(),
                tasks.spawn(Kind::Work, async {}).is_none()
            ),
            (true, 0, true)
        );
    }

    /// RFC §12.10.4: the work a close is called from inside is spared: a
    /// wait of the close passes over it, woken as it is spared, and the
    /// shutdown aborts neither it nor a task of its unit.
    #[tokio::test]
    async fn a_spared_unit_is_neither_waited_for_nor_aborted() {
        let tasks = Arc::new(Tasks::default());
        let (marks_of, marked) = tokio::sync::oneshot::channel();
        let joined = Arc::clone(&tasks);
        let work = tasks
            .spawn(Kind::Work, async move {
                let _helper = joined.spawn_joined(Kind::Work, pending::<()>());
                drop(marks_of.send(Marks::current()));
                pending::<()>().await;
            })
            .unwrap();
        let marks = marked.await.unwrap();
        let waiting = tokio::spawn({
            let tasks = Arc::clone(&tasks);
            async move { tasks.idle_of_kind(Kind::Work).await }
        });
        task::yield_now().await;
        let waited = !waiting.is_finished();
        tasks.spare(&marks);
        let woken = tokio::time::timeout(BOUND, waiting).await.is_ok();
        tokio::time::timeout(BOUND, tasks.shut_down())
            .await
            .unwrap();
        assert_eq!(
            (waited, woken, work.is_finished(), tasks.running()),
            (true, true, false, 2)
        );
    }
}

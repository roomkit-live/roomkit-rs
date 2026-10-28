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

use std::collections::HashMap;
use std::pin::pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tokio::sync::Notify;
use tokio::task::{AbortHandle, JoinHandle};

/// What a tracked task is, which the kit's close waits for in turn.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    /// An inbound run, its deliveries and its turns, or a task a turn runs:
    /// drained within the close's grace, but a run whose deciding commit is
    /// all of it in flight, which the waits for the writes cover.
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
    /// Woken whenever the last task of a kind ends, a task of the work ends
    /// while a run is marked a decided write, such a mark comes or goes,
    /// and when the kit shuts down.
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
    /// The tasks running, each by its key, with its kind and the handle that
    /// aborts it once its spawn returned it.
    running: HashMap<u64, (Kind, Option<AbortHandle>)>,
    /// The inbound runs, each a task of the work, whose commit that decides
    /// their call is all of them in flight ([`Tasks::deciding`]).
    deciding: usize,
}

impl State {
    /// Moves the close on to `phase`, never back.
    fn reach(&mut self, phase: Phase) {
        self.phase = self.phase.max(phase);
    }
}

/// The mark of a tracked task, carried by its future: dropped with it, it
/// untracks the task.
struct Tracked {
    tasks: Arc<Tasks>,
    key: u64,
}

/// The mark of an inbound run whose commit that decides its call is all of
/// it in flight, while it lives ([`Tasks::deciding`]).
#[derive(Debug)]
pub(crate) struct Deciding {
    tasks: Arc<Tasks>,
}

impl Tasks {
    /// Runs `future` as a task of `kind` the kit tracks; `None`, nothing
    /// spawned, once the kit is sealed, but for a write of the store, which
    /// runs whatever the kit's state.
    pub(crate) fn spawn<F>(self: &Arc<Self>, kind: Kind, future: F) -> Option<JoinHandle<F::Output>>
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
            state.running.insert(key, (kind, None));
            key
        };
        let tracked = Tracked {
            tasks: Arc::clone(self),
            key,
        };
        // Spawned with the state unlocked: a runtime shutting down drops the
        // task's future at once, which untracks it.
        let task = tokio::spawn(async move {
            let _tracked = tracked;
            future.await
        });
        let shut_down = {
            let mut state = self.state();
            if let Some((_, handle)) = state.running.get_mut(&key) {
                *handle = Some(task.abort_handle());
            }
            state.phase == Phase::Sealed
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

    /// Marks the inbound run that calls it as a decided write while the
    /// mark lives: its commit that decides its call is all of it in flight,
    /// and the drain no longer waits for it ([`drained`](Self::drained));
    /// the close's waits for the writes do, as Python's held write covers
    /// the commit and the lane's enqueue (`_write_decision`). Only such a
    /// run, a task of the work, takes one, and only while no delivery set
    /// it runs is running or waiting for its turn.
    pub(crate) fn deciding(self: &Arc<Self>) -> Deciding {
        self.state().deciding += 1;
        self.changed.notify_waiters();
        Deciding {
            tasks: Arc::clone(self),
        }
    }

    /// Waits until the work under way is over but the inbound runs whose
    /// deciding commit is in flight: what the kit's drain waits for.
    pub(crate) async fn drained(&self) {
        loop {
            // Listening before looking, as `idle_of` does.
            let mut changed = pin!(self.changed.notified());
            changed.as_mut().enable();
            if self.only_deciding_work() {
                return;
            }
            changed.await;
        }
    }

    /// Whether the only tasks of the work running are inbound runs whose
    /// deciding commit is in flight.
    fn only_deciding_work(&self) -> bool {
        let state = self.state();
        let work = state
            .running
            .values()
            .filter(|(kind, _)| *kind == Kind::Work)
            .count();
        work <= state.deciding
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

    /// Whether the kit's close cut the work under way, or shut it down.
    pub(crate) fn is_cut(&self) -> bool {
        self.state().phase >= Phase::Cut
    }

    /// Waits until no tracked task runs.
    pub(crate) async fn idle(&self) {
        self.idle_of(|_| true).await;
    }

    /// Waits until no tracked task of `kind` runs.
    pub(crate) async fn idle_of_kind(&self, kind: Kind) {
        self.idle_of(|running| running == kind).await;
    }

    async fn idle_of(&self, counts: impl Fn(Kind) -> bool) {
        loop {
            // Listening before looking: a task that ends between the look
            // and the wait still wakes this one.
            let mut changed = pin!(self.changed.notified());
            changed.as_mut().enable();
            if !self.state().running.values().any(|(kind, _)| counts(*kind)) {
                return;
            }
            changed.await;
        }
    }

    /// Aborts the tasks of `kind` running, and waits until they are gone;
    /// tasks of the kind spawned meanwhile run.
    pub(crate) async fn abort(&self, kind: Kind) {
        let running: Vec<AbortHandle> = self
            .state()
            .running
            .values()
            .filter(|(running, _)| *running == kind)
            .filter_map(|(_, handle)| handle.clone())
            .collect();
        for task in running {
            task.abort();
        }
        self.idle_of_kind(kind).await;
    }

    /// Seals the kit: spawns no task any more, aborts the tasks running but
    /// the store's writes, and waits until they are gone.
    pub(crate) async fn shut_down(&self) {
        // A task whose spawn has not returned its handle yet is aborted by
        // its spawn, which finds the kit shut down.
        let running: Vec<AbortHandle> = {
            let mut state = self.state();
            state.reach(Phase::Sealed);
            state
                .running
                .values()
                .filter(|(kind, _)| *kind != Kind::Write)
                .filter_map(|(_, handle)| handle.clone())
                .collect()
        };
        self.changed.notify_waiters();
        for task in running {
            task.abort();
        }
        self.idle_of(|kind| kind != Kind::Write).await;
    }

    /// How many tasks of `kind` run.
    pub(crate) fn running_of(&self, kind: Kind) -> usize {
        self.state()
            .running
            .values()
            .filter(|(running, _)| *running == kind)
            .count()
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

impl Drop for Deciding {
    fn drop(&mut self) {
        let mut state = self.tasks.state();
        state.deciding = state.deciding.saturating_sub(1);
        drop(state);
        self.tasks.changed.notify_waiters();
    }
}

impl Drop for Tracked {
    fn drop(&mut self) {
        let tells = {
            let mut state = self.tasks.state();
            state.running.remove(&self.key).is_some_and(|(ended, _)| {
                let last_of_its_kind = !state.running.values().any(|(kind, _)| *kind == ended);
                // The drain may be over with the work but deciding runs.
                last_of_its_kind || (ended == Kind::Work && state.deciding > 0)
            })
        };
        if tells {
            self.tasks.changed.notify_waiters();
        }
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
}

//! The tasks a kit runs in the background, tracked so that the kit can wait
//! for them and end them (decision 3 of step 2b): the observer hooks that run
//! after the fact (RFC §9.3), the inbound runs (RFC §10.1), whose caller
//! waits for them or went on, answered at the decision or gone, and the
//! timer sweeps (RFC §5.1).
//!
//! A task is tracked from its spawn until its future is dropped: at its end,
//! aborted, or with the runtime. A task that spawns another one spawns it
//! before it ends, so the kit is never seen idle between the two.

use std::collections::HashMap;
use std::pin::pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tokio::sync::Notify;
use tokio::task::{AbortHandle, JoinHandle};

/// The kit's background tasks.
#[derive(Debug, Default)]
pub(crate) struct Tasks {
    state: Mutex<State>,
    /// Woken whenever a task ends, and when the kit shuts down.
    changed: Notify,
}

#[derive(Debug, Default)]
struct State {
    /// Whether the kit is shut down: no task is spawned any more.
    shut_down: bool,
    /// The key the next task is tracked under.
    next: u64,
    /// The tasks running, each by its key, with the handle that aborts it
    /// once its spawn returned it.
    running: HashMap<u64, Option<AbortHandle>>,
}

/// The mark of a tracked task, carried by its future: dropped with it, it
/// untracks the task.
struct Tracked {
    tasks: Arc<Tasks>,
    key: u64,
}

impl Tasks {
    /// Runs `future` as a task the kit tracks; `None`, nothing spawned, once
    /// the kit is shut down.
    pub(crate) fn spawn<F>(self: &Arc<Self>, future: F) -> Option<JoinHandle<F::Output>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let key = {
            let mut state = self.state();
            if state.shut_down {
                return None;
            }
            let key = state.next;
            state.next = key.wrapping_add(1);
            state.running.insert(key, None);
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
            if let Some(handle) = state.running.get_mut(&key) {
                *handle = Some(task.abort_handle());
            }
            state.shut_down
        };
        if shut_down {
            task.abort();
        }
        Some(task)
    }

    /// Whether the kit is shut down.
    pub(crate) fn is_shut_down(&self) -> bool {
        self.state().shut_down
    }

    /// Waits until no tracked task runs.
    pub(crate) async fn idle(&self) {
        loop {
            // Listening before looking: a task that ends between the look
            // and the wait still wakes this one.
            let mut changed = pin!(self.changed.notified());
            changed.as_mut().enable();
            if self.state().running.is_empty() {
                return;
            }
            changed.await;
        }
    }

    /// Spawns no task any more, aborts the tasks running, and waits until
    /// they are gone.
    pub(crate) async fn shut_down(&self) {
        // A task whose spawn has not returned its handle yet is aborted by
        // its spawn, which finds the kit shut down.
        let running: Vec<AbortHandle> = {
            let mut state = self.state();
            state.shut_down = true;
            state.running.values().flatten().cloned().collect()
        };
        self.changed.notify_waiters();
        for task in running {
            task.abort();
        }
        self.idle().await;
    }

    /// Waits until the kit is shut down.
    pub(crate) async fn shutting_down(&self) {
        loop {
            let mut changed = pin!(self.changed.notified());
            changed.as_mut().enable();
            if self.is_shut_down() {
                return;
            }
            changed.await;
        }
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
        let idle = {
            let mut state = self.tasks.state();
            state.running.remove(&self.key);
            state.running.is_empty()
        };
        if idle {
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
        tasks.spawn(async move { drop(wait.await) }).unwrap();
        let early = tokio::time::timeout(Duration::from_millis(20), tasks.idle()).await;
        go.send(()).unwrap();
        let idle = tokio::time::timeout(BOUND, tasks.idle()).await;
        assert_eq!((early.is_err(), idle.is_ok()), (true, true));
    }

    #[tokio::test]
    async fn a_shut_down_kit_aborts_its_tasks_and_spawns_none() {
        let tasks = Arc::new(Tasks::default());
        let task = tasks.spawn(pending::<()>()).unwrap();
        tokio::time::timeout(BOUND, tasks.shut_down())
            .await
            .unwrap();
        let aborted = task.await.is_err_and(|error| error.is_cancelled());
        assert_eq!(
            (aborted, tasks.running(), tasks.spawn(async {}).is_none()),
            (true, 0, true)
        );
    }
}

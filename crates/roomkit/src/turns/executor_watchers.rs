//! The listeners a turn backend runs when the executors it serves change (RFC §10.6.7).
use std::fmt;
use std::sync::{Arc, Mutex};

use super::{TurnFuture, lock};

/// What a room owner has run when the executors of its backend's process
/// change: the listener of Python's `TurnBackend.watch_executors`. It
/// answers `None` once its owner is gone, and the backend forgets it.
pub type ExecutorsListener = Arc<dyn Fn() -> Option<TurnFuture<'static, ()>> + Send + Sync>;

/// The listeners of [`TurnBackend::watch_executors`](super::TurnBackend::watch_executors)
/// a backend keeps, as Python's `TurnBackend` keeps them itself.
#[derive(Default)]
pub struct ExecutorWatchers(Mutex<Vec<ExecutorsListener>>);

impl ExecutorWatchers {
    /// No listener yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Keep `listener` for [`changed`](Self::changed) to run.
    pub fn watch(&self, listener: ExecutorsListener) {
        lock(&self.0).push(listener);
    }

    /// Run the listeners kept, each to its end, in the order given: a
    /// backend calls it once it serves an executor, and when one it serves
    /// closes (Python `executors_changed`). A listener whose owner is gone is
    /// forgotten.
    pub async fn changed(&self) {
        let mut pending = Vec::new();
        lock(&self.0).retain(|listener| {
            if let Some(work) = listener() {
                pending.push(work);
                return true;
            }
            false
        });
        for work in pending {
            work.await;
        }
    }
}

impl fmt::Debug for ExecutorWatchers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExecutorWatchers").finish_non_exhaustive()
    }
}

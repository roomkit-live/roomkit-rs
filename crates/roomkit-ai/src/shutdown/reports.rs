//! Cleanup of standalone external calls, where no kit owns an observer tracker.

use std::sync::{Arc, Mutex, PoisonError};

use crate::tools::BoxFuture;

use super::{ClosingContext, Turn};

/// Separate from addressable loops: reporting a cut never invents a public turn.
#[derive(Debug, Default)]
pub(crate) struct Reports {
    running: Mutex<Vec<Arc<Turn>>>,
}

impl Reports {
    /// Capture now; registration happens synchronously before the cleanup is
    /// spawned, so a subsequent close can join it. The callback's own close
    /// recognizes its identity and never waits on itself.
    pub(crate) fn prepare(
        self: &Arc<Self>,
        work: BoxFuture<'static, ()>,
    ) -> Box<dyn FnOnce() + Send> {
        let reports = Arc::clone(self);
        let turn = Arc::new(Turn::default());
        let finished = Finished(Arc::clone(&turn));
        let context = crate::tools::closing_context().in_turn(Arc::clone(&turn));
        let work =
            roomkit::keep_context(crate::tools::within_closing_context(context, async move {
                let _finished = finished;
                work.await;
            }));
        Box::new(move || {
            let mut running = reports
                .running
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            running.retain(|turn| !turn.has_ended());
            running.push(turn);
            drop(running);
            // The completion guard and the queue track this task independently
            // of a JoinHandle, including panic and a runtime dropping the task.
            tokio::spawn(work);
        })
    }

    pub(crate) async fn wait(&self, current: &ClosingContext) {
        let waiting: Vec<_> = self
            .running
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|turn| !turn.has_ended() && turn.cut(current))
            .cloned()
            .collect();
        crate::stream::join_all(waiting.iter().map(|turn| turn.wait()).collect()).await;
    }
}

struct Finished(Arc<Turn>);

impl Drop for Finished {
    fn drop(&mut self) {
        self.0.end();
    }
}

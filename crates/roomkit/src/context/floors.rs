//! The depth lent to one observer's body, independent of the turn or
//! sibling observers still running (RFC §8.3, §9.5.1).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use super::{BodyRunning, ExecutionContext};

/// A depth kept by a context, either for its whole life or while the body
/// it belongs to runs. Copies share the latter's lifetime.
#[derive(Clone, Debug)]
pub(super) struct Floor {
    depth: u32,
    live: Option<Arc<AtomicBool>>,
}

impl Floor {
    pub(super) fn fixed(depth: u32) -> Self {
        Self { depth, live: None }
    }

    pub(super) fn depth(&self) -> Option<u32> {
        self.live
            .as_ref()
            .is_none_or(|live| live.load(Ordering::Relaxed))
            .then_some(self.depth)
    }
}

impl ExecutionContext {
    /// Runs one observer at the depth its trigger fired at, at least
    /// `floor`, until that body ends. Work it starts keeps the room's
    /// live budget and admission context, but borrows every inherited
    /// depth only while this body runs, as Python's
    /// `floors_lent_while_it_runs` does.
    pub(crate) fn observing<F: Future>(
        self,
        floor: u32,
        future: F,
    ) -> impl Future<Output = F::Output> {
        let depth = self.turn_depth().unwrap_or(0).max(floor);
        let live = Arc::new(AtomicBool::new(true));
        let mut context = self;
        // Keep frames for admission and budget accounting. Their depth is
        // represented by this body's floor instead of the parent's lifetime.
        context.frames = context
            .frames
            .iter()
            .cloned()
            .map(|mut frame| {
                frame.work.response_depth = None;
                frame
            })
            .collect();
        if let Some(body) = &mut context.body {
            body.floor = 0;
        }
        context.floor = Some(Floor {
            depth,
            live: Some(Arc::clone(&live)),
        });
        async move {
            let _running = BodyRunning(live);
            context.scope(future).await
        }
    }
}

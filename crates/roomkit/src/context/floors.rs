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
    live: Vec<Arc<AtomicBool>>,
}

impl Floor {
    pub(super) fn fixed(depth: u32) -> Self {
        Self {
            depth,
            live: Vec::new(),
        }
    }

    pub(super) fn while_running(depth: u32, live: Arc<AtomicBool>) -> Self {
        Self {
            depth,
            live: vec![live],
        }
    }

    /// A borrowed tool depth requires both its scope and every observer
    /// body it was lent through to remain live.
    fn lent_while(mut self, live: Arc<AtomicBool>) -> Self {
        self.live.push(live);
        self
    }

    pub(super) fn depth(&self) -> Option<u32> {
        self.live
            .iter()
            .all(|live| live.load(Ordering::Relaxed))
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
        let mut context = self;
        // Unlike the observer's own floor, the borrowed tool depth also
        // expires when the original tool scope ends. Do not freeze its
        // current value into the observer's independent floor.
        let tool_depth = context.tool_depth.take();
        let depth = context.turn_depth().unwrap_or(0).max(floor);
        let live = Arc::new(AtomicBool::new(true));
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
        context.tool_depth = tool_depth.map(|depth| depth.lent_while(Arc::clone(&live)));
        context.floor = Some(Floor::while_running(depth, Arc::clone(&live)));
        async move {
            let _running = BodyRunning(live);
            context.scope(future).await
        }
    }
}

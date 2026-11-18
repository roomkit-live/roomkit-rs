//! Admission and completion of the local executor's work, independently of
//! the owner's backend. An executor close spares the turns it runs under.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{Notify, watch};

use crate::turns::context::TurnScope;
use crate::turns::lock;

#[derive(Debug, Default)]
pub(crate) struct Lifecycle {
    state: Mutex<State>,
    changed: Notify,
    pub(crate) memory_backends: crate::turns::in_memory::Serving,
}

#[derive(Debug, Default)]
struct State {
    closed: bool,
    running: HashMap<Arc<str>, Active>,
}

#[derive(Debug)]
struct Active {
    cut: watch::Sender<Option<&'static str>>,
    // A self-close keeps work alive if its owner later cancels or disappears.
    // A later external executor close still cuts that work directly.
    spared_by_owner: Arc<AtomicBool>,
}

impl Lifecycle {
    pub(crate) fn is_closed(&self) -> bool {
        lock(&self.state).closed
    }

    /// Register before submitting the turn or changing its previous record.
    /// A concurrent close either refuses admission or waits for this guard.
    pub(crate) fn admit(self: &Arc<Self>, turn: Arc<str>) -> Option<Running> {
        let mut state = lock(&self.state);
        if state.closed || state.running.contains_key(&turn) {
            return None;
        }
        let (cut, _) = watch::channel(None);
        let spared_by_owner = Arc::new(AtomicBool::new(false));
        state.running.insert(
            Arc::clone(&turn),
            Active {
                cut: cut.clone(),
                spared_by_owner: Arc::clone(&spared_by_owner),
            },
        );
        Some(Running {
            lifecycle: Arc::clone(self),
            turn,
            cut,
            spared_by_owner,
        })
    }

    /// Refuse admission and cut other turns, waiting until their executors
    /// finished sending their end. Concurrent closes can spare their callers.
    pub(crate) async fn close(&self) {
        let spared = TurnScope::current();
        {
            let mut state = lock(&self.state);
            state.closed = true;
            for (turn, active) in &state.running {
                if spared.contains(turn) {
                    active.spared_by_owner.store(true, Ordering::Release);
                } else {
                    active.cut.send_replace(Some("cancelled"));
                }
            }
        }
        self.changed.notify_waiters();
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let state = lock(&self.state);
                if state.running.keys().all(|turn| spared.contains(turn)) {
                    return;
                }
            }
            changed.await;
        }
    }
}

/// Held by the executor future through its final frame; dropping an unstarted
/// or aborted future also releases admission, so close cannot miss its end.
pub(crate) struct Running {
    lifecycle: Arc<Lifecycle>,
    turn: Arc<str>,
    cut: watch::Sender<Option<&'static str>>,
    spared_by_owner: Arc<AtomicBool>,
}

impl Running {
    pub(super) fn spared_by_owner(&self) -> bool {
        self.spared_by_owner.load(Ordering::Acquire)
    }

    pub(super) fn cutting(&self) -> watch::Receiver<Option<&'static str>> {
        self.cut.subscribe()
    }

    #[cfg(feature = "redis")]
    pub(super) fn cutter(&self) -> watch::Sender<Option<&'static str>> {
        self.cut.clone()
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        lock(&self.lifecycle.state).running.remove(&self.turn);
        self.lifecycle.changed.notify_waiters();
    }
}

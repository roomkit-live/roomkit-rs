//! Closing the turns a channel currently owns, without cutting the caller.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tokio::sync::Notify;

/// The live ancestors of a loop or call. The tool scope carries this through
/// `keep_context`, including a child task that closes its parent's channel.
#[derive(Clone, Debug, Default)]
pub(crate) struct ClosingContext {
    turns: Vec<Arc<Turn>>,
    calls: Vec<Arc<Call>>,
}

impl ClosingContext {
    pub(crate) fn extend(&mut self, other: &Self) {
        extend_unique(&mut self.turns, &other.turns);
        extend_unique(&mut self.calls, &other.calls);
    }

    pub(crate) fn in_turn(mut self, turn: Arc<Turn>) -> Self {
        self.turns.push(turn);
        self
    }

    fn in_call(mut self, call: Arc<Call>) -> Self {
        self.calls.push(call);
        self
    }
}

fn extend_unique<T>(held: &mut Vec<Arc<T>>, others: &[Arc<T>]) {
    for other in others {
        if !held.iter().any(|own| Arc::ptr_eq(own, other)) {
            held.push(Arc::clone(other));
        }
    }
}

/// A latched notification: waiters arriving after the signal see it too.
#[derive(Debug, Default)]
struct Signal {
    set: AtomicBool,
    changed: Notify,
}

impl Signal {
    fn set(&self) {
        self.set.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }

    async fn wait(&self) {
        let changed = self.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        if !self.set.load(Ordering::Acquire) {
            changed.await;
        }
    }
}

#[derive(Debug, Default)]
struct State {
    closing: bool,
    calls: Vec<Arc<Call>>,
}

#[derive(Debug, Default)]
struct Call {
    cut: Signal,
}

/// One loop, registered until its stream ends or is dropped.
#[derive(Debug, Default)]
pub(crate) struct Turn {
    state: Mutex<State>,
    ended: Signal,
}

impl Turn {
    /// Cut calls except those whose scope is closing us. Whether to wait:
    /// never for a turn already cut, or for an ancestor of the closer.
    pub(crate) fn cut(self: &Arc<Self>, current: &ClosingContext) -> bool {
        let mut state = lock(&self.state);
        if state.closing {
            return false;
        }
        state.closing = true;
        for call in &state.calls {
            if !current.calls.iter().any(|own| Arc::ptr_eq(own, call)) {
                call.cut.set();
            }
        }
        !current.turns.iter().any(|own| Arc::ptr_eq(own, self))
    }

    pub(crate) fn end(&self) {
        self.ended.set();
    }

    pub(crate) async fn wait(&self) {
        self.ended.wait().await;
    }

    pub(crate) fn has_ended(&self) -> bool {
        self.ended.set.load(Ordering::Acquire)
    }

    /// Each invocation has its own identity, even when two calls use the
    /// same wire id and are polled in the same Tokio task.
    pub(crate) async fn call<F: Future>(self: &Arc<Self>, work: F) -> Option<F::Output> {
        let call = Arc::new(Call::default());
        {
            let mut state = lock(&self.state);
            if state.closing {
                return None;
            }
            state.calls.push(Arc::clone(&call));
        }
        let _running = RunningCall {
            turn: Arc::clone(self),
            call: Arc::clone(&call),
        };
        let context = crate::tools::closing_context().in_call(Arc::clone(&call));
        crate::tools::within_closing_context(context, async {
            tokio::select! {
                biased;
                () = call.cut.wait() => None,
                answer = work => Some(answer),
            }
        })
        .await
    }
}

struct RunningCall {
    turn: Arc<Turn>,
    call: Arc<Call>,
}

impl Drop for RunningCall {
    fn drop(&mut self) {
        lock(&self.turn.state)
            .calls
            .retain(|call| !Arc::ptr_eq(call, &self.call));
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

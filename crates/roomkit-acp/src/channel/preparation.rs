//! A public cancel can reach the turn before its first or replacement prompt.

use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::{Mutex, PoisonError};

use tokio::sync::Notify;

#[derive(Default)]
pub(super) struct Preparation {
    state: Mutex<State>,
    changed: Notify,
}

struct State {
    preparing: bool,
    cancelled: bool,
}

impl Default for State {
    fn default() -> Self {
        Self {
            preparing: true,
            cancelled: false,
        }
    }
}

impl Preparation {
    /// Terminal cleanup still holds the room, but no preparation remains
    /// for public cancellation to stop.
    pub(super) fn finish(&self) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .preparing = false;
    }

    /// Mark the preparation stopped without sending a remote cancellation:
    /// there is no prompt for the agent to cancel yet (RFC §6.4 item 7).
    pub(super) fn cancel(&self) -> bool {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if !state.preparing {
            return false;
        }
        state.cancelled = true;
        self.changed.notify_one();
        true
    }

    pub(super) async fn run<F: Future>(&self, work: F) -> Option<F::Output> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .preparing = true;
        tokio::select! {
            biased;
            () = self.cancelled() => None,
            result = work => {
                let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
                (!state.cancelled).then_some(result)
            }
        }
    }

    async fn cancelled(&self) {
        loop {
            let changed = self.changed.notified();
            if self
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .cancelled
            {
                return;
            }
            changed.await;
        }
    }

    /// Admission and cancellation share a lock. A cancel after setup but
    /// before admission prevents the prompt. Never poll a custom transport
    /// while holding that lock: its first poll can itself call public cancel.
    pub(super) async fn prompt<F: Future>(&self, work: F) -> Option<F::Output> {
        let mut work = pin!(work);
        let mut first = true;
        poll_fn(|cx| {
            if !first {
                return work.as_mut().poll(cx).map(Some);
            }
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            if state.cancelled {
                return std::task::Poll::Ready(None);
            }
            state.preparing = false;
            first = false;
            drop(state);
            work.as_mut().poll(cx).map(Some)
        })
        .await
    }
}

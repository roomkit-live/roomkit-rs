//! A public cancel can reach the turn before its first or replacement prompt.

use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::{Mutex, PoisonError};

use tokio::sync::Notify;

#[derive(Default)]
pub(super) struct Preparation {
    turn_id: Option<String>,
    state: Mutex<State>,
    changed: Notify,
}

#[derive(Default)]
struct State {
    phase: Phase,
    cancelled: bool,
}

#[derive(Default)]
enum Phase {
    #[default]
    Preparing,
    Prompting(String),
    Finished,
}

pub(super) enum Cancellation {
    Preparing,
    Prompting(String),
}

impl Preparation {
    pub(super) fn new(turn_id: Option<String>) -> Self {
        Self {
            turn_id,
            ..Self::default()
        }
    }

    /// Capture this turn's cancellation target before releasing its registry.
    /// A later room turn must never become the recipient of this directive.
    pub(super) fn cancel_turn(&self, turn_id: &str) -> Option<Cancellation> {
        if self.turn_id.as_deref() != Some(turn_id) {
            return None;
        }
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        match &state.phase {
            Phase::Preparing => {
                state.cancelled = true;
                self.changed.notify_one();
                Some(Cancellation::Preparing)
            }
            Phase::Prompting(session) => Some(Cancellation::Prompting(session.clone())),
            Phase::Finished => None,
        }
    }

    /// A cancelled preparation has no prompt left to steer. A returned
    /// prompt remains addressable while its output is still being read.
    pub(super) fn finish(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if matches!(state.phase, Phase::Preparing) {
            state.phase = Phase::Finished;
        }
    }

    /// The output has drained. Remove its steering target before terminal
    /// cleanup, which can still hold the room while it closes a session.
    pub(super) fn retire(&self) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .phase = Phase::Finished;
    }

    /// Mark the preparation stopped without sending a remote cancellation:
    /// there is no prompt for the agent to cancel yet (RFC §6.4 item 7).
    pub(super) fn cancel(&self) -> bool {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if !matches!(state.phase, Phase::Preparing) {
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
            .phase = Phase::Preparing;
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
    pub(super) async fn prompt<F: Future>(&self, session: String, work: F) -> Option<F::Output> {
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
            state.phase = Phase::Prompting(session.clone());
            first = false;
            drop(state);
            work.as_mut().poll(cx).map(Some)
        })
        .await
    }
}

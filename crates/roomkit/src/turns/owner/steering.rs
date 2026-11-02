//! Steering one turn from its owner (RFC §10.6.8): a `steer` crosses to the
//! turn's executor, which applies it through the channel, and its
//! `steer_result` says how many loops it reached.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, Weak};

use tokio::sync::oneshot;

use super::{OwnerState, cancel};
use crate::steering::SteeringDirective;
use crate::turns::lock;
use crate::turns::wire::Control;

/// The reason a steering `Cancel` cuts a turn with when it gives none.
const STEERED: &str = "steered";

/// The `steer` messages of one turn not answered yet, by their id.
#[derive(Debug, Default)]
pub(crate) struct Steers {
    waiting: Mutex<HashMap<String, oneshot::Sender<Option<u64>>>>,
}

impl Steers {
    /// The executor's `steer_result` for `steer_id`.
    pub(super) fn answer(&self, steer_id: &str, loops: Option<u64>) {
        if let Some(waiting) = lock(&self.waiting).remove(steer_id) {
            // A steer whose caller stopped waiting is answered to nobody.
            let _unread = waiting.send(loops);
        }
    }

    /// The turn is over: every `steer` waiting reached nothing.
    pub(super) fn end(&self) {
        for (_, waiting) in lock(&self.waiting).drain() {
            let _unread = waiting.send(Some(0));
        }
    }
}

/// A turn the owner started, as steering reaches it: until it is over.
#[derive(Clone, Debug)]
pub(crate) struct Steerable {
    state: Weak<OwnerState>,
}

impl Steerable {
    pub(super) fn new(state: &Arc<OwnerState>) -> Self {
        Self {
            state: Arc::downgrade(state),
        }
    }

    /// Whether the turn is over for the owner.
    pub(crate) fn over(&self) -> bool {
        self.state
            .upgrade()
            .is_none_or(|state| *state.over.borrow())
    }

    /// Sends `directive` to the turn's loop: how many loops it reached.
    pub(crate) async fn steer(&self, directive: &SteeringDirective) -> usize {
        match self.state.upgrade() {
            Some(state) => steer(&state, directive).await,
            None => 0,
        }
    }
}

/// Sends `directive` to the loop of the turn `state` holds: how many loops
/// it reached. A channel without steering takes a `Cancel` as the turn's
/// cancel, which counts as one; the owner then waits for the turn's end as
/// after a close, without the cancel bound (RFC §10.6.8).
async fn steer(state: &Arc<OwnerState>, directive: &SteeringDirective) -> usize {
    let loops = match sent(state, directive) {
        Some(answer) => answer.await.unwrap_or(Some(0)),
        None => Some(0),
    };
    if let Some(loops) = loops {
        return usize::try_from(loops).unwrap_or(usize::MAX);
    }
    let SteeringDirective::Cancel(cut) = directive else {
        return 0;
    };
    if *state.over.borrow() {
        return 0;
    }
    state.steer_cut.store(true, Ordering::Release);
    let reason = if cut.reason.is_empty() {
        STEERED
    } else {
        &cut.reason
    };
    cancel(state, reason);
    1
}

/// Sends `steer` for `directive` on the turn's link: its answer to come;
/// none when the turn is over, or the message cannot cross.
fn sent(
    state: &OwnerState,
    directive: &SteeringDirective,
) -> Option<oneshot::Receiver<Option<u64>>> {
    if *state.over.borrow() {
        return None;
    }
    let steer_id = uuid::Uuid::new_v4().simple().to_string();
    let (answer, answered) = oneshot::channel();
    lock(&state.steers.waiting).insert(steer_id.clone(), answer);
    // The turn may have ended since: its pump answered every steer it held
    // after it said so, which this one may have missed.
    let control = Control::Steer {
        steer_id: steer_id.clone(),
        directive: directive.clone(),
    };
    if *state.over.borrow() || state.turn.controls.put(&control).is_err() {
        lock(&state.steers.waiting).remove(&steer_id);
        return None;
    }
    Some(answered)
}

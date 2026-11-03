//! A delivery set's turn across processes (RFC §13.5 "Delivery lanes across
//! processes"), as the Python implementation's `RoomDeliveryLane` takes it.
//!
//! Within the process, a set's turn comes in its room's lane (`queues`).
//! Across processes sharing a store, the room's delivery cursor orders the
//! sets: a set runs once the cursor passed every index before its own that
//! this process did not commit, and the sets of one room run under its
//! delivery claim, a lock of the kit's lock manager on a key of its own,
//! held by the process while one of its sets runs and until the cursor
//! moved past it, never with the room lock.
//!
//! An index before a set's own that no lane runs (its process stopped
//! between the commit and the delivery, or it was committed straight to the
//! store) holds the cursor. Once the set has waited on it for the kit's
//! `delivery_gap_timeout`, the set takes the claim, which a lane that runs
//! the hole's set holds, reads the cursor again, forces it past the indexes
//! that are not this process's, never past one that is, and the kit emits
//! `delivery_skipped` with the indexes skipped (RFC §8.2).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use roomkit_core::model::{Metadata, RoomId};
use serde_json::Value;
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::Instant;

use crate::events::FrameworkEvent;
use crate::kit::Inner;
use crate::locks::RoomGuard;
use crate::store::{AdvanceDeliveredIndexOptions, ConversationStore};

/// The prefix of a room's delivery claim key, apart from the room locks'
/// keys, as Python's `DELIVERY_CLAIM_PREFIX`.
const DELIVERY_CLAIM_PREFIX: &str = "__delivery__:";

/// How long a set first waits between two reads of a cursor held by a hole,
/// doubled after each read up to [`GAP_BACKOFF_MAX`], as Python's
/// `LaneConfig.gap_backoff_initial`.
const GAP_BACKOFF_INITIAL: Duration = Duration::from_millis(50);

/// Python's `LaneConfig.gap_backoff_max`.
const GAP_BACKOFF_MAX: Duration = Duration::from_millis(250);

/// The rooms' delivery claims this process holds, each while one of its
/// sets of the room runs and until the cursor moved past it.
#[derive(Debug, Default)]
pub(crate) struct DeliveryClaims {
    rooms: Arc<Mutex<HashMap<RoomId, Arc<RoomClaim>>>>,
}

/// One room's claim: how many of the process's sets hold it, the lock that
/// holds it once one does, and the one taking it.
#[derive(Debug, Default)]
struct RoomClaim {
    held: Mutex<Held>,
    taking: AsyncMutex<()>,
}

#[derive(Debug, Default)]
struct Held {
    holders: usize,
    lock: Option<RoomGuard<'static>>,
}

/// A set's hold on its room's delivery claim; dropping the last one lets
/// the claim go.
#[derive(Debug)]
pub(crate) struct ClaimHold {
    room_id: RoomId,
    claim: Arc<RoomClaim>,
    claims: Arc<Mutex<HashMap<RoomId, Arc<RoomClaim>>>>,
    /// Whether the hold counts among the claim's holders: not while it
    /// waits for the claim, so that a wait cancelled leaves the others'.
    holds: bool,
}

impl RoomClaim {
    fn held(&self) -> MutexGuard<'_, Held> {
        self.held.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Counts one holder more when the process holds the claim already.
    fn join(&self) -> bool {
        let mut held = self.held();
        if held.holders == 0 {
            return false;
        }
        held.holders += 1;
        true
    }
}

impl DeliveryClaims {
    /// Holds `room_id`'s delivery claim: at once when one of the process's
    /// sets holds it, else once the lock manager grants its key.
    pub(crate) async fn hold<S>(&self, inner: &Inner<S>, room_id: &RoomId) -> ClaimHold {
        let claim = Arc::clone(rooms(&self.rooms).entry(room_id.clone()).or_default());
        let mut hold = ClaimHold {
            room_id: room_id.clone(),
            claim: Arc::clone(&claim),
            claims: Arc::clone(&self.rooms),
            holds: false,
        };
        hold.holds = claim.join();
        if hold.holds {
            return hold;
        }
        let taking = claim.taking.lock().await;
        hold.holds = claim.join();
        if hold.holds {
            return hold;
        }
        let lock = inner.locks.lock_owned(&claim_key(room_id)).await;
        let mut state = claim.held();
        state.holders = 1;
        state.lock = Some(lock);
        hold.holds = true;
        drop(state);
        drop(taking);
        hold
    }
}

impl Drop for ClaimHold {
    fn drop(&mut self) {
        if self.holds {
            let mut held = self.claim.held();
            held.holders = held.holders.saturating_sub(1);
            if held.holders > 0 {
                return;
            }
            drop(held.lock.take());
        }
        let mut rooms = rooms(&self.claims);
        // Dropped once no hold, and no set taking it, refers to it.
        let unused = rooms
            .get(&self.room_id)
            .is_some_and(|claim| Arc::ptr_eq(claim, &self.claim) && Arc::strong_count(claim) == 2);
        if unused {
            rooms.remove(&self.room_id);
        }
    }
}

fn rooms(
    rooms: &Mutex<HashMap<RoomId, Arc<RoomClaim>>>,
) -> MutexGuard<'_, HashMap<RoomId, Arc<RoomClaim>>> {
    rooms.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The lock manager's key for `room_id`'s delivery claim, as Python's
/// `delivery_claim_key`.
fn claim_key(room_id: &RoomId) -> RoomId {
    RoomId::new(format!("{DELIVERY_CLAIM_PREFIX}{room_id}")).unwrap_or_else(|_| room_id.clone())
}

/// Waits for the turn the room's cursor gives the set of `index`, which
/// this process committed in `room_id`, and holds the room's delivery
/// claim for it: once the cursor passed every index before `index` that
/// is not this process's, skipping those no lane ran within the kit's
/// `delivery_gap_timeout`.
///
/// A store that fails the cursor's read is logged, and the set takes its
/// turn: the cursor orders sets across processes, and within one the lane
/// orders them already.
pub(crate) async fn cursor_turn<S: ConversationStore>(
    inner: &Inner<S>,
    room_id: &RoomId,
    index: u64,
) -> ClaimHold {
    let mut waited_since: Option<(i64, Instant)> = None;
    let mut backoff = GAP_BACKOFF_INITIAL;
    loop {
        let Some(cursor) = read_cursor(inner, room_id).await else {
            return inner.claims.hold(inner, room_id).await;
        };
        if !inner.cursors.foreign_before(room_id, cursor, index) {
            return inner.claims.hold(inner, room_id).await;
        }
        // The gap clock runs while the cursor does not move.
        let since = match waited_since {
            Some((at, since)) if at == cursor => since,
            _ => {
                backoff = GAP_BACKOFF_INITIAL;
                waited_since.insert((cursor, Instant::now())).1
            }
        }
        .elapsed();
        if since >= inner.delivery_gap_timeout {
            let claim = inner.claims.hold(inner, room_id).await;
            let skipped = skip_gap(inner, room_id, index).await;
            drop(claim);
            if skipped {
                continue;
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(GAP_BACKOFF_MAX);
    }
}

/// Under the room's delivery claim, which a lane running the hole's set
/// holds: reads the cursor again, and forces it past the indexes after it
/// that are not this process's, up to the first that is, then has the
/// indexes this process's sets ended past them move it on: whether the
/// cursor moved.
async fn skip_gap<S: ConversationStore>(inner: &Inner<S>, room_id: &RoomId, index: u64) -> bool {
    let Some(cursor) = read_cursor(inner, room_id).await else {
        return false;
    };
    let target = inner.cursors.skip_target(room_id, cursor, index);
    let Ok(to) = u64::try_from(target) else {
        return false;
    };
    if target <= cursor {
        return false;
    }
    let options = AdvanceDeliveredIndexOptions { force: true };
    match inner
        .store
        .advance_delivered_index(room_id, to, options)
        .await
    {
        Ok(true) => {}
        Ok(false) => return false,
        Err(error) => {
            tracing::warn!(target: "roomkit::cursor", room_id = %room_id, %error, "the room's delivery gap could not be skipped");
            return false;
        }
    }
    let from = cursor + 1;
    tracing::warn!(
        target: "roomkit::cursor",
        room_id = %room_id,
        from_index = from,
        to_index = target,
        "delivery gap skipped: the indexes are declared lost"
    );
    let mut data = Metadata::new();
    data.insert("from_index".to_owned(), Value::from(from));
    data.insert("to_index".to_owned(), Value::from(target));
    let event = FrameworkEvent {
        r#type: "delivery_skipped".to_owned(),
        room_id: Some(room_id.clone()),
        channel_id: None,
        event_id: None,
        timestamp: inner.environment().now(),
        data,
    };
    if let Some(handled) = inner.events.emit(&inner.tasks, event) {
        handled.wait().await;
    }
    inner.cursors.catch_up(&*inner.store, room_id).await;
    true
}

/// The room's cursor, `None` when the store fails its read.
async fn read_cursor<S: ConversationStore>(inner: &Inner<S>, room_id: &RoomId) -> Option<i64> {
    match inner.store.get_delivered_index(room_id).await {
        Ok(cursor) => Some(cursor),
        Err(error) => {
            tracing::warn!(target: "roomkit::cursor", room_id = %room_id, %error, "the room's delivery cursor could not be read; the delivery goes on");
            None
        }
    }
}

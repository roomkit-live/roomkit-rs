//! A delivery set's turn across processes (RFC §13.5 "Delivery lanes across
//! processes"), as the Python implementation's `RoomDeliveryLane` takes it.
//!
//! Within the process, a set's turn comes in its room's lane (`queues`).
//! Across processes sharing a store, the room's delivery cursor orders the
//! sets: a set runs once the cursor passed every index before its own that
//! this process did not commit, and a set with no index (an instruction's,
//! an event the persistence policy keeps out, a rerun's) once the cursor
//! reached its anchor, the room's latest index when its place was taken.
//! The sets of one room run under its delivery claim, held by the process
//! while one of its sets runs and until the cursor moved past it, never
//! with the room lock. The claims are
//! taken from the claim manager (`RoomKitOptions::delivery_claim_lock_manager`,
//! the lock manager by default), an in-memory manager keeping them in a map
//! of their own, apart from the room locks, so that no room id, a room
//! named `__delivery__:r1` among them, takes another room's claim; kits
//! sharing a manager share their claims (RMK-1153, RMK-1281).
//!
//! An index before a set's own that no lane runs (its process stopped
//! between the commit and the delivery, or it was committed straight to the
//! store) holds the cursor. Once the cursor has stood still on it for the
//! kit's `delivery_gap_timeout`, the set takes the claim, which a lane that
//! runs the hole's set holds, reads the cursor again, forces it past the
//! indexes that are not this process's, never past one that is, and the
//! kit emits `delivery_skipped` with the indexes skipped (RFC §8.2). A set
//! whose own index another process's lane skipped is dropped, as
//! `delivery_stale`.
//!
//! A set belongs to the room it was placed in, its id and its `created_at`.
//! Once the store holds no room under that id, or one created again, the
//! set takes its turn in its lane's order: the cursor it would read, wait
//! on, skip or be dropped by is no longer its room's (RFC §13.5).

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use roomkit_core::model::{ErrorRecord, Metadata, RoomId};
use serde_json::Value;
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::Instant;

use crate::cursors::RoomCreatedAt;
use crate::events::{FrameworkEvent, Handled};
use crate::kit::Inner;
use crate::locks::{HeldLock, KitLocks};
use crate::store::{AdvanceDeliveredIndexOptions, ConversationStore};

/// How long a set first waits between two reads of a cursor held by a hole,
/// doubled after each read up to [`GAP_BACKOFF_MAX`], as Python's
/// `LaneConfig.gap_backoff_initial`.
const GAP_BACKOFF_INITIAL: Duration = Duration::from_millis(50);

/// Python's `LaneConfig.gap_backoff_max`.
const GAP_BACKOFF_MAX: Duration = Duration::from_millis(250);

/// The rooms' delivery claims this process holds, each while one of its
/// sets of the room runs and until the cursor moved past it.
#[derive(Debug)]
pub(crate) struct DeliveryClaims {
    rooms: Arc<Mutex<HashMap<RoomId, Arc<RoomClaim>>>>,
    /// The claims' locks, from the claim manager's key space of its own:
    /// apart from the room locks, so that a room's claim and any room's
    /// lock never share a key.
    locks: KitLocks,
}

/// The kind of the delivery claims' locks, the lock manager's key space of
/// their own (Python's `_key_space("delivery")`).
pub(crate) const DELIVERY_KIND: &str = "delivery";

/// The prefix of a room's delivery claim key, as Python's
/// `DELIVERY_CLAIM_PREFIX`.
const DELIVERY_CLAIM_PREFIX: &str = "__delivery__:";

/// One room's claim: how many of the process's sets hold it, the lock that
/// holds it once one does, and the one taking it.
#[derive(Debug, Default)]
struct RoomClaim {
    held: Mutex<Held>,
    taking: AsyncMutex<()>,
}

#[derive(Default)]
struct Held {
    holders: usize,
    lock: Option<HeldLock>,
}

impl fmt::Debug for Held {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Held")
            .field("holders", &self.holders)
            .field("lock", &self.lock.is_some())
            .finish()
    }
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
    pub(crate) fn new(locks: KitLocks) -> Self {
        Self {
            rooms: Arc::default(),
            locks,
        }
    }

    /// Holds `room_id`'s delivery claim: at once when one of the process's
    /// sets holds it, else once the claim manager grants its key.
    pub(crate) async fn hold(&self, room_id: &RoomId) -> ClaimHold {
        let mut hold = ClaimHold {
            room_id: room_id.clone(),
            claim: Arc::clone(rooms(&self.rooms).entry(room_id.clone()).or_default()),
            claims: Arc::clone(&self.rooms),
            holds: false,
        };
        if hold.claim.join() {
            hold.holds = true;
            return hold;
        }
        // Cancelled while it waits, the hold counts among no holders, and
        // its drop forgets the room's claim nobody else refers to.
        let taking = hold.claim.taking.lock().await;
        if !hold.claim.join() {
            let lock = self.take(room_id).await;
            let mut state = hold.claim.held();
            state.holders = 1;
            state.lock = Some(lock);
        }
        drop(taking);
        hold.holds = true;
        hold
    }
}

impl DeliveryClaims {
    /// The claim manager's lock of `room_id`'s claim, asked again while
    /// the manager fails to grant it, after a pause doubled from
    /// [`GAP_BACKOFF_INITIAL`] up to [`GAP_BACKOFF_MAX`], as Python's lane
    /// asks again at its next round: the kit's close cuts the wait. Only
    /// the manager's error is asked again: the claim's key cannot fail.
    async fn take(&self, room_id: &RoomId) -> HeldLock {
        let key = claim_key(room_id);
        let mut pause = GAP_BACKOFF_INITIAL;
        loop {
            match self.locks.lock(&key).await {
                Ok(lock) => return lock,
                Err(error) => {
                    tracing::warn!(target: "roomkit::lanes", room_id = %room_id, %error, "taking the room's delivery claim failed; asking again");
                    tokio::time::sleep(pause).await;
                    pause = (pause * 2).min(GAP_BACKOFF_MAX);
                }
            }
        }
    }
}

/// The lock manager's key for `room_id`'s delivery claim, as Python's
/// `delivery_claim_key`, never the room's own. It cannot fail, as
/// Python's f-string cannot.
fn claim_key(room_id: &RoomId) -> RoomId {
    room_id.prefixed(DELIVERY_CLAIM_PREFIX)
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

/// What a set's turn from the room's cursor waits for (RFC §13.5).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SetTurn {
    /// The set of an index this process committed: it runs once the cursor
    /// passed every index before its own, moves the cursor past its own,
    /// and is dropped once another process's lane skipped its own.
    Index(u64),
    /// A set with no index, anchored at the room's latest index when its
    /// place was taken, `-1` for a room with no event (Python's
    /// `anchor_index`): it runs once the cursor has reached its anchor,
    /// and moves the cursor no further (RFC §10.1.1 rule 5).
    After(i64),
}

impl SetTurn {
    /// The first index the set's turn does not wait for: its own, or the
    /// one after its anchor.
    fn bound(self) -> u64 {
        match self {
            Self::Index(index) => index,
            Self::After(anchor) => u64::try_from(anchor.saturating_add(1)).unwrap_or(0),
        }
    }

    /// Whether the cursor at `cursor` passed the set's own index: another
    /// process's lane skipped it. A set with no index has none to skip.
    fn skipped_at(self, cursor: i64) -> bool {
        match self {
            Self::Index(index) => cursor >= i64::try_from(index).unwrap_or(i64::MAX),
            Self::After(_) => false,
        }
    }
}

/// A set's turn from the room's cursor.
pub(crate) enum CursorTurn {
    /// The set runs, under the room's delivery claim, once the handlers of
    /// the `delivery_skipped` its wait emitted are over.
    Turn(Turned),
    /// The cursor passed the set's own index: another process's lane
    /// skipped it, and the set is dropped.
    Stale,
}

/// The room's delivery claim held for a set, and the handlers of the
/// skips its wait emitted, which the set waits for once it left its place:
/// a handler that calls into the room is not held behind the set.
pub(crate) struct Turned {
    pub(crate) claim: ClaimHold,
    pub(crate) skipped: Vec<Handled>,
}

/// Waits for the turn the room's cursor gives a set of `room_id`, created
/// at `created_at`, `turn`, and holds the room's delivery claim for it:
/// once the cursor passed every index before the set's own, or up to its
/// anchor, that is not this process's, skipping those no lane ran within
/// the kit's `delivery_gap_timeout`; at once when its room is gone.
///
/// The gap clock runs while the cursor does not move. A store that fails
/// the cursor's read is logged and read again after the same wait as a
/// hole, as Python's lane retries a round that failed.
pub(crate) async fn cursor_turn<S: ConversationStore>(
    inner: &Inner<S>,
    room_id: &RoomId,
    turn: SetTurn,
    created_at: RoomCreatedAt,
) -> CursorTurn {
    let index = turn.bound();
    let mut skipped = Vec::new();
    let mut waited_since: Option<(i64, Instant)> = None;
    let mut backoff = GAP_BACKOFF_INITIAL;
    loop {
        retry_owed_cursor(inner, room_id).await;
        let Some(cursor) = read_cursor(inner, room_id).await else {
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(GAP_BACKOFF_MAX);
            continue;
        };
        let moved = waited_since.is_none_or(|(at, _)| at != cursor);
        match at_cursor(inner, room_id, turn, created_at, cursor, moved).await {
            AtCursor::Stale => return CursorTurn::Stale,
            AtCursor::Turn => {
                let claim = inner.claims.hold(room_id).await;
                return CursorTurn::Turn(Turned { claim, skipped });
            }
            AtCursor::Wait => {}
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
            let claim = inner.claims.hold(room_id).await;
            // Never a skip of the cursor of a room created again.
            if room_gone(inner, room_id, created_at).await {
                return CursorTurn::Turn(Turned { claim, skipped });
            }
            let skip = skip_gap(inner, room_id, index).await;
            drop(claim);
            if let Some(handled) = skip {
                skipped.extend(handled);
                continue;
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(GAP_BACKOFF_MAX);
    }
}

/// What a read of the room's cursor gives a set.
enum AtCursor {
    /// The set takes its turn.
    Turn,
    /// Another process's lane skipped the set's index: it is dropped.
    Stale,
    /// The set waits for an index that is not this process's.
    Wait,
}

/// What the room's cursor, read at `cursor`, gives the set of `room_id`,
/// created at `created_at`, `turn`. The room is read before the set is
/// dropped as stale, and before it waits: at each read of a cursor at
/// `-1`, which a deleted room's reads for good, and once at each other
/// value the cursor stands at (`moved`), which a room created again may
/// have. A set whose room is gone takes its turn.
async fn at_cursor<S: ConversationStore>(
    inner: &Inner<S>,
    room_id: &RoomId,
    turn: SetTurn,
    created_at: RoomCreatedAt,
    cursor: i64,
    moved: bool,
) -> AtCursor {
    let index = turn.bound();
    if turn.skipped_at(cursor) {
        if room_gone(inner, room_id, created_at).await {
            return AtCursor::Turn;
        }
        tracing::warn!(target: "roomkit::cursor", room_id = %room_id, index, "delivery dropped (delivery_stale): another lane skipped its index");
        return AtCursor::Stale;
    }
    if !inner.cursors.foreign_before(room_id, cursor, index)
        || ((cursor < 0 || moved) && room_gone(inner, room_id, created_at).await)
    {
        return AtCursor::Turn;
    }
    AtCursor::Wait
}

/// Whether the set's room, created at `created_at`, is gone from the
/// store: no room under its id, which reads its cursor at `-1` for good, or
/// one created again, whose cursor is another room's. No lane fills or
/// skips the set's holes any more, so the set, with an index or without,
/// takes its turn in its lane's order, and its run finds the room gone (a
/// rerun is refused, RFC §5.1), as Python's lane reads such a cursor just
/// before its next set (RFC §13.5). A store that fails the read leaves the
/// set waiting.
async fn room_gone<S: ConversationStore>(
    inner: &Inner<S>,
    room_id: &RoomId,
    created_at: RoomCreatedAt,
) -> bool {
    match inner.store.get_room(room_id).await {
        Ok(None) => true,
        Ok(Some(room)) => created_at.is_some_and(|created_at| room.created_at != created_at),
        Err(_) => false,
    }
}

/// Before waiting on another worker, finish any cursor write this worker
/// owes. Otherwise its completed index holds that worker behind it, until
/// the other worker mistakes the failed write for a lost delivery.
async fn retry_owed_cursor<S: ConversationStore>(inner: &Inner<S>, room_id: &RoomId) {
    if !inner.cursors.owes_write(room_id) {
        return;
    }
    let claim = inner.claims.hold(room_id).await;
    inner.cursors.catch_up(&*inner.store, room_id).await;
    drop(claim);
}

/// The anchor of a set with no index whose place is taken in `room_id`'s
/// lane, read under the room's lock: the room's latest index, `-1` for a
/// room with no event, as Python's `anchor_index` reads it when the set's
/// plan is made. A read the store fails is logged, and is the failure the
/// place is answered with: the set waits for no other process's index, and
/// the failure is its cascade's error (RFC §10.1.1 rule 5).
pub(crate) async fn anchor<S: ConversationStore>(
    inner: &Inner<S>,
    room_id: &RoomId,
) -> Result<i64, ErrorRecord> {
    match inner.store.get_room(room_id).await {
        Ok(room) => Ok(room
            .and_then(|room| room.latest_index)
            .map_or(-1, |latest| i64::try_from(latest).unwrap_or(i64::MAX))),
        Err(error) => {
            tracing::warn!(target: "roomkit::cursor", room_id = %room_id, %error, "the room's latest index could not be read; the set waits for no other process's delivery");
            Err(error.to_record())
        }
    }
}

/// Under the room's delivery claim, which a lane running the hole's set
/// holds: reads the cursor again, and forces it past the indexes before
/// `index`, the first the set does not wait for, that are not this
/// process's, up to the first that is, then has the indexes this process's
/// sets ended past them move it on. `None` when the cursor did not move;
/// else the handlers of the `delivery_skipped` the kit emitted, when it has
/// any.
async fn skip_gap<S: ConversationStore>(
    inner: &Inner<S>,
    room_id: &RoomId,
    index: u64,
) -> Option<Option<Handled>> {
    let cursor = read_cursor(inner, room_id).await?;
    let target = inner.cursors.skip_target(room_id, cursor, index);
    let to = u64::try_from(target).ok()?;
    if target <= cursor {
        return None;
    }
    let options = AdvanceDeliveredIndexOptions { force: true };
    match inner
        .store
        .advance_delivered_index(room_id, to, options)
        .await
    {
        Ok(true) => {}
        Ok(false) => return None,
        Err(error) => {
            tracing::warn!(target: "roomkit::cursor", room_id = %room_id, %error, "the room's delivery gap could not be skipped");
            return None;
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
    let handled = inner.events.emit(&inner.tasks, event);
    inner.cursors.catch_up(&*inner.store, room_id).await;
    Some(handled)
}

/// The room's cursor, `None` when the store fails its read.
async fn read_cursor<S: ConversationStore>(inner: &Inner<S>, room_id: &RoomId) -> Option<i64> {
    match inner.store.get_delivered_index(room_id).await {
        Ok(cursor) => Some(cursor),
        Err(error) => {
            tracing::warn!(target: "roomkit::cursor", room_id = %room_id, %error, "the room's delivery cursor could not be read; it is read again");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use roomkit_core::model::Room;

    use super::*;
    use crate::lock_manager::{InMemoryLockManager, RoomLockManager, SharedLockManager, key_space};
    use crate::{RoomKit, RoomKitOptions};

    fn room_id() -> RoomId {
        RoomId::new("room_1").unwrap()
    }

    /// The claims of a kit whose lock manager is its own in-memory one.
    fn in_memory() -> DeliveryClaims {
        let manager: Arc<dyn RoomLockManager> = Arc::new(InMemoryLockManager::default());
        DeliveryClaims::new(KitLocks::new(key_space(&manager, DELIVERY_KIND)))
    }

    /// The claim manager's key of `room_id()`'s claim.
    fn key() -> RoomId {
        claim_key(&room_id())
    }

    fn rooms_held(claims: &DeliveryClaims) -> usize {
        rooms(&claims.rooms).len()
    }

    /// Holds `room_id`'s claim, or fails the test when it waits.
    async fn held_at_once(claims: &DeliveryClaims, room_id: &RoomId) -> ClaimHold {
        tokio::time::timeout(Duration::from_secs(5), claims.hold(room_id))
            .await
            .expect("the claim is held at once")
    }

    #[tokio::test]
    async fn a_hold_cancelled_while_it_waits_leaves_nothing_behind() {
        let claims = Arc::new(in_memory());
        // Another process holds the room's claim.
        let other = claims.locks.lock(&key()).await.unwrap();
        let waiting = {
            let claims = Arc::clone(&claims);
            tokio::spawn(async move {
                drop(claims.hold(&room_id()).await);
            })
        };
        while claims.locks.waiting(&key()) == 0 {
            tokio::task::yield_now().await;
        }
        waiting.abort();
        assert!(waiting.await.unwrap_err().is_cancelled());
        assert_eq!(
            rooms_held(&claims),
            0,
            "the cancelled hold forgets the room"
        );
        drop(other);
    }

    #[tokio::test]
    async fn the_process_s_sets_share_the_room_s_claim() {
        let claims = in_memory();
        let first = claims.hold(&room_id()).await;
        let second = held_at_once(&claims, &room_id()).await;
        drop(first);
        assert_eq!(
            claims.locks.waiting(&room_id()),
            0,
            "nobody waits for the claim"
        );
        drop(second);
        assert_eq!(rooms_held(&claims), 0, "the last hold forgets the room");
    }

    /// A room id is the integrator's or a sender's: the room named
    /// `__delivery__:room_1` takes its own lock, never `room_1`'s claim
    /// (security review of RMK-1153).
    // Python: tests/test_lanes_multiworker.py::test_a_room_named_like_a_delivery_claim_holds_no_claim,
    // with the in-memory manager and one shared across processes.
    #[tokio::test]
    async fn a_room_named_like_a_claim_shares_no_lock_with_it() {
        let shared: Arc<dyn RoomLockManager> = Arc::new(SharedLockManager::default());
        for lock_manager in [None, Some(shared)] {
            let kit = RoomKit::new(RoomKitOptions {
                lock_manager,
                ..RoomKitOptions::default()
            });
            let named = RoomId::new(format!("__delivery__:{}", room_id())).unwrap();
            let section = kit.inner.locks.lock(&named).await.unwrap();
            let claim = held_at_once(&kit.inner.claims, &room_id()).await;
            drop((claim, section));
            kit.close().await.unwrap();
        }
    }

    /// A set of a room deleted and created again under its id, the new
    /// room's cursor standing past the set's index, takes its turn: that
    /// cursor is another room's, which skipped nothing of the set
    /// (RMK-1707, RFC §13.5).
    // Python: tests/test_lanes.py::test_the_entries_of_a_deleted_room_take_their_turn[behind_holes_created_again_past_them]
    #[tokio::test]
    async fn a_set_of_a_room_created_again_past_its_index_is_not_stale() {
        let kit = RoomKit::new(RoomKitOptions::default());
        let (store, room) = (kit.store(), room_id());
        let deleted = store.create_room(Room::new(room.clone())).await.unwrap();
        store.delete_room(&room).await.unwrap();
        let mut again = Room::new(room.clone());
        again.created_at = deleted
            .created_at
            .checked_add(Duration::from_millis(1))
            .unwrap();
        store.create_room(again).await.unwrap();
        let options = AdvanceDeliveredIndexOptions { force: true };
        let moved = store.advance_delivered_index(&room, 3, options);
        assert!(moved.await.unwrap());
        let created_at = Some(deleted.created_at);
        let turn = cursor_turn(&kit.inner, &room, SetTurn::Index(2), created_at);
        let turn = tokio::time::timeout(Duration::from_secs(5), turn)
            .await
            .unwrap();
        assert!(
            matches!(turn, CursorTurn::Turn(_)),
            "the set takes its turn"
        );
        drop(turn);
        kit.close().await.unwrap();
    }

    /// A room's claim never takes the room's own lock, whatever its id
    /// (security review of RMK-1153).
    #[tokio::test]
    async fn a_room_s_claim_is_never_its_own_lock() {
        let kit = RoomKit::new(RoomKitOptions::default());
        let long = RoomId::new("r".repeat(4096)).unwrap();
        for room in [room_id(), long] {
            let section = kit.inner.locks.lock(&room).await.unwrap();
            let claim = held_at_once(&kit.inner.claims, &room).await;
            drop((claim, section));
        }
        kit.close().await.unwrap();
    }
}

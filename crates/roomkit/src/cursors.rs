//! The rooms' delivery cursors, moved as their lanes go (RFC §13.5
//! "Delivery lanes across processes", §5.1 `delivered_index`, gap G-091).
//!
//! A room's cursor is the store's: the highest index whose delivery set has
//! run, moved by `advance_delivered_index`, a compare-and-set from the
//! index before. Every index committed reaches it once its turn is over:
//! an event's once its set left its place in the room's lane, an index
//! with no delivery set (a record stored `BLOCKED`, a lifecycle record, a
//! record committed outside the pipeline) once the section that committed
//! it let the room's lock go. The indexes whose turn is over wait here
//! until the cursor reaches the one before them, so the cursor moves one
//! index at a time, in order, as the Python implementation's lane moves
//! it.
//!
//! The lane orders a room's delivery sets within the process. Across
//! processes, a set also waits for the cursor to pass every index before
//! its own that this process did not commit (`gaps`): those are another
//! process's, or no lane's, and skipped after `gap_timeout`. An index this
//! process committed whose turn never comes (its run ended before its set
//! did, as the kit's shutdown ends it) holds the cursor below it.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use roomkit_core::model::RoomId;
use tokio::sync::Mutex as AsyncMutex;

use crate::store::{AdvanceDeliveredIndexOptions, ConversationStore};

/// Each room's indexes whose turn is over, waiting for its cursor.
#[derive(Debug, Default)]
pub(crate) struct RoomCursors {
    rooms: Mutex<HashMap<RoomId, Arc<Cursor>>>,
}

/// One room's indexes waiting for its cursor, and its one writer.
#[derive(Debug, Default)]
struct Cursor {
    ready: Mutex<BTreeSet<u64>>,
    /// The indexes this process committed that the cursor has not passed:
    /// a set waits for no other process's lane on them.
    local: Mutex<BTreeSet<u64>>,
    /// Held while the cursor is moved: one compare-and-set at a time.
    writing: AsyncMutex<()>,
}

impl Cursor {
    fn ready(&self) -> MutexGuard<'_, BTreeSet<u64>> {
        self.ready.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn local(&self) -> MutexGuard<'_, BTreeSet<u64>> {
        self.local.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The index after `cursor`, when its turn is over; the indexes at or
    /// below the cursor are forgotten.
    fn next_after(&self, cursor: i64) -> Option<u64> {
        let next = u64::try_from(cursor.saturating_add(1)).ok()?;
        let mut local = self.local();
        *local = local.split_off(&next);
        drop(local);
        let mut ready = self.ready();
        *ready = ready.split_off(&next);
        ready.contains(&next).then_some(next)
    }
}

impl RoomCursors {
    /// Records that this process committed `index` in `room_id`.
    pub(crate) fn committed(&self, room_id: &RoomId, index: u64) {
        let cursor = Arc::clone(self.rooms().entry(room_id.clone()).or_default());
        cursor.local().insert(index);
    }

    /// Whether an index after `cursor` and before `index` in `room_id` is
    /// not this process's: another process's, or no lane's.
    pub(crate) fn foreign_before(&self, room_id: &RoomId, cursor: i64, index: u64) -> bool {
        let Ok(first) = u64::try_from(cursor.saturating_add(1)) else {
            return false;
        };
        if first >= index {
            return false;
        }
        let local = self
            .rooms()
            .get(room_id)
            .map_or(0, |held| held.local().range(first..index).count());
        u64::try_from(local).unwrap_or(u64::MAX) < index - first
    }

    /// The highest cursor a skip of the indexes after `cursor` that are not
    /// this process's may set for the set of `index`: below the first index
    /// after `cursor` this process committed, `index` itself at most, so
    /// that no skip passes a set this process holds.
    pub(crate) fn skip_target(&self, room_id: &RoomId, cursor: i64, index: u64) -> i64 {
        let first = u64::try_from(cursor.saturating_add(1)).unwrap_or(0);
        let own = self
            .rooms()
            .get(room_id)
            .and_then(|held| held.local().range(first..).next().copied())
            .map_or(index, |own| own.min(index));
        i64::try_from(own).unwrap_or(i64::MAX) - 1
    }

    /// Records that the turn of `indexes` in `room_id` is over.
    pub(crate) fn over(&self, room_id: &RoomId, indexes: impl IntoIterator<Item = u64>) {
        let cursor = Arc::clone(self.rooms().entry(room_id.clone()).or_default());
        cursor.ready().extend(indexes);
    }

    /// Moves `room_id`'s cursor in `store` past every index whose turn is
    /// over, one at a time from the index after it, and stops at the first
    /// whose turn is not. A store that fails is logged, and the cursor
    /// waits for the room's next index whose turn is over.
    pub(crate) async fn catch_up<S: ConversationStore>(&self, store: &S, room_id: &RoomId) {
        let Some(cursor) = self.rooms().get(room_id).cloned() else {
            return;
        };
        let writing = cursor.writing.lock().await;
        if let Err(error) = Self::advance(&cursor, store, room_id).await {
            tracing::warn!(
                target: "roomkit::cursor",
                room_id = %room_id,
                %error,
                "the room's delivery cursor could not be moved; its delivery goes on"
            );
        }
        drop(writing);
        self.forget_if_idle(room_id, &cursor);
    }

    async fn advance<S: ConversationStore>(
        cursor: &Cursor,
        store: &S,
        room_id: &RoomId,
    ) -> Result<(), crate::store::StoreError> {
        let mut at = store.get_delivered_index(room_id).await?;
        while let Some(next) = cursor.next_after(at) {
            let options = AdvanceDeliveredIndexOptions::default();
            if !store
                .advance_delivered_index(room_id, next, options)
                .await?
            {
                let moved = store.get_delivered_index(room_id).await?;
                // Another writer moved it, or the room is gone: nothing
                // this one can do moves it from here.
                if moved == at {
                    return Ok(());
                }
                at = moved;
                continue;
            }
            at = i64::try_from(next).unwrap_or(i64::MAX);
        }
        Ok(())
    }

    /// Drops the room's entry once nothing waits in it and nobody else
    /// holds it, so rooms that come and go leave nothing behind.
    fn forget_if_idle(&self, room_id: &RoomId, cursor: &Arc<Cursor>) {
        let mut rooms = self.rooms();
        let idle = rooms.get(room_id).is_some_and(|held| {
            Arc::ptr_eq(held, cursor)
                && Arc::strong_count(held) == 2
                && cursor.ready().is_empty()
                && cursor.local().is_empty()
        });
        if idle {
            rooms.remove(room_id);
        }
    }

    fn rooms(&self) -> MutexGuard<'_, HashMap<RoomId, Arc<Cursor>>> {
        self.rooms.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn room_id() -> RoomId {
        RoomId::new("room_1").unwrap()
    }

    // Python: tests/test_lanes.py::test_skip_never_jumps_past_a_local_plan
    #[test]
    fn a_skip_never_passes_an_index_this_process_committed() {
        let cursors = RoomCursors::default();
        cursors.committed(&room_id(), 3);
        cursors.committed(&room_id(), 5);
        assert!(
            cursors.foreign_before(&room_id(), 0, 5),
            "1, 2 and 4 are not ours"
        );
        assert_eq!(
            cursors.skip_target(&room_id(), 0, 5),
            2,
            "the skip stops below index 3, this process's"
        );
        assert_eq!(
            cursors.skip_target(&room_id(), 3, 5),
            4,
            "past our 3, the skip stops below our 5"
        );
    }

    #[test]
    fn an_index_this_process_committed_is_no_hole() {
        let cursors = RoomCursors::default();
        for index in 0..4 {
            cursors.committed(&room_id(), index);
        }
        assert!(!cursors.foreign_before(&room_id(), -1, 3));
        assert!(
            cursors.foreign_before(&room_id(), -1, 5),
            "index 4 is not this process's"
        );
        assert!(
            !cursors.foreign_before(&RoomId::new("room_2").unwrap(), 1, 2),
            "nothing lies between the cursor and the index after it"
        );
    }
}

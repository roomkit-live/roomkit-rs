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
//! The lane orders a room's delivery sets within the process; nothing in
//! it reads the cursor. An index whose turn never comes (its run ended
//! before its set did, as the kit's shutdown ends it) holds the cursor
//! below it: §13.5's skip of such a gap after `gap_timeout`, across
//! processes, is not implemented.

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
    /// Held while the cursor is moved: one compare-and-set at a time.
    writing: AsyncMutex<()>,
}

impl Cursor {
    fn ready(&self) -> MutexGuard<'_, BTreeSet<u64>> {
        self.ready.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The index after `cursor`, when its turn is over; the indexes at or
    /// below the cursor are forgotten.
    fn next_after(&self, cursor: i64) -> Option<u64> {
        let next = u64::try_from(cursor.saturating_add(1)).ok()?;
        let mut ready = self.ready();
        *ready = ready.split_off(&next);
        ready.contains(&next).then_some(next)
    }
}

impl RoomCursors {
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
            Arc::ptr_eq(held, cursor) && Arc::strong_count(held) == 2 && cursor.ready().is_empty()
        });
        if idle {
            rooms.remove(room_id);
        }
    }

    fn rooms(&self) -> MutexGuard<'_, HashMap<RoomId, Arc<Cursor>>> {
        self.rooms.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

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

    /// Whether a completed local index still waits to be written. A new
    /// set retries these writes before deciding that another index is lost.
    pub(crate) fn owes_write(&self, room_id: &RoomId) -> bool {
        self.rooms()
            .get(room_id)
            .is_some_and(|cursor| !cursor.ready().is_empty())
    }

    /// Moves `room_id`'s cursor in `store` past every index whose turn is
    /// over, one at a time from the index after it, and stops at the first
    /// whose turn is not. A store that fails is logged, and the cursor
    /// is retried when a set next asks for its turn or finishes its own.
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
        Self::forget_stale(&cursor, store, room_id).await;
        drop(writing);
        self.forget_if_idle(room_id, &cursor);
    }

    /// Forgets the indexes above the room's latest one, which are none of
    /// its own: those of a room the store no longer holds, whose cursor
    /// nothing moves any more, or of the room deleted before a room created
    /// again under its id, which gives them out anew (RFC §13.5). Read only
    /// while indexes still wait for the cursor; a store that fails the read
    /// leaves them, and so is an index given out while the room is read,
    /// which the room's latest index read before may not count yet.
    async fn forget_stale<S: ConversationStore>(cursor: &Cursor, store: &S, room_id: &RoomId) {
        if cursor.ready().is_empty() {
            return;
        }
        let mut known = cursor.ready().clone();
        known.extend(cursor.local().iter().copied());
        let latest = match store.get_room(room_id).await {
            Ok(Some(room)) => room.latest_index,
            Ok(None) => None,
            Err(_) => return,
        };
        let stale =
            |index: &u64| known.contains(index) && latest.is_none_or(|latest| *index > latest);
        cursor.ready().retain(|index| !stale(index));
        cursor.local().retain(|index| !stale(index));
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
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use roomkit_core::model::{
        ChannelBinding, ChannelId, ChannelType, EventId, EventSource, Identity, IdentityId,
        Observation, Participant, ParticipantId, Room, RoomEvent, Task, TaskId, TextContent,
        ThreadSummary,
    };
    use tokio::sync::Notify;

    use super::*;
    use crate::InMemoryStore;
    use crate::store::{self, StoreError};

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

    /// The indexes of a room deleted while its sets ran are none of a room
    /// created again under its id, which gives them out anew (RMK-1706).
    #[tokio::test]
    async fn a_deleted_room_s_indexes_are_forgotten() {
        let store = InMemoryStore::new();
        let cursors = RoomCursors::default();
        for index in 0..5 {
            cursors.committed(&room_id(), index);
        }
        // The sets of 0 to 3 ended once the room was gone: their cursor
        // writes find no room.
        cursors.over(&room_id(), 0..4);
        cursors.catch_up(&store, &room_id()).await;
        assert!(
            !cursors.owes_write(&room_id()),
            "no write is owed to a gone room"
        );
        assert!(
            cursors.foreign_before(&room_id(), -1, 4),
            "0 to 3 are no longer this process's"
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

    /// An in-memory store whose next read of a room, once armed, reads it,
    /// then tells `entered` and waits for `go` before it answers.
    #[derive(Debug, Default)]
    struct HeldRead {
        store: InMemoryStore,
        armed: AtomicBool,
        entered: Notify,
        go: Notify,
    }

    /// Forwards each of the store's methods named to the in-memory store.
    macro_rules! forward {
        ($($name:ident($($argument:ident: $kind:ty),*) -> $answer:ty;)*) => {
            $(
                async fn $name(&self, $($argument: $kind),*) -> Result<$answer, StoreError> {
                    self.store.$name($($argument),*).await
                }
            )*
        };
    }

    impl ConversationStore for HeldRead {
        async fn get_room(&self, room_id: &RoomId) -> Result<Option<Room>, StoreError> {
            let room = self.store.get_room(room_id).await;
            if self.armed.swap(false, Ordering::SeqCst) {
                self.entered.notify_one();
                self.go.notified().await;
            }
            room
        }

        forward! {
            get_delivered_index(room_id: &RoomId) -> i64;
            create_room(room: Room) -> Room;
            update_room(room: Room) -> Room;
            delete_room(room_id: &RoomId) -> bool;
            list_rooms(options: store::ListRoomsOptions) -> Vec<Room>;
            find_rooms(options: store::FindRoomsOptions) -> Vec<Room>;
            advance_delivered_index(
                room_id: &RoomId,
                index: u64,
                options: AdvanceDeliveredIndexOptions
            ) -> bool;
            add_binding(binding: ChannelBinding) -> ChannelBinding;
            get_binding(room_id: &RoomId, channel_id: &ChannelId) -> Option<ChannelBinding>;
            update_binding(binding: ChannelBinding) -> ChannelBinding;
            remove_binding(room_id: &RoomId, channel_id: &ChannelId) -> bool;
            list_bindings(room_id: &RoomId) -> Vec<ChannelBinding>;
            list_participants(room_id: &RoomId) -> Vec<Participant>;
            add_participant(participant: Participant) -> Participant;
            get_participant(room_id: &RoomId, participant_id: &ParticipantId) -> Option<Participant>;
            update_participant(participant: Participant) -> Participant;
            delete_participant(room_id: &RoomId, participant_id: &ParticipantId) -> ();
            create_identity(identity: Identity) -> Identity;
            get_identity(identity_id: &IdentityId) -> Option<Identity>;
            resolve_identity(
                channel_type: &ChannelType,
                address: &str,
                options: store::ResolveIdentityOptions
            ) -> Option<Identity>;
            link_address(
                identity_id: &IdentityId,
                channel_type: &ChannelType,
                address: &str,
                options: store::LinkAddressOptions
            ) -> ();
            check_idempotency(room_id: &RoomId, key: &str, channel_id: Option<&ChannelId>) -> bool;
            commit_event(
                room_id: &RoomId,
                event: RoomEvent,
                options: store::CommitEventOptions
            ) -> RoomEvent;
            get_event(event_id: &EventId) -> Option<RoomEvent>;
            update_event(event: RoomEvent) -> RoomEvent;
            delete_event(
                room_id: &RoomId,
                event_id: &EventId,
                options: store::DeleteEventOptions
            ) -> Vec<RoomEvent>;
            get_event_count(room_id: &RoomId, options: store::GetEventCountOptions) -> u64;
            list_events(room_id: &RoomId, options: store::ListEventsOptions) -> Vec<RoomEvent>;
            add_task(task: Task) -> Task;
            add_observation(observation: Observation) -> Observation;
            list_tasks(room_id: &RoomId, options: store::ListTasksOptions) -> Vec<Task>;
            list_observations(room_id: &RoomId) -> Vec<Observation>;
            get_task(task_id: &TaskId) -> Option<Task>;
            update_task(task: Task) -> Task;
            add_event(event: RoomEvent) -> RoomEvent;
            mark_read(room_id: &RoomId, channel_id: &ChannelId, event_id: &EventId) -> ();
            mark_all_read(room_id: &RoomId, channel_id: &ChannelId) -> ();
            get_unread_count(room_id: &RoomId, channel_id: &ChannelId) -> u64;
            list_read_markers(room_id: &RoomId) -> std::collections::BTreeMap<ChannelId, u64>;
            find_latest_room(
                participant_id: &ParticipantId,
                options: store::FindLatestRoomOptions
            ) -> Option<Room>;
            find_room_id_by_channel(
                channel_id: &ChannelId,
                options: store::FindRoomIdByChannelOptions
            ) -> Option<RoomId>;
            get_thread_summaries(
                room_id: &RoomId,
                root_event_ids: &[EventId]
            ) -> std::collections::BTreeMap<EventId, ThreadSummary>;
        }
    }

    /// An index this process gives out while the room's latest index is
    /// read, behind indexes still waiting for the cursor, is kept: that read
    /// may not count it yet (light review of RMK-1706).
    #[tokio::test]
    async fn an_index_given_out_while_the_room_is_read_is_kept() {
        let bound = Duration::from_secs(5);
        let store = Arc::new(HeldRead::default());
        store.store.create_room(Room::new(room_id())).await.unwrap();
        for _ in 0..6 {
            let source = EventSource::new(ChannelId::new("sms").unwrap(), ChannelType::SMS);
            let event = RoomEvent::new(room_id(), source, TextContent::new("e"));
            let options = store::CommitEventOptions::default();
            store
                .store
                .commit_event(&room_id(), event, options)
                .await
                .unwrap();
        }
        let cursors = Arc::new(RoomCursors::default());
        for index in 0..6 {
            cursors.committed(&room_id(), index);
        }
        // 0 to 3 delivered; 5 is over, its write waits for 4's set.
        cursors.over(&room_id(), 0..4);
        cursors.catch_up(&store.store, &room_id()).await;
        cursors.over(&room_id(), [5]);
        store.armed.store(true, Ordering::SeqCst);
        let catching_up = {
            let (cursors, store) = (Arc::clone(&cursors), Arc::clone(&store));
            tokio::spawn(async move { cursors.catch_up(&*store, &room_id()).await })
        };
        tokio::time::timeout(bound, store.entered.notified())
            .await
            .unwrap();
        // Another section commits index 6 meanwhile, a record with no set,
        // after the read took the room's latest index.
        let source = EventSource::new(ChannelId::new("sms").unwrap(), ChannelType::SMS);
        let event = RoomEvent::new(room_id(), source, TextContent::new("e"));
        let options = store::CommitEventOptions::default();
        store
            .store
            .commit_event(&room_id(), event, options)
            .await
            .unwrap();
        cursors.committed(&room_id(), 6);
        cursors.over(&room_id(), [6]);
        store.go.notify_one();
        tokio::time::timeout(bound, catching_up)
            .await
            .unwrap()
            .unwrap();
        assert!(
            !cursors.foreign_before(&room_id(), 3, 7),
            "4, 5 and 6 are still this process's"
        );
    }
}

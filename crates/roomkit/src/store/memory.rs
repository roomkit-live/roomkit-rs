//! The in-memory store (RFC §14.2): for tests, prototypes and single-process
//! deployments.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, PoisonError};

use roomkit_core::model::{
    ChannelBinding, ChannelId, EventStatus, Observation, Participant, ParticipantId, Room,
    RoomContext, RoomEvent, RoomId, RoomUpdates, Task,
};
use roomkit_core::ops::RoomCounters;

use super::{ConversationStore, RoomFilter, StoreError};

/// A store that keeps everything in memory, for one process.
///
/// Every read returns copies and every write copies in, so no caller can
/// change stored state outside the store's methods (RFC §14.4).
#[derive(Debug, Default)]
pub struct InMemoryStore {
    rooms: Mutex<HashMap<RoomId, Record>>,
}

#[derive(Debug)]
struct Record {
    room: Room,
    bindings: Vec<ChannelBinding>,
    participants: Vec<Participant>,
    events: Vec<RoomEvent>,
    tasks: Vec<Task>,
    observations: Vec<Observation>,
}

impl Record {
    /// Whether the room has a participant with the id.
    fn has_participant(&self, participant_id: &ParticipantId) -> bool {
        self.participants
            .iter()
            .any(|participant| &participant.id == participant_id)
    }
}

impl InMemoryStore {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// The rooms, held only while a method runs; a method never awaits with
    /// them held. A panic while they were held leaves them as it found them
    /// or as one whole write left them, so the store keeps serving.
    fn rooms(&self) -> MutexGuard<'_, HashMap<RoomId, Record>> {
        self.rooms.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Runs `read` on the room's record.
    fn read<T>(&self, room_id: &RoomId, read: impl FnOnce(&Record) -> T) -> Result<T, StoreError> {
        self.rooms()
            .get(room_id)
            .map(read)
            .ok_or_else(|| StoreError::RoomNotFound(room_id.clone()))
    }

    /// Runs `write` on the room's record.
    fn write<T>(
        &self,
        room_id: &RoomId,
        write: impl FnOnce(&mut Record) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        let mut rooms = self.rooms();
        let record = rooms
            .get_mut(room_id)
            .ok_or_else(|| StoreError::RoomNotFound(room_id.clone()))?;
        write(record)
    }
}

#[expect(
    clippy::unused_async_trait_impl,
    reason = "the store answers at once; written as async fn, it reads as a store that awaits"
)]
impl ConversationStore for InMemoryStore {
    async fn create_room(&self, room: Room) -> Result<(), StoreError> {
        let mut rooms = self.rooms();
        if rooms.contains_key(&room.id) {
            return Err(StoreError::RoomExists(room.id));
        }
        rooms.insert(
            room.id.clone(),
            Record {
                room,
                bindings: Vec::new(),
                participants: Vec::new(),
                events: Vec::new(),
                tasks: Vec::new(),
                observations: Vec::new(),
            },
        );
        Ok(())
    }

    async fn update_room(
        &self,
        room_id: &RoomId,
        updates: RoomUpdates,
    ) -> Result<Room, StoreError> {
        self.write(room_id, |record| {
            updates.apply(&mut record.room);
            Ok(record.room.clone())
        })
    }

    async fn get_room(&self, room_id: &RoomId) -> Result<Option<Room>, StoreError> {
        Ok(self.rooms().get(room_id).map(|record| record.room.clone()))
    }

    async fn list_rooms(&self, filter: &RoomFilter) -> Result<Vec<Room>, StoreError> {
        let mut rooms: Vec<Room> = self
            .rooms()
            .values()
            .filter(|record| filter.matches(&record.room))
            .map(|record| record.room.clone())
            .collect();
        rooms
            .sort_by(|left, right| (left.created_at, &left.id).cmp(&(right.created_at, &right.id)));
        Ok(rooms)
    }

    async fn store_binding(&self, binding: ChannelBinding) -> Result<(), StoreError> {
        let room_id = binding.room_id.clone();
        self.write(&room_id, |record| {
            record
                .bindings
                .retain(|bound| bound.channel_id != binding.channel_id);
            record.bindings.push(binding);
            Ok(())
        })
    }

    async fn delete_binding(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<(), StoreError> {
        self.write(room_id, |record| {
            record
                .bindings
                .retain(|bound| &bound.channel_id != channel_id);
            Ok(())
        })
    }

    async fn list_bindings(&self, room_id: &RoomId) -> Result<Vec<ChannelBinding>, StoreError> {
        self.read(room_id, |record| record.bindings.clone())
    }

    async fn load_context(
        &self,
        room_id: &RoomId,
        recent_events: usize,
    ) -> Result<Option<RoomContext>, StoreError> {
        Ok(self.rooms().get(room_id).map(|record| {
            let skip = record.events.len().saturating_sub(recent_events);
            RoomContext {
                room: record.room.clone(),
                bindings: record.bindings.clone(),
                participants: record.participants.clone(),
                recent_events: record.events.iter().skip(skip).cloned().collect(),
            }
        }))
    }

    async fn create_participant(&self, participant: Participant) -> Result<(), StoreError> {
        let room_id = participant.room_id.clone();
        self.write(&room_id, |record| {
            if record.has_participant(&participant.id) {
                return Err(StoreError::ParticipantExists {
                    room_id: room_id.clone(),
                    participant_id: participant.id,
                });
            }
            record.participants.push(participant);
            Ok(())
        })
    }

    async fn get_participant(
        &self,
        room_id: &RoomId,
        participant_id: &ParticipantId,
    ) -> Result<Option<Participant>, StoreError> {
        self.read(room_id, |record| {
            record
                .participants
                .iter()
                .find(|participant| &participant.id == participant_id)
                .cloned()
        })
    }

    async fn update_participant(&self, participant: Participant) -> Result<(), StoreError> {
        let room_id = participant.room_id.clone();
        self.write(&room_id, |record| {
            let Some(stored) = record
                .participants
                .iter_mut()
                .find(|stored| stored.id == participant.id)
            else {
                return Err(StoreError::ParticipantNotFound {
                    room_id: room_id.clone(),
                    participant_id: participant.id,
                });
            };
            *stored = participant;
            Ok(())
        })
    }

    async fn delete_participant(
        &self,
        room_id: &RoomId,
        participant_id: &ParticipantId,
    ) -> Result<(), StoreError> {
        self.write(room_id, |record| {
            record
                .participants
                .retain(|participant| &participant.id != participant_id);
            Ok(())
        })
    }

    async fn list_participants(&self, room_id: &RoomId) -> Result<Vec<Participant>, StoreError> {
        self.read(room_id, |record| record.participants.clone())
    }

    async fn find_by_idempotency_key(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        key: &str,
    ) -> Result<Option<RoomEvent>, StoreError> {
        self.read(room_id, |record| {
            record
                .events
                .iter()
                .find(|event| {
                    event.idempotency_key.as_deref() == Some(key)
                        && &event.source.channel_id == channel_id
                })
                .cloned()
        })
    }

    async fn commit(&self, event: RoomEvent, counters: RoomCounters) -> Result<(), StoreError> {
        let room_id = event.room_id.clone();
        self.write(&room_id, |record| {
            let next = u64::try_from(record.events.len()).unwrap_or(u64::MAX);
            if event.index != next {
                return Err(StoreError::IndexTaken {
                    room_id: room_id.clone(),
                    index: event.index,
                });
            }
            record.room.event_count = counters.event_count;
            record.room.latest_index = Some(counters.latest_index);
            record.room.timers.last_activity_at = counters.last_activity_at;
            counters.room.apply(&mut record.room);
            record.events.push(event);
            Ok(())
        })
    }

    async fn persist_side_effects(
        &self,
        room_id: &RoomId,
        tasks: Vec<Task>,
        observations: Vec<Observation>,
    ) -> Result<(), StoreError> {
        self.write(room_id, |record| {
            record.tasks.extend(tasks);
            record.observations.extend(observations);
            Ok(())
        })
    }

    async fn list_events(
        &self,
        room_id: &RoomId,
        include_blocked: bool,
    ) -> Result<Vec<RoomEvent>, StoreError> {
        self.read(room_id, |record| {
            record
                .events
                .iter()
                .filter(|event| include_blocked || event.status != EventStatus::Blocked)
                .cloned()
                .collect()
        })
    }

    async fn list_tasks(&self, room_id: &RoomId) -> Result<Vec<Task>, StoreError> {
        self.read(room_id, |record| record.tasks.clone())
    }

    async fn list_observations(&self, room_id: &RoomId) -> Result<Vec<Observation>, StoreError> {
        self.read(room_id, |record| record.observations.clone())
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use roomkit_core::model::{
        ChannelCategory, ChannelDirection, ChannelType, EventContent, EventDirection, EventId,
        EventSource, EventType, RoomStatus, RoomTimers, Timestamp,
    };

    use super::*;

    fn room_id() -> RoomId {
        RoomId::new("room_1").unwrap()
    }

    fn event(index: u64, status: EventStatus) -> RoomEvent {
        let mut event = RoomEvent::new(
            EventId::new(format!("evt_{index}")).unwrap(),
            room_id(),
            EventType::Message,
            EventSource::new(
                ChannelId::new("sms").unwrap(),
                ChannelType::Sms,
                EventDirection::Inbound,
            ),
            EventContent::text("hi"),
            Timestamp::UNIX_EPOCH,
        );
        event.index = index;
        event.status = status;
        event
    }

    fn counters(index: u64) -> RoomCounters {
        RoomCounters {
            event_count: index + 1,
            latest_index: index,
            last_activity_at: None,
            room: RoomUpdates::default(),
        }
    }

    async fn store_with_room() -> InMemoryStore {
        let store = InMemoryStore::new();
        store
            .create_room(Room::new(room_id(), Timestamp::UNIX_EPOCH))
            .await
            .unwrap();
        store
    }

    #[tokio::test]
    async fn a_room_id_cannot_be_created_twice() {
        let store = store_with_room().await;
        let again = store
            .create_room(Room::new(room_id(), Timestamp::UNIX_EPOCH))
            .await;
        assert_eq!(again, Err(StoreError::RoomExists(room_id())));
    }

    #[tokio::test]
    async fn a_commit_sets_the_room_s_counters() {
        let store = store_with_room().await;
        store
            .commit(event(0, EventStatus::Delivered), counters(0))
            .await
            .unwrap();
        let room = store.get_room(&room_id()).await.unwrap().unwrap();
        assert_eq!((room.event_count, room.latest_index), (1, Some(0)));
    }

    #[tokio::test]
    async fn an_index_the_room_holds_is_refused() {
        let store = store_with_room().await;
        store
            .commit(event(0, EventStatus::Delivered), counters(0))
            .await
            .unwrap();
        let again = store
            .commit(event(0, EventStatus::Delivered), counters(0))
            .await;
        assert_eq!(
            again,
            Err(StoreError::IndexTaken {
                room_id: room_id(),
                index: 0
            })
        );
    }

    #[tokio::test]
    async fn a_timeline_read_leaves_refused_events_out_by_default() {
        let store = store_with_room().await;
        store
            .commit(event(0, EventStatus::Blocked), counters(0))
            .await
            .unwrap();
        store
            .commit(event(1, EventStatus::Delivered), counters(1))
            .await
            .unwrap();
        let received = store.list_events(&room_id(), false).await.unwrap();
        assert_eq!(
            received.iter().map(|event| event.index).collect::<Vec<_>>(),
            [1]
        );
    }

    #[tokio::test]
    async fn a_timeline_read_serves_refused_events_when_asked() {
        let store = store_with_room().await;
        store
            .commit(event(0, EventStatus::Blocked), counters(0))
            .await
            .unwrap();
        assert_eq!(store.list_events(&room_id(), true).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_context_holds_the_latest_events_only() {
        let store = store_with_room().await;
        for index in 0..3 {
            store
                .commit(event(index, EventStatus::Delivered), counters(index))
                .await
                .unwrap();
        }
        let context = store.load_context(&room_id(), 2).await.unwrap().unwrap();
        assert_eq!(
            context
                .recent_events
                .iter()
                .map(|event| event.index)
                .collect::<Vec<_>>(),
            [1, 2]
        );
    }

    #[tokio::test]
    async fn a_room_update_writes_only_the_fields_it_names() {
        let store = store_with_room().await;
        store
            .commit(event(0, EventStatus::Delivered), counters(0))
            .await
            .unwrap();
        let updates = RoomUpdates {
            status: Some(RoomStatus::Paused),
            ..RoomUpdates::default()
        };
        let room = store.update_room(&room_id(), updates).await.unwrap();
        assert_eq!((room.status, room.event_count), (RoomStatus::Paused, 1));
    }

    #[tokio::test]
    async fn a_commit_writes_the_room_updates_it_carries() {
        let store = store_with_room().await;
        let mut resumed = counters(0);
        resumed.room.status = Some(RoomStatus::Active);
        store
            .update_room(
                &room_id(),
                RoomUpdates {
                    status: Some(RoomStatus::Paused),
                    ..RoomUpdates::default()
                },
            )
            .await
            .unwrap();
        store
            .commit(event(0, EventStatus::Delivered), resumed)
            .await
            .unwrap();
        let room = store.get_room(&room_id()).await.unwrap().unwrap();
        assert_eq!(room.status, RoomStatus::Active);
    }

    #[tokio::test]
    async fn a_deleted_binding_is_gone() {
        let store = store_with_room().await;
        let binding = ChannelBinding::new(
            ChannelId::new("sms").unwrap(),
            room_id(),
            ChannelType::Sms,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
            Timestamp::UNIX_EPOCH,
        );
        store.store_binding(binding).await.unwrap();
        store
            .delete_binding(&room_id(), &ChannelId::new("sms").unwrap())
            .await
            .unwrap();
        assert_eq!(store.list_bindings(&room_id()).await.unwrap(), []);
    }

    fn jean() -> Participant {
        Participant::new(
            ParticipantId::new("p_jean").unwrap(),
            room_id(),
            ChannelId::new("sms").unwrap(),
            Timestamp::UNIX_EPOCH,
        )
    }

    #[tokio::test]
    async fn a_context_holds_the_room_s_participants() {
        let store = store_with_room().await;
        store.create_participant(jean()).await.unwrap();
        let context = store.load_context(&room_id(), 10).await.unwrap().unwrap();
        assert_eq!(context.participants, [jean()]);
    }

    #[tokio::test]
    async fn a_participant_id_cannot_be_created_twice_in_a_room() {
        let store = store_with_room().await;
        store.create_participant(jean()).await.unwrap();
        assert_eq!(
            store.create_participant(jean()).await,
            Err(StoreError::ParticipantExists {
                room_id: room_id(),
                participant_id: jean().id,
            })
        );
    }

    #[tokio::test]
    async fn a_participant_is_found_within_its_room() {
        let store = store_with_room().await;
        store.create_participant(jean()).await.unwrap();
        assert_eq!(
            store.get_participant(&room_id(), &jean().id).await,
            Ok(Some(jean()))
        );
    }

    #[tokio::test]
    async fn an_updated_participant_is_written_whole() {
        let store = store_with_room().await;
        store.create_participant(jean()).await.unwrap();
        let renamed = Participant {
            display_name: Some("Jean Tremblay".to_owned()),
            ..jean()
        };
        store.update_participant(renamed.clone()).await.unwrap();
        assert_eq!(store.list_participants(&room_id()).await, Ok(vec![renamed]));
    }

    #[tokio::test]
    async fn updating_a_participant_the_room_does_not_have_is_refused() {
        let store = store_with_room().await;
        assert_eq!(
            store.update_participant(jean()).await,
            Err(StoreError::ParticipantNotFound {
                room_id: room_id(),
                participant_id: jean().id,
            })
        );
    }

    #[tokio::test]
    async fn a_deleted_participant_is_gone() {
        let store = store_with_room().await;
        store.create_participant(jean()).await.unwrap();
        store
            .delete_participant(&room_id(), &jean().id)
            .await
            .unwrap();
        assert_eq!(store.list_participants(&room_id()).await, Ok(Vec::new()));
    }

    #[tokio::test]
    async fn listing_rooms_with_timers_armed_skips_the_others() {
        let store = store_with_room().await;
        let mut timed = Room::new(RoomId::new("room_2").unwrap(), Timestamp::UNIX_EPOCH);
        timed.timers = RoomTimers {
            inactive_after_seconds: Some(60),
            ..RoomTimers::default()
        };
        store.create_room(timed).await.unwrap();
        let filter = RoomFilter {
            timers_armed: true,
            ..RoomFilter::default()
        };
        let listed = store.list_rooms(&filter).await.unwrap();
        assert_eq!(
            listed
                .iter()
                .map(|room| room.id.as_str())
                .collect::<Vec<_>>(),
            ["room_2"]
        );
    }

    #[tokio::test]
    async fn listing_rooms_without_a_filter_returns_them_oldest_first() {
        let store = InMemoryStore::new();
        for (id, millis) in [("late", 2_000), ("early", 1_000)] {
            let created = Timestamp::from_unix_millis(millis).unwrap();
            store
                .create_room(Room::new(RoomId::new(id).unwrap(), created))
                .await
                .unwrap();
        }
        let listed = store.list_rooms(&RoomFilter::default()).await.unwrap();
        assert_eq!(
            listed
                .iter()
                .map(|room| room.id.as_str())
                .collect::<Vec<_>>(),
            ["early", "late"]
        );
    }
}

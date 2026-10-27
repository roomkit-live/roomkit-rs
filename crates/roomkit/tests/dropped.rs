//! A call whose caller stops waiting while the store commits its event
//! (RFC §13.6, decision 1 of step 2b, gap G-076): a commit is never within
//! the call's bound, so it is answered, the call is decided, and the event
//! is delivered whoever waits. A store whose commit waits at a gate holds
//! the call there while its caller goes.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelBinding, ChannelId, ChannelType, EventContent, EventId, EventStatus, EventUpdates,
    Identity, IdentityId, InboundMessage, Observation, Participant, ParticipantId, Room,
    RoomContext, RoomEvent, RoomId, RoomUpdates, Task,
};
use roomkit::testing::RecordingTransport;
use roomkit::{
    AttachChannelOptions, ConversationStore, CreateRoomOptions, EventFilter, EventQuery,
    FrameworkEvent, InMemoryStore, ProcessInboundOptions, RoomCounters, RoomFilter, RoomKit,
    RoomKitOptions, SendEventOptions, StoreError, TargetUpdate,
};
use tokio::sync::{Notify, broadcast};

/// How long a call that should not wait may take before the test fails it.
const BOUND: Duration = Duration::from_secs(5);

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("room_1").unwrap()
}

/// An in-memory store whose first commit of an event that is not the
/// framework's own lifecycle record (gap G-046) tells `committing` it
/// started, then waits for `go`, as a slow database does.
#[derive(Debug, Default)]
struct GatedStore {
    store: InMemoryStore,
    started: AtomicBool,
    committing: Arc<Notify>,
    go: Arc<Notify>,
}

impl ConversationStore for GatedStore {
    async fn create_room(&self, room: Room) -> Result<(), StoreError> {
        self.store.create_room(room).await
    }

    async fn update_room(
        &self,
        room_id: &RoomId,
        updates: RoomUpdates,
    ) -> Result<Room, StoreError> {
        self.store.update_room(room_id, updates).await
    }

    async fn get_room(&self, room_id: &RoomId) -> Result<Option<Room>, StoreError> {
        self.store.get_room(room_id).await
    }

    async fn advance_delivered_index(
        &self,
        room_id: &RoomId,
        index: u64,
        force: bool,
    ) -> Result<bool, StoreError> {
        self.store
            .advance_delivered_index(room_id, index, force)
            .await
    }

    async fn list_rooms(&self, filter: &RoomFilter) -> Result<Vec<Room>, StoreError> {
        self.store.list_rooms(filter).await
    }

    async fn find_room_id_by_binding(
        &self,
        channel_id: &ChannelId,
        participant_id: &ParticipantId,
        identity_id: Option<&IdentityId>,
        filter: &RoomFilter,
    ) -> Result<Option<RoomId>, StoreError> {
        self.store
            .find_room_id_by_binding(channel_id, participant_id, identity_id, filter)
            .await
    }

    async fn find_room_id_by_participant(
        &self,
        channel_id: &ChannelId,
        participant_id: &ParticipantId,
        identity_id: Option<&IdentityId>,
        filter: &RoomFilter,
    ) -> Result<Option<RoomId>, StoreError> {
        self.store
            .find_room_id_by_participant(channel_id, participant_id, identity_id, filter)
            .await
    }

    async fn store_binding(&self, binding: ChannelBinding) -> Result<(), StoreError> {
        self.store.store_binding(binding).await
    }

    async fn delete_binding(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<(), StoreError> {
        self.store.delete_binding(room_id, channel_id).await
    }

    async fn list_bindings(&self, room_id: &RoomId) -> Result<Vec<ChannelBinding>, StoreError> {
        self.store.list_bindings(room_id).await
    }

    async fn load_context(
        &self,
        room_id: &RoomId,
        recent_events: usize,
    ) -> Result<Option<RoomContext>, StoreError> {
        self.store.load_context(room_id, recent_events).await
    }

    async fn create_participant(&self, participant: Participant) -> Result<(), StoreError> {
        self.store.create_participant(participant).await
    }

    async fn get_participant(
        &self,
        room_id: &RoomId,
        participant_id: &ParticipantId,
    ) -> Result<Option<Participant>, StoreError> {
        self.store.get_participant(room_id, participant_id).await
    }

    async fn update_participant(&self, participant: Participant) -> Result<(), StoreError> {
        self.store.update_participant(participant).await
    }

    async fn delete_participant(
        &self,
        room_id: &RoomId,
        participant_id: &ParticipantId,
    ) -> Result<(), StoreError> {
        self.store.delete_participant(room_id, participant_id).await
    }

    async fn list_participants(&self, room_id: &RoomId) -> Result<Vec<Participant>, StoreError> {
        self.store.list_participants(room_id).await
    }

    async fn create_identity(&self, identity: Identity) -> Result<(), StoreError> {
        self.store.create_identity(identity).await
    }

    async fn get_identity(&self, identity_id: &IdentityId) -> Result<Option<Identity>, StoreError> {
        self.store.get_identity(identity_id).await
    }

    async fn resolve_identity(
        &self,
        channel_type: ChannelType,
        address: &str,
        organization_id: Option<&str>,
    ) -> Result<Option<Identity>, StoreError> {
        self.store
            .resolve_identity(channel_type, address, organization_id)
            .await
    }

    async fn link_address(
        &self,
        identity_id: &IdentityId,
        channel_type: ChannelType,
        address: &str,
        organization_id: Option<&str>,
    ) -> Result<(), StoreError> {
        self.store
            .link_address(identity_id, channel_type, address, organization_id)
            .await
    }

    async fn unlink_address(
        &self,
        identity_id: &IdentityId,
        channel_type: ChannelType,
        address: &str,
        organization_id: Option<&str>,
    ) -> Result<(), StoreError> {
        self.store
            .unlink_address(identity_id, channel_type, address, organization_id)
            .await
    }

    async fn get_event_by_idempotency_key(
        &self,
        room_id: &RoomId,
        channel_id: Option<&ChannelId>,
        key: &str,
    ) -> Result<Option<RoomEvent>, StoreError> {
        self.store
            .get_event_by_idempotency_key(room_id, channel_id, key)
            .await
    }

    async fn commit(
        &self,
        event: RoomEvent,
        counters: RoomCounters,
        target: Option<TargetUpdate>,
    ) -> Result<(), StoreError> {
        let records = event.source.channel_type == ChannelType::SYSTEM;
        if !records && !self.started.swap(true, Ordering::SeqCst) {
            self.committing.notify_one();
            self.go.notified().await;
        }
        self.store.commit(event, counters, target).await
    }

    async fn get_event(&self, event_id: &EventId) -> Result<Option<RoomEvent>, StoreError> {
        self.store.get_event(event_id).await
    }

    async fn update_event(
        &self,
        event_id: &EventId,
        updates: EventUpdates,
    ) -> Result<RoomEvent, StoreError> {
        self.store.update_event(event_id, updates).await
    }

    async fn delete_event(
        &self,
        room_id: &RoomId,
        event_id: &EventId,
        cascade_replies: bool,
    ) -> Result<Vec<RoomEvent>, StoreError> {
        self.store
            .delete_event(room_id, event_id, cascade_replies)
            .await
    }

    async fn get_event_count(
        &self,
        room_id: &RoomId,
        filter: Option<EventFilter>,
    ) -> Result<u64, StoreError> {
        self.store.get_event_count(room_id, filter).await
    }

    async fn persist_side_effects(
        &self,
        room_id: &RoomId,
        tasks: Vec<Task>,
        observations: Vec<Observation>,
    ) -> Result<(), StoreError> {
        self.store
            .persist_side_effects(room_id, tasks, observations)
            .await
    }

    async fn list_events(
        &self,
        room_id: &RoomId,
        query: &EventQuery,
    ) -> Result<Vec<RoomEvent>, StoreError> {
        self.store.list_events(room_id, query).await
    }

    async fn list_tasks(&self, room_id: &RoomId) -> Result<Vec<Task>, StoreError> {
        self.store.list_tasks(room_id).await
    }

    async fn list_observations(&self, room_id: &RoomId) -> Result<Vec<Observation>, StoreError> {
        self.store.list_observations(room_id).await
    }
}

/// A kit over a gated store, with an SMS sender and a WebSocket reader
/// attached to `room_1`.
async fn gated_room() -> (RoomKit<GatedStore>, RecordingTransport) {
    let kit = RoomKit::new(RoomKitOptions::with_store(GatedStore::default()));
    let ws = RecordingTransport::new(channel("ws"), ChannelType::WEBSOCKET);
    kit.register_channel(RecordingTransport::new(channel("sms"), ChannelType::SMS))
        .unwrap();
    kit.register_channel(ws.clone()).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.attach_channel(&room_id(), &channel("sms"), AttachChannelOptions::default())
        .await
        .unwrap();
    kit.attach_channel(&room_id(), &channel("ws"), AttachChannelOptions::default())
        .await
        .unwrap();
    (kit, ws)
}

/// Drops `call` once the store's commit started, lets the commit go on,
/// and waits for the kit's background work to be over.
async fn drop_during_the_commit<T>(kit: &RoomKit<GatedStore>, call: impl Future<Output = T>) {
    let store = kit.store();
    tokio::select! {
        _ = call => panic!("the call ended before its commit"),
        () = store.committing.notified() => {}
    }
    store.go.notify_one();
    tokio::time::timeout(BOUND, roomkit::testing::idle(kit))
        .await
        .unwrap();
}

/// The room's events but its lifecycle records (gap G-046), as `status
/// text`, and whether `events` reports each one's end.
async fn delivered_and_processed(
    kit: &RoomKit<GatedStore>,
    events: &mut broadcast::Receiver<FrameworkEvent>,
) -> (Vec<(EventStatus, String)>, bool) {
    let mut timeline = kit
        .store()
        .list_events(&room_id(), &every_row())
        .await
        .unwrap();
    timeline.retain(|event| event.source.channel_type != ChannelType::SYSTEM);
    let mut processed = Vec::new();
    while let Ok(event) = events.try_recv() {
        if let FrameworkEvent::EventProcessed { event_id, .. } = event {
            processed.push(event_id);
        }
    }
    let all_processed = timeline.iter().all(|event| processed.contains(&event.id));
    let lines = timeline
        .iter()
        .map(|event| (event.status, roomkit::text_of(&event.content)))
        .collect();
    (lines, all_processed)
}

#[tokio::test]
async fn a_message_dropped_during_its_commit_is_delivered() {
    let (kit, ws) = gated_room().await;
    let mut events = kit.subscribe();
    let room = room_id();
    let message = InboundMessage::new(
        channel("sms"),
        "+15551234567",
        EventContent::text("Bonjour"),
    );
    drop_during_the_commit(
        &kit,
        kit.process_inbound(
            message,
            ProcessInboundOptions {
                room_id: Some(room.clone()),
                ..Default::default()
            },
        ),
    )
    .await;
    assert_eq!(
        (delivered_and_processed(&kit, &mut events).await, ws.texts()),
        (
            (vec![(EventStatus::Delivered, "Bonjour".to_owned())], true),
            vec!["Bonjour".to_owned()]
        )
    );
}

#[tokio::test]
async fn an_injected_event_dropped_during_its_commit_is_delivered() {
    let (kit, ws) = gated_room().await;
    let mut events = kit.subscribe();
    let room = room_id();
    let event = SendEventOptions::default();
    drop_during_the_commit(
        &kit,
        kit.send_event(&room, &channel("sms"), EventContent::text("Bonjour"), event),
    )
    .await;
    assert_eq!(
        (delivered_and_processed(&kit, &mut events).await, ws.texts()),
        (
            (vec![(EventStatus::Delivered, "Bonjour".to_owned())], true),
            vec!["Bonjour".to_owned()]
        )
    );
}

/// Every row of a room's timeline, the refused ones included.
fn every_row() -> EventQuery {
    EventQuery {
        limit: u64::MAX,
        event_filter: Some(EventFilter {
            include_blocked: true,
            ..EventFilter::default()
        }),
        ..EventQuery::default()
    }
}

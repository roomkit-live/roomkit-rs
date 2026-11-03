//! A delivery set whose room's cursor cannot be read (RFC §13.5 "Delivery
//! lanes across processes"): the set reads it again after the wait it
//! gives a hole, as Python's lane retries a round that failed, and is
//! never delivered out of the cursor's order meanwhile.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use roomkit::model::{
    ChannelBinding, ChannelId, ChannelType, EventId, Identity, IdentityId, InboundMessage,
    Observation, Participant, ParticipantId, Room, RoomEvent, RoomId, Task, TextContent,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, ConversationStore, CreateRoomOptions, InMemoryStore,
    ProcessInboundOptions, RoomKit, RoomKitOptions, StoreError, StoreFailure, store,
};

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("r1").unwrap()
}

/// An in-memory store whose cursor reads fail while `down` is set.
#[derive(Debug, Default)]
struct CursorReadsFail {
    store: InMemoryStore,
    down: AtomicBool,
}

macro_rules! forward {
    ($($name:ident($($argument:ident: $kind:ty),*) -> $answer:ty;)*) => {
        $(
            async fn $name(&self, $($argument: $kind),*) -> Result<$answer, StoreError> {
                self.store.$name($($argument),*).await
            }
        )*
    };
}

impl ConversationStore for CursorReadsFail {
    async fn get_delivered_index(&self, room_id: &RoomId) -> Result<i64, StoreError> {
        if self.down.load(Ordering::SeqCst) {
            return Err(StoreError::Unavailable(StoreFailure::new("cursor down")));
        }
        self.store.get_delivered_index(room_id).await
    }

    forward! {
        get_room(room_id: &RoomId) -> Option<Room>;
        list_participants(room_id: &RoomId) -> Vec<Participant>;
        create_room(room: Room) -> Room;
        update_room(room: Room) -> Room;
        delete_room(room_id: &RoomId) -> bool;
        list_rooms(options: store::ListRoomsOptions) -> Vec<Room>;
        find_rooms(options: store::FindRoomsOptions) -> Vec<Room>;
        advance_delivered_index(
            room_id: &RoomId,
            index: u64,
            options: store::AdvanceDeliveredIndexOptions
        ) -> bool;
        add_binding(binding: ChannelBinding) -> ChannelBinding;
        get_binding(room_id: &RoomId, channel_id: &ChannelId) -> Option<ChannelBinding>;
        update_binding(binding: ChannelBinding) -> ChannelBinding;
        remove_binding(room_id: &RoomId, channel_id: &ChannelId) -> bool;
        list_bindings(room_id: &RoomId) -> Vec<ChannelBinding>;
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
        get_task(task_id: &roomkit::model::TaskId) -> Option<Task>;
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
        ) -> std::collections::BTreeMap<EventId, roomkit::model::ThreadSummary>;
    }
}

// Python: src/roomkit/core/lanes.py, RoomDeliveryLane._run (no Python test): a
// round that failed is retried after the lane's backoff.
#[tokio::test(start_paused = true)]
async fn a_set_reads_a_cursor_that_failed_again_before_it_runs() {
    let options = RoomKitOptions::with_store(CursorReadsFail::default());
    let store = Arc::clone(&options.store);
    let kit = RoomKit::new(options);
    let recorder = RecordingTransport::new(
        channel("recorder"),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    );
    let sms = RecordingTransport::new(
        channel("src"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(sms).unwrap();
    kit.register_channel(recorder.clone()).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["src", "recorder"] {
        kit.attach_channel(&room_id(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    store.down.store(true, Ordering::SeqCst);
    let sending = {
        let kit = kit.clone();
        tokio::spawn(async move {
            let message = InboundMessage::new(channel("src"), "u", TextContent::new("m"));
            kit.process_inbound(message, ProcessInboundOptions::default())
                .await
                .unwrap()
        })
    };
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        recorder.texts().is_empty(),
        "nothing runs while the cursor cannot be read"
    );
    store.down.store(false, Ordering::SeqCst);
    sending.await.unwrap();
    assert_eq!(recorder.texts(), ["m"]);
    kit.close().await.unwrap();
}

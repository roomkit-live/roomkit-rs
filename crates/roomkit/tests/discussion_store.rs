//! A discussion whose stored queue cannot be written (RFC §19.7.5): its driver
//! backs off, as Python's `_drive`, rather than give and end a turn at once.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use roomkit::model::*;
use roomkit::orchestration::DiscussionOptions;
use roomkit::store;
use roomkit::testing::{
    EchoAgent, EchoAgentOptions, RecordingTransport, RecordingTransportOptions,
};
use roomkit::{
    AttachChannelOptions, ConversationStore, CreateRoomOptions, Discussion, HookOptions,
    HookTrigger, InMemoryStore, InstallStrategyOptions, ProcessInboundOptions, RoomKit,
    RoomKitOptions, SpeakQueueChange, SpeakQueueEvent, StoreError,
};

#[derive(Default)]
struct FlakyStore {
    store: InMemoryStore,
    fails: Arc<AtomicBool>,
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

impl ConversationStore for FlakyStore {
    async fn patch_room_metadata(
        &self,
        room_id: &RoomId,
        patch: Metadata,
        options: store::PatchRoomMetadataOptions,
    ) -> Result<Option<Room>, StoreError> {
        if self.fails.load(Ordering::SeqCst) {
            return Err(StoreError::Value("the store refuses the write".to_owned()));
        }
        self.store
            .patch_room_metadata(room_id, patch, options)
            .await
    }

    forward! {
        list_participants(room_id: &RoomId) -> Vec<Participant>;
        create_room(room: Room) -> Room;
        get_room(room_id: &RoomId) -> Option<Room>;
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

// Python: orchestration/strategies/discussion/_driver.py `_drive`, its failures
// back off; tests/orchestration/test_discussion_edges.py::test_a_failure_reading_the_queue_is_retried
#[tokio::test]
async fn a_queue_that_cannot_be_written_backs_off_and_tells_no_turn() {
    let room = RoomId::new("r1").unwrap();
    let id = |name: &str| ChannelId::new(name).unwrap();
    let store = FlakyStore::default();
    let fails = Arc::clone(&store.fails);
    let kit = RoomKit::new(RoomKitOptions::with_store(store));
    kit.register_channel(RecordingTransport::new(
        id("ops"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.register_channel(EchoAgent::new(
        id("a"),
        "a answers",
        EchoAgentOptions::default(),
    ))
    .unwrap();
    let created = CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    };
    kit.create_room(created).await.unwrap();
    kit.attach_channel(&room, &id("ops"), AttachChannelOptions::default())
        .await
        .unwrap();
    let asked = Arc::new(AtomicU32::new(0));
    let counted = Arc::clone(&asked);
    let options = DiscussionOptions {
        done: Some(Arc::new(move |_| {
            counted.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { false })
        })),
        ..DiscussionOptions::default()
    };
    let discussion = Discussion::new(vec![id("a").into()], options).unwrap();
    kit.install_strategy(
        &room,
        Arc::new(discussion),
        InstallStrategyOptions::default(),
    )
    .await
    .unwrap();
    let ended = Arc::new(AtomicU32::new(0));
    let told = Arc::clone(&ended);
    kit.hook(
        HookTrigger::OnSpeakQueue,
        move |event: SpeakQueueEvent, _context: RoomContext| {
            if event.change == SpeakQueueChange::TurnEnded {
                told.fetch_add(1, Ordering::SeqCst);
            }
            async {}
        },
        HookOptions::default(),
    )
    .unwrap();
    // A first look done with: the next finds the queue the message fills.
    tokio::time::sleep(Duration::from_millis(30)).await;

    fails.store(true, Ordering::SeqCst);
    let message = InboundMessage::new(id("ops"), "oncall", TextContent::new("@a hello"));
    let options = ProcessInboundOptions::default();
    drop(kit.process_inbound(message, options).await);
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert!(
        asked.load(Ordering::SeqCst) <= 3,
        "done was asked {} times in 200 ms",
        asked.load(Ordering::SeqCst)
    );
    assert_eq!(ended.load(Ordering::SeqCst), 0, "no turn was given");
    fails.store(false, Ordering::SeqCst);
    kit.close().await.unwrap();
}

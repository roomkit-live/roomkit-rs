//! A store implementing what Python's `ConversationStore` makes abstract
//! and nothing else, over the in-memory store: the trait's provided
//! methods serve the kit, as Python's base class serves a store that
//! overrides none of them (RFC §14.1). `get_event_by_idempotency_key` among
//! them reads no event back (`None`), a store that cannot resolve a key.

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelBinding, ChannelId, ChannelType, EventId, Identity, IdentityId, InboundMessage,
    Observation, Participant, ParticipantId, Room, RoomEvent, RoomId, Task, TextContent,
};
use roomkit::testing::{
    EchoAgent, EchoAgentOptions, RecordingTransport, RecordingTransportOptions,
};
use roomkit::{
    AttachChannelOptions, ConversationStore, CreateRoomOptions, InMemoryStore,
    ProcessInboundOptions, RoomKit, RoomKitOptions, SendEventOptions, StoreError, store,
};

/// The in-memory store, behind the methods Python's ABC makes abstract.
#[derive(Debug, Default)]
struct AbstractOnly {
    store: InMemoryStore,
    /// Its `update_event` writes the row as given, its delivery results
    /// included, as a custom store may (Python's `_WritesTheRowAsGiven`);
    /// otherwise it keeps the stored results, as the trait says.
    writes_the_row_as_given: bool,
}

impl ConversationStore for AbstractOnly {
    async fn create_room(&self, room: Room) -> Result<Room, StoreError> {
        self.store.create_room(room).await
    }

    async fn get_room(&self, room_id: &RoomId) -> Result<Option<Room>, StoreError> {
        self.store.get_room(room_id).await
    }

    async fn update_room(&self, room: Room) -> Result<Room, StoreError> {
        self.store.update_room(room).await
    }

    async fn delete_room(&self, room_id: &RoomId) -> Result<bool, StoreError> {
        self.store.delete_room(room_id).await
    }

    async fn list_rooms(&self, options: store::ListRoomsOptions) -> Result<Vec<Room>, StoreError> {
        self.store.list_rooms(options).await
    }

    async fn find_rooms(&self, options: store::FindRoomsOptions) -> Result<Vec<Room>, StoreError> {
        self.store.find_rooms(options).await
    }

    async fn advance_delivered_index(
        &self,
        room_id: &RoomId,
        index: u64,
        options: store::AdvanceDeliveredIndexOptions,
    ) -> Result<bool, StoreError> {
        self.store
            .advance_delivered_index(room_id, index, options)
            .await
    }

    async fn add_binding(&self, binding: ChannelBinding) -> Result<ChannelBinding, StoreError> {
        self.store.add_binding(binding).await
    }

    async fn get_binding(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<Option<ChannelBinding>, StoreError> {
        self.store.get_binding(room_id, channel_id).await
    }

    async fn update_binding(&self, binding: ChannelBinding) -> Result<ChannelBinding, StoreError> {
        self.store.update_binding(binding).await
    }

    async fn remove_binding(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<bool, StoreError> {
        self.store.remove_binding(room_id, channel_id).await
    }

    async fn list_bindings(&self, room_id: &RoomId) -> Result<Vec<ChannelBinding>, StoreError> {
        self.store.list_bindings(room_id).await
    }

    async fn add_participant(&self, participant: Participant) -> Result<Participant, StoreError> {
        self.store.add_participant(participant).await
    }

    async fn get_participant(
        &self,
        room_id: &RoomId,
        participant_id: &ParticipantId,
    ) -> Result<Option<Participant>, StoreError> {
        self.store.get_participant(room_id, participant_id).await
    }

    async fn update_participant(
        &self,
        participant: Participant,
    ) -> Result<Participant, StoreError> {
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

    async fn create_identity(&self, identity: Identity) -> Result<Identity, StoreError> {
        self.store.create_identity(identity).await
    }

    async fn get_identity(&self, identity_id: &IdentityId) -> Result<Option<Identity>, StoreError> {
        self.store.get_identity(identity_id).await
    }

    async fn resolve_identity(
        &self,
        channel_type: &ChannelType,
        address: &str,
        options: store::ResolveIdentityOptions,
    ) -> Result<Option<Identity>, StoreError> {
        self.store
            .resolve_identity(channel_type, address, options)
            .await
    }

    async fn link_address(
        &self,
        identity_id: &IdentityId,
        channel_type: &ChannelType,
        address: &str,
        options: store::LinkAddressOptions,
    ) -> Result<(), StoreError> {
        self.store
            .link_address(identity_id, channel_type, address, options)
            .await
    }

    async fn check_idempotency(
        &self,
        room_id: &RoomId,
        key: &str,
        channel_id: Option<&ChannelId>,
    ) -> Result<bool, StoreError> {
        self.store.check_idempotency(room_id, key, channel_id).await
    }

    async fn commit_event(
        &self,
        room_id: &RoomId,
        event: RoomEvent,
        options: store::CommitEventOptions,
    ) -> Result<RoomEvent, StoreError> {
        self.store.commit_event(room_id, event, options).await
    }

    async fn get_event(&self, event_id: &EventId) -> Result<Option<RoomEvent>, StoreError> {
        self.store.get_event(event_id).await
    }

    async fn update_event(&self, event: RoomEvent) -> Result<RoomEvent, StoreError> {
        let results = event.delivery_results.clone();
        let updated = self.store.update_event(event).await?;
        if self.writes_the_row_as_given {
            self.store
                .record_delivery_results(&updated.room_id, &updated.id, results)
                .await?;
        }
        Ok(updated)
    }

    async fn delete_event(
        &self,
        room_id: &RoomId,
        event_id: &EventId,
        options: store::DeleteEventOptions,
    ) -> Result<Vec<RoomEvent>, StoreError> {
        self.store.delete_event(room_id, event_id, options).await
    }

    async fn get_event_count(
        &self,
        room_id: &RoomId,
        options: store::GetEventCountOptions,
    ) -> Result<u64, StoreError> {
        self.store.get_event_count(room_id, options).await
    }

    async fn list_events(
        &self,
        room_id: &RoomId,
        options: store::ListEventsOptions,
    ) -> Result<Vec<RoomEvent>, StoreError> {
        self.store.list_events(room_id, options).await
    }

    async fn add_task(&self, task: Task) -> Result<Task, StoreError> {
        self.store.add_task(task).await
    }

    async fn add_observation(&self, observation: Observation) -> Result<Observation, StoreError> {
        self.store.add_observation(observation).await
    }

    async fn list_tasks(
        &self,
        room_id: &RoomId,
        options: store::ListTasksOptions,
    ) -> Result<Vec<Task>, StoreError> {
        self.store.list_tasks(room_id, options).await
    }

    async fn list_observations(&self, room_id: &RoomId) -> Result<Vec<Observation>, StoreError> {
        self.store.list_observations(room_id).await
    }

    async fn get_task(&self, task_id: &roomkit::model::TaskId) -> Result<Option<Task>, StoreError> {
        self.store.get_task(task_id).await
    }

    async fn update_task(&self, task: Task) -> Result<Task, StoreError> {
        self.store.update_task(task).await
    }

    async fn add_event(&self, event: RoomEvent) -> Result<RoomEvent, StoreError> {
        self.store.add_event(event).await
    }

    async fn mark_read(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        event_id: &EventId,
    ) -> Result<(), StoreError> {
        self.store.mark_read(room_id, channel_id, event_id).await
    }

    async fn mark_all_read(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<(), StoreError> {
        self.store.mark_all_read(room_id, channel_id).await
    }

    async fn get_unread_count(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<u64, StoreError> {
        self.store.get_unread_count(room_id, channel_id).await
    }

    async fn list_read_markers(
        &self,
        room_id: &RoomId,
    ) -> Result<std::collections::BTreeMap<ChannelId, u64>, StoreError> {
        self.store.list_read_markers(room_id).await
    }

    async fn find_latest_room(
        &self,
        participant_id: &ParticipantId,
        options: store::FindLatestRoomOptions,
    ) -> Result<Option<Room>, StoreError> {
        self.store.find_latest_room(participant_id, options).await
    }

    async fn find_room_id_by_channel(
        &self,
        channel_id: &ChannelId,
        options: store::FindRoomIdByChannelOptions,
    ) -> Result<Option<RoomId>, StoreError> {
        self.store
            .find_room_id_by_channel(channel_id, options)
            .await
    }

    async fn get_thread_summaries(
        &self,
        room_id: &RoomId,
        root_event_ids: &[EventId],
    ) -> Result<std::collections::BTreeMap<EventId, roomkit::model::ThreadSummary>, StoreError>
    {
        self.store
            .get_thread_summaries(room_id, root_event_ids)
            .await
    }
}

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("room_1").unwrap()
}

/// A kit on the store, with `room_1` binding an SMS transport and an echo
/// agent.
async fn sms_and_agent() -> (RoomKit<AbstractOnly>, RecordingTransport) {
    let kit = RoomKit::new(RoomKitOptions::with_store(AbstractOnly::default()));
    let sms = RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    let agent = EchoAgent::new(
        channel("ai"),
        "You said: ",
        EchoAgentOptions { quote: true },
    );
    kit.register_channel(sms.clone()).unwrap();
    kit.register_channel(agent).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    for bound in ["sms", "ai"] {
        kit.attach_channel(&room_id(), &channel(bound), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    (kit, sms)
}

/// `text` from Alice's number on SMS, with the idempotency key `key`.
fn keyed(text: &str, key: &str) -> InboundMessage {
    let mut message = InboundMessage::new(channel("sms"), "+15551234567", TextContent::new(text));
    message.idempotency_key = Some(key.to_owned());
    message
}

/// RFC §13.4, Python's pipeline: a store that holds a key and cannot read
/// its event back answers a redelivery as a blocked duplicate, never
/// processed again: no second event, no second answer.
#[tokio::test]
async fn seen_key_without_event_reads_blocked() {
    let (kit, sms) = sms_and_agent().await;
    let routed = || ProcessInboundOptions {
        room_id: Some(room_id()),
        ..Default::default()
    };
    let count = async || {
        kit.store()
            .get_event_count(&room_id(), store::GetEventCountOptions::default())
            .await
            .unwrap()
    };
    kit.process_inbound(keyed("Bonjour", "SM1"), routed())
        .await
        .unwrap();
    let before = count().await;
    let again = kit
        .process_inbound(keyed("Bonjour", "SM1"), routed())
        .await
        .unwrap();
    let after = count().await;
    kit.close().await.unwrap();
    assert_eq!(
        (
            (again.duplicate, again.blocked, again.reason.as_deref()),
            again.event.is_none(),
            sms.texts(),
            after - before
        ),
        (
            (true, true, Some("duplicate")),
            true,
            vec!["You said: Bonjour".to_owned()],
            0
        )
    );
}

/// RFC §13.4, Python's `send_event`: a direct injection whose key the
/// store holds, its event unread, is not processed again: the call returns
/// the event it injected, nothing stored, nothing delivered.
#[tokio::test]
async fn a_direct_injection_of_a_seen_key_is_not_stored_again() {
    let (kit, sms) = sms_and_agent().await;
    let options = || SendEventOptions {
        idempotency_key: Some("WS1".to_owned()),
        ..SendEventOptions::default()
    };
    let count = async || {
        kit.store()
            .get_event_count(&room_id(), store::GetEventCountOptions::default())
            .await
            .unwrap()
    };
    kit.send_event(
        &room_id(),
        &channel("sms"),
        TextContent::new("Hi"),
        options(),
    )
    .await
    .unwrap();
    let before = count().await;
    let again = kit
        .send_event(
            &room_id(),
            &channel("sms"),
            TextContent::new("Hi"),
            options(),
        )
        .await;
    let after = count().await;
    kit.close().await.unwrap();
    assert_eq!(
        (
            again.map(|event| event.idempotency_key),
            after - before,
            sms.texts().len()
        ),
        (Ok(Some("WS1".to_owned())), 0, 1)
    );
}

/// The provided reads serve a conversation: a message naming no room is
/// routed to the room whose binding names its sender, through the
/// provided `find_room_id_by_binding`, and answered there; the room's
/// register of authors is kept by the provided private state, in the
/// room's metadata.
#[tokio::test]
async fn a_store_of_the_abstract_methods_alone_serves_a_conversation() {
    let (kit, sms) = sms_and_agent().await;
    let mut binding = kit
        .store()
        .get_binding(&room_id(), &channel("sms"))
        .await
        .unwrap()
        .unwrap();
    binding.participant_id = Some(ParticipantId::new("+15551234567").unwrap());
    kit.store().update_binding(binding).await.unwrap();
    let routed = kit
        .process_inbound(keyed("Bonjour", "SM1"), ProcessInboundOptions::default())
        .await
        .unwrap();
    let register = kit
        .store()
        .get_private_state(&room_id(), "author_register")
        .await
        .unwrap();
    kit.close().await.unwrap();
    assert_eq!(
        (
            routed.event.map(|event| event.room_id),
            sms.texts(),
            register.is_some()
        ),
        (Some(room_id()), vec!["You said: Bonjour".to_owned()], true)
    );
}

/// RFC §14.1, Python's base class: a store that overrides neither
/// `room_exists`, `binding_exists` nor `get_timeline` answers them through
/// `get_room`, `get_binding` and `list_events`, as the in-memory store
/// answers them.
#[tokio::test]
async fn the_provided_reads_answer_through_the_abstract_ones() {
    let (kit, _sms) = sms_and_agent().await;
    kit.process_inbound(
        keyed("Bonjour", "SM1"),
        ProcessInboundOptions {
            room_id: Some(room_id()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let store = kit.store();
    let other = RoomId::new("room_2").unwrap();
    let exists = (
        store.room_exists(&room_id()).await.unwrap(),
        store.room_exists(&other).await.unwrap(),
        store
            .binding_exists(&room_id(), &channel("sms"))
            .await
            .unwrap(),
        store
            .binding_exists(&room_id(), &channel("email"))
            .await
            .unwrap(),
    );
    let read = store::GetTimelineOptions {
        newest_first: true,
        limit: 1,
        ..Default::default()
    };
    let timeline = store.get_timeline(&room_id(), read.clone()).await.unwrap();
    let held = store.store.get_timeline(&room_id(), read).await.unwrap();
    kit.close().await.unwrap();
    assert_eq!((exists, timeline), ((true, false, true, false), held));
}

/// Python's base class: a store that does not override
/// `add_event_auto_index` gives the event the room's count of rows as its
/// index, through `get_event_count` and `add_event`.
#[tokio::test]
async fn the_provided_auto_index_takes_the_room_s_count() {
    let (kit, _sms) = sms_and_agent().await;
    kit.process_inbound(
        keyed("Bonjour", "SM1"),
        ProcessInboundOptions {
            room_id: Some(room_id()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let store = kit.store();
    let count = store
        .get_event_count(&room_id(), store::GetEventCountOptions::default())
        .await
        .unwrap();
    let mut event = RoomEvent::new(
        room_id(),
        roomkit::model::EventSource::new(channel("sms"), ChannelType::SMS),
        TextContent::new("added"),
    );
    event.index = 99;
    let added = store.add_event_auto_index(&room_id(), event).await.unwrap();
    kit.close().await.unwrap();
    assert_eq!(added.index, count);
}

/// An event of `room_1` from SMS saying `text`.
fn said(text: &str) -> RoomEvent {
    RoomEvent::new(
        room_id(),
        roomkit::model::EventSource::new(channel("sms"), ChannelType::SMS),
        TextContent::new(text),
    )
}

/// The store over `room_1`, with `said(text)` committed; the event as
/// committed.
async fn with_said(text: &str) -> (AbstractOnly, RoomEvent) {
    let store = AbstractOnly::default();
    store.create_room(Room::new(room_id())).await.unwrap();
    let options = store::CommitEventOptions::default();
    let event = store
        .commit_event(&room_id(), said(text), options)
        .await
        .unwrap();
    (store, event)
}

/// `results`, a channel's delivery result in the serialized form.
fn results(status: &str) -> roomkit::model::Metadata {
    serde_json::from_value(serde_json::json!({ "sms": { "status": status } })).unwrap()
}

/// Python: `tests/test_store_binding_contract.py::test_the_default_delivery_record_writes_the_results_or_says_it_cannot`.
/// The default record writes the results through `update_event`, which a
/// store writing the row as given lets it do; over one keeping the stored
/// results it fails, nothing written, rather than record nothing.
#[tokio::test]
async fn the_default_delivery_record_writes_the_results_or_says_it_cannot() {
    let (keeps, kept) = with_said("hi").await;
    let refused = keeps
        .record_delivery_results(&room_id(), &kept.id, results("failed"))
        .await;
    let unwritten = keeps.get_event(&kept.id).await.unwrap().unwrap();

    let (mut as_given, given) = with_said("hi").await;
    as_given.writes_the_row_as_given = true;
    as_given
        .record_delivery_results(&room_id(), &given.id, results("failed"))
        .await
        .unwrap();
    let written = as_given.get_event(&given.id).await.unwrap().unwrap();

    assert!(
        matches!(&refused, Err(StoreError::Value(message)) if message.contains("override record_delivery_results")),
        "{refused:?}"
    );
    assert_eq!(
        (unwritten.delivery_results, written.delivery_results),
        (roomkit::model::Metadata::new(), results("failed"))
    );
}

/// Python: `tests/test_store_binding_contract.py::test_the_default_target_write_keeps_the_recorded_results`.
/// A change's target written through the provided method keeps the
/// delivery results recorded on the row since its copy was read.
#[tokio::test]
async fn the_default_target_write_keeps_the_recorded_results() {
    let (store, read_before) = with_said("v1").await;
    store
        .store
        .record_delivery_results(&room_id(), &read_before.id, results("failed"))
        .await
        .unwrap();
    let mut edited = read_before.clone();
    edited.content = TextContent::new("changed").into();

    store
        .write_change_target(&room_id(), edited.clone())
        .await
        .unwrap();

    let after = store.get_event(&read_before.id).await.unwrap().unwrap();
    assert_eq!(
        (after.content, after.delivery_results),
        (edited.content, results("failed"))
    );
}

/// Python: `tests/test_store_binding_contract.py::test_the_default_target_write_marks_a_deleted_target_s_edits`.
/// The provided method marks a deleted target's edits, told by their
/// content's target, a row typed otherwise that carries an edit included;
/// a target the room lacks is not found.
#[tokio::test]
async fn the_default_target_write_marks_a_deleted_target_s_edits() {
    let (store, target) = with_said("v1").await;
    let edit_content = roomkit::model::EditContent::new(target.id.clone(), TextContent::new("v2"));
    let mut edit = said("edit");
    edit.r#type = roomkit::model::EventType::Edit;
    edit.content = edit_content.clone().into();
    let mut untyped = said("untyped");
    untyped.content = edit_content.into();
    let options = store::CommitEventOptions::default;
    let edit = store
        .commit_event(&room_id(), edit, options())
        .await
        .unwrap();
    let untyped = store
        .commit_event(&room_id(), untyped, options())
        .await
        .unwrap();
    let mut deleted = target.clone();
    deleted.metadata.insert("deleted".to_owned(), true.into());

    store
        .write_change_target(&room_id(), deleted)
        .await
        .unwrap();
    let missing = store.write_change_target(&room_id(), said("nowhere")).await;

    let mut marks = Vec::new();
    for row in [&edit, &untyped] {
        let read = store.get_event(&row.id).await.unwrap().unwrap();
        marks.push(read.metadata.get("deleted").cloned());
    }
    assert_eq!(
        (
            marks,
            missing.map_err(|error| matches!(error, StoreError::EventNotFound(_)))
        ),
        (vec![Some(true.into()), Some(true.into())], Err(true))
    );
}

/// Python: `tests/test_store_binding_contract.py::test_a_room_is_written_if_it_still_holds_what_the_write_was_computed_from`,
/// on the provided method: a room is written only while it holds the
/// status, the pause and the last activity the write was computed from, its
/// counters kept.
#[tokio::test]
async fn the_default_conditional_room_write_holds_to_what_it_read() {
    let (store, _event) = with_said("hi").await;
    let stored = store.get_room(&room_id()).await.unwrap().unwrap();
    let mut closed = Room::new(room_id());
    closed.status = roomkit::model::RoomStatus::Closed;
    let last = stored.timers.last_activity_at;

    let refused = store
        .update_room_if(closed.clone(), stored.status, stored.paused_at, None)
        .await
        .unwrap();
    let written = store
        .update_room_if(closed, stored.status, stored.paused_at, last)
        .await
        .unwrap();

    assert_eq!(
        (refused, written.map(|room| (room.status, room.event_count))),
        (None, Some((roomkit::model::RoomStatus::Closed, 1)))
    );
}

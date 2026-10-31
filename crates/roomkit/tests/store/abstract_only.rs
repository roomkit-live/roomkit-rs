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
        self.store.update_event(event).await
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

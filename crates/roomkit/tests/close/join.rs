//! RFC §12.10.4, RMK-1019: a close from a tool call joins a close already
//! running, while that close still waits for the store's writes, before it
//! cuts the turns: the joining close spares its own caller's work, the
//! tool's turn, which the first close then does not cut, and both return
//! once the kit is closed. Once the first close cuts the turns, a tool
//! cannot join any more: its turn is cut at once, with no grace (RFC
//! §10.6.8), as Python's is.

use std::sync::Arc;
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelBinding, ChannelId, ChannelType, EventId, EventUpdates, Identity, IdentityId,
    Observation, Participant, ParticipantId, Room, RoomContext, RoomEvent, RoomId, RoomUpdates,
    Task,
};
use roomkit::testing::RecordingTransport;
use roomkit::{
    AttachChannelOptions, ConversationStore, CreateRoomOptions, EventFilter, EventQuery,
    InMemoryStore, RoomCounters, RoomFilter, RoomKit, RoomKitOptions, StoreError, TargetUpdate,
    text_of,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiAgent, ToolDefinition, ToolOutput, Tools};
use serde_json::json;
use tokio::sync::Notify;

use crate::support::{BOUND, channel, into, message, room_id};

/// An in-memory store whose commit of "Slow." tells `committing` it
/// started, then waits for `go`, as a slow database does.
#[derive(Debug, Default)]
struct SlowCommits {
    store: InMemoryStore,
    committing: Arc<Notify>,
    go: Arc<Notify>,
}

impl ConversationStore for SlowCommits {
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
        if text_of(&event.content) == "Slow." {
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

/// RFC §12.10.4, RMK-1019: "Slow." commits in `room_2` for as long as the
/// test holds it, so the first close, from outside, waits for that write
/// before it cuts the turns; meanwhile an agent's tool in `room_1` closes
/// the kit too, joining it: the tool's turn is spared, never cut, and
/// both closes return once the kit is closed, the witness closed by then.
#[tokio::test(start_paused = true)]
async fn a_close_from_a_tool_joining_a_close_still_waiting_for_a_write_spares_its_turn() {
    let kit = RoomKit::new(RoomKitOptions::with_store(SlowCommits::default()));
    let store = kit.store();
    let tool = ClosingTool::default();
    two_rooms(&kit, &tool).await;
    let room_2 = RoomId::new("room_2").unwrap();
    let (entered, go, closes_from_the_tool) = (tool.entered, tool.go, tool.answered);
    let slow = tokio::spawn({
        let kit = kit.clone();
        async move { kit.process_inbound(message("Slow."), into(room_2)).await }
    });
    tokio::time::timeout(BOUND, store.committing.notified())
        .await
        .unwrap();
    let call = tokio::spawn({
        let kit = kit.clone();
        async move { kit.process_inbound(message("Go."), into(room_id())).await }
    });
    tokio::time::timeout(BOUND, entered.notified())
        .await
        .unwrap();
    let first = tokio::spawn({
        let kit = kit.clone();
        async move { kit.close().await }
    });
    // The first close waits for "Slow."'s write; the tool joins it.
    tokio::time::sleep(Duration::from_millis(100)).await;
    go.notify_one();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let joined_early = first.is_finished();
    store.go.notify_one();
    let first = tokio::time::timeout(Duration::from_secs(60), first)
        .await
        .unwrap()
        .unwrap();
    let call = tokio::time::timeout(BOUND, call).await.unwrap().unwrap();
    tokio::time::timeout(BOUND, slow)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let from_the_tool = closes_from_the_tool.lock().await.take();
    assert_eq!(
        (
            joined_early,
            first,
            from_the_tool,
            call.map(|result| result.cancellation_reason)
        ),
        (
            false,
            Ok(()),
            Some(Ok(())),
            Ok(Some("kit_closed".to_owned()))
        )
    );
}

/// What the agent's tool shares with the test: it says it began, waits
/// for `go`, closes the kit and keeps what its close answered.
#[derive(Clone, Default)]
struct ClosingTool {
    entered: Arc<Notify>,
    go: Arc<Notify>,
    answered: Arc<tokio::sync::Mutex<Option<Result<(), roomkit::KitError>>>>,
}

/// `room_1`, with SMS and an agent whose one tool is `tool`, and `room_2`,
/// with SMS.
async fn two_rooms(kit: &RoomKit<SlowCommits>, tool: &ClosingTool) {
    let sms = RecordingTransport::new(channel("sms"), ChannelType::SMS);
    kit.register_channel(sms).unwrap();
    let weak = kit.downgrade();
    let (in_tool, go_tool, answered) = (
        Arc::clone(&tool.entered),
        Arc::clone(&tool.go),
        Arc::clone(&tool.answered),
    );
    let tools = Tools::new().tool(
        ToolDefinition::new("shut_down", "Shut down.", json!({ "type": "object" })),
        move |_call, _context| {
            let (kit, in_tool, go_tool, answered) = (
                weak.upgrade(),
                Arc::clone(&in_tool),
                Arc::clone(&go_tool),
                Arc::clone(&answered),
            );
            async move {
                in_tool.notify_one();
                go_tool.notified().await;
                if let Some(kit) = kit {
                    *answered.lock().await = Some(kit.close().await);
                }
                Ok(ToolOutput::Text("closed".to_owned()))
            }
        },
    );
    let model = ScriptedProvider::new([
        ScriptedRound::calling("", [("c1", "shut_down", "{}")]),
        ScriptedRound::answering("Done."),
    ]);
    let agent = AiAgent::new(channel("agent"), model)
        .with_tools(tools)
        .unwrap();
    kit.register_channel(agent).unwrap();
    let room_2 = RoomId::new("room_2").unwrap();
    for room in [room_id(), room_2] {
        kit.create_room(CreateRoomOptions {
            room_id: Some(room.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
        kit.attach_channel(&room, &channel("sms"), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    kit.attach_channel(
        &room_id(),
        &channel("agent"),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
}

//! Python: `tests/test_turns_channel_control.py` and `tests/test_kit_close.py`.
#![expect(clippy::unwrap_used, reason = "tests fail on invalid fixtures")]

use std::collections::HashMap;
use std::future::{Future, pending, ready};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, InboundMessage,
    Metadata, RoomContext, RoomEvent, RoomId, TextContent,
};
use roomkit::store::PatchRoomMetadataOptions;
use roomkit::testing::{RecordingTransport, RecordingTransportOptions, TurnTap};
use roomkit::{
    AttachChannelOptions, Channel, ChannelCallOptions, ChannelError, ConversationStore,
    CreateRoomOptions, GetRoomOptions, RoomKit, RoomKitError, RoomKitOptions, SendEventOptions,
};
use serde_json::{Value, json};
use tokio::sync::Notify;

#[derive(Default)]
struct State {
    rooms: Mutex<HashMap<RoomId, Metadata>>,
    seen: Mutex<Vec<Metadata>>,
    detached: AtomicUsize,
    entered: Notify,
    cancelled: AtomicBool,
}

struct Cleanup(Arc<State>);
impl Drop for Cleanup {
    fn drop(&mut self) {
        self.0.cancelled.store(true, Ordering::SeqCst);
    }
}

struct Agent(Arc<State>);
impl Channel for Agent {
    fn channel_id(&self) -> ChannelId {
        channel("agent")
    }
    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }
    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }
    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }
    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::empty()))
    }
    fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let seen = self.room_state(&event.room_id).map(|state| {
            self.0.seen.lock().unwrap().push(state);
            ChannelOutput::empty()
        });
        ready(seen)
    }
    fn on_room_detached(
        &self,
        room_id: RoomId,
    ) -> impl Future<Output = Result<(), ChannelError>> + Send {
        self.0.detached.fetch_add(1, Ordering::SeqCst);
        self.0.rooms.lock().unwrap().remove(&room_id);
        ready(Ok(()))
    }
    async fn channel_call(
        &self,
        room_id: RoomId,
        verb: String,
        arguments: Metadata,
    ) -> Result<Value, ChannelError> {
        if verb == "type_error" {
            return Err(RoomKitError::Type("wrong shape".to_owned()).into());
        }
        if verb == "missing_room" {
            return Err(RoomKitError::RoomNotFound(room_id).into());
        }
        if verb == "keep" || verb == "hang" {
            self.0
                .rooms
                .lock()
                .unwrap()
                .entry(room_id.clone())
                .or_default()
                .extend(arguments);
        }
        if verb == "hang" {
            let _cleanup = Cleanup(Arc::clone(&self.0));
            self.0.entered.notify_one();
            pending::<()>().await;
        }
        Ok(Value::Object(self.room_state(&room_id)?))
    }
    fn room_state(&self, room_id: &RoomId) -> Result<Metadata, ChannelError> {
        Ok(self
            .0
            .rooms
            .lock()
            .unwrap()
            .get(room_id)
            .cloned()
            .unwrap_or_default())
    }
    fn restore_room_state(&self, room_id: &RoomId, state: Metadata) -> Result<(), ChannelError> {
        if !state.is_empty() {
            self.0
                .rooms
                .lock()
                .unwrap()
                .entry(room_id.clone())
                .or_insert(state);
        }
        Ok(())
    }
}

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}
fn room() -> RoomId {
    RoomId::new("room").unwrap()
}
fn options() -> ChannelCallOptions {
    ChannelCallOptions {
        arguments: json!({"mode":"fast"}).as_object().unwrap().clone(),
        ..Default::default()
    }
}
async fn kit() -> (RoomKit, Arc<State>, TurnTap) {
    let kit = RoomKit::new(RoomKitOptions::default());
    let state = Arc::new(State::default());
    kit.register_channel(Agent(Arc::clone(&state))).unwrap();
    kit.register_channel(RecordingTransport::new(
        channel("ws"),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["ws", "agent"] {
        kit.attach_channel(&room(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    let tap = TurnTap::on(&kit);
    (kit, state, tap)
}
async fn turn(kit: &RoomKit) {
    kit.send_event(
        &room(),
        &channel("ws"),
        TextContent::new("hello"),
        SendEventOptions::default(),
    )
    .await
    .unwrap();
}
fn messages(tap: &TurnTap, family: &str) -> Vec<Value> {
    tap.messages()
        .iter()
        .filter(|m| m.family == family)
        .map(|m| serde_json::from_str(&m.json).unwrap())
        .collect()
}

// Python: test_a_turn_after_a_channel_call_names_it_and_keeps_its_state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_turn_after_a_channel_call_names_it_and_keeps_its_state() {
    let (kit, state, tap) = kit().await;
    kit.channel_call(&channel("agent"), &room(), "keep", options())
        .await
        .unwrap();
    turn(&kit).await;
    let calls = messages(&tap, "channel_control");
    let answers = messages(&tap, "channel_result");
    assert_eq!(answers[0]["type"], "channel_result");
    let requests = messages(&tap, "request");
    assert_eq!(
        requests[0]["previous_turn"]["turn_id"],
        calls[0]["message"]["call_id"]
    );
    assert_eq!(
        requests[0]["previous_turn"]["executor_id"],
        answers[0]["executor_id"]
    );
    assert_eq!(state.seen.lock().unwrap()[0]["mode"], "fast");
    assert_eq!(state.detached.load(Ordering::SeqCst), 0);
    kit.close().await.unwrap();
}

// Python: test_a_channel_call_the_owner_stopped_waiting_for_is_the_previous_one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_channel_call_the_owner_stopped_waiting_for_is_the_previous_one() {
    let (kit, state, tap) = kit().await;
    turn(&kit).await;
    let running = kit.clone();
    let call = tokio::spawn(async move {
        running
            .channel_call(&channel("agent"), &room(), "hang", options())
            .await
    });
    state.entered.notified().await;
    call.abort();
    assert!(call.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(1), async {
        while !state.cancelled.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    turn(&kit).await;
    let requests = messages(&tap, "request");
    let calls = messages(&tap, "channel_control");
    assert_eq!(
        requests[1]["previous_turn"]["turn_id"],
        calls[0]["message"]["call_id"]
    );
    assert_eq!(state.seen.lock().unwrap()[1]["mode"], "fast");
    assert_eq!(state.detached.load(Ordering::SeqCst), 0);
    assert!(
        !kit.get_room(&room(), GetRoomOptions::default())
            .await
            .unwrap()
            .metadata
            .contains_key("roomkit:channel_state:agent")
    );
    kit.close().await.unwrap();
}

// Python: test_a_turn_hands_the_channel_what_the_room_keeps_for_it.
#[tokio::test]
async fn a_turn_hands_the_channel_what_the_room_keeps_for_it() {
    let (kit, state, _) = kit().await;
    kit.store()
        .patch_room_metadata(
            &room(),
            json!({"roomkit:channel_state:agent":{"mode":"stored"}})
                .as_object()
                .unwrap()
                .clone(),
            PatchRoomMetadataOptions::default(),
        )
        .await
        .unwrap();
    turn(&kit).await;
    assert_eq!(state.seen.lock().unwrap()[0]["mode"], "stored");
    kit.close().await.unwrap();
}

// Python: _changed leaves the room untouched for a verb that changed nothing.
#[tokio::test]
async fn an_unchanged_call_does_not_store_a_channels_private_copy() {
    let (kit, state, tap) = kit().await;
    state
        .rooms
        .lock()
        .unwrap()
        .insert(room(), options().arguments);
    assert_eq!(
        kit.channel_call(
            &channel("agent"),
            &room(),
            "read",
            ChannelCallOptions::default()
        )
        .await
        .unwrap(),
        json!({"mode":"fast"})
    );
    assert!(messages(&tap, "channel_result")[0]["room_state"].is_null());
    assert!(
        !kit.get_room(&room(), GetRoomOptions::default())
            .await
            .unwrap()
            .metadata
            .contains_key("roomkit:channel_state:agent")
    );
    kit.close().await.unwrap();
}

// Python: test_a_channel_verb_ignoring_its_cut_does_not_hold_the_close.
// Dropping a Rust future is its cancellation: no Python cancellation handler.
#[tokio::test(start_paused = true)]
async fn a_held_channel_verb_does_not_hold_the_close() {
    let (kit, state, _) = kit().await;
    let running = kit.clone();
    let call = tokio::spawn(async move {
        running
            .channel_call(&channel("agent"), &room(), "hang", options())
            .await
    });
    state.entered.notified().await;
    tokio::time::timeout(Duration::from_secs(7), kit.close())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(call.await.unwrap(), Err(RoomKitError::Cancelled)));
    assert!(state.cancelled.load(Ordering::SeqCst));
}

// Python: Channel's defaults, and test_a_verb_the_channel_lacks_raises_on_the_owner.
#[tokio::test]
async fn a_channels_default_per_room_contract_keeps_nothing_and_refuses_verbs() {
    let transport = RecordingTransport::new(
        channel("ws"),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    );
    assert!(transport.room_state(&room()).unwrap().is_empty());
    transport
        .restore_room_state(&room(), options().arguments)
        .unwrap();
    assert!(transport.room_state(&room()).unwrap().is_empty());
    let error = transport
        .channel_call(room(), "unknown".to_owned(), Metadata::new())
        .await
        .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<RoomKitError>(),
        Some(RoomKitError::Value(_))
    ));
}

// Python: tests/test_turns_error_classes.py, known errors crossing a channel call.
#[tokio::test]
async fn a_channel_calls_known_errors_reach_the_owner_as_their_types() {
    let (kit, _, _) = kit().await;
    let error = kit
        .channel_call(
            &channel("agent"),
            &room(),
            "type_error",
            ChannelCallOptions::default(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, RoomKitError::Type(message) if message == "wrong shape"));
    let error = kit
        .channel_call(
            &channel("agent"),
            &room(),
            "missing_room",
            ChannelCallOptions::default(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, RoomKitError::RoomNotFound(id) if id == room()));
    kit.close().await.unwrap();
}
struct ReviewHeldDetach(Arc<State>);
impl Channel for ReviewHeldDetach {
    fn channel_id(&self) -> ChannelId {
        channel("held")
    }
    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }
    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }
    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }
    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::empty()))
    }
    async fn channel_call(
        &self,
        room_id: RoomId,
        verb: String,
        arguments: Metadata,
    ) -> Result<Value, ChannelError> {
        Agent(Arc::clone(&self.0))
            .channel_call(room_id, verb, arguments)
            .await
    }
    fn room_state(&self, room_id: &RoomId) -> Result<Metadata, ChannelError> {
        Agent(Arc::clone(&self.0)).room_state(room_id)
    }
    fn restore_room_state(&self, room_id: &RoomId, state: Metadata) -> Result<(), ChannelError> {
        Agent(Arc::clone(&self.0)).restore_room_state(room_id, state)
    }
    async fn on_room_detached(&self, _room_id: RoomId) -> Result<(), ChannelError> {
        let _cleanup = Cleanup(Arc::clone(&self.0));
        self.0.entered.notify_one();
        pending().await
    }
}

// Python: tests/test_channel_call_contract.py::test_cancelled_detach_forgets_stored_state.
#[tokio::test(start_paused = true)]
async fn a_cancelled_detach_forgets_stored_channel_state() {
    let (kit, _, _) = kit().await;
    let state = Arc::new(State::default());
    kit.register_channel(ReviewHeldDetach(Arc::clone(&state)))
        .unwrap();
    kit.attach_channel(&room(), &channel("held"), AttachChannelOptions::default())
        .await
        .unwrap();
    kit.channel_call(&channel("held"), &room(), "keep", options())
        .await
        .unwrap();
    let key = "roomkit:channel_state:held";
    assert!(
        kit.get_room(&room(), GetRoomOptions::default())
            .await
            .unwrap()
            .metadata
            .contains_key(key)
    );
    let worker = kit.clone();
    let detaching = tokio::spawn(async move {
        worker
            .detach_channel(
                &room(),
                &channel("held"),
                roomkit::DetachChannelOptions::default(),
            )
            .await
    });
    state.entered.notified().await;
    detaching.abort();
    assert!(detaching.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(2), async {
        while !state.cancelled.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(2), roomkit::testing::idle(&kit))
        .await
        .unwrap();
    let still_stored = kit
        .get_room(&room(), GetRoomOptions::default())
        .await
        .unwrap()
        .metadata
        .contains_key(key);
    kit.close().await.unwrap();
    assert!(
        !still_stored,
        "a cancelled detach left persisted channel state"
    );
}

struct ReviewCallScope {
    kit: Arc<std::sync::OnceLock<roomkit::WeakRoomKit>>,
    inherited_host: Arc<AtomicBool>,
    finished: Arc<AtomicBool>,
}

impl Channel for ReviewCallScope {
    fn channel_id(&self) -> ChannelId {
        channel("caller")
    }
    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }
    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }
    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }
    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::empty()))
    }
    async fn on_event(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        let kit = self.kit.get().unwrap().upgrade().unwrap();
        let answer = kit
            .channel_call(
                &channel("caller"),
                &room(),
                "close",
                ChannelCallOptions::default(),
            )
            .await?;
        assert_eq!(answer, json!("closed"));
        self.finished.store(true, Ordering::SeqCst);
        Ok(ChannelOutput::empty())
    }
    async fn channel_call(
        &self,
        _room_id: RoomId,
        _verb: String,
        _arguments: Metadata,
    ) -> Result<Value, ChannelError> {
        self.inherited_host
            .store(roomkit::current_turn_host().is_some(), Ordering::SeqCst);
        self.kit
            .get()
            .unwrap()
            .upgrade()
            .unwrap()
            .turn_executor()
            .close()
            .await;
        Ok(json!("closed"))
    }
}

// Python: test_channel_call_can_close_executor_under_its_calling_turn.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_channel_call_closing_its_executor_spares_its_calling_turn() {
    let (kit, _, _) = kit().await;
    let held = Arc::new(std::sync::OnceLock::new());
    let inherited_host = Arc::new(AtomicBool::new(false));
    let finished = Arc::new(AtomicBool::new(false));
    held.set(kit.downgrade()).unwrap();
    kit.register_channel(ReviewCallScope {
        kit: held,
        inherited_host: Arc::clone(&inherited_host),
        finished: Arc::clone(&finished),
    })
    .unwrap();
    kit.attach_channel(&room(), &channel("caller"), AttachChannelOptions::default())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), turn(&kit))
        .await
        .unwrap();
    kit.close().await.unwrap();
    assert!(
        finished.load(Ordering::SeqCst),
        "the channel verb's close cancelled its calling turn"
    );
    assert!(
        inherited_host.load(Ordering::SeqCst),
        "the local channel verb lost its calling turn's host"
    );
}

struct ReviewReattach {
    kit: Arc<std::sync::OnceLock<roomkit::WeakRoomKit>>,
    completed: Arc<AtomicBool>,
}

impl Channel for ReviewReattach {
    fn channel_id(&self) -> ChannelId {
        channel("reattach")
    }
    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }
    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }
    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }
    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::empty()))
    }
    async fn on_room_detached(&self, room_id: RoomId) -> Result<(), ChannelError> {
        let kit = self.kit.get().unwrap().upgrade().unwrap();
        kit.attach_channel(
            &room_id,
            &channel("reattach"),
            AttachChannelOptions::default(),
        )
        .await?;
        self.completed.store(true, Ordering::SeqCst);
        Ok(())
    }
}

// Python: test_detach_callback_can_reattach_its_channel.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_detach_callback_can_reattach_its_channel() {
    let (kit, _, _) = kit().await;
    let held = Arc::new(std::sync::OnceLock::new());
    let completed = Arc::new(AtomicBool::new(false));
    held.set(kit.downgrade()).unwrap();
    kit.register_channel(ReviewReattach {
        kit: held,
        completed: Arc::clone(&completed),
    })
    .unwrap();
    kit.attach_channel(
        &room(),
        &channel("reattach"),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    let detached = tokio::time::timeout(
        Duration::from_secs(2),
        kit.detach_channel(
            &room(),
            &channel("reattach"),
            roomkit::DetachChannelOptions::default(),
        ),
    )
    .await;
    let reattached = kit
        .store()
        .get_binding(&room(), &channel("reattach"))
        .await
        .unwrap()
        .is_some();
    kit.close().await.unwrap();
    assert!(
        detached.is_ok(),
        "reattachment waited for its own detach finalizer"
    );
    detached.unwrap().unwrap();
    assert!(completed.load(Ordering::SeqCst));
    assert!(reattached);
}

// Python: detach's finally runs during a kit close as well as caller cancellation.
#[tokio::test(start_paused = true)]
async fn closing_the_kit_joins_a_cancelled_detachs_state_cleanup() {
    let (kit, _, _) = kit().await;
    let state = Arc::new(State::default());
    kit.register_channel(ReviewHeldDetach(Arc::clone(&state)))
        .unwrap();
    kit.attach_channel(&room(), &channel("held"), AttachChannelOptions::default())
        .await
        .unwrap();
    kit.channel_call(&channel("held"), &room(), "keep", options())
        .await
        .unwrap();
    let store = kit.store();
    let worker = kit.clone();
    let detaching = tokio::spawn(async move {
        worker
            .detach_channel(
                &room(),
                &channel("held"),
                roomkit::DetachChannelOptions::default(),
            )
            .await
    });
    state.entered.notified().await;
    tokio::time::timeout(Duration::from_secs(7), kit.close())
        .await
        .unwrap()
        .unwrap();
    let outcome = detaching.await.unwrap();
    assert!(
        matches!(outcome, Err(RoomKitError::KitClosed)),
        "{outcome:?}"
    );
    assert!(state.cancelled.load(Ordering::SeqCst));
    assert!(
        !store
            .get_room(&room())
            .await
            .unwrap()
            .unwrap()
            .metadata
            .contains_key("roomkit:channel_state:held")
    );
}

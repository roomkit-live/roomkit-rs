//! Edits and deletes on the tokio runtime (RFC §10.3, §10.1 steps 8a and 12,
//! §6.6, Appendix B.6 and B.7): a WhatsApp customer's edit and delete,
//! delivered natively to a WebSocket and as text to SMS, read by an agent;
//! the mutation triggers; a blocked edit; the host's direct calls; and what
//! the room lock keeps whole when changes of one target race.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::future::{Future, ready};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use roomkit::model::hook::{AllowOptions, BlockOptions};
use roomkit::model::{
    ChannelBinding, ChannelCapabilities, ChannelCategory, ChannelDirection, ChannelId,
    ChannelOutput, ChannelType, DeleteContent, EditContent, EditSource, EventContent, EventId,
    EventSource, EventType, HookResult, InboundMessage, RoomContext, RoomEvent, RoomId,
    TextContent, Timestamp,
};
use roomkit::testing::{
    Environment, RecordingTransport, RecordingTransportOptions, SequentialEnvironment,
    kit_with_environment,
};
use roomkit::{
    AttachChannelOptions, Channel, ChannelDeclaration, ChannelError, CommitEventOptions,
    ConversationStore, CreateRoomOptions, DeleteEventOptions, EventFilter, HookTrigger,
    InboundResult, ProcessInboundOptions, RoomKit, RoomKitOptions, SendEventOptions,
    UpdateEventOptions, is_deleted, is_edited, text_of,
};
use tokio::sync::{Barrier, Notify, mpsc};

const CUSTOMER: &str = "+15551234567";
const STRANGER: &str = "+15550000009";

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("room_1").unwrap()
}

/// An event an agent read, with the texts of the history it was handed.
type Reading = (RoomEvent, Vec<String>);

/// An agent that reads what it is asked and says nothing, keeping each
/// event with the history it was handed, as texts and whole. It takes
/// edits and deletes natively unless it is `plain`.
#[derive(Clone, Default)]
struct Reader {
    read: Arc<Mutex<Vec<Reading>>>,
    histories: Arc<Mutex<Vec<Vec<RoomEvent>>>>,
    plain: bool,
}

impl Reader {
    /// A reader that takes no edit nor delete natively: it is handed them
    /// as their text forms.
    fn plain() -> Self {
        Self {
            plain: true,
            ..Self::default()
        }
    }

    fn read(&self) -> Vec<Reading> {
        self.read
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Each history it was handed, whole.
    fn histories(&self) -> Vec<Vec<RoomEvent>> {
        self.histories
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl Channel for Reader {
    fn declaration(&self) -> ChannelDeclaration {
        let mut declaration = ChannelDeclaration::new(
            channel("ai"),
            ChannelType::AI,
            ChannelCategory::Intelligence,
            ChannelDirection::Bidirectional,
        );
        declaration.capabilities = if self.plain {
            ChannelCapabilities::default()
        } else {
            edits_natively()
        };
        declaration
    }

    fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let history = context
            .recent_events
            .iter()
            .map(|event| text_of(&event.content))
            .collect();
        self.histories
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(context.recent_events.clone());
        self.read
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((event, history));
        ready(Ok(ChannelOutput::default()))
    }
}

/// Text, with edits and deletes delivered natively.
fn edits_natively() -> ChannelCapabilities {
    ChannelCapabilities {
        supports_edit: true,
        supports_delete: true,
        ..ChannelCapabilities::default()
    }
}

/// The room of Appendix B.6: the customer on WhatsApp, an advisor on a
/// WebSocket that edits natively, an SMS copy that does not, and an agent
/// that reads. The customer's "I need 5000$" is `evt_1`.
struct Fixture {
    kit: RoomKit,
    ws: RecordingTransport,
    sms: RecordingTransport,
    agent: Reader,
}

/// The fixture's environment: while it is set up, its events, the
/// attaches' lifecycle records (gap G-046) among them, take ids of their
/// own, so that the room's first message is `evt_1`.
#[derive(Debug)]
struct Setup {
    fixture: SequentialEnvironment,
    room: SequentialEnvironment,
    setting_up: AtomicBool,
}

impl Setup {
    fn new() -> Arc<Self> {
        let at = Timestamp::from_unix_millis(1_792_677_780_000).unwrap();
        Arc::new(Self {
            fixture: SequentialEnvironment::new(at),
            room: SequentialEnvironment::new(at),
            setting_up: AtomicBool::new(true),
        })
    }

    /// The room's own events take the room's ids from now on.
    fn done(&self) {
        self.setting_up.store(false, Ordering::Relaxed);
    }
}

impl Environment for Setup {
    fn now(&self) -> Timestamp {
        self.room.now()
    }

    fn new_event_id(&self) -> EventId {
        if self.setting_up.load(Ordering::Relaxed) {
            EventId::new(format!("setup_{}", self.fixture.new_event_id())).unwrap()
        } else {
            self.room.new_event_id()
        }
    }

    fn new_observation_id(&self) -> roomkit::model::ObservationId {
        self.room.new_observation_id()
    }

    fn new_room_id(&self) -> RoomId {
        self.room.new_room_id()
    }
}

async fn room() -> Fixture {
    room_with(Reader::default()).await
}

/// The room, its agent `agent`.
async fn room_with(agent: Reader) -> Fixture {
    let setup = Setup::new();
    let kit = kit_with_environment(RoomKitOptions::default(), Arc::clone(&setup));
    let wa = RecordingTransport::new(
        channel("wa"),
        ChannelType::WHATSAPP,
        RecordingTransportOptions::default(),
    );
    let ws = RecordingTransport::new(
        channel("ws"),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions {
            capabilities: Some(edits_natively()),
        },
    );
    let sms = RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(wa).unwrap();
    kit.register_channel(ws.clone()).unwrap();
    kit.register_channel(sms.clone()).unwrap();
    kit.register_channel(agent.clone()).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["wa", "ws", "sms", "ai"] {
        kit.attach_channel(&room_id(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    // The room's messages start at evt_1, after the attaches' records.
    setup.done();
    let first = say(&kit, CUSTOMER, "I need 5000$").await;
    assert_eq!(first.event.unwrap().id.as_str(), "evt_1");
    Fixture {
        kit,
        ws,
        sms,
        agent,
    }
}

async fn say(kit: &RoomKit, sender: &str, text: &str) -> InboundResult {
    let message = InboundMessage::new(channel("wa"), sender, TextContent::new(text));
    kit.process_inbound(
        message,
        ProcessInboundOptions {
            room_id: Some(room_id()),
            ..Default::default()
        },
    )
    .await
    .unwrap()
}

fn edit_from(sender: &str, text: &str) -> InboundMessage {
    let mut message = InboundMessage::new(
        channel("wa"),
        sender,
        EventContent::Edit(EditContent {
            edit_source: Some(EditSource::Sender),
            ..EditContent::new(EventId::new("evt_1").unwrap(), TextContent::new(text))
        }),
    );
    message.event_type = EventType::Edit;
    message
}

fn delete() -> InboundMessage {
    let mut message = InboundMessage::new(
        channel("wa"),
        CUSTOMER,
        EventContent::Delete(DeleteContent::new(EventId::new("evt_1").unwrap())),
    );
    message.event_type = EventType::Delete;
    message
}

async fn send(kit: &RoomKit, message: InboundMessage) -> InboundResult {
    kit.process_inbound(
        message,
        ProcessInboundOptions {
            room_id: Some(room_id()),
            ..Default::default()
        },
    )
    .await
    .unwrap()
}

/// The stored `evt_1`, if it is still stored.
async fn target(kit: &RoomKit) -> Option<RoomEvent> {
    kit.store()
        .get_event(&EventId::new("evt_1").unwrap())
        .await
        .unwrap()
}

/// The payloads of `trigger` from now on.
fn observed(kit: &RoomKit, trigger: HookTrigger) -> mpsc::UnboundedReceiver<RoomEvent> {
    let (seen, observed) = mpsc::unbounded_channel();
    kit.observe("projection", trigger, move |event, _| {
        let seen = seen.clone();
        async move {
            seen.send(event).unwrap();
        }
    })
    .unwrap();
    observed
}

async fn next(observed: &mut mpsc::UnboundedReceiver<RoomEvent>) -> RoomEvent {
    tokio::time::timeout(Duration::from_secs(1), observed.recv())
        .await
        .unwrap()
        .unwrap()
}

/// Whether nothing is observed for a while.
async fn silent(observed: &mut mpsc::UnboundedReceiver<RoomEvent>) -> bool {
    tokio::time::timeout(Duration::from_millis(100), observed.recv())
        .await
        .is_err()
}

/// RFC Appendix B.6.
mod an_edit {
    use pretty_assertions::assert_eq;

    use super::*;

    #[tokio::test]
    async fn updates_its_target_and_is_stored_after_it() {
        let room = room().await;
        let result = send(&room.kit, edit_from(CUSTOMER, "I need 50000$")).await;
        let target = target(&room.kit).await.unwrap();
        assert_eq!(
            (
                result.event.map(|event| (event.index, event.r#type)),
                target.content.clone(),
                Some(is_edited(&target))
            ),
            (
                // Four lifecycle records and the target come first.
                Some((5, EventType::Edit)),
                TextContent::new("I need 50000$").into(),
                Some(true)
            )
        );
    }

    #[tokio::test]
    async fn reaches_a_channel_that_edits_as_the_edit_itself() {
        let room = room().await;
        send(&room.kit, edit_from(CUSTOMER, "I need 50000$")).await;
        let delivered = room.ws.deliveries().last().unwrap().event.content.clone();
        assert!(matches!(delivered, EventContent::Edit(_)));
    }

    #[tokio::test]
    async fn reaches_sms_as_a_correction() {
        let room = room().await;
        send(&room.kit, edit_from(CUSTOMER, "I need 50000$")).await;
        assert_eq!(
            room.sms.texts(),
            ["I need 5000$", "Correction: I need 50000$"]
        );
    }

    /// RFC Appendix B.6: the agent reads the edit, with the corrected text
    /// in its history, then the edit itself as its newest event, as the
    /// Python implementation's turn reads it (RMK-1023, probe
    /// `py_turn_after_edit.out`).
    #[tokio::test]
    async fn is_read_by_the_agent_with_its_target_corrected() {
        let room = room().await;
        send(&room.kit, edit_from(CUSTOMER, "I need 50000$")).await;
        let (event, history) = room.agent.read().last().cloned().unwrap();
        assert_eq!(
            (event.r#type, history),
            (
                EventType::Edit,
                vec![
                    "I need 50000$".to_owned(),
                    "Correction: I need 50000$".to_owned()
                ]
            )
        );
    }

    /// An agent that takes no edit natively is handed the edit as its
    /// correction's text, and reads the edit in its history as the store
    /// keeps it, as the Python implementation's context holds it (light
    /// review 2, `names_and_transcoded.out`).
    #[tokio::test]
    async fn is_in_the_history_as_stored_for_an_agent_that_takes_no_edit() {
        let room = room_with(Reader::plain()).await;
        send(&room.kit, edit_from(CUSTOMER, "I need 50000$")).await;
        let (event, _) = room.agent.read().last().cloned().unwrap();
        let history = room.agent.histories().last().cloned().unwrap();
        let newest = history.last().unwrap();
        assert_eq!(
            (
                event.content,
                newest.r#type,
                matches!(newest.content, EventContent::Edit(_))
            ),
            (
                TextContent::new("Correction: I need 50000$").into(),
                EventType::Edit,
                true
            )
        );
    }

    #[tokio::test]
    async fn fires_on_event_updated_with_the_edited_target() {
        let room = room().await;
        let mut updated = observed(&room.kit, HookTrigger::OnEventUpdated);
        send(&room.kit, edit_from(CUSTOMER, "I need 50000$")).await;
        let payload = next(&mut updated).await;
        assert_eq!(
            (payload.id.as_str(), payload.content),
            ("evt_1", TextContent::new("I need 50000$").into())
        );
    }

    /// RFC §10.3: a blocked edit leaves its target unmutated and fires no
    /// trigger.
    #[tokio::test]
    async fn blocked_by_a_hook_leaves_its_target_and_fires_nothing() {
        let room = room().await;
        let mut updated = observed(&room.kit, HookTrigger::OnEventUpdated);
        room.kit
            .before_broadcast("no_edits", |event, _| async move {
                Ok(if event.r#type == EventType::Edit {
                    HookResult::block("edits are closed", BlockOptions::default())
                } else {
                    HookResult::allow(AllowOptions::default())
                })
            })
            .unwrap();
        let result = send(&room.kit, edit_from(CUSTOMER, "I need 50000$")).await;
        assert_eq!(
            (
                result.reason.as_deref(),
                target(&room.kit).await.unwrap().content,
                silent(&mut updated).await
            ),
            (
                Some("edits are closed"),
                TextContent::new("I need 5000$").into(),
                true
            )
        );
    }

    /// RFC §10.1 step 8a: nothing of a refused edit is stored.
    #[tokio::test]
    async fn from_someone_else_is_refused_and_stores_nothing() {
        let room = room().await;
        let result = send(&room.kit, edit_from(STRANGER, "I need 0$")).await;
        let stored = room.kit.store().get_event_count(&room_id(), None).await;
        assert_eq!(
            (
                result.blocked,
                result.reason.as_deref(),
                result.event,
                stored
            ),
            (true, Some("not_original_author"), None, Ok(5))
        );
    }
}

/// RFC Appendix B.7.
mod a_delete {
    use pretty_assertions::assert_eq;

    use super::*;

    #[tokio::test]
    async fn marks_its_target_deleted() {
        let room = room().await;
        send(&room.kit, delete()).await;
        let target = target(&room.kit).await.unwrap();
        assert_eq!(
            (target.content.clone(), Some(is_deleted(&target))),
            (TextContent::new("I need 5000$").into(), Some(true))
        );
    }

    #[tokio::test]
    async fn reaches_each_channel_its_way() {
        let room = room().await;
        send(&room.kit, delete()).await;
        let native = room.ws.deliveries().last().unwrap().event.content.clone();
        assert_eq!(
            (
                matches!(native, EventContent::Delete(_)),
                room.sms.texts().last().cloned()
            ),
            (true, Some("[Message deleted]".to_owned()))
        );
    }

    /// Gap G-062: the agent's next turn reads the deleted message with its
    /// text withheld, which the store keeps. Its history is the room's
    /// messages up to the turn's own (RFC §6.4, RMK-1023), as the Python
    /// implementation's conversation reads them: the delete, an event of its
    /// own type, is not one of them.
    #[tokio::test]
    async fn leaves_the_agent_s_next_turn_without_the_deleted_text() {
        let room = room().await;
        send(&room.kit, delete()).await;
        say(&room.kit, CUSTOMER, "Is it gone?").await;
        let (_, history) = room.agent.read().last().cloned().unwrap();
        assert_eq!(
            (history, target(&room.kit).await.unwrap().content),
            (
                vec![String::new(), "Is it gone?".to_owned()],
                TextContent::new("I need 5000$").into()
            )
        );
    }

    /// The agent's turn on the delete reads the deleted message with its
    /// text withheld, then the delete itself as its history's newest event,
    /// as the Python implementation's turn reads it (RMK-1023, light
    /// review's probe `py_turn_after_delete.out`).
    #[tokio::test]
    async fn is_read_by_the_agent_as_its_history_s_newest_event() {
        let room = room().await;
        send(&room.kit, delete()).await;
        let (event, history) = room.agent.read().last().cloned().unwrap();
        assert_eq!(
            (event.r#type, history),
            (
                EventType::Delete,
                vec![String::new(), "[Message deleted]".to_owned()]
            )
        );
    }

    #[tokio::test]
    async fn fires_on_event_deleted_with_the_deleted_target() {
        let room = room().await;
        let mut deleted = observed(&room.kit, HookTrigger::OnEventDeleted);
        send(&room.kit, delete()).await;
        let payload = next(&mut deleted).await;
        assert_eq!(
            (payload.id.as_str(), Some(is_deleted(&payload))),
            ("evt_1", Some(true))
        );
    }
}

/// RFC §10.3 "Direct mutation (host-owned)".
mod a_direct_call {
    use pretty_assertions::assert_eq;

    use super::*;

    #[tokio::test]
    async fn update_rewrites_the_event_without_a_marker_and_fires_on_event_updated() {
        let room = room().await;
        let mut updated = observed(&room.kit, HookTrigger::OnEventUpdated);
        let update = UpdateEventOptions {
            content: Some(TextContent::new("I need 6000$").into()),
            ..UpdateEventOptions::default()
        };
        let returned = room
            .kit
            .update_event(&room_id(), &EventId::new("evt_1").unwrap(), update)
            .await
            .unwrap()
            .unwrap();
        let payload = next(&mut updated).await;
        assert_eq!(
            (returned.metadata.is_empty(), payload),
            (true, target(&room.kit).await.unwrap())
        );
    }

    #[tokio::test]
    async fn update_of_a_missing_event_changes_nothing() {
        let room = room().await;
        let returned = room
            .kit
            .update_event(
                &room_id(),
                &EventId::new("evt_404").unwrap(),
                UpdateEventOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(returned, None);
    }

    /// Gap G-063: a room that does not exist is a no-op, as a missing
    /// event is.
    #[tokio::test]
    async fn update_and_delete_in_a_missing_room_change_nothing() {
        let room = room().await;
        let missing = RoomId::new("room_404").unwrap();
        let event_id = EventId::new("evt_1").unwrap();
        let updated = room
            .kit
            .update_event(&missing, &event_id, UpdateEventOptions::default())
            .await;
        let deleted = room
            .kit
            .delete_event(&missing, &event_id, DeleteEventOptions::default())
            .await;
        assert_eq!((updated, deleted), (Ok(None), Ok(vec![])));
    }

    /// Gap G-064: the hard delete takes the thread; the counters stay.
    #[tokio::test]
    async fn delete_removes_the_thread_and_leaves_the_counters() {
        let room = room().await;
        let mut reply = InboundMessage::new(channel("wa"), CUSTOMER, TextContent::new("Ok?"));
        reply.parent_event_id = Some(EventId::new("evt_1").unwrap());
        send(&room.kit, reply).await;
        say(&room.kit, CUSTOMER, "Unrelated").await;
        let removed = room
            .kit
            .delete_event(
                &room_id(),
                &EventId::new("evt_1").unwrap(),
                DeleteEventOptions::default(),
            )
            .await
            .unwrap();
        let stored = room
            .kit
            .store()
            .get_room(&room_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (
                removed.iter().map(EventId::as_str).collect::<Vec<_>>(),
                stored.event_count,
                room.kit.store().get_event_count(&room_id(), None).await
            ),
            (vec!["evt_1", "evt_2"], 7, Ok(5))
        );
    }

    /// RFC §10.1 step 8, §10.3: every path that commits an event roots its
    /// parent, so a hard delete of the root takes the whole thread, one
    /// level deep: a reply, a direct injection and a record answering that
    /// reply all name the root.
    #[tokio::test]
    async fn delete_of_the_root_takes_every_answer_to_its_replies() {
        let room = room().await;
        let reply_to = |id: &str| Some(EventId::new(id).unwrap());
        let mut reply = InboundMessage::new(channel("wa"), CUSTOMER, TextContent::new("Ok?"));
        reply.parent_event_id = reply_to("evt_1");
        let reply = send(&room.kit, reply).await.event.unwrap().id;
        let injected = SendEventOptions {
            parent_event_id: Some(reply.clone()),
            ..Default::default()
        };
        room.kit
            .send_event(
                &room_id(),
                &channel("ws"),
                TextContent::new("Checking"),
                injected,
            )
            .await
            .unwrap();
        let mut record = RoomEvent {
            id: EventId::new("snap_1").unwrap(),
            created_at: Timestamp::UNIX_EPOCH,
            ..RoomEvent::new(
                room_id(),
                EventSource::new(channel("ws"), ChannelType::WEBSOCKET),
                TextContent::new("Snapshot"),
            )
        };
        record.parent_event_id = Some(reply);
        room.kit
            .commit_event(&room_id(), record, CommitEventOptions::default())
            .await
            .unwrap();
        room.kit
            .delete_event(
                &room_id(),
                &EventId::new("evt_1").unwrap(),
                DeleteEventOptions::default(),
            )
            .await
            .unwrap();
        // The attaches' lifecycle records alone are left.
        assert_eq!(
            room.kit.store().get_event_count(&room_id(), None).await,
            Ok(4)
        );
    }

    #[tokio::test]
    async fn delete_fires_on_event_deleted_for_each_event_removed() {
        let room = room().await;
        let mut reply = InboundMessage::new(channel("wa"), CUSTOMER, TextContent::new("Ok?"));
        reply.parent_event_id = Some(EventId::new("evt_1").unwrap());
        send(&room.kit, reply).await;
        let mut deleted = observed(&room.kit, HookTrigger::OnEventDeleted);
        room.kit
            .delete_event(
                &room_id(),
                &EventId::new("evt_1").unwrap(),
                DeleteEventOptions::default(),
            )
            .await
            .unwrap();
        let mut ids = vec![next(&mut deleted).await.id, next(&mut deleted).await.id];
        ids.sort();
        assert_eq!(
            ids,
            [
                EventId::new("evt_1").unwrap(),
                EventId::new("evt_2").unwrap()
            ]
        );
    }

    /// Gap G-064: an index is never given again.
    #[tokio::test]
    async fn after_a_hard_delete_the_next_message_takes_a_new_index() {
        let room = room().await;
        room.kit
            .delete_event(
                &room_id(),
                &EventId::new("evt_1").unwrap(),
                DeleteEventOptions::default(),
            )
            .await
            .unwrap();
        let next = say(&room.kit, CUSTOMER, "Hello?").await;
        let received = room
            .kit
            .store()
            .get_event_count(&room_id(), Some(EventFilter::default()))
            .await;
        assert_eq!(
            (next.event.map(|event| event.index), received),
            (Some(5), Ok(5))
        );
    }
}

/// RFC §13.5: changes of one target that race are serialized by the room
/// lock, each decided on the target as the previous one left it.
mod changes_at_once {
    use pretty_assertions::assert_eq;

    use super::*;

    /// Runs `first` and `second` on two tasks released together, spawning
    /// `first` first on even rounds and `second` first on odd ones.
    async fn at_once<A, B>(
        round: usize,
        first: impl Future<Output = A> + Send + 'static,
        second: impl Future<Output = B> + Send + 'static,
    ) -> (A, B)
    where
        A: Send + 'static,
        B: Send + 'static,
    {
        let start = Arc::new(Barrier::new(2));
        let (first_start, second_start) = (Arc::clone(&start), start);
        let first = async move {
            first_start.wait().await;
            first.await
        };
        let second = async move {
            second_start.wait().await;
            second.await
        };
        if round.is_multiple_of(2) {
            let first = tokio::spawn(first);
            let second = tokio::spawn(second);
            (first.await.unwrap(), second.await.unwrap())
        } else {
            let second = tokio::spawn(second);
            let first = tokio::spawn(first);
            (first.await.unwrap(), second.await.unwrap())
        }
    }

    /// An edit and a delete of one target at once: the edit commits first
    /// and the delete marks the edited target, or the delete commits first
    /// and the edit is refused. Never an edit of a deleted target.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_edit_and_a_delete_commit_in_one_order_or_the_other() {
        for round in 0..25 {
            let room = room().await;
            let (kit, other) = (room.kit.clone(), room.kit.clone());
            let (edited, deleted) = at_once(
                round,
                Box::pin(async move { send(&kit, edit_from(CUSTOMER, "I need 50000$")).await }),
                Box::pin(async move { send(&other, delete()).await }),
            )
            .await;
            let target = target(&room.kit).await.unwrap();
            let marked = is_deleted(&target);
            let outcome = match (edited.event, deleted.event) {
                (Some(edit), Some(delete)) if edit.index < delete.index => {
                    target.content == TextContent::new("I need 50000$").into() && marked
                }
                (None, Some(_)) => {
                    edited.reason.as_deref() == Some("target_event_not_found")
                        && target.content == TextContent::new("I need 5000$").into()
                        && marked
                }
                _ => false,
            };
            assert!(outcome, "the edit and the delete interleaved");
        }
    }

    /// A direct hard delete and an edit at once: the edit is refused, or it
    /// commits before the delete removes its target, and the edit with it
    /// (gap G-063). Its commit never finds the target gone, which would fail
    /// the message.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_hard_delete_and_an_edit_never_fail_the_edit() {
        for round in 0..25 {
            let room = room().await;
            let (kit, other) = (room.kit.clone(), room.kit.clone());
            let (edited, removed) = at_once(
                round,
                async move {
                    kit.process_inbound(
                        edit_from(CUSTOMER, "I need 50000$"),
                        ProcessInboundOptions {
                            room_id: Some(room_id()),
                            ..Default::default()
                        },
                    )
                    .await
                },
                async move {
                    other
                        .delete_event(
                            &room_id(),
                            &EventId::new("evt_1").unwrap(),
                            DeleteEventOptions::default(),
                        )
                        .await
                },
            )
            .await;
            let edited = edited.unwrap();
            let expected = match &edited.event {
                Some(edit) => vec![EventId::new("evt_1").unwrap(), edit.id.clone()],
                None => vec![EventId::new("evt_1").unwrap()],
            };
            let consistent = removed.unwrap() == expected
                && target(&room.kit).await.is_none()
                && (edited.event.is_some()
                    || edited.reason.as_deref() == Some("target_event_not_found"));
            assert!(consistent, "the edit and the hard delete interleaved");
        }
    }

    /// A hard delete called while an edit holds the room lock waits for it:
    /// the edit commits with its target, then the delete removes the edited
    /// target and the edit, which holds its corrected text (gap G-063).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_hard_delete_waiting_on_an_edit_s_lock_removes_the_edited_target() {
        let room = room().await;
        let locked = Arc::new(Notify::new());
        let holding = Arc::clone(&locked);
        room.kit
            .before_broadcast("slow_scan", move |event, _| {
                let holding = Arc::clone(&holding);
                async move {
                    if event.r#type == EventType::Edit {
                        holding.notify_one();
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    Ok(HookResult::allow(AllowOptions::default()))
                }
            })
            .unwrap();
        let kit = room.kit.clone();
        let editing =
            tokio::spawn(async move { send(&kit, edit_from(CUSTOMER, "I need 50000$")).await });
        locked.notified().await;
        let removed = room
            .kit
            .delete_event(
                &room_id(),
                &EventId::new("evt_1").unwrap(),
                DeleteEventOptions::default(),
            )
            .await
            .unwrap();
        let edited = editing.await.unwrap();
        let left = room
            .kit
            .store()
            .list_events(&room_id(), &every_row())
            .await
            .unwrap();
        assert_eq!(
            (
                edited.event.map(|event| event.index),
                removed,
                left.iter().map(|event| event.r#type).collect::<Vec<_>>()
            ),
            (
                Some(5),
                vec![
                    EventId::new("evt_1").unwrap(),
                    EventId::new("evt_2").unwrap()
                ],
                vec![EventType::ChannelAttached; 4]
            )
        );
    }

    /// Two edits at once both commit; the target holds the one committed
    /// last.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_edits_leave_the_target_with_the_one_committed_last() {
        for round in 0..25 {
            let room = room().await;
            let (kit, other) = (room.kit.clone(), room.kit.clone());
            let (first, second) = at_once(
                round,
                Box::pin(async move { send(&kit, edit_from(CUSTOMER, "I need 50000$")).await }),
                Box::pin(async move { send(&other, edit_from(CUSTOMER, "I need 60000$")).await }),
            )
            .await;
            let (first, second) = (first.event.unwrap(), second.event.unwrap());
            let last = if first.index > second.index {
                "I need 50000$"
            } else {
                "I need 60000$"
            };
            assert_eq!(
                target(&room.kit).await.unwrap().content,
                TextContent::new(last).into()
            );
        }
    }
}

/// Every row of a room's timeline, the refused ones included.
fn every_row() -> roomkit::EventQuery {
    roomkit::EventQuery {
        limit: u64::MAX,
        event_filter: Some(EventFilter {
            include_blocked: true,
            ..EventFilter::default()
        }),
        ..roomkit::EventQuery::default()
    }
}

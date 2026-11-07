//! The spans of a room's delivery cursor (RFC §13.5, §15.7): its reads and
//! writes sit in the trace of a message they deliver, under its
//! `framework.inbound`, never a root, as every other store span of the
//! message. Python's `tests/test_telemetry.py::TestStoreQuerySpan`, on an
//! in-memory store that opens the `store.<operation>` spans
//! `roomkit-postgres` opens.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::collections::BTreeSet;
use std::future::{Future, ready};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::hook::{AllowOptions, BlockOptions};
use roomkit::model::{
    ChannelBinding, ChannelId, ChannelOutput, ChannelType, EventContent, EventId, EventSource,
    HookResult, Identity, IdentityId, InboundMessage, Observation, Participant, ParticipantId,
    Room, RoomContext, RoomEvent, RoomId, Task, TextContent,
};
use roomkit::telemetry::{
    EndSpanOptions, MockTelemetryProvider, Span, SpanKind, StartSpanOptions, TelemetryProvider,
    get_current_span,
};
use roomkit::testing::{EventTap, RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, ConversationStore, CreateRoomOptions, HookOptions,
    HookTrigger, InMemoryStore, ProcessInboundOptions, RoomKit, RoomKitOptions, StoreError, store,
};
use tokio::sync::Notify;

const CURSOR: [&str; 2] = ["store.get_delivered_index", "store.advance_delivered_index"];

fn id(channel: &str) -> ChannelId {
    ChannelId::new(channel).unwrap()
}

fn room() -> RoomId {
    RoomId::new("r1").unwrap()
}

fn message(text: &str) -> InboundMessage {
    InboundMessage::new(id("src"), "u", TextContent::new(text))
}

fn in_room() -> ProcessInboundOptions {
    ProcessInboundOptions {
        room_id: Some(room()),
        ..ProcessInboundOptions::default()
    }
}

/// An in-memory store that opens a `store.<operation>` span over each cursor
/// read and write, under the span current when it is called, as
/// `roomkit-postgres`'s.
#[derive(Default)]
struct CursorSpans {
    store: InMemoryStore,
    telemetry: Mutex<Option<Arc<dyn TelemetryProvider>>>,
}

impl CursorSpans {
    async fn spanned<T>(&self, operation: &str, work: impl Future<Output = T>) -> T {
        let provider = self.telemetry.lock().unwrap().clone();
        let Some(provider) = provider else {
            return work.await;
        };
        let options = StartSpanOptions {
            parent_id: get_current_span(),
            ..StartSpanOptions::default()
        };
        let span =
            provider.start_span(SpanKind::StoreQuery, &format!("store.{operation}"), options);
        let outcome = work.await;
        provider.end_span(&span, EndSpanOptions::default());
        outcome
    }
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

impl ConversationStore for CursorSpans {
    fn set_telemetry(&self, provider: Arc<dyn TelemetryProvider>, _record_sensitive_data: bool) {
        *self.telemetry.lock().unwrap() = Some(provider);
    }

    async fn get_delivered_index(&self, room_id: &RoomId) -> Result<i64, StoreError> {
        let read = self.store.get_delivered_index(room_id);
        self.spanned("get_delivered_index", read).await
    }

    async fn advance_delivered_index(
        &self,
        room_id: &RoomId,
        index: u64,
        options: store::AdvanceDeliveredIndexOptions,
    ) -> Result<bool, StoreError> {
        let write = self.store.advance_delivered_index(room_id, index, options);
        self.spanned("advance_delivered_index", write).await
    }

    forward! {
        get_room(room_id: &RoomId) -> Option<Room>;
        list_participants(room_id: &RoomId) -> Vec<Participant>;
        create_room(room: Room) -> Room;
        update_room(room: Room) -> Room;
        delete_room(room_id: &RoomId) -> bool;
        list_rooms(options: store::ListRoomsOptions) -> Vec<Room>;
        find_rooms(options: store::FindRoomsOptions) -> Vec<Room>;
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

/// A transport whose deliveries wait for `release`, `entered` notified as
/// one starts.
#[derive(Clone, Default)]
struct Held {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

impl Channel for Held {
    fn channel_id(&self) -> ChannelId {
        id("out")
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::WEBSOCKET
    }

    fn handle_inbound(
        &self,
        _message: InboundMessage,
        _context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Err("the held transport receives no message".into()))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let (entered, release) = (Arc::clone(&self.entered), Arc::clone(&self.release));
        async move {
            entered.notify_one();
            release.notified().await;
            Ok(ChannelOutput::empty())
        }
    }
}

/// A kit on a [`CursorSpans`] store reporting to a mock, with a room of `src`,
/// the source, and `out`; a delivery gap of `gap` when given.
async fn kit_with_room(
    out: impl Channel,
    gap: Option<Duration>,
) -> (
    RoomKit<CursorSpans>,
    Arc<MockTelemetryProvider>,
    Arc<CursorSpans>,
) {
    let mock = Arc::new(MockTelemetryProvider::default());
    let provider: Arc<dyn TelemetryProvider> = Arc::<MockTelemetryProvider>::clone(&mock);
    let mut options = RoomKitOptions::with_store(CursorSpans::default());
    options.telemetry = Some(provider.into());
    if let Some(gap) = gap {
        options.delivery_gap_timeout = gap;
    }
    let store = Arc::clone(&options.store);
    let kit = RoomKit::new(options);
    let src = RecordingTransport::new(
        id("src"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(src).unwrap();
    kit.register_channel(out).unwrap();
    let create = CreateRoomOptions {
        room_id: Some(room()),
        ..CreateRoomOptions::default()
    };
    kit.create_room(create).await.unwrap();
    for channel in ["src", "out"] {
        kit.attach_channel(&room(), &id(channel), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    (kit, mock, store)
}

fn recording_out() -> RecordingTransport {
    RecordingTransport::new(
        id("out"),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    )
}

/// The ids of the `framework.inbound` spans of `spans`.
fn inbounds(spans: &[Span]) -> Vec<String> {
    spans
        .iter()
        .filter(|span| span.name == "framework.inbound")
        .map(|span| span.id.clone())
        .collect()
}

/// The cursor's spans of `spans`.
fn cursor(spans: &[Span]) -> Vec<&Span> {
    spans
        .iter()
        .filter(|span| CURSOR.contains(&span.name.as_str()))
        .collect()
}

/// The parents of `spans`.
fn parents(spans: &[&Span]) -> BTreeSet<Option<String>> {
    spans.iter().map(|span| span.parent_id.clone()).collect()
}

// Python: tests/test_telemetry.py::TestStoreQuerySpan::test_the_lane_s_cursor_spans_sit_under_their_message
#[tokio::test]
async fn the_lane_s_cursor_spans_sit_under_their_message() {
    let (kit, mock, _) = kit_with_room(recording_out(), None).await;
    let before = mock.spans().len();
    for text in ["first", "second"] {
        kit.process_inbound(message(text), in_room()).await.unwrap();
    }
    kit.close().await.unwrap();

    let spans = mock.spans()[before..].to_vec();
    let inbounds = inbounds(&spans);
    let cursor = cursor(&spans);
    assert_eq!(inbounds.len(), 2);
    for inbound in &inbounds {
        let under: BTreeSet<&str> = cursor
            .iter()
            .filter(|span| span.parent_id.as_ref() == Some(inbound))
            .map(|span| span.name.as_str())
            .collect();
        assert_eq!(under, BTreeSet::from(CURSOR), "under {inbound}");
    }
    let expected: BTreeSet<Option<String>> = inbounds.into_iter().map(Some).collect();
    assert_eq!(parents(&cursor), expected);
}

// Python: tests/test_telemetry.py::TestStoreQuerySpan::test_a_blocked_message_s_cursor_spans_sit_under_a_message
#[tokio::test]
async fn a_blocked_message_s_cursor_spans_sit_under_a_message() {
    let held = Held::default();
    let (kit, mock, store) = kit_with_room(held.clone(), None).await;
    kit.hook(
        HookTrigger::BeforeBroadcast,
        |event: RoomEvent, _: RoomContext| {
            let blocks =
                matches!(&event.content, EventContent::Text(text) if text.body == "blocked");
            ready(if blocks {
                HookResult::block("blocked", BlockOptions::default())
            } else {
                HookResult::allow(AllowOptions::default())
            })
        },
        HookOptions {
            name: "block".to_owned(),
            ..HookOptions::default()
        },
    )
    .unwrap();
    let before = mock.spans().len();

    let first = tokio::spawn({
        let kit = kit.clone();
        async move { kit.process_inbound(message("first"), in_room()).await }
    });
    tokio::time::timeout(Duration::from_secs(5), held.entered.notified())
        .await
        .unwrap();
    let blocked = kit
        .process_inbound(message("blocked"), in_room())
        .await
        .unwrap();
    assert!(blocked.blocked);
    held.release.notify_one();
    first.await.unwrap().unwrap();
    let spans = mock.spans()[before..].to_vec();
    let index = i64::try_from(blocked.event.unwrap().index).unwrap();
    assert_eq!(store.get_delivered_index(&room()).await.unwrap(), index);
    kit.close().await.unwrap();

    let inbounds = inbounds(&spans);
    assert_eq!(inbounds.len(), 2);
    let cursor = cursor(&spans);
    let names: BTreeSet<&str> = cursor.iter().map(|span| span.name.as_str()).collect();
    assert_eq!(names, BTreeSet::from(CURSOR));
    let under: BTreeSet<Option<String>> = inbounds.into_iter().map(Some).collect();
    let parents = parents(&cursor);
    assert!(parents.is_subset(&under), "{parents:?} not under {under:?}");
}

// Python: tests/test_telemetry.py::TestStoreQuerySpan::test_a_skip_s_cursor_spans_sit_under_the_message_it_lets_through
#[tokio::test(start_paused = true)]
async fn a_skip_s_cursor_spans_sit_under_the_message_it_lets_through() {
    let gap = Some(Duration::from_millis(20));
    let (kit, mock, store) = kit_with_room(recording_out(), gap).await;
    let lost = RoomEvent::new(
        room(),
        EventSource::new(id("src"), ChannelType::SMS),
        TextContent::new("lost"),
    );
    store
        .commit_event(&room(), lost, store::CommitEventOptions::default())
        .await
        .unwrap();
    let skipped = EventTap::on(&kit, &["delivery_skipped"]);
    let before = mock.spans().len();
    kit.process_inbound(message("after"), in_room())
        .await
        .unwrap();
    kit.close().await.unwrap();

    assert_eq!(skipped.events().len(), 1);
    let spans = mock.spans()[before..].to_vec();
    let inbounds = inbounds(&spans);
    assert_eq!(inbounds.len(), 1);
    let cursor = cursor(&spans);
    let names: BTreeSet<&str> = cursor.iter().map(|span| span.name.as_str()).collect();
    assert_eq!(names, BTreeSet::from(CURSOR));
    assert_eq!(
        parents(&cursor),
        BTreeSet::from([Some(inbounds[0].clone())])
    );
}

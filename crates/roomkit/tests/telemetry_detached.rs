//! The span of what a caller does not wait for, `framework.detached`:
//! Python's `tests/test_inbound_deferred.py`, its trace shape.

#![expect(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    reason = "a test fails on a broken fixture"
)]

use std::future::ready;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_core::Stream;
use pretty_assertions::assert_eq;
use roomkit::model::hook::{AllowOptions, BlockOptions};
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, ErrorRecord,
    EventSource, EventType, HookResult, InboundMessage, RoomContext, RoomEvent, RoomId,
    TextContent,
};
use roomkit::telemetry::{MockTelemetryProvider, Span, TelemetryConfig};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, CancelDeliveryOptions, CancelOptions, Channel, ChannelError,
    CreateRoomOptions, HookExecution, HookOptions, HookTrigger, InMemoryLockManager, InboundResult,
    ProcessInboundOptions, RoomKit, RoomKitError, RoomKitOptions, RoomLock, RoomLockManager,
    SendEventOptions,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiContext, AiProvider, AiResponse, ProviderError, ProviderStream,
    StreamDone, StreamEvent, StreamTextDelta,
};
use serde_json::json;
use tokio::sync::Notify;

/// How long a wait that should end may take before the test fails.
const BOUND: Duration = Duration::from_secs(5);

fn id(channel: &str) -> ChannelId {
    ChannelId::new(channel).unwrap()
}

fn room() -> RoomId {
    RoomId::new("r1").unwrap()
}

/// A model that writes nothing until the test lets it.
struct Gated(Arc<Notify>);

/// [`Gated`]'s answer: a text once the gate opened.
struct Waiting {
    opened: Pin<Box<dyn Future<Output = ()> + Send>>,
    over: u8,
}

impl Stream for Waiting {
    type Item = Result<StreamEvent, ProviderError>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.over == 0 {
            if self.opened.as_mut().poll(context).is_pending() {
                return Poll::Pending;
            }
            self.over = 1;
            return Poll::Ready(Some(Ok(StreamEvent::TextDelta(StreamTextDelta {
                text: "At last.".to_owned(),
            }))));
        }
        if self.over == 1 {
            self.over = 2;
            return Poll::Ready(Some(Ok(StreamEvent::Done(StreamDone::default()))));
        }
        Poll::Ready(None)
    }
}

impl AiProvider for Gated {
    fn model_name(&self) -> &'static str {
        "gated"
    }

    fn generate<'a>(
        &'a self,
        _context: &'a AiContext,
    ) -> impl Future<Output = Result<AiResponse, ProviderError>> + Send + 'a {
        ready(Err("this model only streams".into()))
    }

    fn generate_structured_stream<'a>(&'a self, _context: &'a AiContext) -> ProviderStream<'a> {
        let gate = Arc::clone(&self.0);
        Box::pin(Waiting {
            opened: Box::pin(async move { gate.notified().await }),
            over: 0,
        })
    }
}

/// A kit reporting to a mock, with a room of an SMS transport and `agent`.
async fn kit_with<P: AiProvider + 'static>(agent: P) -> (RoomKit, Arc<MockTelemetryProvider>) {
    let mock = Arc::new(MockTelemetryProvider::default());
    kit_reporting_to(&mock, false, agent).await
}

/// A kit reporting to `mock`, recording what can hold personal data when
/// `sensitive`, with a room of an SMS transport and `agent`.
async fn kit_reporting_to<P: AiProvider + 'static>(
    mock: &Arc<MockTelemetryProvider>,
    sensitive: bool,
    agent: P,
) -> (RoomKit, Arc<MockTelemetryProvider>) {
    let mock = Arc::clone(mock);
    let mut telemetry = TelemetryConfig::from(Arc::clone(&mock));
    telemetry.record_sensitive_data = sensitive;
    let kit = RoomKit::new(RoomKitOptions {
        telemetry: Some(telemetry),
        ..RoomKitOptions::default()
    });
    let sms = RecordingTransport::new(
        id("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(sms).unwrap();
    let ai = AiChannel::new(id("ai"), agent, AiChannelOptions::default()).unwrap();
    kit.register_channel(ai).unwrap();
    let create = CreateRoomOptions {
        room_id: Some(room()),
        ..CreateRoomOptions::default()
    };
    kit.create_room(create).await.unwrap();
    for channel in ["sms", "ai"] {
        kit.attach_channel(&room(), &id(channel), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    (kit, mock)
}

fn hello() -> InboundMessage {
    InboundMessage::new(id("sms"), "+15550000001", TextContent::new("Hello"))
}

async fn say(kit: &RoomKit, defer_delivery: bool) -> InboundResult {
    let options = ProcessInboundOptions {
        room_id: Some(room()),
        defer_delivery,
        ..ProcessInboundOptions::default()
    };
    kit.process_inbound(hello(), options).await.unwrap()
}

fn named(spans: &[Span], name: &str) -> Span {
    let found: Vec<&Span> = spans.iter().filter(|span| span.name == name).collect();
    assert_eq!(found.len(), 1, "{name} in {spans:?}");
    found[0].clone()
}

/// `(name, parent's name)` of every span ended, sorted.
fn span_tree(mock: &MockTelemetryProvider) -> Vec<(String, Option<String>)> {
    let spans = mock.completed_spans();
    let parent = |span: &Span| {
        let parent_id = span.parent_id.as_ref()?;
        let parent = spans.iter().find(|other| &other.id == parent_id)?;
        Some(parent.name.clone())
    };
    let mut tree: Vec<(String, Option<String>)> = spans
        .iter()
        .map(|span| (span.name.clone(), parent(span)))
        .collect();
    tree.sort();
    tree
}

/// One whole turn, its message's hooks and the agent's answer, its call
/// deferred when `defer`.
async fn traced_turn(defer: bool) -> Vec<(String, Option<String>)> {
    let model = ScriptedProvider::new([ScriptedRound::answering("Reply.")]);
    let (kit, mock) = kit_with(model).await;
    for (trigger, name, execution) in [
        (HookTrigger::BeforeBroadcast, "before", HookExecution::Sync),
        (HookTrigger::AfterBroadcast, "after", HookExecution::Async),
    ] {
        kit.hook(
            trigger,
            |_: RoomEvent, _| ready(HookResult::allow(AllowOptions::default())),
            HookOptions {
                name: name.to_owned(),
                execution,
                ..HookOptions::default()
            },
        )
        .unwrap();
    }
    let result = say(&kit, defer).await;
    if let Some(delivery) = result.delivery {
        tokio::time::timeout(BOUND, delivery.wait())
            .await
            .unwrap()
            .unwrap();
    }
    kit.close().await.unwrap();
    span_tree(&mock)
}

// Python: tests/test_inbound_deferred.py::test_deferred_span_tree_matches_the_waiting_path
#[tokio::test]
async fn a_deferred_turn_s_spans_keep_the_parents_of_a_call_that_waits() {
    let waiting = traced_turn(false).await;
    let mut deferred = traced_turn(true).await;
    let tail = (
        "framework.detached".to_owned(),
        Some("framework.inbound".to_owned()),
    );
    let at = deferred.iter().position(|span| span == &tail);
    deferred.remove(at.unwrap());
    assert_eq!(deferred, waiting);
}

// Python: tests/test_inbound_deferred.py::test_detached_span_measures_the_deferred_tail
#[tokio::test]
async fn the_detached_span_measures_what_the_caller_did_not_wait_for() {
    let gate = Arc::new(Notify::new());
    let (kit, mock) = kit_with(Gated(Arc::clone(&gate))).await;
    let result = say(&kit, true).await;
    let inbound = named(&mock.completed_spans(), "framework.inbound");
    assert_eq!(inbound.attributes["deferred"], json!(true));
    let active = mock.get_active_spans();
    let open = named(&active, "framework.detached");
    assert_eq!(open.parent_id.as_deref(), Some(inbound.id.as_str()));
    assert_eq!(open.room_id.as_deref(), Some("r1"));

    gate.notify_one();
    let delivery = result.delivery.unwrap();
    tokio::time::timeout(BOUND, delivery.wait())
        .await
        .unwrap()
        .unwrap();
    let tail = named(&mock.completed_spans(), "framework.detached");
    assert_eq!(tail.status, "ok");
    assert_eq!(tail.attributes["streams"], json!(1));
    assert!(tail.end_time >= inbound.end_time);
    kit.close().await.unwrap();
}

// Python: tests/test_inbound_deferred.py::test_detached_span_reports_a_failed_tail
#[tokio::test]
async fn the_detached_span_reports_a_failed_tail() {
    let model = ScriptedProvider::new([ScriptedRound::answering("").failing("stream exploded")]);
    let (kit, mock) = kit_with(model).await;
    let result = say(&kit, true).await;
    let delivery = result.delivery.unwrap();
    tokio::time::timeout(BOUND, delivery.wait())
        .await
        .unwrap()
        .unwrap();
    let tail = named(&mock.completed_spans(), "framework.detached");
    assert_eq!(tail.status, "error");
    assert_eq!(tail.error_message.as_deref(), Some("ProviderError"));
    kit.close().await.unwrap();
}

// Python: tests/test_inbound_deferred.py::test_close_in_flight_ends_the_detached_span
#[tokio::test]
async fn a_close_in_flight_ends_the_detached_span_cancelled() {
    let (kit, mock) = kit_with(Gated(Arc::new(Notify::new()))).await;
    let result = say(&kit, true).await;
    let delivery = result.delivery.unwrap();
    assert!(!delivery.done());
    tokio::time::timeout(BOUND, kit.close())
        .await
        .unwrap()
        .unwrap();
    assert!(delivery.done());
    assert_eq!(mock.get_active_spans(), vec![]);
    let tail = named(&mock.completed_spans(), "framework.detached");
    assert_eq!(tail.status, "error");
    assert_eq!(tail.error_message.as_deref(), Some("cancelled"));
}

// Python: core/lanes.py `cancel`: a delivery cancelled cuts its reader, and the
// span reads `cancelled`, never the caller's text, recording it or not (run on
// Python, 2026-11-04: reentry and streamed, by the handle and by the key).
#[tokio::test]
async fn a_delivery_cancelled_reads_cancelled_never_its_reason() {
    for sensitive in [false, true] {
        let mock = Arc::new(MockTelemetryProvider::default());
        let (kit, _) = kit_reporting_to(&mock, sensitive, Gated(Arc::new(Notify::new()))).await;
        let result = say(&kit, true).await;
        let delivery = result.delivery.unwrap();
        let cancel = CancelOptions {
            reason: "Jean Dupont hung up".to_owned(),
            ..CancelOptions::default()
        };
        tokio::time::timeout(BOUND, delivery.cancel(cancel))
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(BOUND, delivery.wait())
            .await
            .unwrap()
            .unwrap();
        let tail = named(&mock.completed_spans(), "framework.detached");
        assert_eq!(tail.status, "error");
        assert_eq!(tail.error_message.as_deref(), Some("cancelled"));
        kit.close().await.unwrap();
    }
}

// Python: core/lanes.py `track`: a reader of a delivery a key cancelled before
// its commit is cut as it is made, and reads the same.
#[tokio::test]
async fn a_delivery_cancelled_before_its_commit_reads_cancelled_never_its_reason() {
    let (kit, mock) = kit_with(ScriptedProvider::new([ScriptedRound::answering("Yes.")])).await;
    let canceller = kit.clone();
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |_: RoomEvent, _| {
            let kit = canceller.clone();
            async move {
                let options = CancelDeliveryOptions {
                    reason: "Jean Dupont hung up".to_owned(),
                    ..CancelDeliveryOptions::default()
                };
                kit.cancel_delivery("k", options).await;
                HookResult::allow(AllowOptions::default())
            }
        },
        HookOptions {
            name: "cancels".to_owned(),
            ..HookOptions::default()
        },
    )
    .unwrap();
    let mut message = hello();
    message.delivery_key = Some("k".to_owned());
    let options = ProcessInboundOptions {
        room_id: Some(room()),
        defer_delivery: true,
        ..ProcessInboundOptions::default()
    };
    let result = kit.process_inbound(message, options).await.unwrap();
    tokio::time::timeout(BOUND, result.delivery.unwrap().wait())
        .await
        .unwrap()
        .unwrap();
    let tail = named(&mock.completed_spans(), "framework.detached");
    assert_eq!(tail.status, "error");
    assert_eq!(tail.error_message.as_deref(), Some("cancelled"));
    kit.close().await.unwrap();
}

/// Whether `mock` ended a `framework.detached`.
fn tail_of(mock: &MockTelemetryProvider) -> Option<Span> {
    let spans = mock.completed_spans();
    spans
        .into_iter()
        .find(|span| span.name == "framework.detached")
}

// Python: core/mixins/inbound.py `_complete_inbound_delivery`: a refusal shed
// before the room's lock returned with no handle, and leaves no tail; a call
// that waits has none either.
#[tokio::test]
async fn a_call_that_waits_or_was_refused_before_the_lock_opens_no_detached_span() {
    let (kit, mock) = kit_with(ScriptedProvider::new([ScriptedRound::answering("Yes.")])).await;
    say(&kit, false).await;
    assert_eq!(tail_of(&mock), None);
    let mut unaddressed = hello();
    unaddressed.event_type = EventType::Instruction;
    let options = ProcessInboundOptions {
        room_id: Some(room()),
        defer_delivery: true,
        ..ProcessInboundOptions::default()
    };
    let refused = kit.process_inbound(unaddressed, options).await.unwrap();
    assert!(refused.delivery.is_none());
    assert_eq!(tail_of(&mock), None);
    kit.close().await.unwrap();
}

// Python: core/mixins/inbound.py `_complete_inbound_delivery`: every result
// that reached the locked region gets a handle, a hook's refusal included.
#[tokio::test]
async fn a_deferred_call_a_hook_blocks_has_its_tail_too() {
    let (kit, mock) = kit_with(ScriptedProvider::new([])).await;
    kit.hook(
        HookTrigger::BeforeBroadcast,
        |_: RoomEvent, _| ready(HookResult::block("spam", BlockOptions::default())),
        HookOptions {
            name: "refuse".to_owned(),
            ..HookOptions::default()
        },
    )
    .unwrap();
    let result = say(&kit, true).await;
    assert!(result.blocked);
    tokio::time::timeout(BOUND, result.delivery.unwrap().wait())
        .await
        .unwrap()
        .unwrap();
    let tail = tail_of(&mock).unwrap();
    assert_eq!(tail.status, "ok");
    assert_eq!(tail.attributes["streams"], json!(0));
    kit.close().await.unwrap();
}

/// A lock manager whose releases fail while `failing`.
struct LostLock {
    inner: InMemoryLockManager,
    failing: Arc<AtomicBool>,
}

struct HeldLock {
    lock: Box<dyn RoomLock>,
    failing: Arc<AtomicBool>,
}

type Answer<'a, T> = Pin<Box<dyn Future<Output = Result<T, RoomKitError>> + Send + 'a>>;

impl RoomLockManager for LostLock {
    fn locked<'a>(&'a self, room_id: &'a RoomId) -> Answer<'a, Box<dyn RoomLock>> {
        Box::pin(async move {
            let held: Box<dyn RoomLock> = Box::new(HeldLock {
                lock: self.inner.locked(room_id).await?,
                failing: Arc::clone(&self.failing),
            });
            Ok(held)
        })
    }
}

impl RoomLock for HeldLock {
    fn release(self: Box<Self>) -> Answer<'static, ()> {
        let failing = self.failing.load(Ordering::SeqCst);
        let lock = self.lock;
        Box::pin(async move {
            lock.release().await?;
            if failing {
                let lost = ErrorRecord::new("LockReleaseFailedError", "the lease expired");
                return Err(RoomKitError::Lock(lost));
            }
            Ok(())
        })
    }
}

// Python: core/mixins/lane_execution.py `_end_tail`: a failed release is on the
// call's result; the tail reads its cascade's error alone, and it has none.
#[tokio::test]
async fn a_failed_release_is_no_failure_of_the_tail() {
    let mock = Arc::new(MockTelemetryProvider::default());
    let failing = Arc::new(AtomicBool::new(false));
    let kit = RoomKit::new(RoomKitOptions {
        telemetry: Some(TelemetryConfig::from(Arc::clone(&mock))),
        lock_manager: Some(Arc::new(LostLock {
            inner: InMemoryLockManager::default(),
            failing: Arc::clone(&failing),
        })),
        ..RoomKitOptions::default()
    });
    let sms = RecordingTransport::new(
        id("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(sms).unwrap();
    let create = CreateRoomOptions {
        room_id: Some(room()),
        ..CreateRoomOptions::default()
    };
    kit.create_room(create).await.unwrap();
    kit.attach_channel(&room(), &id("sms"), AttachChannelOptions::default())
        .await
        .unwrap();
    failing.store(true, Ordering::SeqCst);
    let result = say(&kit, true).await;
    let delivery = result.delivery.unwrap();
    tokio::time::timeout(BOUND, delivery.wait())
        .await
        .unwrap()
        .unwrap();
    failing.store(false, Ordering::SeqCst);
    let tail = tail_of(&mock).unwrap();
    assert_eq!(tail.status, "ok", "{tail:?}");
    kit.close().await.unwrap();
}

// Python: core/mixins/lane_execution.py `_end_tail`: the tail reads its cascade's
// failure whatever else the call's result reports first.
#[tokio::test]
async fn a_failed_release_hides_no_failure_of_the_tail() {
    let mock = Arc::new(MockTelemetryProvider::default());
    let failing = Arc::new(AtomicBool::new(false));
    let kit = RoomKit::new(RoomKitOptions {
        telemetry: Some(TelemetryConfig::from(Arc::clone(&mock))),
        lock_manager: Some(Arc::new(LostLock {
            inner: InMemoryLockManager::default(),
            failing: Arc::clone(&failing),
        })),
        ..RoomKitOptions::default()
    });
    let sms = RecordingTransport::new(
        id("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(sms).unwrap();
    let model = ScriptedProvider::new([ScriptedRound::answering("").failing("stream exploded")]);
    let agent = AiChannel::new(id("ai"), model, AiChannelOptions::default()).unwrap();
    kit.register_channel(agent).unwrap();
    let create = CreateRoomOptions {
        room_id: Some(room()),
        ..CreateRoomOptions::default()
    };
    kit.create_room(create).await.unwrap();
    for channel in ["sms", "ai"] {
        kit.attach_channel(&room(), &id(channel), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    failing.store(true, Ordering::SeqCst);
    let result = say(&kit, true).await;
    tokio::time::timeout(BOUND, result.delivery.unwrap().wait())
        .await
        .unwrap()
        .unwrap();
    failing.store(false, Ordering::SeqCst);
    let tail = tail_of(&mock).unwrap();
    assert_eq!(tail.status, "error", "{tail:?}");
    assert_eq!(
        tail.error_message.as_deref(),
        Some("ProviderError"),
        "{tail:?}"
    );
    kit.close().await.unwrap();
}

/// An agent whose turn raises on the text `.1`, and answers no other.
#[derive(Clone)]
struct RaisesOn(ChannelId, &'static str);

impl Channel for RaisesOn {
    fn channel_id(&self) -> ChannelId {
        self.0.clone()
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
        let source = EventSource::new(self.0.clone(), ChannelType::AI);
        ready(Ok(RoomEvent::new(context.room.id, source, message.content)))
    }

    fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let raises = serde_json::to_value(&event.content).unwrap()["body"] == self.1;
        ready(if raises {
            Err(ChannelError::from("turn exploded"))
        } else {
            Ok(ChannelOutput::empty())
        })
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::empty()))
    }
}

// Python: core/lanes.py `record_error`: the first one stands. A buffered agent's
// failure, then another's stream's, read the buffered one's.
#[tokio::test]
async fn the_tail_reads_the_first_failure_a_buffered_turn_s_before_a_stream_s() {
    let model = ScriptedProvider::new([ScriptedRound::answering("").failing("stream exploded")]);
    let (kit, mock) = kit_with(model).await;
    let raises = RaisesOn(id("raises"), "Hello");
    kit.register_channel(raises).unwrap();
    kit.attach_channel(&room(), &id("raises"), AttachChannelOptions::default())
        .await
        .unwrap();
    let result = say(&kit, true).await;
    tokio::time::timeout(BOUND, result.delivery.unwrap().wait())
        .await
        .unwrap()
        .unwrap();
    let tail = tail_of(&mock).unwrap();
    assert_eq!(tail.status, "error", "{tail:?}");
    assert_eq!(
        tail.error_message.as_deref(),
        Some("ChannelError"),
        "{tail:?}"
    );
    kit.close().await.unwrap();
}

// Python: core/mixins/lane_execution.py `_report_root_pass`: the caller-facing
// error describes the trigger's own set, never a reentry's. An answer's set
// failing is no failure of the tail, and none on the call's result.
#[tokio::test]
async fn an_answer_s_failed_turn_is_no_failure_of_the_tail() {
    let model = ScriptedProvider::new([ScriptedRound::answering("Yes.")]);
    let (kit, mock) = kit_with(model).await;
    let raises = RaisesOn(id("raises"), "Yes.");
    kit.register_channel(raises).unwrap();
    kit.attach_channel(&room(), &id("raises"), AttachChannelOptions::default())
        .await
        .unwrap();
    let result = say(&kit, true).await;
    let ended = tokio::time::timeout(BOUND, result.delivery.unwrap().wait())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ended.error, None);
    let tail = tail_of(&mock).unwrap();
    assert_eq!(tail.status, "ok", "{tail:?}");
    kit.close().await.unwrap();
}

/// A lock manager that refuses its `.1`th grant, counted from 1, and that one
/// alone.
struct RefusesOneGrant(InMemoryLockManager, usize, AtomicUsize);

impl RoomLockManager for RefusesOneGrant {
    fn locked<'a>(&'a self, room_id: &'a RoomId) -> Answer<'a, Box<dyn RoomLock>> {
        let grant = self.2.fetch_add(1, Ordering::SeqCst) + 1;
        if grant == self.1 {
            return Box::pin(ready(Err(RoomKitError::Runtime(
                "no lock for Jean Dupont".to_owned(),
            ))));
        }
        self.0.locked(room_id)
    }
}

// Python: core/lanes.py `_finish`: a failure past the decision, an answer's commit
// refused its lock, is the cascade's: the result carries it, the tail reads it.
#[tokio::test]
async fn a_failure_past_the_decision_reads_on_the_tail() {
    /// The room's creation, its two attaches, the message and the answer's
    /// commit take the lock in that order.
    const THE_ANSWER_S_COMMIT: usize = 5;
    let mock = Arc::new(MockTelemetryProvider::default());
    let kit = RoomKit::new(RoomKitOptions {
        telemetry: Some(TelemetryConfig::from(Arc::clone(&mock))),
        lock_manager: Some(Arc::new(RefusesOneGrant(
            InMemoryLockManager::default(),
            THE_ANSWER_S_COMMIT,
            AtomicUsize::new(0),
        ))),
        ..RoomKitOptions::default()
    });
    let sms = RecordingTransport::new(
        id("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(sms).unwrap();
    let model = ScriptedProvider::new([ScriptedRound::answering("Yes.")]);
    let agent = AiChannel::new(id("ai"), model, AiChannelOptions::default()).unwrap();
    kit.register_channel(agent).unwrap();
    let create = CreateRoomOptions {
        room_id: Some(room()),
        ..CreateRoomOptions::default()
    };
    drop(kit.create_room(create).await);
    for channel in ["sms", "ai"] {
        drop(
            kit.attach_channel(&room(), &id(channel), AttachChannelOptions::default())
                .await,
        );
    }
    let result = say(&kit, true).await;
    let ended = tokio::time::timeout(BOUND, result.delivery.unwrap().wait())
        .await
        .unwrap()
        .unwrap();
    assert_ne!(ended.error, None, "the answer's commit was refused");
    let tail = tail_of(&mock).unwrap();
    assert_eq!(tail.status, "error", "{tail:?}");
    assert_eq!(
        tail.error_message.as_deref(),
        Some("RuntimeError"),
        "{tail:?}"
    );
    kit.close().await.unwrap();
}

/// A transport whose send of the text `.1` waits until the test lets it.
#[derive(Clone)]
struct HeldSend(Arc<Notify>, &'static str);

impl Channel for HeldSend {
    fn channel_id(&self) -> ChannelId {
        id("held")
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::SMS
    }

    fn recipient_key(&self) -> Option<String> {
        None
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        let source = EventSource::new(id("held"), ChannelType::SMS);
        ready(Ok(RoomEvent::new(context.room.id, source, message.content)))
    }

    fn deliver(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let gate = Arc::clone(&self.0);
        let held = serde_json::to_value(&event.content).unwrap()["body"] == self.1;
        async move {
            if held {
                gate.notified().await;
            }
            Ok(ChannelOutput::empty())
        }
    }
}

// Python: core/lanes.py `add_streams`: a stream a cancelled cascade is handed is
// closed unread, and no stream of it.
#[tokio::test]
async fn a_stream_the_cancel_drops_is_not_counted() {
    let model = ScriptedProvider::new([ScriptedRound::answering("Yes.")]);
    let (kit, mock) = kit_with(model).await;
    let gate = Arc::new(Notify::new());
    kit.register_channel(HeldSend(Arc::clone(&gate), "Hello"))
        .unwrap();
    kit.attach_channel(&room(), &id("held"), AttachChannelOptions::default())
        .await
        .unwrap();
    let result = say(&kit, true).await;
    tokio::time::sleep(Duration::from_millis(5)).await;
    let delivery = result.delivery.unwrap();
    tokio::time::timeout(BOUND, delivery.cancel(CancelOptions::default()))
        .await
        .unwrap()
        .unwrap();
    // The send goes on, and is no part of what the caller's cancel cut.
    let tail = tail_of(&mock).unwrap();
    assert_eq!(tail.error_message.as_deref(), Some("cancelled"));
    assert_eq!(tail.attributes["streams"], json!(0));
    gate.notify_one();
    tokio::time::timeout(BOUND, delivery.wait())
        .await
        .unwrap()
        .unwrap();
    kit.close().await.unwrap();
}

// Python: core/mixins/lane_execution.py: `len(cascade.streams)` is the caller's
// own cascade's: the stream of an event its hook sends is another's. The event
// is held in its set past the caller's answer, so that the tail is there to count.
#[tokio::test]
async fn an_adopted_event_s_stream_is_not_the_caller_s() {
    let model = ScriptedProvider::new([
        ScriptedRound::answering("One."),
        ScriptedRound::answering("Two."),
    ]);
    let (kit, mock) = kit_with(model).await;
    let gate = Arc::new(Notify::new());
    kit.register_channel(HeldSend(Arc::clone(&gate), "adopted"))
        .unwrap();
    kit.attach_channel(&room(), &id("held"), AttachChannelOptions::default())
        .await
        .unwrap();
    let hooked = kit.clone();
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |event: RoomEvent, _| {
            let kit = hooked.clone();
            async move {
                let body = serde_json::to_value(&event.content).unwrap();
                if body["body"] == "Hello" {
                    kit.send_event(
                        &room(),
                        &id("sms"),
                        TextContent::new("adopted"),
                        SendEventOptions::default(),
                    )
                    .await
                    .unwrap();
                }
                HookResult::allow(AllowOptions::default())
            }
        },
        HookOptions {
            name: "sends".to_owned(),
            ..HookOptions::default()
        },
    )
    .unwrap();
    let result = say(&kit, true).await;
    gate.notify_one();
    tokio::time::timeout(BOUND, result.delivery.unwrap().wait())
        .await
        .unwrap()
        .unwrap();
    let tail = tail_of(&mock).unwrap();
    assert_eq!(tail.attributes["streams"], json!(1), "{tail:?}");
    kit.close().await.unwrap();
}

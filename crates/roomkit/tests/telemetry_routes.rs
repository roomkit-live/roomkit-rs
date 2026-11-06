//! A message's way to its room, `framework.route`: what it reads however the
//! way ends, Python's `_route_to_room` (core/mixins/inbound.py), run on Python
//! for each case below.

#![expect(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    reason = "a test fails on a broken fixture"
)]

use std::future::{Future, pending, ready};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::hook::AllowOptions;
use roomkit::model::{
    ChannelBinding, ChannelId, ChannelOutput, ChannelType, HookResult, InboundMessage, RoomContext,
    RoomEvent, RoomId, TextContent,
};
use roomkit::telemetry::{MockTelemetryProvider, TelemetryConfig};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, ConversationStore, CreateRoomOptions, HookOptions,
    HookTrigger, InMemoryLockManager, InboundRoomRouter, ProcessInboundOptions, RoomKit,
    RoomKitError, RoomKitOptions, RoomLock, RoomLockManager, RouteOptions, RouterFuture,
    SessionStartedEvent,
};
use serde_json::{Value, json};

fn id(channel: &str) -> ChannelId {
    ChannelId::new(channel).unwrap()
}

fn room() -> RoomId {
    RoomId::new("r1").unwrap()
}

/// A router that names one room.
struct To(RoomId);

impl InboundRoomRouter for To {
    fn route<'a>(
        &'a self,
        _channel_id: &'a ChannelId,
        _channel_type: &'a ChannelType,
        _options: RouteOptions,
    ) -> RouterFuture<'a> {
        Box::pin(ready(Ok(Some(self.0.clone()))))
    }
}

/// A router that never answers.
struct Stuck;

impl InboundRoomRouter for Stuck {
    fn route<'a>(
        &'a self,
        _channel_id: &'a ChannelId,
        _channel_type: &'a ChannelType,
        _options: RouteOptions,
    ) -> RouterFuture<'a> {
        Box::pin(pending())
    }
}

/// A kit reporting to a mock with `options`, with a registered `sms`.
fn kit_of(options: RoomKitOptions) -> (RoomKit, Arc<MockTelemetryProvider>) {
    let mock = Arc::new(MockTelemetryProvider::default());
    let kit = RoomKit::new(RoomKitOptions {
        telemetry: Some(TelemetryConfig::from(Arc::clone(&mock))),
        ..options
    });
    let sms = RecordingTransport::new(
        id("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(sms).unwrap();
    (kit, mock)
}

async fn create_room(kit: &RoomKit, organization_id: Option<&str>) {
    let create = CreateRoomOptions {
        room_id: Some(room()),
        organization_id: organization_id.map(str::to_owned),
        ..CreateRoomOptions::default()
    };
    kit.create_room(create).await.unwrap();
}

fn hello() -> InboundMessage {
    InboundMessage::new(id("sms"), "+15551234567", TextContent::new("hello"))
}

/// Each `framework.route` the mock holds, open or ended: its status, its
/// error, its room.
fn routes(mock: &MockTelemetryProvider) -> Vec<(String, Option<String>, Option<Value>)> {
    let active = mock.get_active_spans().into_iter();
    active
        .chain(mock.completed_spans())
        .filter(|span| span.name == "framework.route")
        .map(|span| {
            let room_id = span.attributes.get("room_id").cloned();
            (span.status, span.error_message, room_id)
        })
        .collect()
}

fn found(room_id: &str) -> (String, Option<String>, Option<Value>) {
    ("ok".to_owned(), None, Some(json!(room_id)))
}

fn failed(class: &str) -> (String, Option<String>, Option<Value>) {
    ("error".to_owned(), Some(class.to_owned()), None)
}

// Python: _find_or_open_room raises RoomNotFoundError under the span.
#[tokio::test]
async fn a_named_room_of_another_organization_reads_not_found() {
    let (kit, mock) = kit_of(RoomKitOptions::default());
    create_room(&kit, Some("other")).await;
    let options = ProcessInboundOptions {
        room_id: Some(room()),
        organization_id: Some("mine".to_owned()),
        ..ProcessInboundOptions::default()
    };
    assert!(kit.process_inbound(hello(), options).await.is_err());
    kit.close().await.unwrap();
    assert_eq!(routes(&mock), vec![failed("RoomNotFoundError")]);
}

// Python: _route_and_claim raises RoomNotFoundError under the span.
#[tokio::test]
async fn a_router_naming_another_organization_s_room_reads_not_found() {
    let (kit, mock) = kit_of(RoomKitOptions {
        inbound_router: Some(Arc::new(To(room()))),
        ..RoomKitOptions::default()
    });
    create_room(&kit, Some("other")).await;
    let options = ProcessInboundOptions {
        organization_id: Some("mine".to_owned()),
        ..ProcessInboundOptions::default()
    };
    assert!(kit.process_inbound(hello(), options).await.is_err());
    kit.close().await.unwrap();
    assert_eq!(routes(&mock), vec![failed("RoomNotFoundError")]);
}

// Python: a locked hook's routed message raises ReentrantCallError on its way.
#[tokio::test]
async fn a_locked_hook_routing_back_reads_its_reentrant_call() {
    let (kit, mock) = kit_of(RoomKitOptions {
        inbound_router: Some(Arc::new(To(room()))),
        ..RoomKitOptions::default()
    });
    create_room(&kit, None).await;
    kit.attach_channel(&room(), &id("sms"), AttachChannelOptions::default())
        .await
        .unwrap();
    let hooked = kit.clone();
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |event: RoomEvent, _| {
            let kit = hooked.clone();
            async move {
                let body = serde_json::to_value(&event.content).unwrap();
                if body["body"] == "hello" {
                    let message =
                        InboundMessage::new(id("sms"), "+15551234567", TextContent::new("back"));
                    let routed = kit
                        .process_inbound(message, ProcessInboundOptions::default())
                        .await;
                    assert!(routed.is_err());
                }
                HookResult::allow(AllowOptions::default())
            }
        },
        HookOptions {
            name: "routes_back".to_owned(),
            ..HookOptions::default()
        },
    )
    .unwrap();
    let named = ProcessInboundOptions {
        room_id: Some(room()),
        ..ProcessInboundOptions::default()
    };
    kit.process_inbound(hello(), named).await.unwrap();
    kit.close().await.unwrap();
    assert_eq!(
        routes(&mock),
        vec![found("r1"), failed("ReentrantCallError")]
    );
}

// A span the kit opened and whose work stopped ends `cancelled` (`OpenSpan`):
// Python's stays open, never ended.
#[tokio::test]
async fn a_caller_gone_during_the_router_reads_cancelled() {
    let (kit, mock) = kit_of(RoomKitOptions {
        inbound_router: Some(Arc::new(Stuck)),
        ..RoomKitOptions::default()
    });
    let call = kit.process_inbound(hello(), ProcessInboundOptions::default());
    assert!(
        tokio::time::timeout(Duration::from_millis(20), call)
            .await
            .is_err()
    );
    kit.close().await.unwrap();
    let ended = routes(&mock);
    assert_eq!(ended, vec![("cancelled".to_owned(), None, None)]);
}

// Python: the router past the call's deadline raises _RoutingTimeoutError.
#[tokio::test]
async fn a_router_past_the_deadline_reads_its_timeout() {
    let (kit, mock) = kit_of(RoomKitOptions {
        inbound_router: Some(Arc::new(Stuck)),
        process_timeout: Duration::from_millis(10),
        ..RoomKitOptions::default()
    });
    let result = kit
        .process_inbound(hello(), ProcessInboundOptions::default())
        .await;
    assert!(result.is_ok_and(|result| result.blocked));
    kit.close().await.unwrap();
    assert_eq!(routes(&mock), vec![failed("_RoutingTimeoutError")]);
}

// Python: the span ends before `_prepare_event`: what a session's hook does
// past the deadline is not the route's.
#[tokio::test]
async fn a_session_hook_past_the_deadline_leaves_the_route_found() {
    let (kit, mock) = kit_of(RoomKitOptions {
        process_timeout: Duration::from_millis(20),
        ..RoomKitOptions::default()
    });
    kit.hook(
        HookTrigger::OnSessionStarted,
        |_: SessionStartedEvent, _| async {
            tokio::time::sleep(Duration::from_secs(5)).await;
            HookResult::allow(AllowOptions::default())
        },
        HookOptions {
            name: "_slow".to_owned(),
            timeout: Duration::from_secs(10),
            ..HookOptions::default()
        },
    )
    .unwrap();
    drop(
        kit.process_inbound(hello(), ProcessInboundOptions::default())
            .await,
    );
    kit.close().await.unwrap();
    let ended = routes(&mock);
    assert_eq!(ended.len(), 1, "{ended:?}");
    assert_eq!(ended[0].0, "ok", "{ended:?}");
    assert!(ended[0].2.is_some(), "{ended:?}");
}

/// A transport that, told a message, sends `routed` messages the router takes
/// back to its room, and keeps nothing else.
struct Bot {
    kit: Arc<OnceLock<RoomKit>>,
    routed: usize,
    reasons: Arc<Mutex<Vec<Option<String>>>>,
}

impl Channel for Bot {
    fn channel_id(&self) -> ChannelId {
        id("bot")
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::WEBSOCKET
    }

    fn recipient_key(&self) -> Option<String> {
        None
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        let source = roomkit::model::EventSource::new(id("bot"), ChannelType::WEBSOCKET);
        ready(Ok(RoomEvent::new(context.room.id, source, message.content)))
    }

    fn deliver(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let (kit, routed) = (self.kit.get().cloned(), self.routed);
        let reasons = Arc::clone(&self.reasons);
        async move {
            let body = serde_json::to_value(&event.content).unwrap();
            if body["body"] != "hello" {
                return Ok(ChannelOutput::empty());
            }
            let kit = kit.unwrap();
            for _ in 0..routed {
                let message =
                    InboundMessage::new(id("sms"), "+15551234567", TextContent::new("routed"));
                let result = kit
                    .process_inbound(message, ProcessInboundOptions::default())
                    .await
                    .unwrap();
                reasons.lock().unwrap().push(result.reason);
            }
            Ok(ChannelOutput::empty())
        }
    }
}

// Python: the route found its room; the reentry budget refuses the message
// after it, every route span `ok`.
#[tokio::test]
async fn a_routed_message_past_the_budget_found_its_room() {
    let (kit, mock) = kit_of(RoomKitOptions {
        max_chain_depth: 2,
        inbound_router: Some(Arc::new(To(room()))),
        ..RoomKitOptions::default()
    });
    let holder = Arc::new(OnceLock::new());
    assert!(holder.set(kit.clone()).is_ok());
    let reasons = Arc::new(Mutex::new(Vec::new()));
    let bot = Bot {
        kit: holder,
        routed: 21,
        reasons: Arc::clone(&reasons),
    };
    kit.register_channel(bot).unwrap();
    create_room(&kit, None).await;
    for channel in ["sms", "bot"] {
        kit.attach_channel(&room(), &id(channel), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    let named = ProcessInboundOptions {
        room_id: Some(room()),
        ..ProcessInboundOptions::default()
    };
    kit.process_inbound(hello(), named).await.unwrap();
    kit.close().await.unwrap();
    assert!(
        reasons.lock().unwrap().iter().any(Option::is_some),
        "one message is past the budget"
    );
    let ended = routes(&mock);
    assert_eq!(ended.len(), 22, "{ended:?}");
    assert!(ended.iter().all(|span| span == &found("r1")), "{ended:?}");
}

/// A lock manager whose lock is never granted while `stuck`.
struct StuckLock {
    inner: InMemoryLockManager,
    stuck: Arc<AtomicBool>,
}

impl RoomLockManager for StuckLock {
    fn locked<'a>(
        &'a self,
        room_id: &'a RoomId,
    ) -> Pin<Box<dyn Future<Output = Result<Box<dyn RoomLock>, RoomKitError>> + Send + 'a>> {
        let stuck = self.stuck.load(Ordering::SeqCst);
        Box::pin(async move {
            if stuck {
                pending::<()>().await;
            }
            self.inner.locked(room_id).await
        })
    }
}

// Python: _RoutingTimeoutError, "or in a read of the room routing gave it or the
// wait for its lock": a busy room, routed or named.
#[tokio::test]
async fn a_lock_wait_past_the_deadline_reads_its_timeout() {
    for routed in [true, false] {
        let stuck = Arc::new(AtomicBool::new(false));
        let (kit, mock) = kit_of(RoomKitOptions {
            process_timeout: Duration::from_millis(10),
            lock_manager: Some(Arc::new(StuckLock {
                inner: InMemoryLockManager::default(),
                stuck: Arc::clone(&stuck),
            })),
            inbound_router: routed.then(|| -> Arc<dyn InboundRoomRouter> { Arc::new(To(room())) }),
            ..RoomKitOptions::default()
        });
        create_room(&kit, None).await;
        stuck.store(true, Ordering::SeqCst);
        let options = ProcessInboundOptions {
            room_id: (!routed).then(room),
            ..ProcessInboundOptions::default()
        };
        let result = kit.process_inbound(hello(), options).await;
        assert!(result.is_ok_and(|result| result.blocked), "{routed}");
        stuck.store(false, Ordering::SeqCst);
        kit.close().await.unwrap();
        assert_eq!(
            routes(&mock),
            vec![failed("_RoutingTimeoutError")],
            "{routed}"
        );
    }
}

/// A router that answers a new missing room id each time it is asked.
struct Fresh(Arc<AtomicUsize>);

impl InboundRoomRouter for Fresh {
    fn route<'a>(
        &'a self,
        _channel_id: &'a ChannelId,
        _channel_type: &'a ChannelType,
        _options: RouteOptions,
    ) -> RouterFuture<'a> {
        let asked = self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(ready(Ok(Some(
            RoomId::new(format!("new-{asked}")).unwrap(),
        ))))
    }
}

// Python: tests/test_shared_channel_routing.py::TestARoomBeingCreated::test_a_deadline_passed_in_the_second_room_s_hooks_opens_no_third;
// `_still_within` raises _RoutingTimeoutError before the last new room, the
// deadline passed in the second new room's creation hooks, each room bound by
// a hook to another number (RMK-1396).
#[tokio::test]
async fn a_deadline_passed_before_the_last_new_room_reads_its_timeout() {
    let (kit, mock) = kit_of(RoomKitOptions {
        inbound_router: Some(Arc::new(Fresh(Arc::default()))),
        process_timeout: Duration::from_millis(40),
        ..RoomKitOptions::default()
    });
    let created = Arc::new(AtomicUsize::new(0));
    let binding = kit.clone();
    kit.hook(
        HookTrigger::OnRoomCreated,
        move |event: RoomEvent, _| {
            let (kit, created) = (binding.clone(), Arc::clone(&created));
            async move {
                let options = AttachChannelOptions {
                    participant_id: Some(
                        roomkit::model::ParticipantId::new("+15550000002").unwrap(),
                    ),
                    ..AttachChannelOptions::default()
                };
                kit.attach_channel(&event.room_id, &id("sms"), options)
                    .await
                    .unwrap();
                if created.fetch_add(1, Ordering::SeqCst) == 1 {
                    tokio::time::sleep(Duration::from_millis(80)).await;
                }
            }
        },
        HookOptions {
            name: "bind".to_owned(),
            ..HookOptions::default()
        },
    )
    .unwrap();
    let result = kit
        .process_inbound(hello(), ProcessInboundOptions::default())
        .await
        .unwrap();
    let rooms = kit
        .store()
        .list_rooms(roomkit::store::ListRoomsOptions::default())
        .await
        .unwrap();
    kit.close().await.unwrap();
    assert_eq!(
        (result.blocked, result.reason.as_deref()),
        (true, Some("process_timeout"))
    );
    // No third room: the deadline is read before it is opened.
    assert_eq!(rooms.len(), 2);
    assert_eq!(routes(&mock), vec![failed("_RoutingTimeoutError")]);
}

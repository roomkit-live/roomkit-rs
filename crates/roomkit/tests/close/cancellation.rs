//! RFC §10.1 explicit cancellation before commit, including a context read.

mod support;

use std::future::{Future, ready};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{ChannelType, RoomEvent};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions, idle};
use roomkit::{
    AttachChannelOptions, CancelDeliveryOptions, CreateRoomOptions, HookExecution, HookOptions,
    HookTrigger, InboundResult, ProcessInboundOptions, RoomKit, RoomKitOptions,
};
use roomkit_core::rules::transcoding::text_of;

use crate::held::{into, room, said, stored};
use crate::store::StuckStore;
use crate::support::channel;
use support::{Agent, Identifying, Input, Pause};

#[derive(Clone, Copy)]
enum At {
    Context,
    Parsing,
    Identity,
}

struct Fixture {
    kit: RoomKit<StuckStore>,
    agent: Agent,
    ws: RecordingTransport,
    broadcast: Arc<Mutex<Vec<String>>>,
    pause: Pause,
}

async fn bounded<T>(work: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), work)
        .await
        .unwrap()
}

async fn fixture(at: At) -> Fixture {
    let pause = Pause::default();
    let identity_resolver: Option<Arc<dyn roomkit::identity::IdentityResolver>> =
        matches!(at, At::Identity).then(|| -> Arc<dyn roomkit::identity::IdentityResolver> {
            Arc::new(Identifying(pause.clone()))
        });
    let kit = RoomKit::new(RoomKitOptions {
        process_timeout: Duration::from_secs(1),
        identity_resolver,
        ..RoomKitOptions::with_store(StuckStore::default())
    });
    let ws = RecordingTransport::new(
        channel("ws"),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    );
    let agent = Agent::default();
    kit.register_channel(Input(matches!(at, At::Parsing).then(|| pause.clone())))
        .unwrap();
    kit.register_channel(ws.clone()).unwrap();
    kit.register_channel(agent.clone()).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room("r")),
        organization_id: Some("org-1".to_owned()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["sms", "ws", "ai"] {
        kit.attach_channel(&room("r"), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    let broadcast = observe(&kit);
    Fixture {
        kit,
        agent,
        ws,
        broadcast,
        pause,
    }
}

fn observe(kit: &RoomKit<StuckStore>) -> Arc<Mutex<Vec<String>>> {
    let broadcast = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&broadcast);
    kit.hook(
        HookTrigger::AfterBroadcast,
        move |event: RoomEvent, _| {
            seen.lock().unwrap().push(text_of(&event.content));
            ready(())
        },
        HookOptions {
            name: "broadcast".to_owned(),
            execution: HookExecution::Async,
            ..Default::default()
        },
    )
    .unwrap();
    broadcast
}

fn call(
    f: &Fixture,
    body: &str,
    key: &str,
    deferred: bool,
) -> tokio::task::JoinHandle<InboundResult> {
    let mut message = said(body);
    message.delivery_key = Some(key.to_owned());
    let kit = f.kit.clone();
    tokio::spawn(async move {
        kit.process_inbound(
            message,
            ProcessInboundOptions {
                defer_delivery: deferred,
                ..into("r")
            },
        )
        .await
        .unwrap()
    })
}

fn release_context(f: &Fixture) {
    *f.kit.store().held_read.lock().unwrap() = None;
    f.kit.store().release_read.notify_one();
}

async fn cancel(f: &Fixture, organization: Option<&str>) -> usize {
    f.kit
        .cancel_delivery(
            "key",
            CancelDeliveryOptions {
                reason: "stopped".to_owned(),
                organization_id: organization.map(str::to_owned),
            },
        )
        .await
}

// Python: tests/test_delivery_cancel_turns.py::test_a_delivery_cancelled_before_its_commit_starts_no_turn[context]
#[tokio::test(start_paused = true)]
async fn a_context_read_cancel_keeps_the_deferred_delivery_and_starts_no_turn() {
    let f = fixture(At::Context).await;
    *f.kit.store().held_read.lock().unwrap() = Some("bindings");
    let pending = call(&f, "task B", "key", true);
    bounded(f.kit.store().reading.notified()).await;
    assert_eq!(cancel(&f, None).await, 1);
    release_context(&f);
    let result = bounded(pending).await.unwrap();
    let done = bounded(result.delivery.unwrap().wait()).await.unwrap();
    bounded(idle(&f.kit)).await;
    assert_eq!(done.cancellation_reason.as_deref(), Some("stopped"));
    assert_eq!(*f.agent.started.lock().unwrap(), Vec::<String>::new());
    assert_eq!(f.ws.texts(), ["task B"]);
    assert_eq!(*f.broadcast.lock().unwrap(), ["task B"]);
    assert_eq!(cancel(&f, None).await, 0);
    f.kit.close().await.unwrap();
}

// Python: tests/test_delivery_cancel_turns.py::test_an_awaited_call_cancelled_before_its_commit_waits_for_no_turn[context]
#[tokio::test(start_paused = true)]
async fn a_context_read_cancel_returns_before_the_preceding_turn_ends() {
    let f = fixture(At::Context).await;
    let first = bounded(call(&f, "task A", "a", true)).await.unwrap();
    bounded(f.agent.held.reached.notified()).await;
    *f.kit.store().held_read.lock().unwrap() = Some("bindings");
    let pending = call(&f, "task B", "key", false);
    bounded(f.kit.store().reading.notified()).await;
    assert_eq!(cancel(&f, None).await, 1);
    release_context(&f);
    let result = bounded(pending).await.unwrap();
    assert_eq!(result.cancellation_reason.as_deref(), Some("stopped"));
    assert_eq!(*f.agent.finished.lock().unwrap(), Vec::<String>::new());
    f.agent.held.release.notify_one();
    bounded(first.delivery.unwrap().wait()).await.unwrap();
    bounded(idle(&f.kit)).await;
    assert_eq!(*f.agent.started.lock().unwrap(), ["task A"]);
    assert_eq!(*f.agent.finished.lock().unwrap(), ["task A"]);
    assert_eq!(f.ws.texts(), ["task A", "task B"]);
    f.kit.close().await.unwrap();
}

// Python: tests/test_delivery_cancel_turns.py::test_a_scoped_cancel_before_the_commit_reaches_only_its_organization[room-being-read]
#[tokio::test(start_paused = true)]
async fn a_context_read_locates_an_unscoped_cancel_only_once_it_returns() {
    let f = fixture(At::Context).await;
    *f.kit.store().held_read.lock().unwrap() = Some("bindings");
    let pending = call(&f, "task B", "key", true);
    bounded(f.kit.store().reading.notified()).await;
    assert_eq!(cancel(&f, Some("org-1")).await, 0);
    release_context(&f);
    let result = bounded(pending).await.unwrap();
    let done = bounded(result.delivery.unwrap().wait()).await.unwrap();
    bounded(idle(&f.kit)).await;
    assert_eq!(done.cancellation_reason.as_deref(), Some("stopped"));
    assert_eq!(*f.agent.started.lock().unwrap(), Vec::<String>::new());
    assert_eq!(f.ws.texts(), ["task B"]);
    f.kit.close().await.unwrap();
}

// Python: tests/test_delivery_cancel_turns.py::test_a_cancelled_call_that_times_out_before_its_commit_names_nothing_after
#[tokio::test(start_paused = true)]
async fn a_cancel_then_precommit_timeout_has_no_handle_at_any_budgeted_boundary() {
    for at in [At::Context, At::Parsing, At::Identity] {
        let f = fixture(at).await;
        if matches!(at, At::Context) {
            *f.kit.store().held_read.lock().unwrap() = Some("bindings");
        }
        let pending = call(&f, "task B", "key", true);
        if matches!(at, At::Context) {
            bounded(f.kit.store().reading.notified()).await;
        } else {
            bounded(f.pause.reached.notified()).await;
        }
        assert_eq!(cancel(&f, None).await, 1);
        let result = bounded(pending).await.unwrap();
        assert!(result.blocked);
        assert_eq!(result.reason.as_deref(), Some("process_timeout"));
        assert!(result.delivery.is_none());
        assert!(result.event.is_none());
        assert!(result.cancellation_reason.is_none());
        assert_eq!(cancel(&f, None).await, 0);
        release_context(&f);
        assert_eq!(stored(&f.kit, "r").await, Vec::<String>::new());
        assert_eq!(*f.agent.started.lock().unwrap(), Vec::<String>::new());
        f.kit.close().await.unwrap();
    }
}

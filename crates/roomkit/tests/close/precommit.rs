//! Pre-commit deadlines bound reads, routing and admission, not committed work.

use std::sync::Arc;
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::hook::AllowOptions;
use roomkit::model::{ChannelId, ChannelType, HookResult, RoomEvent, TextContent};
use roomkit::testing::{EventTap, RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, ConversationStore, CreateRoomOptions, HookExecution, HookOptions,
    HookTrigger, InMemoryLockManager, InboundRoomRouter, ProcessInboundOptions, RoomKit,
    RoomKitError, RoomKitOptions, RoomLockManager, RouteOptions, RouterFuture, SendEventOptions,
};

use crate::held::{into, room, said, stored};
use crate::store::StuckStore;
use crate::support::channel;

const BUDGET: Duration = Duration::from_secs(1);

async fn bounded<T>(work: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), work)
        .await
        .expect("pre-commit work never settled")
}

async fn kit_with(router: Option<Arc<dyn InboundRoomRouter>>) -> RoomKit<StuckStore> {
    let kit = RoomKit::new(RoomKitOptions {
        process_timeout: BUDGET,
        inbound_router: router,
        ..RoomKitOptions::with_store(StuckStore::default())
    });
    kit.register_channel(RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room("r")),
        organization_id: Some("org".to_owned()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.attach_channel(&room("r"), &channel("sms"), AttachChannelOptions::default())
        .await
        .unwrap();
    kit
}

// Python: tests/test_precommit_timeout.py::TestTheWindowCoversWhatItClaims::test_a_store_that_hangs_during_the_context_build
#[tokio::test(start_paused = true)]
async fn a_context_read_that_hangs_is_refused_without_a_commit() {
    let kit = kit_with(None).await;
    *kit.store().held_read.lock().unwrap() = Some("bindings");
    let result = bounded(kit.process_inbound(said("hi"), into("r")))
        .await
        .unwrap();
    *kit.store().held_read.lock().unwrap() = None;
    assert_eq!(
        (
            result.blocked,
            result.reason.as_deref(),
            stored(&kit, "r").await
        ),
        (true, Some("process_timeout"), Vec::<String>::new())
    );
    kit.close().await.unwrap();
}

// Python: tests/test_precommit_timeout.py::TestTheWindowStopsWhereItShould::test_a_healthy_message_commits
#[tokio::test(start_paused = true)]
async fn a_healthy_message_commits_within_its_budget() {
    let kit = kit_with(None).await;
    let result = bounded(kit.process_inbound(said("hi"), into("r")))
        .await
        .unwrap();
    let event = result.event.unwrap();
    assert!(!result.blocked);
    assert_eq!(kit.store().get_event(&event.id).await.unwrap(), Some(event));
    kit.close().await.unwrap();
}

struct Routing(Duration);

impl InboundRoomRouter for Routing {
    fn route<'a>(
        &'a self,
        _: &'a ChannelId,
        _: &'a ChannelType,
        _: RouteOptions,
    ) -> RouterFuture<'a> {
        Box::pin(async move {
            tokio::time::sleep(self.0).await;
            Ok(Some(room("r")))
        })
    }
}

// Python: tests/test_precommit_timeout.py::TestOneDeadlineFromTheCallsStart::test_routing_that_hangs_is_refused_with_no_room
#[tokio::test(start_paused = true)]
async fn routing_that_hangs_is_refused_with_no_room() {
    for identity in [false, true] {
        let delay = if identity {
            Duration::ZERO
        } else {
            Duration::from_secs(30)
        };
        let kit = kit_with(Some(Arc::new(Routing(delay)))).await;
        let tap = EventTap::on(&kit, &["process_timeout"]);
        if identity {
            *kit.store().held_read.lock().unwrap() = Some("identity");
        }
        let result = bounded(kit.process_inbound(said("hi"), ProcessInboundOptions::default()))
            .await
            .unwrap();
        if identity {
            tokio::time::timeout(Duration::from_millis(10), kit.store().reading.notified())
                .await
                .unwrap();
        }
        *kit.store().held_read.lock().unwrap() = None;
        let events = tap.events();
        assert_eq!(
            (result.blocked, result.reason.as_deref(), events.len()),
            (true, Some("process_timeout"), 1)
        );
        assert_eq!(
            (events[0].room_id.clone(), events[0].channel_id.clone()),
            (None, Some(channel("sms")))
        );
        assert_eq!(stored(&kit, "r").await, Vec::<String>::new());
        assert_eq!(
            kit.store()
                .list_rooms(Default::default())
                .await
                .unwrap()
                .len(),
            1
        );
        kit.close().await.unwrap();
    }
}

// Python: tests/test_precommit_timeout.py::TestOneDeadlineFromTheCallsStart::test_routing_spends_the_budget_the_rest_of_the_call_has
#[tokio::test(start_paused = true)]
async fn routing_spends_the_budget_the_rest_of_the_call_has() {
    let kit = kit_with(Some(Arc::new(Routing(Duration::from_millis(800))))).await;
    kit.hook(
        HookTrigger::BeforeBroadcast,
        |_: RoomEvent, _| async {
            tokio::time::sleep(Duration::from_millis(600)).await;
            HookResult::allow(AllowOptions::default())
        },
        HookOptions {
            name: "slow".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    let started = tokio::time::Instant::now();
    let result = bounded(kit.process_inbound(said("hi"), ProcessInboundOptions::default()))
        .await
        .unwrap();
    assert_eq!(
        (result.blocked, result.reason.as_deref()),
        (true, Some("process_timeout"))
    );
    assert!(started.elapsed() < Duration::from_millis(1100));
    assert!(stored(&kit, "r").await.is_empty());
    kit.close().await.unwrap();
}

// Python: tests/test_precommit_timeout.py::TestDirectInjectionRaisesWhatItCannotReturn::test_it_raises_and_writes_nothing
#[tokio::test(start_paused = true)]
async fn a_direct_injection_past_its_deadline_raises_and_writes_nothing() {
    for at in ["lock", "hook", "room"] {
        let kit = kit_with(None).await;
        let tap = EventTap::on(&kit, &["process_timeout"]);
        let locks = kit.lock_manager();
        let r = room("r");
        let held = if at == "lock" {
            Some(locks.locked(&r).await.unwrap())
        } else {
            None
        };
        if at == "room" {
            *kit.store().held_read.lock().unwrap() = Some("room");
        }
        if at == "hook" {
            kit.hook(
                HookTrigger::BeforeBroadcast,
                |_: RoomEvent, _| async { std::future::pending::<HookResult>().await },
                HookOptions {
                    name: "held".to_owned(),
                    ..Default::default()
                },
            )
            .unwrap();
        }
        let result = bounded(kit.send_event(
            &r,
            &channel("sms"),
            TextContent::new("hi"),
            SendEventOptions::default(),
        ))
        .await;
        *kit.store().held_read.lock().unwrap() = None;
        if let Some(lock) = held {
            lock.release().await.unwrap();
        }
        assert!(
            matches!(result, Err(RoomKitError::ProcessTimeout { .. })),
            "{at}: {result:?}"
        );
        assert_eq!(
            (stored(&kit, "r").await, tap.types()),
            (Vec::<String>::new(), vec!["process_timeout".to_owned()]),
            "{at}"
        );
        kit.close().await.unwrap();
    }
}

// Python: tests/test_precommit_timeout.py::TestOneDeadlineFromTheCallsStart::test_step_two_s_attach_is_bounded_on_its_wait_for_the_lock
#[tokio::test(start_paused = true)]
async fn an_attach_waiting_for_the_room_lock_times_out_without_a_binding() {
    let locks = Arc::new(InMemoryLockManager::default());
    let kit = RoomKit::new(RoomKitOptions {
        lock_manager: Some(locks.clone()),
        process_timeout: BUDGET,
        ..Default::default()
    });
    kit.register_channel(RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room("r")),
        ..Default::default()
    })
    .await
    .unwrap();
    let r = room("r");
    let held = locks.locked(&r).await.unwrap();
    let result = bounded(kit.process_inbound(said("hi"), into("r")))
        .await
        .unwrap();
    held.release().await.unwrap();
    assert_eq!(
        (result.blocked, result.reason.as_deref()),
        (true, Some("process_timeout"))
    );
    assert!(
        kit.store()
            .get_binding(&r, &channel("sms"))
            .await
            .unwrap()
            .is_none()
    );
    kit.close().await.unwrap();
}

// Python: tests/test_precommit_timeout.py::TestOneDeadlineFromTheCallsStart::test_the_scope_read_before_a_room_s_own_message_draws_is_bounded
#[tokio::test(start_paused = true)]
async fn the_scope_read_before_a_room_s_own_message_draws_is_bounded() {
    let kit = kit_with(None).await;
    let results = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (hook_kit, seen) = (kit.clone(), results.clone());
    kit.hook(
        HookTrigger::AfterBroadcast,
        move |_: RoomEvent, _| {
            let (kit, seen) = (hook_kit.clone(), seen.clone());
            async move {
                *kit.store().held_read.lock().unwrap() = Some("room");
                let result = kit
                    .process_inbound(
                        said("again"),
                        ProcessInboundOptions {
                            organization_id: Some("org".to_owned()),
                            ..into("r")
                        },
                    )
                    .await
                    .unwrap();
                *kit.store().held_read.lock().unwrap() = None;
                seen.lock().unwrap().push((result.blocked, result.reason));
            }
        },
        HookOptions {
            name: "again".to_owned(),
            execution: HookExecution::Async,
            ..Default::default()
        },
    )
    .unwrap();
    bounded(kit.process_inbound(said("hi"), into("r")))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while results.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    roomkit::testing::idle(&kit).await;
    assert_eq!(
        *results.lock().unwrap(),
        vec![(true, Some("process_timeout".to_owned()))]
    );
    kit.close().await.unwrap();
}

// Python: tests/test_precommit_timeout.py::TestOneDeadlineFromTheCallsStart
// The scope read and the driver's subsequent hook spend the same window.
#[tokio::test(start_paused = true)]
async fn a_scope_read_spends_the_budget_the_hook_has_left() {
    let kit = kit_with(None).await;
    let timeouts = EventTap::on(&kit, &["process_timeout"]);
    let (answer, result) = tokio::sync::oneshot::channel();
    let answer = Arc::new(std::sync::Mutex::new(Some(answer)));
    kit.hook(
        HookTrigger::BeforeBroadcast,
        |event: RoomEvent, _| async move {
            if roomkit_core::rules::transcoding::text_of(&event.content) == "again" {
                tokio::time::sleep(Duration::from_millis(600)).await;
            }
            HookResult::allow(AllowOptions::default())
        },
        HookOptions {
            name: "slow-again".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    let nested = kit.clone();
    kit.hook(
        HookTrigger::AfterBroadcast,
        move |_: RoomEvent, _| {
            let (kit, answer) = (nested.clone(), answer.clone());
            async move {
                let Some(answer) = answer.lock().unwrap().take() else {
                    return;
                };
                *kit.store().delayed_read.lock().unwrap() =
                    Some(("room", Duration::from_millis(800)));
                let start = tokio::time::Instant::now();
                let result = kit
                    .process_inbound(
                        said("again"),
                        ProcessInboundOptions {
                            organization_id: Some("org".to_owned()),
                            ..into("r")
                        },
                    )
                    .await
                    .unwrap();
                answer.send((result, start.elapsed())).unwrap();
            }
        },
        HookOptions {
            name: "again".to_owned(),
            execution: HookExecution::Async,
            ..Default::default()
        },
    )
    .unwrap();
    bounded(kit.process_inbound(said("hi"), into("r")))
        .await
        .unwrap();
    let (result, elapsed) = tokio::time::timeout(Duration::from_secs(5), result)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (result.blocked, result.reason.as_deref()),
        (true, Some("process_timeout"))
    );
    assert!(elapsed < Duration::from_millis(1100), "{elapsed:?}");
    assert_eq!(stored(&kit, "r").await, ["hi"]);
    let reported = timeouts.events();
    assert_eq!(reported.len(), 1);
    assert_eq!(reported[0].data["timeout"], serde_json::json!(1.0));
    kit.close().await.unwrap();
}

// Python: tests/test_precommit_timeout.py::test_a_scope_read_under_a_locked_hook_keeps_close_grace
// A call already inside its parent's locked hook keeps that parent's grace.
#[tokio::test(start_paused = true)]
async fn an_adoptable_scope_read_keeps_the_locked_hooks_close_grace() {
    let kit = kit_with(None).await;
    let (answer, result) = tokio::sync::oneshot::channel();
    let answer = Arc::new(std::sync::Mutex::new(Some(answer)));
    let nested = kit.clone();
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |event: RoomEvent, _| {
            let (kit, answer) = (nested.clone(), answer.clone());
            async move {
                if roomkit_core::rules::transcoding::text_of(&event.content) == "hi" {
                    *kit.store().held_read.lock().unwrap() = Some("room");
                    let result = kit
                        .process_inbound(
                            said("again"),
                            ProcessInboundOptions {
                                organization_id: Some("org".to_owned()),
                                ..into("r")
                            },
                        )
                        .await
                        .unwrap();
                    answer.lock().unwrap().take().unwrap().send(result).unwrap();
                }
                HookResult::allow(AllowOptions::default())
            }
        },
        HookOptions {
            name: "again".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    let call = crate::held::spawn_said(&kit, "hi", "r");
    kit.store().reading.notified().await;
    let close = crate::held::closing(&kit).await;
    assert!(matches!(kit.start().await, Err(RoomKitError::KitClosed)));
    *kit.store().held_read.lock().unwrap() = None;
    kit.store().release_read.notify_one();
    let nested = tokio::time::timeout(Duration::from_secs(5), result)
        .await
        .unwrap()
        .unwrap();
    assert!(!nested.blocked, "{nested:?}");
    call.await.unwrap().unwrap();
    crate::held::closed(close).await;
    assert!(stored(&kit, "r").await.contains(&"again".to_owned()));
}

// Python: tests/test_reentrance.py::TestAnAdoptedCallBeforeItsCommit::test_a_call_a_hook_s_task_makes_keeps_its_own_deadline
// Preparation spends the adopted child's own window, not a new one at adoption.
#[tokio::test]
async fn an_adopted_call_keeps_the_window_its_scope_read_spent() {
    let kit = kit_with(None).await;
    let timeouts = EventTap::on(&kit, &["process_timeout"]);
    let child = Arc::new(std::sync::Mutex::new(None));
    let (hook_kit, started) = (kit.clone(), child.clone());
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |event: RoomEvent, _| {
            let (kit, started) = (hook_kit.clone(), started.clone());
            async move {
                if roomkit_core::rules::transcoding::text_of(&event.content) == "hi" {
                    *kit.store().delayed_read.lock().unwrap() =
                        Some(("room", Duration::from_millis(800)));
                    *started.lock().unwrap() =
                        Some(tokio::spawn(roomkit::keep_context(async move {
                            kit.process_inbound(
                                said("again"),
                                ProcessInboundOptions {
                                    organization_id: Some("org".to_owned()),
                                    ..into("r")
                                },
                            )
                            .await
                        })));
                    std::future::pending::<()>().await;
                }
                HookResult::allow(AllowOptions::default())
            }
        },
        HookOptions {
            name: "spawn-child".to_owned(),
            needs_lock: false,
            ..Default::default()
        },
    )
    .unwrap();
    kit.hook(
        HookTrigger::BeforeBroadcast,
        |event: RoomEvent, _| async move {
            if roomkit_core::rules::transcoding::text_of(&event.content) == "again" {
                tokio::time::sleep(Duration::from_millis(600)).await;
            }
            HookResult::allow(AllowOptions::default())
        },
        HookOptions {
            name: "slow-child".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    let parent = bounded(kit.process_inbound(said("hi"), into("r")))
        .await
        .unwrap();
    let child = child.lock().unwrap().take().unwrap();
    let child = bounded(child).await.unwrap().unwrap();
    assert_eq!(parent.reason.as_deref(), Some("process_timeout"));
    assert_eq!(child.reason.as_deref(), Some("process_timeout"));
    assert!(stored(&kit, "r").await.is_empty());
    assert_eq!(timeouts.events().len(), 2);
    kit.close().await.unwrap();
}

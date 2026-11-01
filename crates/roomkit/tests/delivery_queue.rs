//! A delivery's execution, its strategies, the delivery queue and its
//! worker (RFC §22), what the shared scenarios cannot drive: a strategy of
//! the test's own, a backend that fails, the worker run by hand, the
//! transport a context finds.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit::delivery::{
    ExecuteDeliveryOptions, RunWorkerLoopOptions, execute_delivery, run_worker_loop,
};
use roomkit::model::DeliveryOutcome;
use roomkit::model::DeliveryOutcomeStatus;
use roomkit::model::hook::{BlockOptions, ModifyOptions};
use roomkit::model::{
    ChannelCategory, ChannelId, ChannelType, EventContent, HookResult, RoomContext, RoomEvent,
    RoomId, TextContent,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions, idle};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, DeliverOptions, DeliveryBackend, DeliveryBackendError,
    DeliveryContext, DeliveryItem, DeliveryStrategy, DequeueOptions, GetDeadLetterItemsOptions,
    HookExecution, HookOptions, HookTrigger, InMemoryDeliveryBackend,
    InMemoryDeliveryBackendOptions, InMemoryStore, NackOptions, RoomKit, RoomKitError,
    RoomKitOptions,
};

type Kit = RoomKit<InMemoryStore>;

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room() -> RoomId {
    RoomId::new("r1").unwrap()
}

/// A kit whose room `r1` binds the transport `text`.
async fn kit_with_room(options: RoomKitOptions) -> Kit {
    let kit = RoomKit::new(options);
    let text = RecordingTransport::new(
        channel("text"),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(text).unwrap();
    let create = CreateRoomOptions {
        room_id: Some(room()),
        ..CreateRoomOptions::default()
    };
    kit.create_room(create).await.unwrap();
    kit.attach_channel(&room(), &channel("text"), AttachChannelOptions::default())
        .await
        .unwrap();
    kit
}

/// A strategy that records what it is given and answers as told.
#[derive(Default)]
struct Recording {
    given: Mutex<Vec<DeliveryContext>>,
    order: Option<Arc<Mutex<Vec<&'static str>>>>,
    fails: Option<String>,
    answers: Option<DeliveryOutcome>,
}

impl DeliveryStrategy for Recording {
    fn deliver(
        &self,
        ctx: DeliveryContext,
    ) -> Pin<Box<dyn Future<Output = Result<Option<DeliveryOutcome>, RoomKitError>> + Send + '_>>
    {
        Box::pin(async move {
            if let Some(order) = &self.order {
                order.lock().unwrap().push("deliver");
            }
            self.given.lock().unwrap().push(ctx);
            match &self.fails {
                Some(message) => Err(RoomKitError::Value(message.clone())),
                None => Ok(Some(self.answers.clone().unwrap_or_else(|| {
                    DeliveryOutcome::new(DeliveryOutcomeStatus::Sent)
                }))),
            }
        })
    }
}

fn with(strategy: &Arc<Recording>) -> ExecuteDeliveryOptions {
    let strategy: Arc<dyn DeliveryStrategy> = Arc::<Recording>::clone(strategy);
    ExecuteDeliveryOptions {
        strategy: Some(strategy),
        hook_engine: None,
    }
}

// Python: tests/delivery/test_worker.py::TestExecuteDelivery::test_calls_strategy_deliver
// and test_passes_channel_id_and_metadata.
#[tokio::test]
async fn the_strategy_is_given_the_item_s_request() {
    let kit = kit_with_room(RoomKitOptions::default()).await;
    let strategy = Arc::new(Recording::default());
    let mut item = DeliveryItem {
        channel_id: Some(channel("ch1")),
        metadata: serde_json::from_value(serde_json::json!({"key": "val"})).unwrap(),
        ..DeliveryItem::new(room(), "hello")
    };
    execute_delivery(&kit, &mut item, with(&strategy)).await;
    {
        let given = strategy.given.lock().unwrap();
        assert_eq!(given.len(), 1);
        assert_eq!(
            (given[0].room_id.clone(), given[0].content.as_str()),
            (room(), "hello")
        );
        assert_eq!(given[0].channel_id, Some(channel("ch1")));
        assert_eq!(given[0].metadata["key"], "val");
    }
    kit.close().await.unwrap();
}

/// Registers hooks on both delivery triggers that push `hook` on `order`;
/// the `AFTER_DELIVER` one also hands its event over `seen`.
fn delivery_hooks(kit: &Kit, order: &Arc<Mutex<Vec<&'static str>>>) -> Arc<Mutex<Vec<RoomEvent>>> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let before = Arc::clone(order);
    kit.hook(
        HookTrigger::BeforeDeliver,
        move |_event: RoomEvent, _context: RoomContext| {
            before.lock().unwrap().push("hook");
            async {}
        },
        HookOptions::default(),
    )
    .unwrap();
    let (after, events) = (Arc::clone(order), Arc::clone(&seen));
    kit.hook(
        HookTrigger::AfterDeliver,
        move |event: RoomEvent, _context: RoomContext| {
            after.lock().unwrap().push("hook");
            events.lock().unwrap().push(event);
            async {}
        },
        HookOptions {
            execution: HookExecution::Async,
            ..HookOptions::default()
        },
    )
    .unwrap();
    seen
}

// Python: tests/delivery/test_worker.py::TestDeliveryHooks::test_before_deliver_fires_before_strategy
#[tokio::test]
async fn before_deliver_runs_before_the_strategy_and_after_deliver_after_it() {
    let kit = kit_with_room(RoomKitOptions::default()).await;
    let order = Arc::new(Mutex::new(Vec::new()));
    delivery_hooks(&kit, &order);
    let strategy = Arc::new(Recording {
        order: Some(Arc::clone(&order)),
        ..Recording::default()
    });
    let mut item = DeliveryItem::new(room(), "hello");
    execute_delivery(&kit, &mut item, with(&strategy)).await;
    idle(&kit).await;
    assert_eq!(*order.lock().unwrap(), ["hook", "deliver", "hook"]);
    kit.close().await.unwrap();
}

// Python: tests/delivery/test_worker.py::TestDeliveryHooks::test_after_deliver_fires_even_on_failure
// and test_after_deliver_metadata_contains_error.
#[tokio::test]
async fn after_deliver_reports_a_strategy_that_raised() {
    let kit = kit_with_room(RoomKitOptions::default()).await;
    let order = Arc::new(Mutex::new(Vec::new()));
    let seen = delivery_hooks(&kit, &order);
    let strategy = Arc::new(Recording {
        fails: Some("bad".to_owned()),
        ..Recording::default()
    });
    let mut item = DeliveryItem::new(room(), "hello");
    let outcome = execute_delivery(&kit, &mut item, with(&strategy)).await;
    idle(&kit).await;
    assert_eq!(outcome.status, DeliveryOutcomeStatus::Failed);
    assert_eq!(outcome.reason.as_deref(), Some("strategy_failed"));
    assert_eq!(outcome.error.unwrap().code, "ValueError");
    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].metadata["error"], "bad");
    kit.close().await.unwrap();
}

// Python: delivery/worker.py's fire_delivery_hooks: an unsuccessful outcome
// with neither error nor reason reports a null error.
#[tokio::test]
async fn after_deliver_reports_no_error_an_outcome_does_not_name() {
    let kit = kit_with_room(RoomKitOptions::default()).await;
    let order = Arc::new(Mutex::new(Vec::new()));
    let seen = delivery_hooks(&kit, &order);
    let strategy = Arc::new(Recording {
        answers: Some(DeliveryOutcome::new(DeliveryOutcomeStatus::Failed)),
        ..Recording::default()
    });
    let mut item = DeliveryItem::new(room(), "hello");
    execute_delivery(&kit, &mut item, with(&strategy)).await;
    idle(&kit).await;
    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen[0].metadata["error"], serde_json::Value::Null);
    kit.close().await.unwrap();
}

// Python: tests/delivery/test_worker.py::TestDeliveryHooks::test_hook_failure_does_not_abort_delivery:
// hooks whose room's context cannot be read deliver the item all the same.
#[tokio::test]
async fn hooks_that_cannot_run_do_not_stop_the_delivery() {
    let kit = kit_with_room(RoomKitOptions::default()).await;
    let order = Arc::new(Mutex::new(Vec::new()));
    delivery_hooks(&kit, &order);
    let strategy = Arc::new(Recording::default());
    let mut item = DeliveryItem::new(RoomId::new("missing").unwrap(), "hello");
    execute_delivery(&kit, &mut item, with(&strategy)).await;
    assert_eq!(strategy.given.lock().unwrap().len(), 1);
    assert!(order.lock().unwrap().is_empty());
    kit.close().await.unwrap();
}

// Python: tests/delivery/test_targeted.py::test_worker_after_hook_sees_rewrite_and_real_outcome
#[tokio::test]
async fn after_deliver_sees_the_rewrite_and_the_real_outcome() {
    let kit = kit_with_room(RoomKitOptions::default()).await;
    kit.hook(
        HookTrigger::BeforeDeliver,
        |event: RoomEvent, _context: RoomContext| async move {
            let rewritten = RoomEvent {
                content: TextContent::new("redacted").into(),
                ..event
            };
            HookResult::modify(rewritten, ModifyOptions::default())
        },
        HookOptions::default(),
    )
    .unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let events = Arc::clone(&seen);
    kit.hook(
        HookTrigger::AfterDeliver,
        move |event: RoomEvent, _context: RoomContext| {
            events.lock().unwrap().push(event);
            async {}
        },
        HookOptions::default(),
    )
    .unwrap();
    let mut item = DeliveryItem {
        idempotency_key: Some("k".to_owned()),
        ..DeliveryItem::new(room(), "secret")
    };
    let outcome = execute_delivery(&kit, &mut item, ExecuteDeliveryOptions::default()).await;
    idle(&kit).await;
    assert_eq!(outcome.status, DeliveryOutcomeStatus::Sent);
    let seen = seen.lock().unwrap().clone();
    assert_eq!(
        seen[0].content,
        EventContent::from(TextContent::new("redacted"))
    );
    let reported = seen[0].metadata["delivery_outcome"]["event_id"].clone();
    assert_eq!(reported, serde_json::to_value(&outcome.event_id).unwrap());
    assert_eq!(seen[0].idempotency_key.as_deref(), Some("k"));
    kit.close().await.unwrap();
}

/// A backend over an in-memory one that counts its nacks, the first
/// `failing` of which fail.
struct Flaky {
    inner: InMemoryDeliveryBackend,
    nacks: Mutex<usize>,
    failing: usize,
}

type Answer<'a, T> = Pin<Box<dyn Future<Output = Result<T, DeliveryBackendError>> + Send + 'a>>;

impl DeliveryBackend for Flaky {
    fn enqueue(&self, item: DeliveryItem) -> Answer<'_, ()> {
        DeliveryBackend::<InMemoryStore>::enqueue(&self.inner, item)
    }
    fn dequeue<'a>(
        &'a self,
        worker_id: &'a str,
        options: DequeueOptions,
    ) -> Answer<'a, Vec<DeliveryItem>> {
        DeliveryBackend::<InMemoryStore>::dequeue(&self.inner, worker_id, options)
    }
    fn ack<'a>(&'a self, item_id: &'a str) -> Answer<'a, ()> {
        DeliveryBackend::<InMemoryStore>::ack(&self.inner, item_id)
    }
    fn nack<'a>(&'a self, item_id: &'a str, options: NackOptions) -> Answer<'a, ()> {
        let attempt = {
            let mut nacks = self.nacks.lock().unwrap();
            *nacks += 1;
            *nacks
        };
        if attempt <= self.failing {
            return Box::pin(async { Err("offline".into()) });
        }
        DeliveryBackend::<InMemoryStore>::nack(&self.inner, item_id, options)
    }
    fn dead_letter<'a>(&'a self, item_id: &'a str, error: &'a str) -> Answer<'a, ()> {
        DeliveryBackend::<InMemoryStore>::dead_letter(&self.inner, item_id, error)
    }
    fn get_queue_depth(&self) -> Answer<'_, usize> {
        DeliveryBackend::<InMemoryStore>::get_queue_depth(&self.inner)
    }
    fn get_dead_letter_items(
        &self,
        options: GetDeadLetterItemsOptions,
    ) -> Answer<'_, Vec<DeliveryItem>> {
        DeliveryBackend::<InMemoryStore>::get_dead_letter_items(&self.inner, options)
    }
}

fn worker_options() -> RunWorkerLoopOptions {
    RunWorkerLoopOptions {
        batch_size: 1,
        poll_timeout: Duration::from_millis(100),
    }
}

/// Waits, within a bound, until `done` holds.
async fn until(mut done: impl AsyncFnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !done().await {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

/// How many messages the framework's own sender published in the room
/// `r1`, its lifecycle records left out.
async fn published(kit: &Kit) -> usize {
    use roomkit::ConversationStore;
    let options = roomkit::store::ListEventsOptions::default();
    let events = kit.store().list_events(&room(), options).await.unwrap();
    events
        .iter()
        .filter(|event| {
            event
                .source
                .participant_id
                .as_ref()
                .is_some_and(|id| id.as_str() == "system")
        })
        .count()
}

async fn dead_letters(backend: &InMemoryDeliveryBackend) -> Vec<DeliveryItem> {
    let options = GetDeadLetterItemsOptions::default();
    DeliveryBackend::<InMemoryStore>::get_dead_letter_items(backend, options)
        .await
        .unwrap()
}

async fn depth(backend: &InMemoryDeliveryBackend) -> usize {
    DeliveryBackend::<InMemoryStore>::get_queue_depth(backend)
        .await
        .unwrap()
}

async fn enqueue(backend: &InMemoryDeliveryBackend, item: DeliveryItem) {
    DeliveryBackend::<InMemoryStore>::enqueue(backend, item)
        .await
        .unwrap();
}

fn retrying_at_once() -> InMemoryDeliveryBackend {
    InMemoryDeliveryBackend::new(InMemoryDeliveryBackendOptions {
        retry_delay: Duration::ZERO,
        ..InMemoryDeliveryBackendOptions::default()
    })
    .unwrap()
}

// Python: tests/delivery/test_worker.py::TestRunWorkerLoop::test_acks_on_success
// and test_loop_cancellation.
#[tokio::test]
async fn the_worker_acks_an_item_sent() {
    let kit = kit_with_room(RoomKitOptions::default()).await;
    let backend = InMemoryDeliveryBackend::default();
    enqueue(&backend, DeliveryItem::new(room(), "hello")).await;
    let shared: Arc<dyn DeliveryBackend> = Arc::new(backend.clone());
    let worker = tokio::spawn(run_worker_loop(shared, kit.clone(), "w1", worker_options()));
    until(async || backend.in_flight() == 0 && published(&kit).await == 1).await;
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
    assert_eq!(dead_letters(&backend).await, Vec::<DeliveryItem>::new());
    assert_eq!(depth(&backend).await, 0);
    kit.close().await.unwrap();
}

// Python: tests/delivery/test_worker.py::TestRunWorkerLoop::test_nacks_on_failure
#[tokio::test]
async fn the_worker_gives_back_a_failure_until_its_retries_are_spent() {
    let kit = kit_with_room(RoomKitOptions::default()).await;
    let backend = retrying_at_once();
    let failing = DeliveryItem {
        max_retries: 2,
        channel_id: Some(channel("missing")),
        ..DeliveryItem::new(room(), "hello")
    };
    enqueue(&backend, failing).await;
    let shared: Arc<dyn DeliveryBackend> = Arc::new(backend.clone());
    let worker = tokio::spawn(run_worker_loop(shared, kit.clone(), "w1", worker_options()));
    until(async || !dead_letters(&backend).await.is_empty()).await;
    worker.abort();
    let dead = dead_letters(&backend).await;
    assert_eq!(dead[0].status, roomkit::DeliveryItemStatus::DeadLetter);
    // Two retries after the first attempt: three attempts failed, the
    // outcome of the last kept on the item.
    assert_eq!(dead[0].retry_count, 3);
    let outcome = dead[0].outcome.clone().unwrap();
    assert_eq!(outcome.status, DeliveryOutcomeStatus::Unavailable);
    kit.close().await.unwrap();
}

// Python: tests/delivery/test_worker.py::test_nack_write_is_retried_without_repeating_delivery
#[tokio::test(start_paused = true)]
async fn a_failed_nack_is_retried_without_repeating_the_delivery() {
    let kit = kit_with_room(RoomKitOptions::default()).await;
    let flaky = Arc::new(Flaky {
        inner: retrying_at_once(),
        nacks: Mutex::new(0),
        failing: 1,
    });
    let item = DeliveryItem {
        max_retries: 0,
        channel_id: Some(channel("missing")),
        ..DeliveryItem::new(room(), "hello")
    };
    enqueue(&flaky.inner, item).await;
    let order = Arc::new(Mutex::new(Vec::new()));
    let seen = delivery_hooks(&kit, &order);
    let shared: Arc<dyn DeliveryBackend> = Arc::<Flaky>::clone(&flaky);
    let worker = tokio::spawn(run_worker_loop(shared, kit.clone(), "w1", worker_options()));
    until(async || !dead_letters(&flaky.inner).await.is_empty()).await;
    worker.abort();
    assert_eq!(*flaky.nacks.lock().unwrap(), 2);
    assert_eq!(seen.lock().unwrap().len(), 1, "executed once");
    kit.close().await.unwrap();
}

// Python: tests/delivery/test_targeted.py::test_real_worker_dead_letters_missing_target_and_records_refusal,
// the kit starting its backend: an item with no transport is dead-lettered
// once its retries are spent; one a hook refuses is acked, never retried.
#[tokio::test]
async fn the_kit_s_worker_dead_letters_a_target_it_never_reaches() {
    let backend = retrying_at_once();
    let options = RoomKitOptions {
        delivery_backend: Some(Arc::new(backend.clone())),
        ..RoomKitOptions::default()
    };
    let kit = RoomKit::new(options);
    let create = CreateRoomOptions {
        room_id: Some(room()),
        ..CreateRoomOptions::default()
    };
    kit.create_room(create).await.unwrap();
    kit.start().await.unwrap();
    kit.start().await.unwrap();
    enqueue(
        &backend,
        DeliveryItem {
            max_retries: 2,
            ..DeliveryItem::new(room(), "result")
        },
    )
    .await;
    until(async || !dead_letters(&backend).await.is_empty()).await;
    let dead = dead_letters(&backend).await;
    assert_eq!(
        dead[0].outcome.as_ref().unwrap().status,
        DeliveryOutcomeStatus::Unavailable
    );
    assert_eq!(dead[0].retry_count, 3);
    kit.hook(
        HookTrigger::BeforeDeliver,
        |_event: RoomEvent, _context: RoomContext| async {
            HookResult::block("policy", BlockOptions::default())
        },
        HookOptions::default(),
    )
    .unwrap();
    enqueue(&backend, DeliveryItem::new(room(), "refused")).await;
    until(async || depth(&backend).await == 0 && backend.in_flight() == 0).await;
    assert_eq!(dead_letters(&backend).await.len(), 1);
    kit.close().await.unwrap();
    // The kit's close closed its backend: nothing is claimed any more.
    assert_eq!(backend.in_flight(), 0);
}

// Python: delivery/memory.py's close() re-enqueues with put_nowait, which
// wakes a get() waiting: a dequeue waiting on another worker is given the
// items the close queues again, not left to its poll timeout.
#[tokio::test(start_paused = true)]
async fn a_dequeue_waiting_reads_the_items_a_close_queues_again() {
    let backend = InMemoryDeliveryBackend::default();
    enqueue(&backend, DeliveryItem::new(room(), "hello")).await;
    let options = DequeueOptions {
        batch_size: 1,
        timeout: Duration::from_secs(5),
    };
    let claimed = DeliveryBackend::<InMemoryStore>::dequeue(&backend, "w1", options)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    let waiting = backend.clone();
    let other = tokio::spawn(async move {
        DeliveryBackend::<InMemoryStore>::dequeue(&waiting, "w2", options)
            .await
            .unwrap()
    });
    tokio::task::yield_now().await;
    DeliveryBackend::<InMemoryStore>::close(&backend)
        .await
        .unwrap();
    let read = other.await.unwrap();
    assert_eq!(read.len(), 1);
    assert_eq!(read[0].id, claimed[0].id);
}

/// A kit started on a backend whose first kit went while its worker waited
/// for an item; the item, on its last attempt, enqueued before the kit
/// starts when `claimed_as_it_went`, so the gone kit's worker claims it, or
/// after. Whether the kit published it.
async fn a_kit_sharing_the_backend_of_a_gone_kit(claimed_as_it_went: bool) -> bool {
    let backend = InMemoryDeliveryBackend::default();
    let options = || RoomKitOptions {
        delivery_backend: Some(Arc::new(backend.clone())),
        ..RoomKitOptions::default()
    };
    let item = DeliveryItem {
        max_retries: 0,
        ..DeliveryItem::new(room(), "result")
    };
    let gone: Kit = RoomKit::new(options());
    gone.start().await.unwrap();
    // Its worker waits for an item when the kit goes.
    tokio::task::yield_now().await;
    drop(gone);
    let item = if claimed_as_it_went {
        enqueue(&backend, item).await;
        tokio::task::yield_now().await;
        assert_eq!(backend.in_flight(), 1, "the gone kit's worker claimed it");
        None
    } else {
        Some(item)
    };
    let kit = kit_with_room(options()).await;
    kit.start().await.unwrap();
    if let Some(item) = item {
        enqueue(&backend, item).await;
    }
    let published = tokio::time::timeout(Duration::from_secs(60), async {
        while published(&kit).await == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .is_ok();
    kit.close().await.unwrap();
    published
}

// Rust alone: a kit sharing a backend whose kit is gone starts a worker of
// its own at once, the gone kit's worker still waiting for an item.
#[tokio::test(start_paused = true)]
async fn a_kit_sharing_a_backend_whose_kit_is_gone_starts_its_worker() {
    assert!(a_kit_sharing_the_backend_of_a_gone_kit(false).await);
}

// Rust alone: an item the gone kit's worker claimed as the kit went is
// queued again uncounted, as Python's close queues an item held, and the
// new worker publishes it on its last attempt.
#[tokio::test(start_paused = true)]
async fn an_item_claimed_as_the_kit_went_is_queued_again_uncounted() {
    assert!(a_kit_sharing_the_backend_of_a_gone_kit(true).await);
}

// Rust alone: the kit holds its backend, whose worker holds the kit weakly,
// so a kit no one holds is gone though its worker was started.
#[tokio::test]
async fn a_started_kit_no_one_holds_is_gone() {
    let options = RoomKitOptions {
        delivery_backend: Some(Arc::new(InMemoryDeliveryBackend::default())),
        ..RoomKitOptions::default()
    };
    let kit: Kit = RoomKit::new(options);
    kit.start().await.unwrap();
    let weak = kit.downgrade();
    drop(kit);
    assert!(weak.upgrade().is_none());
}

// Python: tests/delivery/test_targeted.py::test_enqueue_replay_preserves_address_and_key
#[tokio::test]
async fn a_queued_item_keeps_its_address_and_key_across_a_retry() {
    let backend = retrying_at_once();
    let options = RoomKitOptions {
        delivery_backend: Some(Arc::new(backend.clone())),
        ..RoomKitOptions::default()
    };
    let kit = kit_with_room(options).await;
    let queued = kit
        .deliver(
            &room(),
            "external result",
            DeliverOptions {
                addressed_to: Some(vec![channel("a")]),
                idempotency_key: Some("external:1".to_owned()),
                ..DeliverOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(queued.status, DeliveryOutcomeStatus::Queued);
    assert_eq!(queued.event_id, None);
    assert_eq!(published(&kit).await, 0, "acceptance is not transmission");
    let claim = DequeueOptions {
        batch_size: 1,
        timeout: Duration::from_millis(100),
    };
    let item = DeliveryBackend::<InMemoryStore>::dequeue(&backend, "worker", claim)
        .await
        .unwrap()
        .remove(0);
    let mut restored: DeliveryItem =
        serde_json::from_str(&serde_json::to_string(&item).unwrap()).unwrap();
    assert_eq!(Some(restored.id.clone()), queued.delivery_item_id);
    assert_eq!(restored.addressed_to, Some(vec![channel("a")]));
    assert_eq!(restored.idempotency_key.as_deref(), Some("external:1"));
    let first = execute_delivery(&kit, &mut restored, ExecuteDeliveryOptions::default()).await;
    let error = NackOptions {
        error: Some("ack connection lost".to_owned()),
    };
    DeliveryBackend::<InMemoryStore>::nack(&backend, &item.id, error)
        .await
        .unwrap();
    let mut retry = DeliveryBackend::<InMemoryStore>::dequeue(&backend, "worker", claim)
        .await
        .unwrap()
        .remove(0);
    let second = execute_delivery(&kit, &mut retry, ExecuteDeliveryOptions::default()).await;
    assert_eq!(first.event_id, second.event_id);
    assert!(second.duplicate);
    assert_eq!(published(&kit).await, 1);
    kit.close().await.unwrap();
}

/// A kit whose room `r1` binds the transports `bound`, each of the type
/// its name says, and the agent `agent` as a transport.
async fn kit_binding(bound: &[(&str, ChannelType)]) -> Kit {
    let kit = RoomKit::new(RoomKitOptions::default());
    let create = CreateRoomOptions {
        room_id: Some(room()),
        ..CreateRoomOptions::default()
    };
    kit.create_room(create).await.unwrap();
    for (id, channel_type) in bound {
        let transport = RecordingTransport::new(
            channel(id),
            channel_type.clone(),
            RecordingTransportOptions::default(),
        );
        kit.register_channel(transport).unwrap();
        kit.attach_channel(&room(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    kit
}

// Python: tests/test_delivery_strategies.py::TestDeliveryContext (test_find_transport_prefers_voice,
// test_find_transport_prefers_realtime_voice, test_find_transport_falls_back_to_text,
// test_find_transport_no_bindings, test_resolve_channel_id_explicit,
// test_resolve_channel_id_auto_detects).
#[tokio::test]
async fn a_context_finds_the_room_s_voice_transport_first() {
    for (voice, voice_type) in [
        ("voice-1", ChannelType::VOICE),
        ("rtv", ChannelType::REALTIME_VOICE),
    ] {
        let kit = kit_binding(&[("sms-1", ChannelType::SMS), (voice, voice_type)]).await;
        let context = DeliveryContext::new(kit.clone(), room(), "hi");
        assert_eq!(
            context.find_transport_channel_id().await.unwrap(),
            Some(channel(voice))
        );
        kit.close().await.unwrap();
    }
    let kit = kit_binding(&[
        ("sms-1", ChannelType::SMS),
        ("ws-1", ChannelType::WEBSOCKET),
    ])
    .await;
    let mut context = DeliveryContext::new(kit.clone(), room(), "hi");
    assert_eq!(
        context.find_transport_channel_id().await.unwrap(),
        Some(channel("sms-1"))
    );
    assert_eq!(
        context.resolve_channel_id().await.unwrap(),
        Some(channel("sms-1"))
    );
    context.channel_id = Some(channel("explicit"));
    assert_eq!(
        context.resolve_channel_id().await.unwrap(),
        Some(channel("explicit"))
    );
    kit.close().await.unwrap();
    let kit = kit_binding(&[]).await;
    let context = DeliveryContext::new(kit.clone(), room(), "hi");
    assert_eq!(context.find_transport_channel_id().await.unwrap(), None);
    kit.close().await.unwrap();
}

// Python: tests/test_delivery_strategies.py::TestWaitForIdleStrategy::test_init_defaults,
// test_init_custom, and TestQueuedStrategy's.
#[test]
fn the_strategies_take_python_s_defaults() {
    let idle = roomkit::WaitForIdle::default();
    assert_eq!(
        (idle.buffer(), idle.playback_timeout()),
        (Duration::from_secs(1), Duration::from_secs(15))
    );
    let custom = roomkit::WaitForIdle::new(roomkit::WaitForIdleOptions {
        buffer: Duration::from_millis(2500),
        playback_timeout: Duration::from_secs(10),
    });
    assert_eq!(custom.buffer(), Duration::from_millis(2500));
    let queued: roomkit::Queued = roomkit::Queued::default();
    assert_eq!(
        (
            queued.buffer(),
            queued.playback_timeout(),
            queued.separator()
        ),
        (Duration::from_secs(1), Duration::from_secs(15), "\n\n")
    );
    let custom: roomkit::Queued = roomkit::Queued::new(roomkit::QueuedOptions {
        buffer: Duration::from_millis(500),
        playback_timeout: Duration::from_secs(5),
        separator: " | ".to_owned(),
    });
    assert_eq!(custom.separator(), " | ");
    let _ = ChannelCategory::Transport;
}

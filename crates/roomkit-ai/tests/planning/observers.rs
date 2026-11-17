use super::*;
use roomkit::model::RoomContext;
use roomkit::realtime::{
    EphemeralCallback, EphemeralEvent, EphemeralEventType, InMemoryRealtime, RealtimeBackend,
    RealtimeFuture,
};
use roomkit::{HookExecution, HookOptions, HookTrigger};
use roomkit_ai::PlanUpdatedEvent;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

#[derive(Default)]
struct Publication {
    inner: InMemoryRealtime,
    entered: Notify,
    release: Notify,
    plans: Mutex<Vec<EphemeralEvent>>,
    fail: bool,
    failed: AtomicBool,
}

impl RealtimeBackend for Publication {
    fn publish<'a>(
        &'a self,
        channel: &'a str,
        event: EphemeralEvent,
    ) -> RealtimeFuture<'a, Result<(), roomkit::ChannelError>> {
        Box::pin(async move {
            if event.r#type == EphemeralEventType::Custom
                && event.data.get("type") == Some(&json!("plan_updated"))
            {
                self.plans.lock().unwrap().push(event.clone());
                self.entered.notify_one();
                self.release.notified().await;
                if self.fail {
                    self.failed.store(true, Ordering::SeqCst);
                    return Err(std::io::Error::other("plan publisher failed").into());
                }
            }
            self.inner.publish(channel, event).await
        })
    }
    fn subscribe<'a>(
        &'a self,
        channel: &'a str,
        callback: EphemeralCallback,
    ) -> RealtimeFuture<'a, String> {
        self.inner.subscribe(channel, callback)
    }
    fn unsubscribe<'a>(&'a self, id: &'a str) -> RealtimeFuture<'a, bool> {
        self.inner.unsubscribe(id)
    }
}

fn options() -> HookOptions {
    HookOptions {
        execution: HookExecution::Async,
        ..Default::default()
    }
}

// Python: test_planning_hooks.py, including its unrelated SMS filter.
#[tokio::test]
async fn a_failing_observer_ignores_channel_type_filters_and_keeps_the_plan() {
    for streaming in [false, true] {
        let provider = model(
            vec![
                plan(tasks("Follow up")),
                AiResponse::new("Done."),
                AiResponse::new("Done."),
            ],
            streaming,
        );
        let kit = setup(provider.clone(), &["r"], RoomKitOptions::default()).await;
        let seen = Arc::new(Mutex::new(Vec::new()));
        let saving = Arc::clone(&seen);
        kit.hook(
            HookTrigger::OnPlanUpdated,
            move |event: PlanUpdatedEvent, _: RoomContext| {
                saving.lock().unwrap().push(event);
                async {
                    Err::<roomkit::model::HookResult<PlanUpdatedEvent>, roomkit::HookError>(
                        "observer failed".into(),
                    )
                }
            },
            HookOptions {
                channel_types: Some([ChannelType::SMS].into_iter().collect()),
                ..Default::default()
            },
        )
        .unwrap();
        say(&kit, "r", "Plan.").await;
        tokio::time::timeout(BOUND, roomkit::testing::idle(&kit))
            .await
            .unwrap();
        assert_eq!(provider.calls().len(), 2);
        assert_eq!(seen.lock().unwrap().len(), 1);
        assert_eq!(
            task_values(&seen.lock().unwrap().first().unwrap().tasks),
            tasks("Follow up")
        );
        say(&kit, "r", "Continue.").await;
        let current = input(provider.calls().last().unwrap());
        assert!(current.contains("Follow up") && current.contains("## Current Task Plan"));
        kit.close().await.unwrap();
    }
}

// The existing publisher proxy, not the backend completion, is the boundary.
#[tokio::test]
async fn a_plan_hook_runs_before_a_retained_physical_publication_finishes() {
    for streaming in [false, true] {
        for fail in [false, true] {
            publication_boundary(streaming, fail).await;
        }
    }
}

#[expect(
    clippy::panic,
    reason = "test helper rejects a malformed publication trace"
)]
async fn publication_boundary(streaming: bool, fail: bool) {
    let backend = Arc::new(Publication {
        fail,
        ..Publication::default()
    });
    let provider = model(
        vec![
            plan(tasks("Follow up")),
            AiResponse::new("Done."),
            AiResponse::new("Done."),
        ],
        streaming,
    );
    let kit = setup(
        provider.clone(),
        &["r"],
        RoomKitOptions {
            realtime: Some(Arc::<Publication>::clone(&backend)),
            ..Default::default()
        },
    )
    .await;
    let heard = Arc::new(Notify::new());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let keeping = Arc::clone(&seen);
    let hearing = Arc::clone(&heard);
    kit.hook(
        HookTrigger::OnPlanUpdated,
        move |event: PlanUpdatedEvent, context: RoomContext| {
            assert_eq!(event.room_id, context.room.id.as_str());
            keeping.lock().unwrap().push(event);
            hearing.notify_one();
            async {}
        },
        options(),
    )
    .unwrap();
    let running = kit.clone();
    let turn = tokio::spawn(async move {
        say(&running, "r", "Plan.").await;
    });
    tokio::time::timeout(BOUND, backend.entered.notified())
        .await
        .unwrap();
    tokio::time::timeout(BOUND, heard.notified()).await.unwrap();
    let observed = seen.lock().unwrap().clone();
    assert_eq!(observed.len(), 1);
    assert_eq!(
        task_values(&observed.first().unwrap().tasks),
        tasks("Follow up")
    );
    assert!(!backend.failed.load(Ordering::SeqCst));
    let published = backend.plans.lock().unwrap().clone();
    let [event] = published.as_slice() else {
        panic!("one publication required")
    };
    assert_eq!(event.room_id.as_str(), "r");
    assert_eq!(event.channel_id.as_ref().unwrap().as_str(), "agent");
    assert_eq!(event.user_id, "agent");
    assert_eq!(
        event.data,
        serde_json::from_value(json!({"type":"plan_updated", "tasks":tasks("Follow up")})).unwrap()
    );
    backend.release.notify_one();
    tokio::time::timeout(BOUND, turn).await.unwrap().unwrap();
    assert_eq!(provider.calls().len(), 2);
    tokio::time::timeout(BOUND, roomkit::testing::idle(&kit))
        .await
        .unwrap();
    assert_eq!(backend.failed.load(Ordering::SeqCst), fail);
    say(&kit, "r", "Continue.").await;
    assert_eq!(provider.calls().len(), 3);
    let current = input(provider.calls().last().unwrap());
    assert!(current.contains("## Current Task Plan"));
    assert!(current.contains("- [ ] “Follow up” (pending)"));
    assert_eq!(backend.plans.lock().unwrap().len(), 1);
    kit.close().await.unwrap();
    assert_eq!(seen.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn retained_observers_do_not_block_generation_but_close_and_idle_join_them() {
    for streaming in [false, true] {
        for close in [false, true] {
            observer_boundary(streaming, close).await;
        }
    }
}

async fn observer_boundary(streaming: bool, close: bool) {
    let provider = model(
        vec![plan(tasks("Follow up")), AiResponse::new("Done.")],
        streaming,
    );
    let kit = setup(provider.clone(), &["r"], RoomKitOptions::default()).await;
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let finished = Arc::new(AtomicBool::new(false));
    let (entering, releasing, finishing) = (
        Arc::clone(&entered),
        Arc::clone(&release),
        Arc::clone(&finished),
    );
    kit.hook(
        HookTrigger::OnPlanUpdated,
        move |_: PlanUpdatedEvent, _: RoomContext| {
            let (entered, release, finished) = (
                Arc::clone(&entering),
                Arc::clone(&releasing),
                Arc::clone(&finishing),
            );
            async move {
                entered.notify_one();
                release.notified().await;
                finished.store(true, Ordering::SeqCst);
            }
        },
        options(),
    )
    .unwrap();
    say(&kit, "r", "Plan.").await;
    tokio::time::timeout(BOUND, entered.notified())
        .await
        .unwrap();
    assert_eq!(provider.calls().len(), 2);
    let joining = kit.clone();
    let mut waiter = tokio::spawn(async move {
        if close {
            joining.close().await.unwrap();
        } else {
            roomkit::testing::idle(&joining).await;
        }
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut waiter)
            .await
            .is_err()
    );
    assert!(!finished.load(Ordering::SeqCst));
    release.notify_one();
    tokio::time::timeout(BOUND, waiter).await.unwrap().unwrap();
    assert!(finished.load(Ordering::SeqCst));
    kit.close().await.unwrap();
}

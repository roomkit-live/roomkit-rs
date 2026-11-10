//! The parent's deadline still owns an adopted call after its caller
//! leaves, whether the spawning hook body has returned yet or not.

use super::*;
use pretty_assertions::assert_eq;

pub(super) async fn nested_call_keeps_deadline(body_returned: bool, cancels: usize) {
    let (kit, _) = room_on(kit_with_environment(
        RoomKitOptions {
            process_timeout: Duration::from_millis(50),
            ..Default::default()
        },
        TokioClock::new(),
    ))
    .await;
    let tap = EventTap::on(&kit, &["process_timeout", "hook_timeout"]);
    let (entered, hook_entered, returned) = (
        Arc::new(Notify::new()),
        Arc::new(Notify::new()),
        Arc::new(Notify::new()),
    );
    let started: Started = Arc::default();
    hold_child(&kit, Arc::clone(&entered), Arc::clone(&hook_entered));
    start_child(
        &kit,
        Arc::clone(&started),
        hook_entered,
        Arc::clone(&returned),
        body_returned,
    );
    let caller_kit = kit.clone();
    let began = tokio::time::Instant::now();
    let caller = tokio::spawn(async move { send(&caller_kit, "hello").await });
    tokio::time::timeout(BOUND, entered.notified())
        .await
        .unwrap();
    if body_returned {
        tokio::time::timeout(BOUND, returned.notified())
            .await
            .unwrap();
    }
    for _ in 0..cancels {
        caller.abort();
        tokio::task::yield_now().await;
    }
    assert!(caller.await.unwrap_err().is_cancelled());
    let task = started.lock().unwrap().pop().unwrap();
    let sent = tokio::time::timeout(BOUND, task).await.unwrap().unwrap();
    assert!((Duration::from_millis(50)..Duration::from_millis(70)).contains(&began.elapsed()));
    assert_eq!(
        sent.err(),
        Some(RoomKitError::ProcessTimeout { room_id: room_id() })
    );
    assert_eq!(message_texts(&kit).await, Vec::<String>::new());
    let other = send(&kit, "other").await.unwrap();
    assert!(!other.blocked);
    assert_eq!(message_texts(&kit).await, ["other"]);
    assert!(
        tap.events().is_empty(),
        "body_returned={body_returned}, cancels={cancels}"
    );
    kit.close().await.unwrap();
}

fn hold_child(kit: &RoomKit, entered: Arc<Notify>, hook_entered: Arc<Notify>) {
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |event: RoomEvent, _| {
            let (entered, hook_entered) = (Arc::clone(&entered), Arc::clone(&hook_entered));
            async move {
                if text_of(&event.content) == "Heads up" {
                    entered.notify_one();
                    hook_entered.notify_one();
                    std::future::pending::<()>().await;
                }
                Ok(HookResult::allow(AllowOptions::default()))
            }
        },
        before_broadcast("slow", true),
    )
    .unwrap();
}

fn start_child(
    kit: &RoomKit,
    started: Started,
    entered: Arc<Notify>,
    returned: Arc<Notify>,
    body_returned: bool,
) {
    let weak = kit.downgrade();
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |event: RoomEvent, _| {
            let (kit, tasks, entered, returned) = (
                weak.upgrade().unwrap(),
                Arc::clone(&started),
                Arc::clone(&entered),
                Arc::clone(&returned),
            );
            async move {
                if text_of(&event.content) == "hello" {
                    let _returned = NotifyOnDrop(returned);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    let task = tokio::spawn(keep_context(async move {
                        kit.send_event(
                            &room_id(),
                            &channel("ws"),
                            TextContent::new("Heads up"),
                            SendEventOptions::default(),
                        )
                        .await
                    }));
                    tasks.lock().unwrap().push(task);
                    entered.notified().await;
                    if !body_returned {
                        std::future::pending::<()>().await;
                    }
                }
                Ok(HookResult::allow(AllowOptions::default()))
            }
        },
        before_broadcast("notifier", true),
    )
    .unwrap();
}

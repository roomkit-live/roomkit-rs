//! Awaiting a task with cancellation-on-drop matches asyncio.Task; dropping
//! a bare Tokio `JoinHandle` detaches it, as asyncio.shield does.

use super::*;
use pretty_assertions::assert_eq;

struct AbortOnDrop(tokio::task::AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

// Python: tests/test_reentrance.py::TestACallFromATaskALockedHookStarted::test_a_later_hook_awaiting_it_waits_until_its_timeout
#[tokio::test]
async fn a_later_hook_awaiting_a_guarded_task_cancels_it_at_its_timeout() {
    let (kit, _) = room().await;
    let left: Arc<Mutex<Option<tokio::task::JoinHandle<RoomEvent>>>> = Arc::default();
    let started = Arc::new(Notify::new());
    let ended = Arc::new(Notify::new());
    let completed = Arc::new(AtomicBool::new(false));
    let tap = EventTap::on(&kit, &["hook_timeout"]);
    let (weak, put, begun, done, finished) = (
        kit.downgrade(),
        Arc::clone(&left),
        Arc::clone(&started),
        Arc::clone(&ended),
        Arc::clone(&completed),
    );
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |event: RoomEvent, _| {
            let (kit, put, begun, done, finished) = (
                weak.upgrade().unwrap(),
                Arc::clone(&put),
                Arc::clone(&begun),
                Arc::clone(&done),
                Arc::clone(&finished),
            );
            async move {
                if text_of(&event.content) == "hello" {
                    let task = tokio::spawn(keep_context(async move {
                        let _ended = NotifyOnDrop(done);
                        begun.notify_one();
                        let event = kit
                            .send_event(
                                &room_id(),
                                &channel("ws"),
                                TextContent::new("Heads up"),
                                SendEventOptions::default(),
                            )
                            .await
                            .unwrap();
                        finished.store(true, AtomicOrdering::SeqCst);
                        event
                    }));
                    *put.lock().unwrap() = Some(task);
                }
                Ok(HookResult::allow(AllowOptions::default()))
            }
        },
        before_broadcast("notifier", true),
    )
    .unwrap();
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |event: RoomEvent, _| {
            let (take, begun) = (Arc::clone(&left), Arc::clone(&started));
            async move {
                if text_of(&event.content) == "hello" {
                    begun.notified().await;
                    let task = take.lock().unwrap().take().unwrap();
                    let _cancel = AbortOnDrop(task.abort_handle());
                    let _result = task.await;
                }
                Ok(HookResult::allow(AllowOptions::default()))
            }
        },
        HookOptions {
            priority: 10,
            timeout: Duration::from_millis(50),
            ..before_broadcast("waiter", true)
        },
    )
    .unwrap();
    let result = send(&kit, "hello").await.unwrap();
    tokio::time::timeout(BOUND, ended.notified()).await.unwrap();
    tokio::time::timeout(BOUND, roomkit::testing::idle(&kit))
        .await
        .unwrap();
    assert!(!result.blocked);
    assert!(!completed.load(AtomicOrdering::SeqCst));
    assert_eq!(message_texts(&kit).await, ["hello"]);
    assert_eq!(timeout_names(&tap), ["waiter"]);
    kit.close().await.unwrap();
}

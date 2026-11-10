//! Each observer body owns its lifetime, including when another body of
//! the same event keeps working (RFC §9.5.1).

use super::*;
use pretty_assertions::assert_eq;
use tokio::sync::{Notify, oneshot};

const BOUND: Duration = Duration::from_secs(5);

// Python: tests/test_reentrance.py::TestACallFromAnAsyncHook::test_a_task_the_hook_starts_calls_as_work_of_its_own
pub(super) async fn after_its_body(trigger: HookTrigger, ends: Ends) {
    let kit = room(1).await;
    let tap = roomkit::testing::EventTap::on(&kit, &["hook_timeout"]);
    let over = Arc::new(Notify::new());
    let task = Arc::new(Mutex::new(None));
    let (weak, later, put) = (kit.downgrade(), Arc::clone(&over), Arc::clone(&task));
    kit.hook(
        trigger,
        move |event: RoomEvent, _| {
            let (kit, over, put) = (
                weak.upgrade().unwrap(),
                Arc::clone(&later),
                Arc::clone(&put),
            );
            async move {
                if text_of(&event.content) == "hi" {
                    let task = tokio::spawn(keep_context(async move {
                        over.notified().await;
                        let mut sent = Vec::new();
                        for n in 0..=2 * BUDGET {
                            let event = kit
                                .send_event(
                                    &room_id(),
                                    &channel("sms"),
                                    TextContent::new(format!("later {n}")),
                                    SendEventOptions::default(),
                                )
                                .await
                                .unwrap();
                            sent.push((event.chain_depth, event.blocked_by));
                        }
                        sent
                    }));
                    *put.lock().unwrap() = Some(task);
                    match ends {
                        Ends::Returns => {}
                        Ends::Raises => return Err("the hook's body fails".into()),
                        Ends::TimesOut => std::future::pending::<()>().await,
                    }
                }
                Ok(HookResult::allow(AllowOptions::default()))
            }
        },
        HookOptions {
            name: "start".to_owned(),
            execution: HookExecution::Async,
            timeout: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .unwrap();
    kit.send_event(
        &room_id(),
        &channel("sms"),
        TextContent::new("hi"),
        SendEventOptions::default(),
    )
    .await
    .unwrap();
    // The test, outside the kit's work, joins the bodies before releasing
    // their task. A timeout must have been attributed to this exact hook.
    tokio::time::timeout(BOUND, roomkit::testing::idle(&kit))
        .await
        .unwrap();
    let names: Vec<_> = tap
        .events()
        .iter()
        .map(|event| {
            event
                .data
                .get("hook_name")
                .unwrap()
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect();
    assert_eq!(
        names,
        if matches!(ends, Ends::TimesOut) {
            vec!["start".to_owned()]
        } else {
            vec![]
        }
    );
    over.notify_one();
    let task = task.lock().unwrap().take().unwrap();
    let sent = tokio::time::timeout(BOUND, task).await.unwrap().unwrap();
    assert_eq!(
        sent,
        vec![(0, None); 2 * BUDGET + 1],
        "{trigger:?}, {ends:?}"
    );
    kit.close().await.unwrap();
}

// Python: tests/test_reentrance.py::TestACallFromAnAsyncHook::test_a_task_one_observer_of_the_model_s_words_starts_calls_as_work_of_its_own
// Generic body lifetime here; ON_TRANSCRIPTION's voice adapter is separate.
#[tokio::test]
async fn a_task_one_observer_left_calls_at_its_own_depth_while_a_sibling_runs() {
    let kit = room(10).await;
    let (answer, received) = oneshot::channel();
    let answer = Arc::new(Mutex::new(Some(answer)));
    let (auditing, sent) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let (weak, begun, done) = (kit.downgrade(), Arc::clone(&auditing), Arc::clone(&sent));
    kit.hook(
        HookTrigger::AfterBroadcast,
        move |event: RoomEvent, _| {
            let (kit, answer, begun, done) = (
                weak.upgrade().unwrap(),
                Arc::clone(&answer),
                Arc::clone(&begun),
                Arc::clone(&done),
            );
            async move {
                if text_of(&event.content) == "hi" {
                    tokio::spawn(keep_context(async move {
                        begun.notified().await;
                        let result = kit
                            .send_event(
                                &room_id(),
                                &channel("sms"),
                                TextContent::new("later"),
                                SendEventOptions::default(),
                            )
                            .await
                            .unwrap();
                        answer.lock().unwrap().take().unwrap().send(result).unwrap();
                        done.notify_one();
                    }));
                }
            }
        },
        HookOptions {
            name: "starts".to_owned(),
            execution: HookExecution::Async,
            ..Default::default()
        },
    )
    .unwrap();
    kit.hook(
        HookTrigger::AfterBroadcast,
        move |event: RoomEvent, _| {
            let (begun, done) = (Arc::clone(&auditing), Arc::clone(&sent));
            async move {
                if text_of(&event.content) == "hi" {
                    begun.notify_one();
                    done.notified().await;
                }
            }
        },
        HookOptions {
            name: "slow".to_owned(),
            execution: HookExecution::Async,
            ..Default::default()
        },
    )
    .unwrap();
    kit.send_event(
        &room_id(),
        &channel("sms"),
        TextContent::new("hi"),
        SendEventOptions {
            chain_depth: 3,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let posted = tokio::time::timeout(BOUND, received)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((posted.chain_depth, posted.blocked_by), (0, None));
    tokio::time::timeout(BOUND, roomkit::testing::idle(&kit))
        .await
        .unwrap();
    kit.close().await.unwrap();
}

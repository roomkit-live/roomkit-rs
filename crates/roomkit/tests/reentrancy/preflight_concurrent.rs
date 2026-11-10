//! The last acquired unit belongs to one call, even across runtime workers.

use super::preflight::{AddressCalls, install_work};
use super::*;
use pretty_assertions::assert_eq;
use roomkit::InboundResult;
use tokio::sync::Barrier;

type Answers = Arc<Mutex<Vec<InboundResult>>>;

async fn spend(kit: &RoomKit, saved: &Answers) {
    for _ in 0..9 {
        let answer = kit
            .process_inbound(message("again"), in_room(room_id()))
            .await
            .unwrap();
        assert!(!answer.blocked);
    }
    let barrier = Arc::new(Barrier::new(2));
    let tasks: Vec<_> = ["one", "two"]
        .into_iter()
        .map(|target| {
            let kit = kit.clone();
            let barrier = Arc::clone(&barrier);
            tokio::spawn(keep_context(async move {
                barrier.wait().await;
                kit.process_inbound(
                    InboundMessage::new(channel(target), "RAW", TextContent::new("last")),
                    in_room(room_id()),
                )
                .await
                .unwrap()
            }))
        })
        .collect();
    let mut results = Vec::new();
    for task in tasks {
        results.push(task.await.unwrap());
    }
    *saved.lock().unwrap() = results;
}

async fn check(work: &str) {
    let (kit, _) = room_with(RoomKitOptions {
        max_chain_depth: 1,
        ..Default::default()
    })
    .await;
    let calls = [
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(Mutex::new(Vec::new())),
    ];
    for (target, calls) in ["one", "two"].into_iter().zip(&calls) {
        kit.register_channel(AddressCalls(target, Arc::clone(calls)))
            .unwrap();
        kit.attach_channel(
            &room_id(),
            &channel(target),
            AttachChannelOptions::default(),
        )
        .await
        .unwrap();
    }
    let before: Vec<_> = calls
        .iter()
        .map(|calls| calls.lock().unwrap().clone())
        .collect();
    let saved = Arc::new(Mutex::new(Vec::new()));
    let answers = Arc::clone(&saved);
    let weak = kit.downgrade();
    install_work(
        &kit,
        work,
        Arc::new(move |event| {
            let kit = weak.upgrade().unwrap();
            let answers = Arc::clone(&answers);
            Box::pin(async move {
                if text_of(&event.content) == "hello" {
                    spend(&kit, &answers).await;
                }
                Ok(ChannelOutput::empty())
            })
        }),
    );
    if work == "delivery" {
        kit.attach_channel(
            &room_id(),
            &channel("spender"),
            AttachChannelOptions::default(),
        )
        .await
        .unwrap();
    }
    send(&kit, "hello").await.unwrap();
    tokio::time::timeout(BOUND, roomkit::testing::idle(&kit))
        .await
        .unwrap();
    let answers = std::mem::take(&mut *saved.lock().unwrap());
    assert_eq!(answers.len(), 2);
    assert_eq!(answers.iter().filter(|answer| answer.blocked).count(), 1);
    for ((answer, calls), before) in answers.iter().zip(&calls).zip(&before) {
        if answer.blocked {
            assert_eq!(answer.reason.as_deref(), Some("reentry_loop_cap"));
            assert!(answer.event.is_none());
            assert_eq!(&*calls.lock().unwrap(), before);
        } else {
            assert!(answer.event.is_some());
            assert_ne!(&*calls.lock().unwrap(), before);
        }
    }
    let texts = message_texts(&kit).await;
    assert_eq!(texts.iter().filter(|text| *text == "again").count(), 9);
    assert_eq!(texts.iter().filter(|text| *text == "last").count(), 1);
    assert_eq!(texts.len(), 11);
    kit.close().await.unwrap();
}

// Python: tests/test_reentrance_preflight.py::test_two_named_calls_share_the_last_unit_before_address_callbacks
async fn last_unit() {
    for work in ["delivery", "observer"] {
        check(work).await;
    }
}

#[tokio::test]
async fn two_named_calls_share_the_last_unit_before_callbacks() {
    last_unit().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_named_calls_share_the_last_unit_before_callbacks_on_threads() {
    last_unit().await;
}

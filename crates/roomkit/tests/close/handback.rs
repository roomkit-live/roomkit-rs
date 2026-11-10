//! A completed task survives the close cutting its recipient's answer.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{EventType, TaskStatus};
use roomkit::tasks::{OnCompleteCallback, WaitOptions};
use roomkit::{AttachChannelOptions, ConversationStore, DelegateOptions, InMemoryStore};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiChannel, AiChannelOptions, Tools};
use roomkit_core::rules::transcoding::text_of;
use serde_json::json;

use crate::delegation::worker;
use crate::held::{kit_of, room};
use crate::readers::{Pace, Receiving, first_received};
use crate::stream_witness::WaitingModel;
use crate::support::channel;
use crate::warnings::Warnings;

async fn handback_room() -> (roomkit::RoomKit, WaitingModel, Receiving) {
    let kit = kit_of(InMemoryStore::default(), &[], &["r"]).await;
    let model = WaitingModel::new("Thanks, ", false);
    kit.register_channel(
        AiChannel::new(channel("boss"), model.clone(), AiChannelOptions::default()).unwrap(),
    )
    .unwrap();
    kit.attach_channel(
        &room("r"),
        &channel("boss"),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    worker(
        &kit,
        ScriptedProvider::new([ScriptedRound::answering("APPROVED")]),
        Tools::new(),
    );
    let target = Receiving::new(Pace::Now);
    kit.register_channel(target.clone()).unwrap();
    kit.attach_channel(&room("r"), &channel("ws"), AttachChannelOptions::default())
        .await
        .unwrap();
    (kit, model, target)
}

// Python: tests/test_close_during_handback.py::test_the_delegation_still_ends_when_its_hand_back_is_cut
// Python: tests/test_close_during_handback.py::test_close_cuts_a_hand_back_under_way (task-runner door)
#[tokio::test(start_paused = true)]
async fn a_cut_handback_keeps_the_workers_result_and_the_recipients_prefix() {
    let (kit, model, target) = handback_room().await;
    let completed = Arc::new(Mutex::new(Vec::new()));
    let callback: OnCompleteCallback = {
        let completed = Arc::clone(&completed);
        Arc::new(move |result| {
            let completed = Arc::clone(&completed);
            Box::pin(async move {
                completed.lock().unwrap().push(result.status);
            })
        })
    };
    let (logs, _capture) = Warnings::capture_at(tracing::Level::INFO);
    let task = kit
        .delegate(
            &room("r"),
            &channel("worker"),
            "Find it.",
            DelegateOptions {
                notify: Some(channel("boss")),
                on_complete: Some(callback),
                ..DelegateOptions::default()
            },
        )
        .await
        .unwrap();
    first_received(&target).await;
    assert_eq!(target.received(), "Thanks, ");
    tokio::time::timeout(Duration::from_secs(1), kit.close())
        .await
        .unwrap()
        .unwrap();
    let result = task
        .wait(WaitOptions {
            timeout: Some(Duration::from_secs(1)),
        })
        .await
        .unwrap();
    assert_eq!(
        (result.status, result.output.as_deref()),
        (TaskStatus::Completed, Some("APPROVED"))
    );
    assert_eq!(*completed.lock().unwrap(), [TaskStatus::Completed]);
    assert!(model.dropped.load(Ordering::SeqCst));
    assert_prefix(&kit).await;
    assert_eq!(logs.containing("WARN"), Vec::<String>::new());
    assert_eq!(logs.containing("ERROR"), Vec::<String>::new());
    let ended = logs.containing("not handed back: the framework closed");
    assert_eq!(ended.len(), 1, "{:?}", logs.containing(""));
    assert!(ended[0].starts_with("INFO "));
}

async fn assert_prefix(kit: &roomkit::RoomKit) {
    let events = kit
        .store()
        .list_events(&room("r"), roomkit::store::ListEventsOptions::default())
        .await
        .unwrap();
    let answers: Vec<_> = events
        .iter()
        .filter(|event| {
            event.r#type == EventType::Message && event.source.channel_id == channel("boss")
        })
        .map(|event| {
            (
                text_of(&event.content),
                event.metadata.get("cancelled") == Some(&json!(true)),
            )
        })
        .collect();
    assert_eq!(answers, [("Thanks, ".to_owned(), true)]);
}

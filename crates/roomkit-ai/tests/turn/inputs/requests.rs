//! Owned snapshots of open requests, read through the public kit (RFC §10.6.5).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit::human_input::{HumanInputHandler, PendingInputEvent, RejectOptions};
use roomkit::model::hook::AllowOptions;
use roomkit::model::{HookResult, RoomContext};
use roomkit::{HookOptions, HookTrigger, RoomKit, RoomKitOptions};
use roomkit_ai::{AiChannelOptions, Tools};
use serde_json::json;
use tokio::sync::oneshot;

use super::{calling, human};
use crate::support::Room;

async fn asking() -> (
    Room,
    HumanInputHandler,
    oneshot::Receiver<PendingInputEvent>,
) {
    let human = human(Duration::from_secs(2));
    let handler = human.handler().clone();
    let room = Room::equipped(calling("ask_user"), Tools::new(), |options| {
        AiChannelOptions {
            human_input_handler: Some(human),
            ..options
        }
    })
    .await;
    let (send, receive) = oneshot::channel();
    let send = Arc::new(Mutex::new(Some(send)));
    room.kit
        .hook(
            HookTrigger::OnUserInputRequired,
            move |event: PendingInputEvent, _context: RoomContext| {
                drop(send.lock().unwrap().take().unwrap().send(event));
                async { Ok(HookResult::allow(AllowOptions::default())) }
            },
            HookOptions::default(),
        )
        .unwrap();
    (room, handler, receive)
}

async fn held(kit: &RoomKit, id: &str) -> PendingInputEvent {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(request) = kit.input_request(id, Default::default()) {
                return request;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap()
}

// Python: tests/test_turns_inputs.py::test_the_owner_reads_an_open_request_as_its_hook_did
// and test_an_open_request_is_read_within_its_organization_only.
#[tokio::test]
async fn the_owner_reads_an_independent_snapshot_with_the_original_creation_time() {
    let (room, handler, notified) = asking().await;
    let kit = room.kit.clone();
    let turn = tokio::spawn(async move { Room::says_on(&kit, "Hi").await });
    let mut expected = notified.await.unwrap();
    let id = expected.pending_id.clone();
    let mut read = held(&room.kit, &id).await;
    expected.timestamp = handler.pending().get(&id).unwrap().created_at;
    assert_eq!(read, expected);

    read.arguments
        .insert("question".to_owned(), json!("Changed by the reader"));
    assert_eq!(
        room.kit.input_request(&id, Default::default()),
        Some(expected)
    );
    let other = RoomKit::new(RoomKitOptions::default());
    assert!(other.input_request(&id, Default::default()).is_none());
    other.close().await.unwrap();

    assert!(room.kit.answer_input(&id, "Blue").await.unwrap());
    assert!(room.kit.input_request(&id, Default::default()).is_none());
    turn.await.unwrap();
    room.kit.close().await.unwrap();
}

// Python: test_a_request_settled_by_its_handler_is_no_longer_read and
// test_the_channels_handler_answers_where_the_tool_waits_first_come_first_served.
#[tokio::test]
async fn a_request_the_handler_settles_is_no_longer_read() {
    for outcome in ["resolve", "reject", "release"] {
        let (room, handler, notified) = asking().await;
        let kit = room.kit.clone();
        let turn = tokio::spawn(async move { Room::says_on(&kit, "Hi").await });
        let event = notified.await.unwrap();
        held(&room.kit, &event.pending_id).await;
        let taken = match outcome {
            "resolve" => handler.resolve(&event.pending_id, "Green"),
            "reject" => handler.reject(
                &event.pending_id,
                RejectOptions {
                    reason: "Not now".to_owned(),
                },
            ),
            _ => handler.release(&event.pending_id),
        };
        assert!(taken);
        turn.await.unwrap();
        assert!(
            room.kit
                .input_request(&event.pending_id, Default::default())
                .is_none()
        );
        room.kit.close().await.unwrap();
    }
}

//! A turn's host as a tool reaches it (RFC §10.6.5, §21.4): Python's
//! `TurnHost.send_event(room_id, channel_id, content, **options)`, `open`
//! and `reachable`.

use std::sync::{Arc, Mutex};

use roomkit::model::{RoomId, TextContent};
use roomkit::{
    CreateRoomOptions, GetTimelineOptions, RoomKitError, SendEventOptions, TurnHost,
    current_turn_host,
};
use roomkit_ai::ToolResult;

use crate::support::{asks, channel, kit};

/// RFC §10.6.5 (MUST): the host of a turn writes to the turn's room only;
/// a send naming another room is refused by the owner with Python's
/// `ValueError`, nothing stored there.
// Python: tests/test_turns_host.py::test_a_host_acts_only_for_its_turns_room_and_organization[send_event]
#[tokio::test(start_paused = true)]
async fn a_turn_s_host_refuses_another_room() {
    let read: Arc<Mutex<Option<String>>> = Arc::default();
    let noted = Arc::clone(&read);
    let kit = kit(move |_call, _context| {
        let noted = Arc::clone(&noted);
        async move {
            let turn = current_turn_host().unwrap();
            let other = RoomId::new("r2").unwrap();
            let sent = turn
                .send_event(
                    &other,
                    &channel("ai"),
                    TextContent::new("Elsewhere."),
                    SendEventOptions::default(),
                )
                .await;
            *noted.lock().unwrap() = Some(match sent {
                Err(RoomKitError::Value(_)) => "ValueError".to_owned(),
                other => format!("{other:?}"),
            });
            Ok(ToolResult::Text("42".to_owned()))
        }
    })
    .await;
    kit.create_room(CreateRoomOptions {
        room_id: Some(RoomId::new("r2").unwrap()),
        ..Default::default()
    })
    .await
    .unwrap();

    asks(&kit, "What is it?").await;

    let elsewhere = kit
        .get_timeline(&RoomId::new("r2").unwrap(), GetTimelineOptions::default())
        .await
        .unwrap();
    assert_eq!(
        (read.lock().unwrap().clone(), elsewhere.len()),
        (Some("ValueError".to_owned()), 0)
    );
}

/// Python's `open` and `reachable`: during the turn both; after it the turn
/// is no longer open, and its owner is still reached while the kit lives.
#[tokio::test(start_paused = true)]
async fn a_turn_s_host_is_open_during_the_turn_and_reachable_after_it() {
    let kept: Arc<Mutex<Option<(TurnHost, bool, bool)>>> = Arc::default();
    let keeps = Arc::clone(&kept);
    let kit = kit(move |_call, _context| {
        let keeps = Arc::clone(&keeps);
        async move {
            let turn = current_turn_host().unwrap();
            let (open, reachable) = (turn.open(), turn.reachable());
            *keeps.lock().unwrap() = Some((turn, open, reachable));
            Ok(ToolResult::Text("42".to_owned()))
        }
    })
    .await;

    asks(&kit, "What is it?").await;
    roomkit::testing::idle(&kit).await;

    let (turn, open, reachable) = kept.lock().unwrap().take().unwrap();
    assert_eq!(
        (open, reachable, turn.open(), turn.reachable()),
        (true, true, false, true)
    );
}

/// Python's `current_turn_host()` (turns/host.py): `None` for a host that
/// can no longer reach its owner (RMK-1094). Work a turn began reads the
/// host while the kit lives, and none once the kit is gone.
#[tokio::test(start_paused = true)]
async fn work_a_turn_began_reads_no_host_once_the_kit_is_gone() {
    let (go, went) = tokio::sync::oneshot::channel::<()>();
    let went = Arc::new(Mutex::new(Some(went)));
    let (read, reads) = tokio::sync::oneshot::channel::<(bool, bool)>();
    let read = Arc::new(Mutex::new(Some(read)));
    let kit = kit(move |_call, _context| {
        let went = went.lock().unwrap().take();
        let read = read.lock().unwrap().take();
        async move {
            if let (Some(went), Some(read)) = (went, read) {
                tokio::spawn(roomkit::keep_context(async move {
                    let before = current_turn_host().is_some();
                    let _ = went.await;
                    let _ = read.send((before, current_turn_host().is_some()));
                }));
            }
            Ok(ToolResult::Text("42".to_owned()))
        }
    })
    .await;

    asks(&kit, "What is it?").await;
    roomkit::testing::idle(&kit).await;
    kit.close().await.unwrap();
    drop(kit);
    go.send(()).unwrap();

    assert_eq!(reads.await.unwrap(), (true, false));
}

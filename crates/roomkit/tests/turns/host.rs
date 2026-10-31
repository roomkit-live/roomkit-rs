//! A turn's host as a tool reaches it (RFC §10.6.5, §21.4): Python's
//! `TurnHost.send_event(room_id, channel_id, content, **options)`, `open`
//! and `reachable`.

use std::sync::{Arc, Mutex};

use roomkit::model::{RoomId, TextContent};
use roomkit::{CreateRoomOptions, RoomKitError, SendEventOptions, TurnHost, current_turn_host};
use roomkit_ai::ToolOutput;

use crate::support::{asks, channel, kit};

/// RFC §10.6.5 (MUST): the host of a turn writes to the turn's room only;
/// a send naming another room is refused by the owner, nothing stored there.
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
                Err(RoomKitError::RemoteTurn { kind, .. }) => kind,
                other => format!("{other:?}"),
            });
            Ok(ToolOutput::Text("42".to_owned()))
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
        .get_timeline(&RoomId::new("r2").unwrap(), Default::default())
        .await
        .unwrap();
    assert_eq!(
        (read.lock().unwrap().clone(), elsewhere.len()),
        (Some("PermissionError".to_owned()), 0)
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
            Ok(ToolOutput::Text("42".to_owned()))
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

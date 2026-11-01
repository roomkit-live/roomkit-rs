//! A turn's host as a tool reaches it (RFC §10.6.5, §21.4): Python's
//! `TurnHost.send_event(room_id, channel_id, content, **options)`, `open`
//! and `reachable`.

use std::sync::{Arc, Mutex};

use roomkit::model::{InboundMessage, Metadata, RoomId, TextContent};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, GetRoomOptions, GetTimelineOptions,
    ProcessInboundOptions, RoomKitError, SendEventOptions, TurnHost, UpdateRoomMetadataOptions,
    current_turn_host,
};
use roomkit_ai::ToolResult;

use crate::support::{asks, channel, kit, room};

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

/// What a verb after its turn came to: `ok`, Python's `ValueError`, or the
/// error.
fn verdict<T>(outcome: Result<T, RoomKitError>) -> String {
    match outcome {
        Ok(_) => "ok".to_owned(),
        Err(RoomKitError::Value(_)) => "ValueError".to_owned(),
        Err(error) => format!("{error:?}"),
    }
}

/// The verbs work a turn began makes once `go` lets it, after the turn:
/// in the turn's room `r2` of organization `acme`, named or not, then for
/// another organization and another room.
async fn after_the_turn(turn: TurnHost) -> Vec<String> {
    let (here, elsewhere) = (RoomId::new("r2").unwrap(), RoomId::new("r1").unwrap());
    let acme = || UpdateRoomMetadataOptions {
        organization_id: Some("acme".to_owned()),
    };
    let globex = || UpdateRoomMetadataOptions {
        organization_id: Some("globex".to_owned()),
    };
    let later = |organization: &str| SendEventOptions {
        organization_id: Some(organization.to_owned()),
        ..SendEventOptions::default()
    };
    let mut metadata = Metadata::new();
    metadata.insert("later".to_owned(), true.into());
    vec![
        format!("open: {}", turn.open()),
        verdict(
            turn.update_room_metadata(&here, metadata.clone(), acme())
                .await,
        ),
        verdict(
            turn.send_event(
                &here,
                &channel("ai"),
                TextContent::new("Later."),
                later("acme"),
            )
            .await,
        ),
        verdict(
            turn.update_room_metadata(&here, metadata.clone(), globex())
                .await,
        ),
        verdict(
            turn.send_event(
                &here,
                &channel("ai"),
                TextContent::new("No."),
                later("globex"),
            )
            .await,
        ),
        verdict(
            turn.send_event(
                &elsewhere,
                &channel("sms"),
                TextContent::new("No."),
                SendEventOptions::default(),
            )
            .await,
        ),
        verdict(
            turn.update_room_metadata(&elsewhere, metadata, acme())
                .await,
        ),
    ]
}

/// RFC §10.6.5, §17.2: once its turn ended, work the turn began writes its
/// room in the room's own organization, named or not; a verb for another
/// organization or another room is refused with Python's `ValueError`,
/// nothing written there.
// Python: tests/test_turns_between.py::test_the_write_verbs_between_turns_keep_their_refusals
#[tokio::test(start_paused = true)]
async fn work_a_turn_began_writes_its_room_in_its_organization_after_the_turn() {
    let (go, went) = tokio::sync::oneshot::channel::<()>();
    let went = Arc::new(Mutex::new(Some(went)));
    let (done, verdicts) = tokio::sync::oneshot::channel::<Vec<String>>();
    let done = Arc::new(Mutex::new(Some(done)));
    let kit = kit(move |_call, _context| {
        let went = went.lock().unwrap().take();
        let done = done.lock().unwrap().take();
        async move {
            if let (Some(went), Some(done)) = (went, done) {
                let turn = current_turn_host().unwrap();
                tokio::spawn(roomkit::keep_context(async move {
                    let _ = went.await;
                    drop(done.send(after_the_turn(turn).await));
                }));
            }
            Ok(ToolResult::Text("42".to_owned()))
        }
    })
    .await;
    let here = RoomId::new("r2").unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(here.clone()),
        organization_id: Some("acme".to_owned()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["sms", "ai"] {
        kit.attach_channel(&here, &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }

    let message = InboundMessage::new(channel("sms"), "+15550000001", TextContent::new("Go."));
    let options = ProcessInboundOptions {
        room_id: Some(here.clone()),
        ..Default::default()
    };
    kit.process_inbound(message, options).await.unwrap();
    roomkit::testing::idle(&kit).await;
    go.send(()).unwrap();
    let verdicts = verdicts.await.unwrap();
    roomkit::testing::idle(&kit).await;

    let room = kit
        .get_room(&room(), GetRoomOptions::default())
        .await
        .unwrap();
    let stored = kit
        .get_room(&here, GetRoomOptions::default())
        .await
        .unwrap();
    let later: Vec<String> = kit
        .get_timeline(&here, GetTimelineOptions::default())
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.source.channel_id == channel("ai"))
        .filter_map(|event| {
            let roomkit::model::EventContent::Text(text) = event.content else {
                return None;
            };
            Some(text.body)
        })
        .collect();
    assert_eq!(
        (
            verdicts,
            stored.metadata.get("later").cloned(),
            room.metadata.get("later").cloned(),
            later.contains(&"Later.".to_owned()),
            later.contains(&"No.".to_owned()),
        ),
        (
            vec![
                "open: false".to_owned(),
                "ok".to_owned(),
                "ok".to_owned(),
                "ValueError".to_owned(),
                "ValueError".to_owned(),
                "ValueError".to_owned(),
                "ValueError".to_owned(),
            ],
            Some(serde_json::Value::Bool(true)),
            None,
            true,
            false,
        )
    );
}

/// RFC §10.6.5: after its turn ended, a host sends the verbs that write
/// its room only; one that needs the turn, `delegate`, is refused before
/// it is sent, with Python's `RuntimeError`.
// Python: tests/test_turns_between.py::test_a_verb_that_needs_its_turn_is_refused_after_it
#[tokio::test(start_paused = true)]
async fn a_verb_that_needs_its_turn_is_refused_after_it() {
    let (go, went) = tokio::sync::oneshot::channel::<()>();
    let went = Arc::new(Mutex::new(Some(went)));
    let (done, refused) = tokio::sync::oneshot::channel::<String>();
    let done = Arc::new(Mutex::new(Some(done)));
    let kit = kit(move |_call, _context| {
        let went = went.lock().unwrap().take();
        let done = done.lock().unwrap().take();
        async move {
            if let (Some(went), Some(done)) = (went, done) {
                let turn = current_turn_host().unwrap();
                tokio::spawn(roomkit::keep_context(async move {
                    drop(went.await);
                    let delegated = turn
                        .delegate(
                            &room(),
                            &channel("ai"),
                            "More.",
                            roomkit::DelegateOptions::default(),
                        )
                        .await;
                    drop(done.send(match delegated {
                        Err(RoomKitError::RemoteTurn { kind, message, .. }) => {
                            format!("{kind}: {}", message.contains("needs a turn"))
                        }
                        other => format!("{other:?}"),
                    }));
                }));
            }
            Ok(ToolResult::Text("42".to_owned()))
        }
    })
    .await;

    asks(&kit, "What is it?").await;
    roomkit::testing::idle(&kit).await;
    go.send(()).unwrap();

    assert_eq!(refused.await.unwrap(), "RuntimeError: true");
}

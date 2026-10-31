//! Many rooms at once on a multi-threaded runtime (RFC §10.2, §13.5): four
//! tasks send into eight rooms at the same time, each room answered by an
//! agent. Whatever the interleaving, each room commits every message and
//! every answer once, with consecutive indices, delivers each event, and
//! delivers its events in index order; each sender's messages keep their
//! order.
//!
//! The calls overlap by construction: an off-lock check holds each task's
//! first message into `room_0` until all four are inside it at once, which
//! only calls running at the same time can reach (RFC §9.5.1). Past that
//! first meeting the interleaving is the runtime's; the test proves the
//! invariants hold for the interleavings it met, not for every one.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]
use std::sync::Arc;
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::hook::AllowOptions;
use roomkit::model::{
    ChannelId, ChannelType, EventStatus, EventType, HookResult, InboundMessage, RoomId, TextContent,
};
use roomkit::testing::{
    EchoAgent, EchoAgentOptions, RecordingTransport, RecordingTransportOptions,
};
use roomkit::{
    AttachChannelOptions, ConversationStore, CreateRoomOptions, HookExecution, HookSpec,
    HookTrigger, ProcessInboundOptions, RoomKit, RoomKitOptions, text_of,
};
use tokio::sync::Barrier;

const ROOMS: usize = 8;
const TASKS: usize = 4;
/// The messages each task sends into each room.
const MESSAGES: usize = 5;

/// How long the whole exchange may take before the test fails it.
const BOUND: Duration = Duration::from_secs(10);

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room(index: usize) -> RoomId {
    RoomId::new(format!("room_{index}")).unwrap()
}

/// The text task `task` sends as its message `message`.
fn sent(task: usize, message: usize) -> String {
    format!("t{task} m{message}")
}

/// A kit with `ROOMS` rooms, each binding an SMS sender, a WebSocket reader
/// and an agent echoing every message, and an off-lock check that holds
/// each task's first message into `room_0` until all four are inside it.
async fn rooms() -> (RoomKit, RecordingTransport) {
    let kit = RoomKit::new(RoomKitOptions::default());
    let ws = RecordingTransport::new(
        channel("ws"),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.register_channel(ws.clone()).unwrap();
    kit.register_channel(EchoAgent::new(
        channel("ai"),
        "Echo: ",
        EchoAgentOptions { quote: true },
    ))
    .unwrap();
    let meeting = Arc::new(Barrier::new(TASKS));
    let mut check = HookSpec::new("meeting", HookTrigger::BeforeBroadcast, HookExecution::Sync);
    check.needs_lock = false;
    kit.hook(check, move |event, _| {
        let meeting = Arc::clone(&meeting);
        async move {
            let first = (0..TASKS).any(|task| text_of(&event.content) == sent(task, 0));
            if first && event.room_id == room(0) {
                meeting.wait().await;
            }
            Ok(HookResult::allow(AllowOptions::default()))
        }
    })
    .unwrap();
    for index in 0..ROOMS {
        kit.create_room(CreateRoomOptions {
            room_id: Some(room(index)),
            ..Default::default()
        })
        .await
        .unwrap();
        for bound in ["sms", "ws", "ai"] {
            kit.attach_channel(
                &room(index),
                &channel(bound),
                AttachChannelOptions::default(),
            )
            .await
            .unwrap();
        }
    }
    (kit, ws)
}

/// Task `task`'s messages: `MESSAGES` into each room, the rooms in turn.
async fn send_all(kit: RoomKit, task: usize) {
    for message in 0..MESSAGES {
        for index in 0..ROOMS {
            let sender = format!("+1555000000{task}");
            let text = TextContent::new(sent(task, message));
            let inbound = InboundMessage::new(channel("sms"), sender, text);
            kit.process_inbound(
                inbound,
                ProcessInboundOptions {
                    room_id: Some(room(index)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        }
    }
}

/// What room `index` shows: whether its indices are consecutive from 0 and
/// every event delivered, whether each task's messages kept their order,
/// whether it holds the agent's answer to each message once and nothing
/// else but its attaches' lifecycle records (gap G-046), and whether the
/// reader got every message once, in index order.
async fn as_seen(kit: &RoomKit, ws: &RecordingTransport, index: usize) -> (bool, bool, bool, bool) {
    let timeline = kit
        .store()
        .list_events(&room(index), &every_row())
        .await
        .unwrap();
    let messages: Vec<_> = timeline
        .iter()
        .filter(|event| event.r#type == EventType::Message)
        .collect();
    let texts: Vec<String> = messages
        .iter()
        .map(|event| text_of(&event.content))
        .collect();
    let consecutive = timeline.iter().enumerate().all(|(position, event)| {
        event.index == u64::try_from(position).unwrap() && event.status == EventStatus::Delivered
    });
    let in_order = (0..TASKS).all(|task| {
        let from_task = texts
            .iter()
            .filter(|text| text.starts_with(&format!("t{task} ")));
        let wanted: Vec<String> = (0..MESSAGES).map(|message| sent(task, message)).collect();
        from_task.eq(wanted.iter())
    });
    let mut answers: Vec<&String> = texts
        .iter()
        .filter(|text| text.starts_with("Echo: "))
        .collect();
    answers.sort();
    let mut wanted: Vec<String> = (0..TASKS)
        .flat_map(|task| (0..MESSAGES).map(move |message| format!("Echo: {}", sent(task, message))))
        .collect();
    wanted.sort();
    let answered_once =
        answers.into_iter().eq(wanted.iter()) && texts.len() == 2 * TASKS * MESSAGES;
    let read: Vec<u64> = ws
        .deliveries()
        .into_iter()
        .filter(|delivery| delivery.event.room_id == room(index))
        .map(|delivery| delivery.event.index)
        .collect();
    let indices: Vec<u64> = messages.iter().map(|event| event.index).collect();
    (consecutive, in_order, answered_once, read == indices)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eight_rooms_take_four_senders_at_once() {
    let (kit, ws) = rooms().await;
    let senders: Vec<_> = (0..TASKS)
        .map(|task| tokio::spawn(send_all(kit.clone(), task)))
        .collect();
    tokio::time::timeout(BOUND, async {
        for sender in senders {
            sender.await.unwrap();
        }
        roomkit::testing::idle(&kit).await;
    })
    .await
    .unwrap();
    let mut seen = Vec::new();
    for index in 0..ROOMS {
        seen.push(as_seen(&kit, &ws, index).await);
    }
    assert_eq!(seen, vec![(true, true, true, true); ROOMS]);
}

/// Every row of a room's timeline, the refused ones included.
fn every_row() -> roomkit::EventQuery {
    roomkit::EventQuery {
        limit: u64::MAX,
        event_filter: Some(roomkit::EventFilter {
            include_blocked: true,
            ..roomkit::EventFilter::default()
        }),
        ..roomkit::EventQuery::default()
    }
}

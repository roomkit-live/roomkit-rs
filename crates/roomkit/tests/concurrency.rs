//! Many rooms at once on a multi-threaded runtime (RFC §10.2, §13.5): four
//! tasks send into eight rooms at the same time, each room answered by an
//! agent. Whatever the interleaving, each room commits every message and
//! every answer once, with consecutive indices, delivers each event, and
//! delivers its events in index order; each sender's messages keep their
//! order.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{ChannelId, ChannelType, EventContent, EventStatus, InboundMessage, RoomId};
use roomkit::testing::{EchoAgent, RecordingTransport};
use roomkit::{ConversationStore, RoomKit, text_of};

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

/// A kit with `ROOMS` rooms, each binding an SMS sender, a WebSocket reader
/// and an agent echoing every message.
async fn rooms() -> (RoomKit, RecordingTransport) {
    let kit = RoomKit::new();
    let ws = RecordingTransport::new(channel("ws"), ChannelType::Websocket);
    kit.register_channel(RecordingTransport::new(channel("sms"), ChannelType::Sms))
        .unwrap();
    kit.register_channel(ws.clone()).unwrap();
    kit.register_channel(EchoAgent::new(channel("ai"), "Echo: ").quoting())
        .unwrap();
    for index in 0..ROOMS {
        kit.create_room(&room(index)).await.unwrap();
        for bound in ["sms", "ws", "ai"] {
            kit.attach(&room(index), &channel(bound)).await.unwrap();
        }
    }
    (kit, ws)
}

/// Task `task`'s messages: `MESSAGES` into each room, the rooms in turn.
async fn send_all(kit: RoomKit, task: usize) {
    for message in 0..MESSAGES {
        for index in 0..ROOMS {
            let text = format!("t{task} m{message}");
            let sender = format!("+1555000000{task}");
            let inbound = InboundMessage::new(channel("sms"), sender, EventContent::text(text));
            kit.process_inbound_in(&room(index), inbound).await.unwrap();
        }
    }
}

/// What room `index` shows: whether its indices are consecutive from 0 and
/// every event delivered, how many events it holds, whether the reader got
/// them in index order, each once, and whether each task's messages kept
/// their order.
async fn as_seen(
    kit: &RoomKit,
    ws: &RecordingTransport,
    index: usize,
) -> (bool, usize, bool, bool) {
    let timeline = kit.store().list_events(&room(index), true).await.unwrap();
    let consecutive = timeline.iter().enumerate().all(|(position, event)| {
        event.index == u64::try_from(position).unwrap() && event.status == EventStatus::Delivered
    });
    let read: Vec<u64> = ws
        .deliveries()
        .into_iter()
        .filter(|delivery| delivery.event.room_id == room(index))
        .map(|delivery| delivery.event.index)
        .collect();
    let expected: Vec<u64> = timeline.iter().map(|event| event.index).collect();
    let in_order = (0..TASKS).all(|task| {
        let sent: Vec<String> = timeline
            .iter()
            .map(|event| text_of(&event.content))
            .filter(|text| text.starts_with(&format!("t{task} ")))
            .collect();
        let wanted: Vec<String> = (0..MESSAGES)
            .map(|message| format!("t{task} m{message}"))
            .collect();
        sent == wanted
    });
    (consecutive, timeline.len(), read == expected, in_order)
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
        kit.idle().await;
    })
    .await
    .unwrap();
    let mut seen = Vec::new();
    for index in 0..ROOMS {
        seen.push(as_seen(&kit, &ws, index).await);
    }
    let every_room = (true, 2 * TASKS * MESSAGES, true, true);
    assert_eq!(seen, vec![every_room; ROOMS]);
}

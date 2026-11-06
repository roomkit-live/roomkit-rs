//! What a hook reads of the room (RFC §7.5 rule 8, §9.4): the whole
//! timeline, an event hidden from every channel and a refused one
//! included, as the Python implementation's hooks do. Host code holds the
//! store anyway; only the history rebuilt for a channel is filtered.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::future::ready;
use std::sync::{Arc, Mutex};

use pretty_assertions::assert_eq;
use roomkit::model::hook::{AllowOptions, BlockOptions};
use roomkit::model::{
    ChannelId, ChannelType, EventStatus, HookResult, InboundMessage, RoomContext, RoomEvent,
    RoomId, TextContent, Visibility,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, HookOptions, HookTrigger, ProcessInboundOptions,
    RoomKit, RoomKitOptions,
};
use roomkit_core::rules::transcoding::text_of;

const MARKER: &str = "SECRET-MARKER";

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("r1").unwrap()
}

/// What a `BEFORE_BROADCAST` hook read of the room at each call: the text
/// and status of each event of its context.
type Seen = Arc<Mutex<Vec<(String, EventStatus)>>>;

/// A room binding the transports `ws1` and `ws2`, and a `BEFORE_BROADCAST`
/// hook noting what its context holds, which blocks a message saying
/// [`MARKER`] when `blocks` is set.
async fn room(blocks: bool) -> (RoomKit, Seen) {
    let kit = RoomKit::new(RoomKitOptions::default());
    for id in ["ws1", "ws2"] {
        let transport = RecordingTransport::new(
            channel(id),
            ChannelType::WEBSOCKET,
            RecordingTransportOptions::default(),
        );
        kit.register_channel(transport).unwrap();
    }
    let seen: Seen = Arc::default();
    let noting = Arc::clone(&seen);
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |event: RoomEvent, context: RoomContext| {
            noting.lock().unwrap().extend(
                context
                    .recent_events
                    .iter()
                    .map(|event| (text_of(&event.content), event.status)),
            );
            let result = if blocks && text_of(&event.content) == MARKER {
                HookResult::block("spam", BlockOptions::default())
            } else {
                HookResult::allow(AllowOptions::default())
            };
            ready(Ok(result))
        },
        HookOptions {
            name: "capture".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["ws1", "ws2"] {
        kit.attach_channel(&room_id(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    (kit, seen)
}

async fn say(kit: &RoomKit, message: InboundMessage) {
    let options = ProcessInboundOptions {
        room_id: Some(room_id()),
        ..Default::default()
    };
    kit.process_inbound(message, options).await.unwrap();
}

// Python: tests/test_context_visibility.py::TestHiddenEventsStayHidden::test_hooks_still_see_the_whole_timeline
#[tokio::test]
async fn hooks_still_see_the_whole_timeline() {
    let (kit, seen) = room(false).await;
    let hidden = InboundMessage {
        visibility: Visibility::None,
        ..InboundMessage::new(channel("ws1"), "u1", TextContent::new(MARKER))
    };
    say(&kit, hidden).await;
    say(
        &kit,
        InboundMessage::new(channel("ws2"), "u2", TextContent::new("hello")),
    )
    .await;
    kit.close().await.unwrap();
    let seen = seen.lock().unwrap().clone();
    assert!(seen.iter().any(|(text, _)| text == MARKER), "{seen:?}");
}

// Python: tests/test_context_visibility.py::TestRefusedEventsStayRefused::test_hooks_still_see_the_blocked_record
#[tokio::test]
async fn hooks_still_see_the_blocked_record() {
    let (kit, seen) = room(true).await;
    say(
        &kit,
        InboundMessage::new(channel("ws1"), "u1", TextContent::new(MARKER)),
    )
    .await;
    say(
        &kit,
        InboundMessage::new(channel("ws2"), "u2", TextContent::new("hello")),
    )
    .await;
    kit.close().await.unwrap();
    let seen = seen.lock().unwrap().clone();
    assert_eq!(
        seen.iter()
            .filter(|(text, _)| text == MARKER)
            .map(|(_, status)| *status)
            .collect::<Vec<_>>(),
        [EventStatus::Blocked]
    );
}

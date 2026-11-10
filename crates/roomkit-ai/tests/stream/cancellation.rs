//! Cancellation and renderer failure with an unfinished provider stream.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{ChannelType, EventType, InboundMessage, RoomEvent, TextContent};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, ConversationStore, CreateRoomOptions, HookExecution, HookOptions,
    HookTrigger, InboundResult, ProcessInboundOptions, RoomKit, RoomKitOptions,
};
use roomkit_ai::{AiChannel, AiChannelOptions, StreamEvent, StreamTextDelta};
use serde_json::json;

use super::{Controlled, Read, Room, Stop, Streaming, channel, room_id};

pub(super) fn count_errors(kit: &RoomKit) -> Arc<AtomicUsize> {
    let errors = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&errors);
    kit.hook(
        HookTrigger::OnError,
        move |_: RoomEvent, _| {
            seen.fetch_add(1, Ordering::SeqCst);
            std::future::ready(())
        },
        HookOptions {
            name: "cancellation errors".to_owned(),
            execution: HookExecution::Async,
            ..Default::default()
        },
    )
    .unwrap();
    errors
}

async fn controlled(stop: Stop) -> (Room, tokio::sync::mpsc::UnboundedSender<StreamEvent>) {
    let (feed, events) = tokio::sync::mpsc::unbounded_channel();
    let model = Controlled::default();
    *model.events.lock().unwrap() = Some(events);
    let dropped = Arc::clone(&model.dropped);
    let kit = RoomKit::new(RoomKitOptions::default());
    let agent = AiChannel::new(channel("agent"), model, AiChannelOptions::default()).unwrap();
    let ws = Streaming::new(stop);
    let web = RecordingTransport::new(
        channel("web"),
        ChannelType::WEBHOOK,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(web.clone()).unwrap();
    kit.register_channel(ws.clone()).unwrap();
    kit.register_channel(agent).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["web", "ws", "agent"] {
        kit.attach_channel(&room_id(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    (
        Room {
            kit,
            web,
            ws,
            dropped,
        },
        feed,
    )
}

async fn call(room: &Room, deferred: bool) -> InboundResult {
    tokio::time::timeout(
        Duration::from_secs(5),
        room.kit.process_inbound(
            InboundMessage::new(channel("web"), "alice", TextContent::new("Hi")),
            ProcessInboundOptions {
                room_id: Some(room_id()),
                defer_delivery: deferred,
                ..Default::default()
            },
        ),
    )
    .await
    .unwrap()
    .unwrap()
}

// Python: tests/test_inbound_stream_cancellation.py::test_transport_failure_closes_stream_before_returning
#[tokio::test]
async fn a_failed_renderer_closes_the_pending_provider_before_returning() {
    let (room, feed) = controlled(Stop::FailsAfter("Partial")).await;
    feed.send(StreamEvent::TextDelta(StreamTextDelta {
        text: "Partial".to_owned(),
    }))
    .unwrap();
    // The sender remains live and supplies no end: this provider cannot
    // have completed by itself when the rendering target fails.
    let result = call(&room, false).await;
    assert_eq!(room.dropped.load(Ordering::SeqCst), 1);
    assert!(feed.is_closed());
    assert!(
        result
            .error
            .as_ref()
            .is_some_and(|e| e.message.contains("the socket broke"))
    );
    assert_eq!(
        room.rows().await,
        [(EventType::Message, "Partial".to_owned(), None, None)]
    );
    assert_eq!(
        (room.ws.delivered(), room.web_rows()),
        (vec![3, 4], vec![(EventType::Message, 4)])
    );
    room.kit.close().await.unwrap();
}

// Python: tests/test_inbound_stream_cancellation.py::test_deferred_cancellation_retains_persisted_response_events
#[tokio::test]
async fn a_deferred_handle_keeps_the_exact_partial_response_persisted_at_close() {
    let (room, feed) = controlled(Stop::Never).await;
    let handle = call(&room, true).await.delivery.unwrap();
    feed.send(StreamEvent::TextDelta(StreamTextDelta {
        text: "Partial".to_owned(),
    }))
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while room.ws.read().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(room.ws.read(), [Read::Text("Partial".to_owned())]);
    room.kit.close().await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), handle.wait())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.cancellation_reason.as_deref(), Some("kit_closed"));
    assert_eq!(result.response_events.len(), 1);
    let answer = &result.response_events[0];
    assert_eq!(
        roomkit_core::rules::transcoding::text_of(&answer.content),
        "Partial"
    );
    assert_eq!(answer.metadata.get("cancelled"), Some(&json!(true)));
    assert_eq!(
        room.kit.store().get_event(&answer.id).await.unwrap(),
        Some(answer.clone())
    );
    assert_eq!(room.dropped.load(Ordering::SeqCst), 1);
}

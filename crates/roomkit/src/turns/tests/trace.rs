//! A turn's request names the span it runs under (RFC §10.6.9): an executor
//! in a context of its own continues it.

use std::future::{Future, ready};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use pretty_assertions::assert_eq;
use roomkit_core::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, InboundMessage,
    RoomContext, RoomEvent, RoomId, TextContent,
};

use crate::telemetry::{MockTelemetryProvider, SpanKind, get_current_span};
use crate::testing::{RecordingTransport, RecordingTransportOptions};
use crate::{
    AttachChannelOptions, Channel, ChannelError, CreateRoomOptions, ProcessInboundOptions, RoomKit,
    RoomKitOptions,
};

fn id(channel: &str) -> ChannelId {
    ChannelId::new(channel).unwrap()
}

/// An agent that keeps the span its turn runs under.
#[derive(Clone)]
struct ReadsItsSpan(Arc<Mutex<Vec<Option<String>>>>);

impl Channel for ReadsItsSpan {
    fn channel_id(&self) -> ChannelId {
        id("ai")
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }

    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        let source = roomkit_core::model::EventSource::new(id("ai"), ChannelType::AI);
        ready(Ok(RoomEvent::new(context.room.id, source, message.content)))
    }

    fn on_event(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        self.0.lock().unwrap().push(get_current_span());
        ready(Ok(ChannelOutput::empty()))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::empty()))
    }
}

// Python: tests/test_turns_wiring.py::test_a_turn_in_a_context_of_its_own_continues_the_owner_s_span
#[tokio::test]
async fn a_turn_in_a_context_of_its_own_continues_the_broadcast_s_span() {
    let mock = Arc::new(MockTelemetryProvider::default());
    let kit = RoomKit::new(RoomKitOptions {
        telemetry: Some(Arc::clone(&mock).into()),
        ..RoomKitOptions::default()
    });
    kit.inner.turns.executor_apart.store(true, Ordering::SeqCst);
    let sms = RecordingTransport::new(
        id("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(sms).unwrap();
    let spans = Arc::new(Mutex::new(Vec::new()));
    kit.register_channel(ReadsItsSpan(Arc::clone(&spans)))
        .unwrap();
    let room = RoomId::new("r1").unwrap();
    let create = CreateRoomOptions {
        room_id: Some(room.clone()),
        ..CreateRoomOptions::default()
    };
    kit.create_room(create).await.unwrap();
    for channel in ["sms", "ai"] {
        kit.attach_channel(&room, &id(channel), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    let message = InboundMessage::new(id("sms"), "+15550000001", TextContent::new("Hello"));
    let options = ProcessInboundOptions {
        room_id: Some(room),
        ..ProcessInboundOptions::default()
    };
    kit.process_inbound(message, options).await.unwrap();
    kit.close().await.unwrap();
    let broadcast = mock.get_spans(SpanKind::Broadcast);
    let read = spans.lock().unwrap().clone();
    let of_a_broadcast = |span: &Option<String>| {
        broadcast
            .iter()
            .any(|broadcast| Some(&broadcast.id) == span.as_ref())
    };
    assert_eq!(read.len(), 1);
    assert!(of_a_broadcast(&read[0]), "{read:?} {broadcast:?}");
}

//! Closing precisely after a provider call's END, without polling past it.

use super::{Stop, Streaming, channel, room_id};
use futures_core::Stream;
use pretty_assertions::assert_eq;
use roomkit::model::{InboundMessage, TextContent};
use roomkit::realtime::{EphemeralEvent, EphemeralEventType};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, ProcessInboundOptions, RoomKit, RoomKitOptions,
};
use roomkit::{CancelOptions, SubscribeRoomOptions};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiContext, AiProvider, AiResponse, ProviderStream, StreamEvent,
};
use roomkit_ai::{AiToolCall, ProviderError, ServedCall};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Clone, Default)]
struct BoundaryProvider {
    pending: bool,
    polled: Arc<AtomicBool>,
    dropped: Arc<Notify>,
}

struct OneCall {
    call: Option<StreamEvent>,
    polled_past_call: Arc<AtomicBool>,
    dropped: Arc<Notify>,
}

impl Drop for OneCall {
    fn drop(&mut self) {
        self.dropped.notify_one();
    }
}

impl Stream for OneCall {
    type Item = Result<StreamEvent, ProviderError>;
    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(call) = self.call.take() {
            return Poll::Ready(Some(Ok(call)));
        }
        self.polled_past_call.store(true, Ordering::SeqCst);
        Poll::Pending
    }
}

impl AiProvider for BoundaryProvider {
    fn model_name(&self) -> &'static str {
        "boundary"
    }
    fn generate(
        &self,
        _: &AiContext,
    ) -> impl Future<Output = Result<AiResponse, ProviderError>> + Send {
        std::future::ready(Err("the boundary fixture requires streaming".into()))
    }
    fn generate_structured_stream<'a>(&'a self, _: &'a AiContext) -> ProviderStream<'a> {
        let mut call = AiToolCall::new("c1", "lookup");
        if !self.pending {
            call.served = Some(ServedCall {
                result: "found".into(),
                is_error: false,
            });
        }
        Box::pin(OneCall {
            call: Some(StreamEvent::ToolCall(call.into())),
            polled_past_call: Arc::clone(&self.polled),
            dropped: Arc::clone(&self.dropped),
        })
    }
}

// Python: tests/test_ai_streaming_tool_events.py::test_a_stream_closed_on_a_call_s_end_still_publishes_it[provider-s-call]
#[tokio::test]
async fn a_provider_stream_closed_on_its_end_still_publishes_it() {
    for cut in [false, true] {
        close_at_end(cut, false).await;
    }
}

// Python: _ExternalStreamTools._hand_end uses the same reader boundary for
// pending external decisions and calls the provider already served.
#[tokio::test]
async fn an_external_decision_closed_on_its_end_still_publishes_it() {
    for cut in [false, true] {
        close_at_end(cut, true).await;
    }
}

async fn close_at_end(cut: bool, pending: bool) {
    let model = BoundaryProvider {
        pending,
        ..BoundaryProvider::default()
    };
    let kit = RoomKit::new(RoomKitOptions::default());
    let reached = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let ws = Streaming::new(Stop::ProviderEnd(
        Arc::clone(&reached),
        cut.then(|| Arc::clone(&release)),
    ));
    let agent = AiChannel::new(channel("agent"), model.clone(), call_text(pending)).unwrap();
    kit.register_channel(ws).unwrap();
    kit.register_channel(agent).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["ws", "agent"] {
        kit.attach_channel(&room_id(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    let events = Arc::new(Mutex::new(Vec::new()));
    let capture = Arc::clone(&events);
    kit.subscribe_room(
        &room_id(),
        move |event: EphemeralEvent| {
            if matches!(
                event.r#type,
                EphemeralEventType::ToolCallStart | EphemeralEventType::ToolCallEnd
            ) {
                capture.lock().unwrap().push((event.r#type, event.data));
            }
            std::future::ready(())
        },
        SubscribeRoomOptions::default(),
    )
    .await
    .unwrap();
    let sent = kit
        .process_inbound(
            InboundMessage::new(channel("ws"), "alice", TextContent::new("Go")),
            ProcessInboundOptions {
                room_id: Some(room_id()),
                defer_delivery: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), reached.notified())
        .await
        .unwrap();
    assert!(
        !model.polled.load(Ordering::SeqCst),
        "the loop must still be suspended handing END"
    );
    let handle = sent.delivery.unwrap();
    if cut {
        let mut cancelling = Box::pin(handle.cancel(CancelOptions::default()));
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                result = &mut cancelling => panic!("the held target must still need cleanup: {result:?}"),
                () = model.dropped.notified() => {}
            }
        }).await.unwrap();
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(10), cancelling)
            .await
            .unwrap()
            .unwrap();
    } else {
        tokio::time::timeout(Duration::from_secs(5), handle.wait())
            .await
            .unwrap()
            .unwrap();
    }
    roomkit::testing::idle(&kit).await;
    kit.close().await.unwrap();
    assert_events(&events.lock().unwrap(), cut, pending);
}

fn assert_events(
    events: &[(EphemeralEventType, roomkit::model::Metadata)],
    cut: bool,
    pending: bool,
) {
    assert_eq!(events.len(), 2);
    let start = events.first().unwrap();
    assert_eq!(start.0, EphemeralEventType::ToolCallStart);
    let starts = start.1.get("tool_calls").unwrap().as_array().unwrap();
    assert_eq!(starts.len(), 1);
    assert_eq!(
        starts.first().unwrap().get("id"),
        Some(&serde_json::Value::from("c1"))
    );
    let end = events.last().unwrap();
    assert_eq!(end.0, EphemeralEventType::ToolCallEnd);
    let event = &end.1;
    let calls = event.get("tool_calls").unwrap().as_array().unwrap();
    assert_eq!(calls.len(), 1);
    let call = calls.first().unwrap();
    let expected = if cut {
        "{\"error\": \"Tool call aborted\"}"
    } else if pending {
        ""
    } else {
        "found"
    };
    assert_eq!(call.get("result"), Some(&serde_json::Value::from(expected)));
    assert_eq!(
        call.get("status"),
        Some(&serde_json::Value::from(if cut {
            "failed"
        } else {
            "completed"
        }))
    );
    assert_eq!(event.get("duration_ms").is_some(), !cut);
}

/// The agent's options: the end's result is the point, its preview is
/// published only for a channel that publishes its calls' text (RFC §8.4).
fn call_text(pending: bool) -> AiChannelOptions {
    AiChannelOptions {
        publish_tool_call_text: true,
        external_tool_handler: pending.then(|| {
            let handler: Arc<dyn roomkit_ai::ExternalToolHandler> =
                Arc::new(roomkit_ai::PolicyExternalToolHandler::default());
            handler
        }),
        ..AiChannelOptions::default()
    }
}

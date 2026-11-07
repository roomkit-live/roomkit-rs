//! RFC §10.6.4, §12.2 step 13s (RMK-1362, HITL e151b3f4): a call whose end
//! is told before its end item is closed with it at a cut, as Python's
//! `ToolCallStartMarker.ended`, a cut between a round's end items included:
//! two calls under one id stay two calls.

use std::sync::Arc;
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelOutput, ChannelType, EventContent,
    EventType, InboundMessage, RoomContext, RoomEvent, TextContent, ToolCallOutcome,
};
use roomkit::{
    AttachChannelOptions, CancelOptions, Channel, ChannelError, ConversationStore,
    CreateRoomOptions, DeliveryStream, ProcessInboundOptions, RoomKit, RoomKitOptions, StreamItem,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiChannel, AiChannelOptions, ToolResult, Tools};
use roomkit_core::ops::ChannelConfig;
use tokio::sync::Notify;

use crate::support::{channel, lookup, received, room};

/// A streaming transport that stops reading once it was handed its
/// `stop_after`-th end row, tells `stopped`, and returns once `released`.
struct StopsAtAnEnd {
    declaration: ChannelConfig,
    stop_after: usize,
    stopped: Arc<Notify>,
    released: Arc<Notify>,
}

impl Channel for StopsAtAnEnd {
    fn channel_id(&self) -> roomkit::model::ChannelId {
        self.declaration.channel_id.clone()
    }

    fn channel_type(&self) -> ChannelType {
        self.declaration.channel_type.clone()
    }

    fn category(&self) -> ChannelCategory {
        self.declaration.category
    }

    fn direction(&self) -> ChannelDirection {
        self.declaration.direction
    }

    fn capabilities(&self) -> roomkit::model::ChannelCapabilities {
        self.declaration.capabilities.clone()
    }

    fn recipient_key(&self) -> Option<String> {
        None
    }

    fn supports_streaming_delivery(&self) -> bool {
        true
    }

    async fn deliver_stream(
        &self,
        mut stream: DeliveryStream,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        let mut ends = 0;
        while let Some(item) = stream.next().await {
            if let Ok(StreamItem::Row(row)) = item
                && row.r#type == EventType::ToolCallEnd
            {
                ends += 1;
                if ends == self.stop_after {
                    self.stopped.notify_one();
                    self.released.notified().await;
                    return Ok(ChannelOutput::default());
                }
            }
        }
        Ok(ChannelOutput::default())
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        std::future::ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        std::future::ready(Ok(ChannelOutput::empty()))
    }
}

/// The end rows a round of `lookup` calls stores, each its id, its query and
/// its outcome, in order, its deferred call cut once its streaming target
/// was handed `stop_after` of them.
async fn ends_cut_after(
    stop_after: usize,
    calls: &[(&str, &str)],
) -> Vec<(String, String, String)> {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(roomkit::testing::RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        roomkit::testing::RecordingTransportOptions::default(),
    ))
    .unwrap();
    let (stopped, released) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    kit.register_channel(StopsAtAnEnd {
        declaration: ChannelConfig::new(
            channel("ws"),
            ChannelType::WEBSOCKET,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
        ),
        stop_after,
        stopped: Arc::clone(&stopped),
        released: Arc::clone(&released),
    })
    .unwrap();
    let arguments: Vec<String> = calls
        .iter()
        .map(|(_, q)| format!(r#"{{"q":"{q}"}}"#))
        .collect();
    let scripted: Vec<(&str, &str, &str)> = calls
        .iter()
        .zip(&arguments)
        .map(|((id, _), arguments)| (*id, "lookup", arguments.as_str()))
        .collect();
    let model = ScriptedProvider::new([
        ScriptedRound::calling("", scripted),
        ScriptedRound::answering("never"),
    ]);
    let tools = Tools::new()
        .tool(lookup(), |_call, _context| async {
            Ok(ToolResult::Text("fine".to_owned()))
        })
        .into();
    let options = AiChannelOptions {
        tools,
        ..AiChannelOptions::default()
    };
    kit.register_channel(AiChannel::new(channel("ai"), model, options).unwrap())
        .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["sms", "ws", "ai"] {
        kit.attach_channel(&room(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    let message = InboundMessage::new(channel("sms"), "+15550000001", TextContent::new("Go."));
    let options = ProcessInboundOptions {
        room_id: Some(room()),
        defer_delivery: true,
        ..Default::default()
    };
    let decided = kit.process_inbound(message, options).await.unwrap();
    let handle = decided.delivery.unwrap();
    tokio::time::timeout(Duration::from_secs(5), stopped.notified())
        .await
        .unwrap();
    let cut = CancelOptions {
        reason: "stopped".to_owned(),
        timeout: Duration::from_millis(50),
    };
    drop(handle.cancel(cut).await);
    released.notify_one();
    roomkit::testing::idle(&kit).await;
    let rows = kit.store().list_events(&room(), received()).await.unwrap();
    kit.close().await.unwrap();
    rows.into_iter()
        .filter(|row| row.r#type == EventType::ToolCallEnd)
        .filter_map(|row| {
            let EventContent::ToolCall(end) = row.content else {
                return None;
            };
            Some((
                end.tool_id.clone(),
                end.arguments["q"].as_str().unwrap_or_default().to_owned(),
                format!("{:?}", end.outcome.unwrap_or(ToolCallOutcome::Failed)),
            ))
        })
        .collect()
}

// Three calls of a round, two under one id, the second refused: the turn cut
// once its target read one end row, or two, closes each call left open as it
// ended, the second under the id its own end, on 4 worker threads.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cut_between_a_round_s_end_items_keeps_each_call_s_end() {
    let calls = [("c1", "first"), ("c1", "second"), ("c2", "third")];
    for stop_after in 1..=2 {
        let mut ends = ends_cut_after(stop_after, &calls).await;
        ends.sort();
        let expected = [
            ("c1", "first", "Served"),
            ("c1", "second", "Refused"),
            ("c2", "third", "Served"),
        ]
        .map(|(id, q, outcome)| (id.to_owned(), q.to_owned(), outcome.to_owned()));
        assert_eq!(ends, expected, "stop_after={stop_after}");
    }
}

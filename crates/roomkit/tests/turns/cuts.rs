//! RFC §10.6.8, §10.6.4, §12.2 step 13s: a turn ended early crosses as
//! its own message: the kit's close while a tool runs as `cancel`, a
//! streaming reader that leaves as `close`.

use std::sync::{Arc, Mutex};

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelOutput, ChannelType, EventContent,
    EventType, RoomContext, TextContent, ToolCallOutcome,
};
use roomkit::testing::TurnTap;
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, ConversationStore, CreateRoomOptions,
    DeliveryStream, HookExecution, HookOptions, HookTrigger, ProcessInboundOptions, RoomKit,
    RoomKitOptions, ToolCallEvent,
};
use roomkit_ai::ToolResult;
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiChannel, AiChannelOptions};
use roomkit_core::ops::ChannelConfig;
use serde_json::Value;
use tokio::sync::Notify;

use crate::support::{asks, channel, kit, received, room};

/// What crossed, as `family type`, a cancel with its reason.
fn crossed(tap: &TurnTap) -> Vec<String> {
    tap.messages()
        .into_iter()
        .filter(|message| matches!(message.family, "frame" | "control"))
        .map(|message| {
            let json: Value = serde_json::from_str(&message.json).unwrap();
            let kind = json["message"]["type"]
                .as_str()
                .unwrap_or_default()
                .to_owned();
            match json["message"]["reason"].as_str() {
                Some(reason) => format!("{} {kind} {reason}", message.family),
                None => format!("{} {kind}", message.family),
            }
        })
        .collect()
}

/// Notes that a tool's call was dropped before it answered.
struct Cut(Arc<Mutex<Vec<&'static str>>>);

impl Drop for Cut {
    fn drop(&mut self) {
        self.0.lock().unwrap().push("cut");
    }
}

/// RFC §10.6.8, §12.10.4: the kit's close while a tool runs crosses as
/// `cancel` with its reason; the executor cuts the handler and reports the
/// call `cancelled` to the kit's observers; the turn's end follows; the
/// reader stores the call's end, its error `kit_closed`, as Python's; the
/// caller receives its result, `kit_closed`, the call under way as the
/// close started. (Milestone 6 left the call's start with no end, and
/// reported nothing; on fefbfac the end's error read `cancelled`, the call
/// no reason.)
#[tokio::test(start_paused = true)]
async fn the_kit_s_close_while_a_tool_runs_crosses_as_cancel_and_ends_the_call_cancelled() {
    let running = Arc::new(Notify::new());
    let log = Arc::new(Mutex::new(Vec::new()));
    let (started, cut) = (Arc::clone(&running), Arc::clone(&log));
    let kit = kit(move |_call, _context| {
        let (started, cut) = (Arc::clone(&started), Cut(Arc::clone(&cut)));
        async move {
            let _cut = cut;
            started.notify_one();
            std::future::pending::<()>().await;
            Ok(ToolResult::Text("never".to_owned()))
        }
    })
    .await;
    let observed = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&observed);
    kit.hook(
        HookTrigger::OnToolCall,
        move |event: ToolCallEvent, _context| {
            seen.lock()
                .unwrap()
                .push((event.cancelled, event.error_detail.clone()));
            async {}
        },
        HookOptions {
            execution: HookExecution::Async,
            name: "audit".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    let tap = TurnTap::on(&kit);
    let asking = tokio::spawn({
        let kit = kit.clone();
        async move {
            kit.process_inbound(
                message(),
                ProcessInboundOptions {
                    room_id: Some(room()),
                    ..Default::default()
                },
            )
            .await
        }
    });
    running.notified().await;
    kit.close().await.unwrap();
    let asked = asking.await.unwrap();
    let crossed = crossed(&tap);
    let tail: Vec<&str> = crossed
        .iter()
        .rev()
        .take(2)
        .rev()
        .map(String::as_str)
        .collect();
    let end = kit
        .store()
        .list_events(&room(), received())
        .await
        .unwrap()
        .into_iter()
        .find(|event| event.r#type == EventType::ToolCallEnd)
        .map(|event| {
            if let EventContent::ToolCall(row) = event.content {
                (row.outcome, row.error)
            } else {
                (None, None)
            }
        });
    assert_eq!(
        (
            asked.map(|result| (result.event.is_some(), result.cancellation_reason)),
            tail,
            log.lock().unwrap().clone(),
            observed.lock().unwrap().clone(),
            end
        ),
        (
            Ok((true, Some("kit_closed".to_owned()))),
            vec!["control cancel kit_closed", "frame end"],
            vec!["cut"],
            vec![(true, Some("cancelled: kit_closed".to_owned()))],
            Some((
                Some(ToolCallOutcome::Cancelled),
                Some("kit_closed".to_owned())
            ))
        )
    );
}

fn message() -> roomkit::model::InboundMessage {
    roomkit::model::InboundMessage::new(
        channel("sms"),
        "+15550000001",
        TextContent::new("What is it?"),
    )
}

/// A WebSocket transport that streams, and leaves once it read one item.
struct Leaving {
    declaration: ChannelConfig,
}

impl Channel for Leaving {
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

    fn provider_name(&self) -> Option<String> {
        self.declaration.provider_name.clone()
    }

    fn recipient_key(&self) -> Option<String> {
        self.declaration.recipient_key.clone()
    }

    fn supports_streaming_delivery(&self) -> bool {
        self.declaration.supports_streaming_delivery
    }

    fn address_config(&self) -> (roomkit::ChannelAddressing, Option<roomkit::CountryCode>) {
        let config = &self.declaration;
        let addressing = roomkit::ChannelAddressing {
            addressing: config.addressing,
            recipient_key: config.recipient_key.clone(),
            chat_key: config.chat_key.clone(),
            replies_to_sender: false,
        };
        (addressing, config.country_code.clone())
    }

    async fn deliver_stream(
        &self,
        mut stream: DeliveryStream,
        _event: roomkit::model::RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        stream.next().await;
        Ok(ChannelOutput::default())
    }

    fn handle_inbound(
        &self,
        message: roomkit::model::InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<roomkit::model::RoomEvent, ChannelError>> + Send {
        std::future::ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }

    fn deliver(
        &self,
        _event: roomkit::model::RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        std::future::ready(Ok(ChannelOutput::empty()))
    }
}

/// RFC §10.6.4, §12.2 step 13s: a streaming reader that leaves mid-answer
/// crosses as `close`, the reader's close, not a cancel; the turn ends,
/// and what it was handed is stored `cancelled`: the answer's two deltas,
/// yielded together, crossed as one item (RMK-1017, as Python's
/// `FrameSender`), which the reader was handed whole.
#[tokio::test(start_paused = true)]
async fn a_streaming_reader_that_leaves_crosses_as_close() {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(roomkit::testing::RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        roomkit::testing::RecordingTransportOptions::default(),
    ))
    .unwrap();
    let mut declaration = ChannelConfig::new(
        channel("ws"),
        ChannelType::WEBSOCKET,
        ChannelCategory::Transport,
        ChannelDirection::Bidirectional,
    );
    declaration.supports_streaming_delivery = true;
    declaration.recipient_key = None;
    kit.register_channel(Leaving { declaration }).unwrap();
    let model = ScriptedProvider::new([ScriptedRound::writing(["The answer ", "is 42."])]);
    kit.register_channel(
        AiChannel::new(channel("ai"), model, AiChannelOptions::default()).unwrap(),
    )
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
    let tap = TurnTap::on(&kit);
    asks(&kit, "What is it?").await;
    roomkit::testing::idle(&kit).await;
    let crossed = crossed(&tap);
    let stored: Vec<(String, Option<Value>)> = kit
        .store()
        .list_events(&room(), received())
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.source.channel_id == channel("ai"))
        .map(|event| {
            (
                roomkit_core::rules::transcoding::text_of(&event.content),
                event.metadata.get("cancelled").cloned(),
            )
        })
        .collect();
    assert_eq!(
        (
            crossed.contains(&"control close".to_owned()),
            crossed
                .iter()
                .any(|step| step.starts_with("control cancel")),
            crossed.last().map(String::as_str),
            stored
        ),
        (
            true,
            false,
            Some("frame end"),
            vec![("The answer is 42.".to_owned(), Some(Value::Bool(true)))]
        )
    );
}

/// RFC §10.1: explicitly cancelling a deferred handle joins its running tool,
/// retains committed rows, and leaves the kit/provider usable for another call.
#[tokio::test]
async fn cancelling_a_detached_handle_joins_its_tool_and_keeps_the_kit_usable() {
    let running = Arc::new(Notify::new());
    let log = Arc::new(Mutex::new(Vec::new()));
    let (started, cut) = (Arc::clone(&running), Arc::clone(&log));
    let kit = kit(move |_call, _context| {
        let (started, cut) = (Arc::clone(&started), Cut(Arc::clone(&cut)));
        async move {
            let _cut = cut;
            started.notify_one();
            std::future::pending::<()>().await;
            Ok(ToolResult::Text("never".to_owned()))
        }
    })
    .await;
    let admitted = kit
        .process_inbound(
            message(),
            ProcessInboundOptions {
                room_id: Some(room()),
                defer_delivery: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let handle = admitted.delivery.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), running.notified())
        .await
        .unwrap();
    assert!(handle.request_cancel());
    let done = tokio::time::timeout(std::time::Duration::from_secs(5), handle.wait())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        done.cancellation_reason.as_deref(),
        Some("caller_cancelled")
    );
    assert!(done.error.is_none());
    assert_eq!(*log.lock().unwrap(), vec!["cut"]);
    assert!(!handle.request_cancel());
    assert_eq!(
        handle.wait().await.unwrap().cancellation_reason,
        done.cancellation_reason
    );
    let next = asks(&kit, "Continue after cancellation.").await;
    assert!(next.error.is_none());
    let rows = kit.store().list_events(&room(), received()).await.unwrap();
    assert!(
        rows.iter().any(
            |row| roomkit_core::rules::transcoding::text_of(&row.content).contains("It is 42.")
        )
    );
    kit.close().await.unwrap();
}

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
    AttachChannelOptions, Channel, ChannelDeclaration, ChannelError, ConversationStore,
    CreateRoomOptions, DeliveryStream, ProcessInboundOptions, RoomKit, RoomKitOptions,
};
use roomkit_ai::AiAgent;
use roomkit_ai::ToolOutput;
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
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

/// RFC §10.6.8: the kit's close while a tool runs crosses as `cancel`
/// with its reason; the executor cuts the handler and reports the call
/// `cancelled` to the kit's observers; the turn's end follows; the reader
/// stores the call's end `cancelled`; the caller receives its result, the
/// turn cut at once and its delivery over, no reason. (Milestone 6 left
/// the call's start with no end, and reported nothing.)
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
            Ok(ToolOutput::Text("never".to_owned()))
        }
    })
    .await;
    let observed = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&observed);
    kit.observe_tool_calls("audit", move |event, _context| {
        seen.lock()
            .unwrap()
            .push((event.outcome, event.error_detail.clone()));
        async {}
    })
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
        .list_events(&room(), &received())
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
            // The caller receives the call it made, as Python's awaited
            // caller does: the turn is cut at once, with no grace, and
            // what its reader read is delivered as the lanes drain, its
            // own delivery over, no reason (RMK-1017; Python's
            // `rmk1017-lightreview/lr_probe_py.out` c3: `None`).
            Ok((true, None)),
            vec!["control cancel kit_closed", "frame end"],
            vec!["cut"],
            vec![(
                Some(ToolCallOutcome::Cancelled),
                Some("cancelled: kit_closed".to_owned())
            )],
            Some((
                Some(ToolCallOutcome::Cancelled),
                Some("cancelled".to_owned())
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
    declaration: ChannelDeclaration,
}

impl Channel for Leaving {
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
    }

    async fn deliver_stream(
        &self,
        mut stream: DeliveryStream,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        stream.next().await;
        Ok(ChannelOutput::default())
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
    ))
    .unwrap();
    let mut declaration = ChannelDeclaration::new(
        channel("ws"),
        ChannelType::WEBSOCKET,
        ChannelCategory::Transport,
        ChannelDirection::Bidirectional,
    );
    declaration.supports_streaming_delivery = true;
    declaration.recipient_key = None;
    kit.register_channel(Leaving { declaration }).unwrap();
    let model = ScriptedProvider::new([ScriptedRound::writing(["The answer ", "is 42."])]);
    kit.register_channel(AiAgent::new(channel("ai"), model))
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
        .list_events(&room(), &received())
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.source.channel_id == channel("ai"))
        .map(|event| {
            (
                roomkit::text_of(&event.content),
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

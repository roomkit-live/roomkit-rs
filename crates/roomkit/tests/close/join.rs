//! RFC §12.10.4, RMK-1019: a close from a tool call joins a close already
//! running, while that close still waits for the store's writes, before it
//! cuts the turns: the joining close spares its own caller's work, the
//! tool's turn, which the first close then does not cut, and both return
//! once the kit is closed. Once the first close cuts the turns, a tool
//! cannot join any more: its turn is cut at once, with no grace (RFC
//! §10.6.8), as Python's is.

use std::sync::Arc;
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{ChannelType, RoomId};
use roomkit::testing::RecordingTransport;
use roomkit::{AttachChannelOptions, CreateRoomOptions, RoomKit, RoomKitOptions};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiAgent, ToolDefinition, ToolOutput, Tools};
use serde_json::json;
use tokio::sync::Notify;

use crate::store::SlowCommits;
use crate::support::{BOUND, channel, into, message, room_id};

/// RFC §12.10.4, RMK-1019: "Slow." commits in `room_2` for as long as the
/// test holds it, so the first close, from outside, waits for that write
/// before it cuts the turns; meanwhile an agent's tool in `room_1` closes
/// the kit too, joining it: the tool's turn is spared, never cut, and
/// both closes return once the kit is closed, the witness closed by then.
#[tokio::test(start_paused = true)]
async fn a_close_from_a_tool_joining_a_close_still_waiting_for_a_write_spares_its_turn() {
    let kit = RoomKit::new(RoomKitOptions::with_store(SlowCommits::default()));
    let store = kit.store();
    let tool = ClosingTool::default();
    two_rooms(&kit, &tool).await;
    let room_2 = RoomId::new("room_2").unwrap();
    let (entered, go, closes_from_the_tool) = (tool.entered, tool.go, tool.answered);
    let slow = tokio::spawn({
        let kit = kit.clone();
        async move { kit.process_inbound(message("Slow."), into(room_2)).await }
    });
    tokio::time::timeout(BOUND, store.committing.notified())
        .await
        .unwrap();
    let call = tokio::spawn({
        let kit = kit.clone();
        async move { kit.process_inbound(message("Go."), into(room_id())).await }
    });
    tokio::time::timeout(BOUND, entered.notified())
        .await
        .unwrap();
    let first = tokio::spawn({
        let kit = kit.clone();
        async move { kit.close().await }
    });
    // The first close waits for "Slow."'s write; the tool joins it.
    tokio::time::sleep(Duration::from_millis(100)).await;
    go.notify_one();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let joined_early = first.is_finished();
    store.go.notify_one();
    let first = tokio::time::timeout(Duration::from_secs(60), first)
        .await
        .unwrap()
        .unwrap();
    let call = tokio::time::timeout(BOUND, call).await.unwrap().unwrap();
    tokio::time::timeout(BOUND, slow)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let from_the_tool = closes_from_the_tool.lock().await.take();
    assert_eq!(
        (
            joined_early,
            first,
            from_the_tool,
            call.map(|result| result.cancellation_reason)
        ),
        (
            false,
            Ok(()),
            Some(Ok(())),
            Ok(Some("kit_closed".to_owned()))
        )
    );
}

/// What the agent's tool shares with the test: it says it began, waits
/// for `go`, closes the kit and keeps what its close answered.
#[derive(Clone, Default)]
struct ClosingTool {
    entered: Arc<Notify>,
    go: Arc<Notify>,
    answered: Arc<tokio::sync::Mutex<Option<Result<(), roomkit::RoomKitError>>>>,
}

/// `room_1`, with SMS and an agent whose one tool is `tool`, and `room_2`,
/// with SMS.
async fn two_rooms(kit: &RoomKit<SlowCommits>, tool: &ClosingTool) {
    let sms = RecordingTransport::new(channel("sms"), ChannelType::SMS);
    kit.register_channel(sms).unwrap();
    let weak = kit.downgrade();
    let (in_tool, go_tool, answered) = (
        Arc::clone(&tool.entered),
        Arc::clone(&tool.go),
        Arc::clone(&tool.answered),
    );
    let tools = Tools::new().tool(
        ToolDefinition::new("shut_down", "Shut down.", json!({ "type": "object" })),
        move |_call, _context| {
            let (kit, in_tool, go_tool, answered) = (
                weak.upgrade(),
                Arc::clone(&in_tool),
                Arc::clone(&go_tool),
                Arc::clone(&answered),
            );
            async move {
                in_tool.notify_one();
                go_tool.notified().await;
                if let Some(kit) = kit {
                    *answered.lock().await = Some(kit.close().await);
                }
                Ok(ToolOutput::Text("closed".to_owned()))
            }
        },
    );
    let model = ScriptedProvider::new([
        ScriptedRound::calling("", [("c1", "shut_down", "{}")]),
        ScriptedRound::answering("Done."),
    ]);
    let agent = AiAgent::new(channel("agent"), model)
        .with_tools(tools)
        .unwrap();
    kit.register_channel(agent).unwrap();
    let room_2 = RoomId::new("room_2").unwrap();
    for room in [room_id(), room_2] {
        kit.create_room(CreateRoomOptions {
            room_id: Some(room.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
        kit.attach_channel(&room, &channel("sms"), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    kit.attach_channel(
        &room_id(),
        &channel("agent"),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
}

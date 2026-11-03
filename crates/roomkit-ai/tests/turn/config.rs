//! A turn's own configuration (RFC Appendix A.9, per-turn settings), where
//! the shared scenarios (`conformance/scenarios/turn-config/`) cannot say
//! it: two turns at once, and a cost budget.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{ChannelBinding, EventType, InboundMessage, RoomContext, RoomId, TextContent};
use roomkit::store::ConversationStore as _;
use roomkit::testing::TurnTap;
use roomkit::{
    AttachChannelOptions, ChannelError, CreateRoomOptions, ProcessInboundOptions, RoomKit,
    UpdateBindingMetadataOptions,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiChannelTurnConfig, AiTool, ToolHandler, ToolResult,
};
use serde_json::{Value, json};
use tokio::sync::Notify;

use super::bounds::{Priced, loop_end_budgets};
use super::support::{Room, channel, every_row, room_id};

/// The tool each turn's configuration declares.
fn lookup() -> AiTool {
    AiTool::new("lookup", "Read this member's records").unwrap()
}

/// What a config provider answers: `config`, at once.
fn answered(
    config: AiChannelTurnConfig,
) -> Pin<Box<dyn Future<Output = Result<Option<AiChannelTurnConfig>, ChannelError>> + Send>> {
    Box::pin(async move { Ok(Some(config)) })
}

/// A round calling `lookup`.
fn call(id: &str) -> ScriptedRound {
    ScriptedRound::calling("", [(id, "lookup", "{}")])
}

/// A second room on `kit`, its SMS transport and its agent attached.
async fn second_room(kit: &RoomKit) -> RoomId {
    let room = RoomId::new("room_2").unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["sms", "assistant"] {
        kit.attach_channel(&room, &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    room
}

/// Alice's message by SMS in `room`, its turn run to its end.
async fn says_in(kit: &RoomKit, room: &RoomId, text: &str) {
    let message = InboundMessage::new(channel("sms"), "+15550001", TextContent::new(text));
    let options = ProcessInboundOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    };
    kit.process_inbound(message, options).await.unwrap();
}

/// What the calls of `room` answered, as its `TOOL_CALL_END` rows carry
/// them.
async fn results_in(kit: &RoomKit, room: &RoomId) -> Vec<String> {
    let rows = kit.store().list_events(room, every_row()).await.unwrap();
    rows.into_iter()
        .filter(|row| row.r#type == EventType::ToolCallEnd)
        .map(|row| serde_json::to_string(&row.content).unwrap())
        .collect()
}

// Python: tests/test_channels/test_turn_tool_handler.py::test_concurrent_turns_keep_their_resolved_handler
#[tokio::test]
async fn concurrent_turns_keep_their_resolved_handler() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let calls: Arc<Mutex<Vec<String>>> = Arc::default();
    let (entered_at, release_at, calls_at) = (
        Arc::clone(&entered),
        Arc::clone(&release),
        Arc::clone(&calls),
    );
    let provider = move |binding: ChannelBinding, _context: RoomContext| {
        let room = binding.room_id.to_string();
        let (entered, release, calls) = (
            Arc::clone(&entered_at),
            Arc::clone(&release_at),
            Arc::clone(&calls_at),
        );
        let serve = ToolHandler::new(move |_name, _arguments| {
            let (room, entered, release, calls) = (
                room.clone(),
                Arc::clone(&entered),
                Arc::clone(&release),
                Arc::clone(&calls),
            );
            async move {
                calls
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(room.clone());
                if room == "room_1" {
                    entered.notify_one();
                    tokio::time::timeout(Duration::from_secs(1), release.notified())
                        .await
                        .unwrap();
                } else {
                    release.notify_one();
                }
                Ok(ToolResult::Text(room))
            }
        });
        let config = AiChannelTurnConfig {
            tools: Some(vec![lookup()]),
            tool_handler: Some(serve),
            ..AiChannelTurnConfig::default()
        };
        answered(config)
    };
    let model = ScriptedProvider::new([
        call("call-1"),
        call("call-2"),
        ScriptedRound::answering("done"),
        ScriptedRound::answering("done"),
    ]);
    let options = AiChannelOptions {
        config_provider: Some(Arc::new(provider)),
        ..AiChannelOptions::default()
    };
    let agent = AiChannel::new(channel("assistant"), model, options).unwrap();
    let shared = agent.clone();
    let room = Room::with_agent(agent).await;
    let second = second_room(&room.kit).await;
    let fallback_calls = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&fallback_calls);
    let fallback = ToolHandler::new(move |_name, _arguments| {
        counted.fetch_add(1, Ordering::SeqCst);
        async { Ok(ToolResult::Text("wrong attach".to_owned())) }
    });
    let first = room_id();
    tokio::join!(says_in(&room.kit, &first, "Mine?"), async {
        entered.notified().await;
        shared.set_tool_handler(Some(fallback));
        says_in(&room.kit, &second, "Mine?").await;
    });
    let first_results = results_in(&room.kit, &room_id()).await;
    let second_results = results_in(&room.kit, &second).await;
    assert!(first_results[0].contains("room_1"), "{first_results:?}");
    assert!(second_results[0].contains("room_2"), "{second_results:?}");
    let recorded = calls.lock().unwrap_or_else(PoisonError::into_inner).clone();
    assert_eq!(recorded, ["room_1", "room_2"]);
    assert_eq!(fallback_calls.load(Ordering::SeqCst), 0);
    room.kit.close().await.unwrap();
}

/// The budgets the loop of a turn ran under, its channel's tokens 500 and
/// cost 0.5, `config` its configuration and `metadata` its binding's.
async fn budgets_under(
    config: AiChannelTurnConfig,
    metadata: Value,
) -> Result<(Value, Value), String> {
    let provider = move |_binding: ChannelBinding, _context: RoomContext| answered(config.clone());
    let model = ScriptedProvider::new([ScriptedRound::answering("Done.")]);
    let options = AiChannelOptions {
        turn_budget_tokens: Some(500),
        turn_budget_usd: Some(0.5),
        config_provider: Some(Arc::new(provider)),
        ..AiChannelOptions::default()
    };
    let agent = AiChannel::new(channel("assistant"), Priced(model), options).unwrap();
    let room = Room::with_agent(agent).await;
    if let Value::Object(metadata) = metadata {
        let options = UpdateBindingMetadataOptions::default();
        room.kit
            .update_binding_metadata(&room_id(), &channel("assistant"), metadata, options)
            .await
            .unwrap();
    }
    let tap = TurnTap::on(&room.kit);
    let result = room.says("Order 1?").await;
    let budgets = match result.error {
        None => Ok(loop_end_budgets(&tap)),
        Some(error) => Err(error.message),
    };
    room.kit.close().await.unwrap();
    budgets
}

// Python: tests/test_channels/test_turn_budget.py::test_a_null_never_lifts_the_channel_budget[turn-turn_budget_usd]
// and [turn-turn_budget_tokens], test_the_turn_config_and_the_room_outrank_the_channel
// (both budgets).
#[tokio::test]
async fn a_turn_config_s_budgets_sit_between_the_binding_s_and_the_channel_s() {
    let unset = AiChannelTurnConfig::default();
    let set = AiChannelTurnConfig {
        turn_budget_tokens: Some(300),
        turn_budget_usd: Some(0.25),
        ..AiChannelTurnConfig::default()
    };
    assert_eq!(
        budgets_under(unset, json!(null)).await,
        Ok((json!(500), json!(0.5)))
    );
    assert_eq!(
        budgets_under(set.clone(), json!(null)).await,
        Ok((json!(300), json!(0.25)))
    );
    let room = json!({ "turn_budget_tokens": 400, "turn_budget_usd": 0.4 });
    assert_eq!(budgets_under(set, room).await, Ok((json!(400), json!(0.4))));
    // A budget is a positive number at every level, as Python's
    // `turn_budget` checks the one the turn resolved.
    let zero = AiChannelTurnConfig {
        turn_budget_tokens: Some(0),
        ..AiChannelTurnConfig::default()
    };
    assert_eq!(
        budgets_under(zero, json!(null)).await,
        Err("turn_budget_tokens must be a positive number, got 0".to_owned())
    );
}

// Python: channels/_ai_context.py _build_context reads the binding's tools
// (`_turn_base_tools`) before its budgets (`_turn_budget`): a turn whose
// binding gets both wrong fails on its tools.
#[tokio::test]
async fn a_binding_s_tools_are_read_before_its_budgets() {
    let room = json!({ "tools": [{ "description": "x" }], "turn_budget_tokens": 0 });
    let read = budgets_under(AiChannelTurnConfig::default(), room).await;
    assert_eq!(read, Err("'name'".to_owned()));
}

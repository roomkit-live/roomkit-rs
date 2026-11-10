//! RFC §12.10.4 (RMK-1030): a model turn the close cuts, as its caller
//! and the log read it, and a tool that closes the kit from its own turn:
//! the turn is cut, the call awaiting `close()` runs on, and the open
//! call's end row is stored before the close returns. Python's
//! `tests/test_kit_close_doors.py` and `tests/test_kit_close.py`.
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{EventContent, EventType, RoomEvent, TextContent, ToolCallStatus};
use roomkit::{
    AttachChannelOptions, ConversationStore, HookExecution, HookOptions, HookTrigger,
    InMemoryStore, ProcessInboundOptions, RegenerateResponseOptions, RoomKit, SendEventOptions,
    TurnTimeouts,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiChannelOptions, AiResponseEvent, AiTool, ToolResult, Tools};
use serde_json::json;
use tokio::sync::Notify;

use crate::held::{Holding, attach_agent, closed, into, kit_of, room, said, spawn_close};
use crate::stream_witness::WaitingModel;
use crate::support::channel;
use crate::warnings::Warnings;

/// Whether the agent's last message in `r` is marked `cancelled`.
async fn cancelled<S: ConversationStore>(kit: &RoomKit<S>) -> bool {
    let query = roomkit::store::ListEventsOptions {
        limit: u64::MAX,
        ..roomkit::store::ListEventsOptions::default()
    };
    let events = kit.store().list_events(&room("r"), query).await.unwrap();
    events
        .iter()
        .rev()
        .find(|event| event.source.channel_id == channel("agent"))
        .is_some_and(|last| last.metadata.get("cancelled") == Some(&json!(true)))
}

/// The warnings and errors `logs` holds.
fn warned(logs: &Warnings) -> Vec<String> {
    logs.containing("")
}

/// RFC §12.10.4: a turn streaming as the kit closes is cut, what it said
/// stored marked `cancelled`, its caller reading no error and the turn's
/// end, `cancelled`, in its result's `response_metadata["turns"]`, and
/// nothing is logged as a warning: a failure the close causes is no
/// failure (Python's `test_a_room_turn_returns_cancelled_to_its_caller`,
/// RMK-1037).
/// The call is awaited, deferred, a `send_event`, or `regenerate_response`.
#[tokio::test(start_paused = true)]
async fn a_room_turn_returns_cancelled_to_its_caller() {
    let mut rows = Vec::new();
    for door in ["awaited", "deferred", "send_event", "regenerate_response"] {
        let (logs, logging) = Warnings::capture();
        let kit = kit_of(InMemoryStore::default(), &[&Holding::new("ws", [])], &["r"]).await;
        let model = WaitingModel::new("The ", door == "regenerate_response");
        attach_agent(&kit, model.clone(), Tools::new()).await;
        if door == "regenerate_response" {
            kit.process_inbound(said("Go."), into("r")).await.unwrap();
            roomkit::testing::idle(&kit).await;
        }
        let responses = response_count(&kit);
        let call = tokio::spawn({
            let kit = kit.clone();
            async move {
                if door == "send_event" {
                    let (content, sms) = (TextContent::new("Go."), channel("sms"));
                    let options = SendEventOptions::default();
                    let sent = kit.send_event(&room("r"), &sms, content, options).await;
                    return (sent.is_ok(), None, None);
                }
                let options = ProcessInboundOptions {
                    defer_delivery: door == "deferred",
                    ..into("r")
                };
                let mut result = if door == "regenerate_response" {
                    kit.regenerate_response(&room("r"), RegenerateResponseOptions::default())
                        .await
                        .unwrap()
                        .unwrap()
                } else {
                    kit.process_inbound(said("Go."), options).await.unwrap()
                };
                if let Some(delivery) = result.delivery.take() {
                    result = delivery.wait().await.unwrap();
                }
                let turns = result.response_metadata.get("turns").unwrap_or_default();
                let ended = turns["agent"]["loop_end_reason"]
                    .as_str()
                    .map(str::to_owned);
                (true, result.error.map(|error| error.error_type), ended)
            }
        });
        tokio::time::timeout(Duration::from_secs(1), model.started.notified())
            .await
            .unwrap();

        closed(spawn_close(&kit)).await;

        let ended = tokio::time::timeout(Duration::from_secs(5), call)
            .await
            .unwrap()
            .unwrap();
        drop(logging);
        let cancelled = cancelled(&kit).await;
        assert!(model.dropped.load(Ordering::SeqCst));
        assert_eq!(responses.load(Ordering::SeqCst), 0);
        rows.push((door, ended, cancelled, warned(&logs)));
    }
    let row =
        |door, ended: Option<&str>| (door, (true, None, ended.map(str::to_owned)), true, vec![]);
    assert_eq!(
        rows,
        [
            row("awaited", Some("cancelled")),
            row("deferred", Some("cancelled")),
            row("send_event", None),
            row("regenerate_response", Some("cancelled"))
        ]
    );
}

/// Count only response hooks installed after any completed warmup turn.
fn response_count(kit: &RoomKit) -> Arc<AtomicUsize> {
    let count = Arc::new(AtomicUsize::new(0));
    let heard = Arc::clone(&count);
    kit.hook(
        HookTrigger::OnAiResponse,
        move |_event: AiResponseEvent, _| {
            let heard = Arc::clone(&heard);
            async move {
                heard.fetch_add(1, Ordering::SeqCst);
            }
        },
        HookOptions {
            execution: HookExecution::Async,
            ..HookOptions::default()
        },
    )
    .unwrap();
    count
}

/// The tool-call end rows `r` stores, each with its status and error.
pub(super) async fn ends<S: ConversationStore>(
    kit: &RoomKit<S>,
) -> Vec<(ToolCallStatus, Option<String>)> {
    let query = roomkit::store::ListEventsOptions {
        limit: u64::MAX,
        ..roomkit::store::ListEventsOptions::default()
    };
    let events = kit.store().list_events(&room("r"), query).await.unwrap();
    events
        .into_iter()
        .filter(|event| event.r#type == EventType::ToolCallEnd)
        .filter_map(|event| {
            let EventContent::ToolCall(call) = event.content else {
                return None;
            };
            Some((call.status, call.error))
        })
        .collect()
}

/// RFC §12.10.4: a tool awaits `close()` in its own turn, then runs on:
/// its turn, cut and let go by its executor, watches its owner no more, so
/// nothing is logged once the close returned, not an owner fallen silent
/// (Python's `test_a_tool_awaiting_the_close_runs_on_and_logs_nothing_after_it`).
#[tokio::test(start_paused = true)]
async fn a_tool_awaiting_the_close_runs_on_and_logs_nothing_after_it() {
    let kit = kit_of(InMemoryStore::default(), &[&Holding::new("ws", [])], &["r"]).await;
    let timeouts = TurnTimeouts {
        heartbeat_interval: Duration::from_millis(10),
        heartbeat_timeout: Duration::from_millis(50),
        ..TurnTimeouts::default()
    };
    roomkit::testing::set_turn_timeouts(&kit, timeouts);
    let (closed_now, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let (weak, telling, waiting) = (
        kit.downgrade(),
        Arc::clone(&closed_now),
        Arc::clone(&release),
    );
    let tools = Tools::new().tool(
        AiTool {
            parameters: json!({ "type": "object" }),
            ..AiTool::new("quit", "Quit.").unwrap()
        },
        move |_call, _context| {
            let (weak, telling, waiting) =
                (weak.clone(), Arc::clone(&telling), Arc::clone(&waiting));
            async move {
                if let Some(kit) = weak.upgrade() {
                    drop(kit.close().await);
                }
                telling.notify_one();
                waiting.notified().await;
                Ok(ToolResult::Text("closed".to_owned()))
            }
        },
    );
    let model = ScriptedProvider::new([
        ScriptedRound::calling("", [("c1", "quit", "{}")]),
        ScriptedRound::answering("after"),
    ]);
    attach_agent(&kit, model, tools).await;
    let call = tokio::spawn({
        let kit = kit.clone();
        async move { kit.process_inbound(said("Go."), into("r")).await }
    });
    tokio::time::timeout(Duration::from_secs(10), closed_now.notified())
        .await
        .unwrap();
    let (logs, logging) = Warnings::capture();
    // Past the owner's heartbeat timeout.
    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(logging);
    let after = warned(&logs);
    release.notify_one();
    drop(tokio::time::timeout(Duration::from_secs(5), call).await);
    roomkit::testing::idle(&kit).await;

    assert_eq!(after, Vec::<String>::new());
}

/// An agent in `r` whose round runs `a_close`, which closes the kit once
/// `entered` is told, beside `b_send`, which sends a note through its turn
/// and notes what that verb came to in `notes`; a SYNC hook holds the
/// note's pass a second, telling `entered`.
async fn sibling_agent(kit: &RoomKit, notes: &Arc<Mutex<Vec<String>>>) {
    use roomkit::model::HookResult;
    use roomkit::model::hook::AllowOptions;
    use roomkit::{HookTrigger, current_turn_host};

    let entered = Arc::new(Notify::new());
    let told = Arc::clone(&entered);
    let spec = HookOptions {
        name: "slow_note".to_owned(),
        ..Default::default()
    };
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |event: RoomEvent, _context| {
            let told = Arc::clone(&told);
            let ours = roomkit_core::rules::transcoding::text_of(&event.content) == "note";
            async move {
                if ours {
                    told.notify_one();
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Ok(HookResult::allow(AllowOptions::default()))
            }
        },
        spec,
    )
    .unwrap();
    let (weak, noting_a, noting_b) = (kit.downgrade(), Arc::clone(notes), Arc::clone(notes));
    let tools = Tools::new()
        .tool(
            AiTool {
                parameters: json!({ "type": "object" }),
                ..AiTool::new("a_close", "Close.").unwrap()
            },
            move |_call, _context| {
                let (weak, waiting, noting) =
                    (weak.clone(), Arc::clone(&entered), Arc::clone(&noting_a));
                async move {
                    waiting.notified().await;
                    if let Some(kit) = weak.upgrade() {
                        let closed = kit.close().await;
                        noting.lock().unwrap().push(format!("a closed {closed:?}"));
                    }
                    Ok(ToolResult::Text("closed".to_owned()))
                }
            },
        )
        .tool(
            AiTool {
                parameters: json!({ "type": "object" }),
                ..AiTool::new("b_send", "Send.").unwrap()
            },
            move |_call, context| {
                let noting = Arc::clone(&noting_b);
                async move {
                    let turn = current_turn_host().unwrap();
                    let note = TextContent::new("note");
                    let read = match turn
                        .send_event(
                            &context.room_id,
                            &channel("agent"),
                            note,
                            SendEventOptions::default(),
                        )
                        .await
                    {
                        Ok(_) => "sent".to_owned(),
                        Err(roomkit::RoomKitError::TurnLink { reason, .. }) => reason,
                        Err(roomkit::RoomKitError::RemoteTurn { kind, .. }) => kind,
                        Err(error) => format!("{error:?}"),
                    };
                    noting.lock().unwrap().push(format!("b {read}"));
                    Ok(ToolResult::Text("sent".to_owned()))
                }
            },
        );
    let model = ScriptedProvider::new([
        ScriptedRound::calling("", [("c1", "a_close", "{}"), ("c2", "b_send", "{}")]),
        ScriptedRound::answering("Done."),
    ]);
    let agent = roomkit_ai::AiChannel::new(
        channel("agent"),
        model,
        AiChannelOptions {
            tools: tools.into(),
            tool_timeout: None,
            ..AiChannelOptions::default()
        },
    )
    .unwrap();
    kit.register_channel(agent).unwrap();
    kit.attach_channel(
        &room("r"),
        &channel("agent"),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
}

/// RFC §12.10.4, §10.6.8: a tool awaits `close()` in its turn while its
/// sibling in the same model round awaits a turn verb its owner still
/// runs, a SYNC hook holding it: the turn is let go, and the sibling's verb
/// fails at once, `owner_lost`, as the Python implementation's
/// `TurnLinkError` reads (`rmk1030-rs/pass1/py_sibling_verb.out`); both
/// calls get their end row, `failed`, `kit_closed`, the verb's event is not
/// stored, and the kit's work ends with the close. On 01c8420 the sibling
/// waited for an answer nobody read any more, its executor outliving the
/// close (`rmk1030-rs-review-A/probe6.out`).
#[tokio::test(start_paused = true)]
async fn a_sibling_verb_awaiting_its_owner_as_a_tool_closes_the_kit_fails_owner_lost() {
    let kit = kit_of(InMemoryStore::default(), &[&Holding::new("ws", [])], &["r"]).await;
    let notes: Arc<Mutex<Vec<String>>> = Arc::default();
    sibling_agent(&kit, &notes).await;
    let started = tokio::time::Instant::now();
    let call = tokio::time::timeout(
        Duration::from_secs(30),
        kit.process_inbound(said("Go."), into("r")),
    )
    .await
    .unwrap()
    .unwrap();
    tokio::time::timeout(Duration::from_secs(30), kit.close())
        .await
        .unwrap()
        .unwrap();
    let idle = tokio::time::timeout(Duration::from_secs(60), roomkit::testing::idle(&kit))
        .await
        .is_ok();
    let mut notes = notes.lock().unwrap().clone();
    notes.sort();
    assert_eq!(
        (
            call.cancellation_reason.as_deref(),
            (idle, started.elapsed() < Duration::from_secs(1)),
            notes,
            ends(&kit).await,
            crate::held::stored(&kit, "r")
                .await
                .contains(&"note".to_owned()),
        ),
        (
            Some("kit_closed"),
            (true, true),
            vec!["a closed Ok(())".to_owned(), "b owner_lost".to_owned()],
            vec![(ToolCallStatus::Failed, Some("kit_closed".to_owned())); 2],
            false,
        )
    );
}

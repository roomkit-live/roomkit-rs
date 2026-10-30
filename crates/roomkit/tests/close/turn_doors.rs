//! RFC §12.10.4 (RMK-1030): a model turn the close cuts, as its caller
//! and the log read it, and a tool that closes the kit from its own turn:
//! the turn is cut, the call awaiting `close()` runs on, and the open
//! call's end row is stored before the close returns. Python's
//! `tests/test_kit_close_doors.py` and `tests/test_kit_close.py`.
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{EventContent, EventType, TextContent, ToolCallStatus};
use roomkit::{
    ConversationStore, EventQuery, InMemoryStore, ProcessInboundOptions, RoomKit, SendEventOptions,
    TurnTimeouts, keep_context,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{ToolDefinition, ToolOutput, Tools};
use serde_json::json;
use tokio::sync::Notify;

use crate::held::{Holding, Pausing, attach_agent, closed, into, kit_of, room, said, spawn_close};
use crate::support::channel;
use crate::warnings::Warnings;

/// Whether the agent's last message in `r` is marked `cancelled`.
async fn cancelled<S: ConversationStore>(kit: &RoomKit<S>) -> bool {
    let query = EventQuery {
        limit: u64::MAX,
        ..EventQuery::default()
    };
    let events = kit.store().list_events(&room("r"), &query).await.unwrap();
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
/// stored marked `cancelled`, its caller reading no error, and nothing is
/// logged as a warning: a failure the close causes is no failure (Python's
/// `test_a_room_turn_returns_cancelled_to_its_caller`, whose caller reads
/// the turn's record in its result's `response_metadata`, which Rust's has
/// not).
/// The call is awaited, deferred, or a `send_event`.
#[tokio::test(start_paused = true)]
async fn a_room_turn_returns_cancelled_to_its_caller() {
    let mut rows = Vec::new();
    for door in ["awaited", "deferred", "send_event"] {
        let (logs, logging) = Warnings::capture();
        let kit = kit_of(InMemoryStore::default(), &[&Holding::new("ws", [])], &["r"]).await;
        attach_agent(&kit, Pausing { first: "The " }, Tools::new()).await;
        let call = tokio::spawn({
            let kit = kit.clone();
            async move {
                if door == "send_event" {
                    let (content, sms) = (TextContent::new("Go."), channel("sms"));
                    let options = SendEventOptions::default();
                    let sent = kit.send_event(&room("r"), &sms, content, options).await;
                    return (sent.is_ok(), None);
                }
                let options = ProcessInboundOptions {
                    defer_delivery: door == "deferred",
                    ..into("r")
                };
                let mut result = kit.process_inbound(said("Go."), options).await.unwrap();
                if let Some(delivery) = result.delivery.take() {
                    result = delivery.wait().await.unwrap();
                }
                (true, result.error.map(|error| error.error_type))
            }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;

        closed(spawn_close(&kit)).await;

        let ended = tokio::time::timeout(Duration::from_secs(5), call)
            .await
            .unwrap()
            .unwrap();
        drop(logging);
        let cancelled = cancelled(&kit).await;
        rows.push((door, ended, cancelled, warned(&logs)));
    }
    let row = |door| (door, (true, None), true, vec![]);
    assert_eq!(rows, [row("awaited"), row("deferred"), row("send_event")]);
}

/// The tool-call end rows `r` stores, each with its status and error.
async fn ends<S: ConversationStore>(kit: &RoomKit<S>) -> Vec<(ToolCallStatus, Option<String>)> {
    let query = EventQuery {
        limit: u64::MAX,
        ..EventQuery::default()
    };
    let events = kit.store().list_events(&room("r"), &query).await.unwrap();
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

/// How a tool closes the kit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum How {
    /// It awaits `close()` itself.
    Itself,
    /// It awaits a task of its own, spawned with `keep_context`, that
    /// closes the kit.
    FromATask,
}

/// Notes a tool call dropped before it ended.
struct ToolNote(Arc<Mutex<Vec<String>>>);

impl Drop for ToolNote {
    fn drop(&mut self) {
        self.0.lock().unwrap().push("cut".to_owned());
    }
}

/// An agent in `r` whose one tool closes the kit as `how` says, noting in
/// `steps` how its close ended and, once it returned, the end rows the
/// room stores then.
async fn closing_agent(kit: &RoomKit, how: How, steps: &Arc<Mutex<Vec<String>>>) {
    let (weak, steps) = (kit.downgrade(), Arc::clone(steps));
    let tools = Tools::new().tool(
        ToolDefinition::new("shut_down", "Shut down.", json!({ "type": "object" })),
        move |_call, _context| {
            let (weak, steps) = (weak.clone(), Arc::clone(&steps));
            async move {
                let Some(kit) = weak.upgrade() else {
                    return Ok(ToolOutput::Text("gone".to_owned()));
                };
                let mut note = Some(ToolNote(Arc::clone(&steps)));
                let closed = match how {
                    How::Itself => kit.close().await,
                    How::FromATask => {
                        let kit = kit.clone();
                        tokio::spawn(keep_context(async move { kit.close().await }))
                            .await
                            .unwrap()
                    }
                };
                std::mem::forget(note.take());
                let rows = ends(&kit).await;
                steps
                    .lock()
                    .unwrap()
                    .push(format!("closed {:?} {rows:?}", closed.is_ok()));
                Ok(ToolOutput::Text("closed".to_owned()))
            }
        },
    );
    let model = ScriptedProvider::new([
        ScriptedRound::calling("", [("c1", "shut_down", "{}")]),
        ScriptedRound::answering("Done."),
    ]);
    attach_agent(kit, model, tools).await;
}

/// RFC §12.10.4: a tool closes the kit from its own turn, or from a task
/// it starts and awaits: the close cuts that turn as any other, the open
/// call's end row is stored before the close returns, `failed` with the
/// error `kit_closed`, no warning is logged, and the call reads its cut,
/// `kit_closed`, never that cut as its error. The tool's own close
/// returns and the tool runs on; a tool awaiting a close it started on a
/// task of its own is cut with its turn, the close running on (Python's
/// `test_a_tool_closing_the_kit_gets_its_end_row_before_the_close_returns`).
/// On fefbfac the turn was spared, its end row refused once the kit was
/// sealed.
#[tokio::test(start_paused = true)]
async fn a_tool_closing_the_kit_gets_its_end_row_before_the_close_returns() {
    let mut rows = Vec::new();
    for how in [How::Itself, How::FromATask] {
        let (logs, logging) = Warnings::capture();
        let kit = kit_of(InMemoryStore::default(), &[&Holding::new("ws", [])], &["r"]).await;
        let steps = Arc::new(Mutex::new(Vec::new()));
        closing_agent(&kit, how, &steps).await;

        let result = tokio::time::timeout(
            Duration::from_secs(10),
            kit.process_inbound(said("Go."), into("r")),
        )
        .await
        .unwrap()
        .unwrap();
        tokio::time::timeout(Duration::from_secs(10), kit.close())
            .await
            .unwrap()
            .unwrap();
        roomkit::testing::idle(&kit).await;
        drop(logging);
        let steps = steps.lock().unwrap().clone();

        rows.push((
            how,
            steps,
            (
                result.cancellation_reason,
                result.error.map(|error| error.error_type),
            ),
            ends(&kit).await,
            warned(&logs),
        ));
    }
    let end = || vec![(ToolCallStatus::Failed, Some("kit_closed".to_owned()))];
    let kit_closed = || (Some("kit_closed".to_owned()), None);
    assert_eq!(
        rows,
        [
            (
                How::Itself,
                vec![format!("closed true {:?}", end())],
                kit_closed(),
                end(),
                vec![],
            ),
            (
                How::FromATask,
                vec!["cut".to_owned()],
                kit_closed(),
                end(),
                vec![]
            ),
        ]
    );
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
        ToolDefinition::new("quit", "Quit.", json!({ "type": "object" })),
        move |_call, _context| {
            let (weak, telling, waiting) =
                (weak.clone(), Arc::clone(&telling), Arc::clone(&waiting));
            async move {
                if let Some(kit) = weak.upgrade() {
                    drop(kit.close().await);
                }
                telling.notify_one();
                waiting.notified().await;
                Ok(ToolOutput::Text("closed".to_owned()))
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

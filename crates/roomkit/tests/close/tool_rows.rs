//! RFC §12.10.4: a tool's terminal row precedes store release on every local lane.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{EventType, ToolCallStatus};
use roomkit::{InboundResult, RoomKit, RoomKitError, keep_context};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiTool, ToolResult, Tools};
use serde_json::json;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::held::{Hold, Holding, attach_agent, into, kit_of, said};
use crate::store::{CommittedRow, StuckStore};
use crate::turn_doors::ends;
use crate::warnings::Warnings;

#[derive(Clone, Copy, Debug, PartialEq)]
enum How {
    Itself,
    FromATask,
}

type InboundCall = JoinHandle<Result<InboundResult, RoomKitError>>;

#[derive(Clone, Default)]
struct Trace {
    steps: Arc<Mutex<Vec<&'static str>>>,
    ended: Arc<Notify>,
    callers: Arc<Mutex<Vec<InboundCall>>>,
}

struct ToolNote {
    trace: Trace,
    cut: bool,
}

impl Drop for ToolNote {
    fn drop(&mut self) {
        if self.cut {
            self.trace.steps.lock().unwrap().push("cut");
        }
        self.trace.ended.notify_one();
    }
}

async fn hold_lane(kit: &RoomKit<StuckStore>, target: &Holding, trace: &Trace) {
    // An independent inbound call occupies the lane; the tool waits only
    // for its transport to start, not for that call's result.
    let kit = kit.clone();
    let call = tokio::spawn(async move { kit.process_inbound(said("hold"), into("r")).await });
    trace.callers.lock().unwrap().push(call);
    tokio::time::timeout(Duration::from_secs(1), target.entered("hold"))
        .await
        .unwrap();
}

async fn tool_closes(
    kit: RoomKit<StuckStore>,
    how: How,
    busy: bool,
    target: Holding,
    trace: Trace,
) {
    let mut note = ToolNote {
        trace: trace.clone(),
        cut: true,
    };
    if busy {
        hold_lane(&kit, &target, &trace).await;
    }
    match how {
        How::Itself => kit.close().await.unwrap(),
        How::FromATask => {
            let closer = kit.clone();
            tokio::spawn(keep_context(async move { closer.close().await }))
                .await
                .unwrap()
                .unwrap();
        }
    }
    // The positive witness is taken inside the tool, at close's return.
    assert_eq!(
        ends(&kit).await,
        [(ToolCallStatus::Failed, Some("kit_closed".to_owned()))]
    );
    trace.steps.lock().unwrap().push("closed");
    note.cut = false;
}

async fn closing_agent(
    kit: &RoomKit<StuckStore>,
    how: How,
    busy: bool,
    target: &Holding,
    trace: &Trace,
) {
    let (weak, target, trace) = (kit.downgrade(), target.clone(), trace.clone());
    let tools = Tools::new().tool(
        AiTool {
            parameters: json!({"type":"object"}),
            ..AiTool::new("shutdown", "Shut down.").unwrap()
        },
        move |_, _| {
            let (weak, target, trace) = (weak.clone(), target.clone(), trace.clone());
            async move {
                tool_closes(weak.upgrade().unwrap(), how, busy, target, trace).await;
                Ok(ToolResult::Text("closed".to_owned()))
            }
        },
    );
    let model = ScriptedProvider::new([
        ScriptedRound::calling("", [("c1", "shutdown", "{}")]),
        ScriptedRound::answering("Done."),
    ]);
    attach_agent(kit, model, tools).await;
}

async fn a_terminal_row(how: How, busy: bool, slow: bool) {
    let target = Holding::new("ws", [("hold", Hold::ForEver)]);
    let kit = kit_of(StuckStore::default(), &[&target], &["r"]).await;
    if slow {
        *kit.store().cursor_delay.lock().unwrap() = Some(Duration::from_millis(10));
    }
    let trace = Trace::default();
    closing_agent(&kit, how, busy, &target, &trace).await;
    let (logs, _capture) = Warnings::capture();
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
    tokio::time::timeout(Duration::from_secs(1), trace.ended.notified())
        .await
        .unwrap();
    drain_callers(&kit, &trace).await;
    assert_eq!(
        (result.cancellation_reason.as_deref(), result.error),
        (Some("kit_closed"), None)
    );
    assert_eq!(
        *trace.steps.lock().unwrap(),
        [if how == How::Itself { "closed" } else { "cut" }]
    );
    assert_eq!(
        ends(&kit).await,
        [(ToolCallStatus::Failed, Some("kit_closed".to_owned()))]
    );
    assert_commits(&kit, how, busy, slow);
    assert_eq!(
        logs.containing(""),
        Vec::<String>::new(),
        "{how:?}, busy={busy}, slow={slow}"
    );
}

async fn drain_callers(kit: &RoomKit<StuckStore>, trace: &Trace) {
    let callers = std::mem::take(&mut *trace.callers.lock().unwrap());
    for caller in callers {
        tokio::time::timeout(Duration::from_secs(1), caller)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(1), roomkit::testing::idle(kit))
        .await
        .unwrap();
}

fn assert_commits(kit: &RoomKit<StuckStore>, how: How, busy: bool, slow: bool) {
    let commits = kit.store().commits.lock().unwrap().clone();
    assert!(
        commits.iter().all(|row| !row.after_close),
        "{how:?}, busy={busy}, slow={slow}: {commits:?}"
    );
    assert_eq!(
        commits
            .into_iter()
            .filter(|row| row.kind == EventType::ToolCallEnd)
            .collect::<Vec<_>>(),
        [CommittedRow {
            kind: EventType::ToolCallEnd,
            after_close: false,
            error: Some("kit_closed".to_owned())
        }]
    );
    assert_eq!(kit.store().closes.lock().unwrap().len(), 1);
    assert_eq!(kit.store().delayed_cursors.load(Ordering::SeqCst) > 0, slow);
}

// Python: tests/test_kit_close_doors.py::test_a_tool_closing_the_kit_gets_its_end_row_before_the_close_returns
// The eight memory/slow-cursor variants; SQLite remains a separate store gate.
#[tokio::test(start_paused = true)]
async fn a_tool_closing_the_kit_gets_its_end_row_before_store_release() {
    for how in [How::Itself, How::FromATask] {
        for busy in [false, true] {
            for slow in [false, true] {
                a_terminal_row(how, busy, slow).await;
            }
        }
    }
}

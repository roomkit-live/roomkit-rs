//! RFC §12.10.4: an externally cut tool leaves no heartbeat or late write.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{EventContent, EventType, ToolCallStatus};
use roomkit::{ConversationStore, InMemoryStore, TurnTimeouts};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiTool, ToolResult, Tools};
use serde_json::json;
use tokio::sync::Notify;

use crate::held::{Holding, attach_agent, into, kit_of, room, said};
use crate::warnings::Warnings;

struct Dropped(Arc<AtomicBool>);

impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

fn held_tool(entered: Arc<Notify>, dropped: Arc<AtomicBool>) -> Tools {
    Tools::new().tool(
        AiTool {
            parameters: json!({"type": "object"}),
            ..AiTool::new("lookup", "Look up.").unwrap()
        },
        move |_call, _context| {
            let (entered, dropped) = (Arc::clone(&entered), Arc::clone(&dropped));
            async move {
                let _note = Dropped(dropped);
                entered.notify_one();
                std::future::pending::<()>().await;
                Ok(ToolResult::Text("late".to_owned()))
            }
        },
    )
}

// Python: tests/test_kit_close.py::test_a_turn_left_running_logs_nothing_once_the_close_returned
// Local Tokio counterpart: Drop confirms cancellation. A remote executor
// ignoring its cut is a separate integration contract, not simulated here.
#[tokio::test(start_paused = true)]
async fn an_external_close_cuts_the_tool_and_leaves_no_late_write_or_warning() {
    let target = Holding::new("ws", []);
    let kit = kit_of(InMemoryStore::default(), &[&target], &["r"]).await;
    roomkit::testing::set_turn_timeouts(
        &kit,
        TurnTimeouts {
            heartbeat_interval: Duration::from_millis(10),
            heartbeat_timeout: Duration::from_millis(50),
            ..Default::default()
        },
    );
    let (entered, dropped) = (Arc::new(Notify::new()), Arc::new(AtomicBool::new(false)));
    let model = ScriptedProvider::new([
        ScriptedRound::calling("", [("c1", "lookup", "{}")]),
        ScriptedRound::answering("late answer"),
    ]);
    attach_agent(
        &kit,
        model,
        held_tool(Arc::clone(&entered), Arc::clone(&dropped)),
    )
    .await;
    let call = tokio::spawn({
        let kit = kit.clone();
        async move { kit.process_inbound(said("Go."), into("r")).await }
    });
    entered.notified().await;
    kit.close().await.unwrap();
    let (logs, _capture) = Warnings::capture();
    assert!(dropped.load(Ordering::SeqCst));
    let result = call.await.unwrap().unwrap();
    assert_eq!(result.cancellation_reason.as_deref(), Some("kit_closed"));
    let query = roomkit::store::ListEventsOptions {
        limit: u64::MAX,
        ..Default::default()
    };
    let before = kit
        .store()
        .list_events(&room("r"), query.clone())
        .await
        .unwrap();
    let ends: Vec<_> = before
        .iter()
        .filter(|event| event.r#type == EventType::ToolCallEnd)
        .map(|event| {
            let EventContent::ToolCall(call) = &event.content else {
                panic!("tool end payload")
            };
            (call.status, call.error.as_deref())
        })
        .collect();
    assert_eq!(ends, [(ToolCallStatus::Failed, Some("kit_closed"))]);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(logs.containing(""), Vec::<String>::new());
    assert_eq!(
        kit.store().list_events(&room("r"), query).await.unwrap(),
        before
    );
    assert_eq!(target.sent(), ["Go.", "[Tool call: lookup]"]);
}

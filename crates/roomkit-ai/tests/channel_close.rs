//! Channel shutdown, before its host resources close.
//! Python: `tests/test_channel_close_turns.py` and `tests/skills/test_script_executor.py`.
#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

#[path = "channel_close/arguments.rs"]
mod arguments;
#[path = "channel_close/before_calls.rs"]
mod before_calls;
#[path = "channel_close/gates.rs"]
mod gates;
#[path = "channel_close/joiners.rs"]
mod joiners;
#[path = "channel_close/kit.rs"]
mod kit;
#[path = "channel_close/provider.rs"]
mod provider;
use roomkit::Channel;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelType, EventSource, Room, RoomContext,
    RoomEvent, RoomId, TextContent,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiContext, AiProvider, AiResponse, AiTool, ProviderError,
    ProviderStream, ToolHandler, ToolResult,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

type Trace = Arc<Mutex<Vec<&'static str>>>;
#[derive(Clone)]
struct Model {
    inner: ScriptedProvider,
    streaming: bool,
    trace: Trace,
}
impl AiProvider for Model {
    fn model_name(&self) -> &'static str {
        "witness"
    }
    fn supports_structured_streaming(&self) -> bool {
        self.streaming
    }
    async fn generate(&self, context: &AiContext) -> Result<AiResponse, ProviderError> {
        self.inner.generate(context).await
    }
    fn generate_structured_stream<'a>(&'a self, context: &'a AiContext) -> ProviderStream<'a> {
        self.inner.generate_structured_stream(context)
    }
    async fn close(&self) {
        self.trace.lock().unwrap().push("provider closed");
    }
}
struct Dropped(Trace);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.lock().unwrap().push("call ended");
    }
}

async fn turn<P: AiProvider + 'static>(agent: AiChannel<P>) {
    let room = RoomId::new("r").unwrap();
    let event = RoomEvent::new(
        room.clone(),
        EventSource::new(ChannelId::new("web").unwrap(), ChannelType::WEBHOOK),
        TextContent::new("Go"),
    );
    let binding = ChannelBinding {
        category: ChannelCategory::Intelligence,
        ..ChannelBinding::new(ChannelId::new("ai").unwrap(), room.clone(), ChannelType::AI)
    };
    agent
        .on_event(event, binding, RoomContext::new(Room::new(room)))
        .await
        .unwrap();
}

async fn external_close(streaming: bool) {
    let trace: Trace = Arc::default();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let model = Model {
        inner: ScriptedProvider::new([
            ScriptedRound::calling("", [("c1", "held", "{}")]),
            ScriptedRound::answering("should not be asked"),
        ]),
        streaming,
        trace: Arc::clone(&trace),
    };
    let handler = {
        let (trace, started, release) = (
            Arc::clone(&trace),
            Arc::clone(&started),
            Arc::clone(&release),
        );
        ToolHandler::new(move |_, _| {
            let (trace, started, release) = (
                Arc::clone(&trace),
                Arc::clone(&started),
                Arc::clone(&release),
            );
            async move {
                let _dropped = Dropped(trace);
                started.notify_one();
                release.notified().await;
                Ok(ToolResult::Text("late".to_owned()))
            }
        })
    };
    let agent = AiChannel::new(
        ChannelId::new("ai").unwrap(),
        model.clone(),
        AiChannelOptions {
            tools: vec![AiTool::new("held", "Wait").unwrap().into()],
            tool_handler: Some(handler),
            ..AiChannelOptions::default()
        },
    )
    .unwrap();
    let task = tokio::spawn(turn(agent.clone()));
    tokio::time::timeout(Duration::from_secs(1), started.notified())
        .await
        .unwrap();
    assert_eq!(agent.active_turns(), 1);
    tokio::time::timeout(Duration::from_secs(1), agent.close())
        .await
        .unwrap();
    let at_close = trace.lock().unwrap().clone();
    let active_at_close = agent.active_turns();
    // Release and join even on the broken baseline, before asserting the oracle.
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
    let rounds = model.inner.requests().len();
    assert_eq!(at_close, ["call ended", "provider closed"]);
    assert_eq!(active_at_close, 0);
    assert_eq!(rounds, 1);
}

// Python: tests/skills/test_script_executor.py::test_a_channel_close_ends_its_script_before_closing_the_executor
#[tokio::test]
async fn close_cuts_active_tool_before_provider_plain() {
    external_close(false).await;
}
#[tokio::test]
async fn close_cuts_active_tool_before_provider_streaming() {
    external_close(true).await;
}

struct Ended(Arc<Notify>);
impl Drop for Ended {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

// Python: tests/test_channel_close_turns.py::test_a_handler_closing_its_channel_spares_itself_not_its_round
#[tokio::test]
async fn own_handler_survives_but_its_sibling_is_cut_and_no_round_follows() {
    own_close(false).await;
    own_close(true).await;
}

async fn own_close(from_child: bool) {
    let trace: Trace = Arc::default();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let ended = Arc::new(Notify::new());
    let returned = Arc::new(Notify::new());
    let slot: Arc<Mutex<Option<AiChannel<Model>>>> = Arc::default();
    let model = Model {
        inner: ScriptedProvider::new([
            ScriptedRound::calling("", [("h1", "hangup", "{}"), ("c1", "held", "{}")]),
            ScriptedRound::answering("should not be asked"),
        ]),
        streaming: false,
        trace: Arc::clone(&trace),
    };
    let handler = {
        let (started, release, ended, returned, slot) = (
            Arc::clone(&started),
            Arc::clone(&release),
            Arc::clone(&ended),
            Arc::clone(&returned),
            Arc::clone(&slot),
        );
        ToolHandler::new(move |name, _| {
            let (started, release, ended, returned, slot) = (
                Arc::clone(&started),
                Arc::clone(&release),
                Arc::clone(&ended),
                Arc::clone(&returned),
                Arc::clone(&slot),
            );
            async move {
                if name == "hangup" {
                    started.notified().await;
                    let agent = slot.lock().unwrap().as_ref().unwrap().clone();
                    if from_child {
                        tokio::spawn(roomkit::keep_context(async move { agent.close().await }))
                            .await
                            .unwrap();
                    } else {
                        agent.close().await;
                    }
                    returned.notify_one();
                    Ok(ToolResult::Text("closed by myself".to_owned()))
                } else {
                    let _ended = Ended(ended);
                    started.notify_one();
                    release.notified().await;
                    Ok(ToolResult::Text("late sibling".to_owned()))
                }
            }
        })
    };
    let agent = AiChannel::new(
        ChannelId::new("ai").unwrap(),
        model.clone(),
        AiChannelOptions {
            tools: vec![
                AiTool::new("hangup", "Close").unwrap().into(),
                AiTool::new("held", "Wait").unwrap().into(),
            ],
            tool_handler: Some(handler),
            ..AiChannelOptions::default()
        },
    )
    .unwrap();
    *slot.lock().unwrap() = Some(agent.clone());
    let (kit, reports) = kit::room(agent).await;
    let task = tokio::spawn(kit::says(kit.clone()));
    let own_survived = tokio::time::timeout(Duration::from_secs(1), returned.notified())
        .await
        .is_ok();
    let sibling_cut = tokio::time::timeout(Duration::from_secs(3), ended.notified())
        .await
        .is_ok();
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
    *slot.lock().unwrap() = None;
    let rounds = model.inner.requests().len();
    assert!(own_survived);
    assert!(sibling_cut);
    assert_eq!(rounds, 1);
    roomkit::testing::idle(&kit).await;
    assert_own_reports(&reports.lock().unwrap());
    let rows = kit::rows(&kit).await;
    assert_eq!(rows.len(), 2);
    assert!(rows.contains(&roomkit::model::ToolCallOutcome::Served));
    assert!(rows.contains(&roomkit::model::ToolCallOutcome::Cancelled));
    kit.close().await.unwrap();
}

fn assert_own_reports(reports: &[roomkit::ToolCallEvent]) {
    let mut outcomes: Vec<_> = reports
        .iter()
        .map(|r| (r.tool_call_id.clone(), r.cancelled, r.is_error))
        .collect();
    outcomes.sort();
    assert_eq!(
        outcomes,
        [("c1".into(), true, true), ("h1".into(), false, false)]
    );
}

async fn bounded<F: Future>(work: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(3), work)
        .await
        .unwrap()
}

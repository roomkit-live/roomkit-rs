//! A cut tool's explicitly transferred teardown joins the one shutdown.
//!
//! Rust does not resume an async finally when a future is dropped. The
//! integrator retains a cleanup future, captures its context while the
//! tool lives, and transfers it in Drop. This tests that explicit idiom.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit::model::{
    ChannelBinding, ChannelId, ChannelOutput, ChannelType, InboundMessage, RoomContext, RoomEvent,
};
use roomkit::{Channel, ChannelError, InMemoryStore, keep_context};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiTool, ToolResult, Tools};
use serde_json::json;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::held::{attach_agent, into, kit_of, said};
use crate::support::channel;

#[derive(Clone, Default)]
struct ClosingGate {
    entered: Arc<Notify>,
    release: Arc<Notify>,
    closed: Arc<AtomicUsize>,
}

impl Channel for ClosingGate {
    fn channel_id(&self) -> ChannelId {
        channel("gate")
    }
    fn channel_type(&self) -> ChannelType {
        ChannelType::WEBSOCKET
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
        _: RoomEvent,
        _: ChannelBinding,
        _: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        std::future::ready(Ok(ChannelOutput::empty()))
    }
    async fn close(&self) {
        self.entered.notify_one();
        self.release.notified().await;
        self.closed.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Clone, Default)]
struct CleanupTrace {
    steps: Arc<Mutex<Vec<&'static str>>>,
    pending: Arc<Notify>,
    handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

struct OnCut {
    future: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    trace: CleanupTrace,
}

impl Drop for OnCut {
    fn drop(&mut self) {
        self.trace.steps.lock().unwrap().push("cut");
        self.trace
            .handles
            .lock()
            .unwrap()
            .push(tokio::spawn(self.future.take().unwrap()));
    }
}

fn cleaning_tools(
    kit: &roomkit::RoomKit,
    trace: &CleanupTrace,
    entered: &Arc<Notify>,
    from_task: bool,
) -> Tools {
    let (weak, observing, entering) = (kit.downgrade(), trace.clone(), Arc::clone(entered));
    Tools::new().tool(
        AiTool {
            parameters: json!({"type":"object"}),
            ..AiTool::new("wait", "Wait.").unwrap()
        },
        move |_, _| {
            let (kit, trace, entered) = (
                weak.upgrade().unwrap(),
                observing.clone(),
                Arc::clone(&entering),
            );
            async move {
                let cleanup_trace = trace.clone();
                let cleanup = keep_context(async move {
                    let mut close = Box::pin(kit.close());
                    std::future::poll_fn(|cx| {
                        let state = close.as_mut().poll(cx);
                        if state.is_pending() {
                            cleanup_trace.pending.notify_one();
                        }
                        state
                    })
                    .await
                    .unwrap();
                    cleanup_trace.steps.lock().unwrap().push("closed");
                });
                let future = keep_context(async move {
                    if from_task {
                        tokio::spawn(cleanup).await.unwrap();
                    } else {
                        cleanup.await;
                    }
                });
                let _on_cut = OnCut {
                    future: Some(Box::pin(future)),
                    trace,
                };
                entered.notify_one();
                std::future::pending::<()>().await;
                Ok(ToolResult::Text("unreachable".to_owned()))
            }
        },
    )
}

async fn joins_after_cut(from_task: bool) {
    let kit = kit_of(InMemoryStore::default(), &[], &["r"]).await;
    let gate = ClosingGate::default();
    kit.register_channel(gate.clone()).unwrap();
    let trace = CleanupTrace::default();
    let entered = Arc::new(Notify::new());
    let tools = cleaning_tools(&kit, &trace, &entered, from_task);
    attach_agent(
        &kit,
        ScriptedProvider::new([ScriptedRound::calling("", [("c1", "wait", "{}")])]),
        tools,
    )
    .await;
    let caller = tokio::spawn({
        let kit = kit.clone();
        async move { kit.process_inbound(said("Go."), into("r")).await }
    });
    tokio::time::timeout(Duration::from_secs(1), entered.notified())
        .await
        .unwrap();
    let closer = tokio::spawn({
        let kit = kit.clone();
        async move { kit.close().await }
    });
    tokio::time::timeout(Duration::from_secs(8), gate.entered.notified())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), trace.pending.notified())
        .await
        .unwrap();
    assert_eq!(*trace.steps.lock().unwrap(), ["cut"]);
    assert!(!closer.is_finished());
    gate.release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), closer)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let handles = std::mem::take(&mut *trace.handles.lock().unwrap());
    assert_eq!(handles.len(), 1);
    for handle in handles {
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(1), caller)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(*trace.steps.lock().unwrap(), ["cut", "closed"]);
    assert_eq!(gate.closed.load(Ordering::SeqCst), 1);
    tokio::time::timeout(Duration::from_secs(1), roomkit::testing::idle(&kit))
        .await
        .unwrap();
}

// Python: tests/test_kit_close_doors.py::test_a_close_from_a_cut_tool_joins_the_close_that_cut_it
// Agent-tool door, own/transferred close. The explicit Drop adaptation is
// narrower than Python's automatic cancellation cleanup; no realtime claim.
#[tokio::test(start_paused = true)]
async fn a_cut_tools_explicit_teardown_joins_the_close_that_cut_it() {
    for from_task in [false, true] {
        joins_after_cut(from_task).await;
    }
}

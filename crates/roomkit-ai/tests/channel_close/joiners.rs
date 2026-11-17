//! All callers of a kit shutdown survive its channel teardown.
use super::*;
use roomkit::model::{ChannelOutput, InboundMessage};
use roomkit::{ChannelError, RoomKit, RoomKitOptions, WeakRoomKit};
use tokio::sync::Barrier;

struct ClosingChannel {
    agent: AiChannel<Model>,
    release: Arc<Notify>,
    from_child: bool,
}

impl Channel for ClosingChannel {
    fn channel_id(&self) -> ChannelId {
        "closing".parse().unwrap()
    }
    fn channel_type(&self) -> ChannelType {
        ChannelType::WEBHOOK
    }
    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> {
        std::future::ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }
    fn deliver(
        &self,
        _: RoomEvent,
        _: ChannelBinding,
        _: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> {
        std::future::ready(Ok(ChannelOutput::empty()))
    }
    async fn close(&self) {
        self.release.notified().await;
        // Shutdown carries identities, never the caller's public tool context.
        assert!(roomkit_ai::current_tool_call().is_none());
        if self.from_child {
            let agent = self.agent.clone();
            tokio::spawn(roomkit::keep_context(async move {
                assert!(roomkit_ai::current_tool_call().is_none());
                agent.close().await;
            }))
            .await
            .unwrap();
        } else {
            self.agent.close().await;
        }
    }
}

#[derive(Clone)]
struct Calls {
    kit: WeakRoomKit,
    joined: Arc<Barrier>,
    first_joined: Arc<Notify>,
    first_wait_dropped: Arc<Notify>,
    held: Arc<Notify>,
    closed: Arc<Notify>,
    trace: Trace,
    abandon_first_wait: bool,
}

impl Calls {
    async fn call(&self, name: &str) -> ToolResult {
        if name == "held" {
            let _dropped = Dropped(Arc::clone(&self.trace));
            self.held.notify_one();
            return std::future::pending().await;
        }
        self.close(name).await;
        self.trace.lock().unwrap().push(if name == "first" {
            "first returned"
        } else {
            "second returned"
        });
        ToolResult::Text("closed".into())
    }

    async fn close(&self, name: &str) {
        if name == "second" {
            self.first_joined.notified().await;
        }
        let kit = self.kit.upgrade().unwrap();
        let mut close = Box::pin(kit.close());
        // Actually join before opening the channel's close, also on two workers.
        std::future::poll_fn(|cx| {
            assert!(close.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        if name == "first" {
            self.first_joined.notify_one();
        }
        self.joined.wait().await;
        if name == "first" && self.abandon_first_wait {
            drop(close);
            self.first_wait_dropped.notify_one();
            self.closed.notified().await;
        } else {
            close.await.unwrap();
        }
        if name == "second" {
            self.closed.notify_one();
        }
    }
}

// Python: tests/test_kit_close_doors.py::test_a_close_from_the_kit_s_own_work_joining_another_spares_that_work
// Python: core/task_utils.py::Callers (later joiners and a cancelled wait remain callers).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn joining_tools_survive_the_kit_close_but_their_sibling_does_not() {
    for streaming in [false, true] {
        for from_child in [false, true] {
            for abandon_first_wait in [false, true] {
                bounded(joiners(streaming, from_child, abandon_first_wait)).await;
            }
        }
    }
}

async fn joiners(streaming: bool, from_child: bool, abandon_first_wait: bool) {
    let kit = RoomKit::new(RoomKitOptions::default());
    let calls = Calls {
        kit: kit.downgrade(),
        joined: Arc::new(Barrier::new(3)),
        first_joined: Arc::default(),
        first_wait_dropped: Arc::default(),
        held: Arc::default(),
        closed: Arc::default(),
        trace: Arc::default(),
        abandon_first_wait,
    };
    let model = Model {
        inner: ScriptedProvider::new([
            ScriptedRound::calling(
                "",
                [
                    ("1", "first", "{}"),
                    ("2", "second", "{}"),
                    ("3", "held", "{}"),
                ],
            ),
            ScriptedRound::answering("must not run"),
        ]),
        streaming,
        trace: Arc::clone(&calls.trace),
    };
    let agent = agent(&calls, &model);
    let release = Arc::new(Notify::new());
    kit.register_channel(ClosingChannel {
        agent: agent.clone(),
        release: Arc::clone(&release),
        from_child,
    })
    .unwrap();
    let turn = tokio::spawn(turn(agent));
    calls.joined.wait().await;
    calls.held.notified().await;
    if abandon_first_wait {
        calls.first_wait_dropped.notified().await;
    }
    release.notify_one();
    turn.await.unwrap();
    kit.close().await.unwrap();
    let mut trace = calls.trace.lock().unwrap().clone();
    trace.sort_unstable();
    assert_eq!(trace, ["call ended", "first returned", "second returned"]);
    assert_eq!(model.inner.requests().len(), 1);
}

fn agent(calls: &Calls, model: &Model) -> AiChannel<Model> {
    let calls = calls.clone();
    AiChannel::new(
        "ai".parse().unwrap(),
        model.clone(),
        AiChannelOptions {
            tools: ["first", "second", "held"]
                .map(|name| AiTool::new(name, name).unwrap().into())
                .to_vec(),
            tool_handler: Some(ToolHandler::new(move |name, _| {
                let calls = calls.clone();
                async move { Ok(calls.call(&name).await) }
            })),
            ..AiChannelOptions::default()
        },
    )
    .unwrap()
}

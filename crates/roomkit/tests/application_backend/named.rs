//! A host's transport submits each turn to the executor it reserved (RFC
//! §10.6.7): over the in-memory backend, and over a transport that keeps the
//! trait's default, which refuses.
use roomkit::model::{
    ChannelCategory, ChannelId, ChannelType, EventContent, EventType, InboundMessage, RoomId,
    TextContent,
};
use roomkit::store::ListEventsOptions;
use roomkit::testing::{
    EchoAgent, EchoAgentOptions, RecordingTransport, RecordingTransportOptions,
};
use roomkit::turns::{
    ChannelControl, ChannelResult, ExecutorInfo, ExecutorInfoOptions, InMemoryTurnBackend,
    RoomOwner, ServedExecutor, TurnBackend, TurnBackendHandle, TurnFuture, TurnLink, TurnRequest,
};
use roomkit::{
    AgentIdentity, AttachChannelOptions, ChannelDeclaration, ConversationStore, CreateRoomOptions,
    ProcessInboundOptions, RoomKit, RoomKitError, RoomKitOptions, TurnExecutor,
    TurnExecutorOptions,
};
use std::sync::{Arc, Mutex};

/// A host's transport: a turn goes to the executor it reserved, when it
/// reserved one, and what each such submission came to is kept.
struct Reserving<B> {
    inner: B,
    reserved: Mutex<Option<ExecutorInfo>>,
    submitted: Mutex<Vec<(Option<String>, bool)>>,
}

impl<B: TurnBackend> Reserving<B> {
    fn over(inner: B) -> Arc<Self> {
        Arc::new(Self {
            inner,
            reserved: Mutex::default(),
            submitted: Mutex::default(),
        })
    }

    fn reserve(&self, executor_id: Option<&str>) {
        self.reserve_at(executor_id, None);
    }

    /// Reserves the executor `executor_id` of the process `node`, when named.
    fn reserve_at(&self, executor_id: Option<&str>, node: Option<&str>) {
        *self.reserved.lock().unwrap() = executor_id.map(|id| {
            ExecutorInfo::new(
                id,
                ExecutorInfoOptions {
                    node: node.map(str::to_owned),
                    ..Default::default()
                },
            )
        });
    }

    fn submitted(&self) -> Vec<(Option<String>, bool)> {
        self.submitted.lock().unwrap().clone()
    }
}

impl<B: TurnBackend> TurnBackend for Reserving<B> {
    fn submit(
        &self,
        request: TurnRequest,
    ) -> TurnFuture<'_, Result<Arc<dyn TurnLink>, RoomKitError>> {
        Box::pin(async move {
            let Some(executor) = self.reserved.lock().unwrap().clone() else {
                return self.inner.submit(request).await;
            };
            // A clone shares the request's mark.
            let witness = request.clone();
            let link = self.inner.submit_to(request, executor).await;
            let reason = match &link {
                Ok(_) => None,
                Err(RoomKitError::TurnLink { reason, .. }) => Some(reason.clone()),
                Err(other) => Some(other.to_string()),
            };
            self.submitted
                .lock()
                .unwrap()
                .push((reason, witness.handed()));
            link
        })
    }
    fn serve(&self, executor: Arc<dyn ServedExecutor>) -> TurnFuture<'_, Result<(), RoomKitError>> {
        self.inner.serve(executor)
    }
    fn control(
        &self,
        message: ChannelControl,
    ) -> TurnFuture<'_, Result<Option<ChannelResult>, RoomKitError>> {
        self.inner.control(message)
    }
    fn attend(&self, owner: Arc<dyn RoomOwner>) -> TurnFuture<'_, Result<(), RoomKitError>> {
        self.inner.attend(owner)
    }
    fn close(&self) -> TurnFuture<'_, Result<(), RoomKitError>> {
        self.inner.close()
    }
}

/// A transport of the host's own that keeps the trait's `submit_to`.
struct Plain(InMemoryTurnBackend);

impl TurnBackend for Plain {
    fn submit(
        &self,
        request: TurnRequest,
    ) -> TurnFuture<'_, Result<Arc<dyn TurnLink>, RoomKitError>> {
        self.0.submit(request)
    }
    fn serve(&self, executor: Arc<dyn ServedExecutor>) -> TurnFuture<'_, Result<(), RoomKitError>> {
        self.0.serve(executor)
    }
    fn control(
        &self,
        message: ChannelControl,
    ) -> TurnFuture<'_, Result<Option<ChannelResult>, RoomKitError>> {
        self.0.control(message)
    }
    fn attend(&self, owner: Arc<dyn RoomOwner>) -> TurnFuture<'_, Result<(), RoomKitError>> {
        self.0.attend(owner)
    }
    fn close(&self) -> TurnFuture<'_, Result<(), RoomKitError>> {
        self.0.close()
    }
}

/// A worker declaring `agent`, whose agent answers its own name.
fn worker(name: &'static str) -> Arc<TurnExecutor> {
    Arc::new(
        TurnExecutor::new(
            move |id| async move { Ok(Some(EchoAgent::new(id, name, EchoAgentOptions::default()))) },
            TurnExecutorOptions {
                executor_id: Some(name.into()),
                hosts: Some(["agent".into()].into()),
                ..Default::default()
            },
        )
        .unwrap(),
    )
}

/// A kit over `backend` holding `agent` by its declaration, in a room.
async fn kit_in_a_room(backend: Arc<dyn TurnBackend>) -> RoomKit {
    let kit = RoomKit::new(RoomKitOptions {
        turn_backend: Some(TurnBackendHandle::new(backend)),
        ..Default::default()
    });
    let (room, text, agent) = ids();
    kit.register_channel(RecordingTransport::new(
        text.clone(),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.register_channel(ChannelDeclaration {
        identity: Some(AgentIdentity::default()),
        ..ChannelDeclaration::new(agent.clone())
    })
    .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.attach_channel(&room, &text, AttachChannelOptions::default())
        .await
        .unwrap();
    let intelligence = AttachChannelOptions {
        category: Some(ChannelCategory::Intelligence),
        ..Default::default()
    };
    kit.attach_channel(&room, &agent, intelligence)
        .await
        .unwrap();
    kit
}

fn ids() -> (RoomId, ChannelId, ChannelId) {
    (
        "r".parse().unwrap(),
        "text".parse().unwrap(),
        "agent".parse().unwrap(),
    )
}

/// Sends the room a message: whether its turn failed, and the agent's answers.
async fn say(kit: &RoomKit) -> (bool, Vec<String>) {
    let (room, text, agent) = ids();
    let result = kit
        .process_inbound(
            InboundMessage::new(text, "person", TextContent::new("Hi")),
            ProcessInboundOptions {
                room_id: Some(room.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let rows = kit
        .store()
        .list_events(&room, ListEventsOptions::default())
        .await
        .unwrap();
    let answers = rows
        .iter()
        .filter(|row| row.source.channel_id == agent && row.r#type == EventType::Message)
        .filter_map(|row| {
            if let EventContent::Text(text) = &row.content {
                Some(text.body.clone())
            } else {
                None
            }
        })
        .collect();
    (result.error.is_some(), answers)
}

#[tokio::test]
async fn a_turn_submitted_to_an_executor_runs_on_it_alone_and_the_room_follows() {
    let memory = InMemoryTurnBackend::new();
    let (first, second) = (worker("first"), worker("second"));
    memory
        .serve(Arc::<TurnExecutor>::clone(&first))
        .await
        .unwrap();
    memory
        .serve(Arc::<TurnExecutor>::clone(&second))
        .await
        .unwrap();
    let backend = Reserving::over(memory);
    let kit = kit_in_a_room(Arc::<Reserving<_>>::clone(&backend)).await;

    assert_eq!(say(&kit).await, (false, vec!["first".to_owned()]));
    backend.reserve(Some("second"));
    assert_eq!(say(&kit).await.1, ["first", "second"]);
    backend.reserve(None);
    assert_eq!(say(&kit).await.1, ["first", "second", "second"]);
    // One no worker answers to: refused, nothing handed, the room unmoved.
    backend.reserve(Some("ghost"));
    assert!(say(&kit).await.0);
    backend.reserve(None);
    assert_eq!(say(&kit).await.1, ["first", "second", "second", "second"]);
    assert_eq!(
        backend.submitted(),
        [(None, true), (Some("no_executor".to_owned()), false)]
    );
    kit.close().await.unwrap();
    first.close().await;
    second.close().await;
}

#[tokio::test]
async fn a_transport_that_keeps_the_default_refuses_a_named_submission_unhanded() {
    let memory = InMemoryTurnBackend::new();
    let first = worker("first");
    memory
        .serve(Arc::<TurnExecutor>::clone(&first))
        .await
        .unwrap();
    let backend = Reserving::over(Plain(memory));
    let kit = kit_in_a_room(Arc::<Reserving<_>>::clone(&backend)).await;

    backend.reserve(Some("first"));
    assert_eq!(say(&kit).await, (true, Vec::new()));
    assert_eq!(
        backend.submitted(),
        [(Some("no_executor".to_owned()), false)]
    );
    // Placed, the same worker takes the room's turn.
    backend.reserve(None);
    assert_eq!(say(&kit).await, (false, vec!["first".to_owned()]));
    kit.close().await.unwrap();
    first.close().await;
}

/// Across Redis, where each backend stands for a process (`REDIS_URL`).
#[cfg(feature = "redis")]
mod across_redis {
    use super::{Reserving, kit_in_a_room, say, worker};
    use roomkit::turns::TurnBackend;
    use roomkit::{RedisTurnBackend, RedisTurnBackendOptions, TurnExecutor, TurnTimeouts};
    use std::sync::Arc;
    use std::time::Duration;

    fn processes<const N: usize>() -> Option<[RedisTurnBackend; N]> {
        let url = std::env::var("REDIS_URL").ok()?;
        let options = RedisTurnBackendOptions {
            key_prefix: format!("roomkit:named:{}", uuid::Uuid::new_v4()),
            timeouts: TurnTimeouts {
                heartbeat_interval: Duration::from_millis(50),
                heartbeat_timeout: Duration::from_secs(2),
                accept: Duration::from_millis(300),
                ..Default::default()
            },
            ..Default::default()
        };
        Some(std::array::from_fn(|_| {
            RedisTurnBackend::new(Some(&url), options.clone()).unwrap()
        }))
    }

    /// Serves a worker `name` on `node` from a runtime of its own, which the
    /// returned sender stops as a killed process stops: no close, no last beat.
    async fn serve_apart(
        node: RedisTurnBackend,
        name: &'static str,
    ) -> std::sync::mpsc::Sender<()> {
        let (ready, serving) = tokio::sync::oneshot::channel();
        let (kill, killed) = std::sync::mpsc::channel::<()>();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                TurnBackend::serve(&node, worker(name)).await.unwrap();
                ready.send(()).unwrap();
                tokio::task::spawn_blocking(move || killed.recv())
                    .await
                    .unwrap()
                    .ok();
                // Never closed: its handle goes with its runtime.
                drop(node);
            });
            runtime.shutdown_background();
        });
        serving.await.unwrap();
        kill
    }

    #[tokio::test]
    async fn a_hand_over_never_answered_is_lost_handed_and_given_to_no_other() {
        let Some([owner, a, b]) = processes() else {
            return;
        };
        let first = worker("a");
        TurnBackend::serve(&a, Arc::<TurnExecutor>::clone(&first))
            .await
            .unwrap();
        let node = b.node_id().to_owned();
        let kill = serve_apart(b, "b").await;
        let backend = Reserving::over(owner);
        let kit = kit_in_a_room(Arc::<Reserving<_>>::clone(&backend)).await;
        // Its process dies: it reads no request and beats no more, while the
        // registry still counts it alive within the heartbeat timeout.
        kill.send(()).unwrap();
        backend.reserve_at(Some("b"), Some(&node));

        // a declares the channel too, and is given nothing.
        assert_eq!(say(&kit).await, (true, Vec::new()));
        assert_eq!(
            backend.submitted(),
            [(Some("executor_lost".to_owned()), true)]
        );
        kit.close().await.unwrap();
        a.close().await.unwrap();
        first.close().await;
    }

    #[tokio::test]
    async fn an_executor_of_the_owners_own_process_is_named_as_any_other() {
        let Some([owner, a]) = processes() else {
            return;
        };
        let (first, here) = (worker("a"), worker("here"));
        TurnBackend::serve(&a, Arc::<TurnExecutor>::clone(&first))
            .await
            .unwrap();
        let node = owner.node_id().to_owned();
        let backend = Reserving::over(owner.clone());
        let kit = kit_in_a_room(Arc::<Reserving<_>>::clone(&backend)).await;
        assert_eq!(say(&kit).await.1, ["a"]);
        TurnBackend::serve(&owner, Arc::<TurnExecutor>::clone(&here))
            .await
            .unwrap();

        // Named as the backend lists it, then by its id alone; the room follows.
        backend.reserve_at(Some("here"), Some(&node));
        assert_eq!(say(&kit).await.1, ["a", "here"]);
        backend.reserve(Some("here"));
        assert_eq!(say(&kit).await.1, ["a", "here", "here"]);
        backend.reserve(None);
        assert_eq!(say(&kit).await.1, ["a", "here", "here", "here"]);
        // Closed, it refuses before the hand-over.
        here.close().await;
        backend.reserve(Some("here"));
        assert!(say(&kit).await.0);
        assert_eq!(
            backend.submitted().last(),
            Some(&(Some("no_executor".to_owned()), false))
        );
        kit.close().await.unwrap();
        a.close().await.unwrap();
        first.close().await;
    }
}

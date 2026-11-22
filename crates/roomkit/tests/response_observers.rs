//! A turn's `ON_AI_RESPONSE` hooks get their room context and start before
//! the turn returns, on an AI channel and on an ACP channel, a standalone ACP
//! turn's before its session closes: Python's turn awaits its report, room
//! read included, before its stream closes, so a later turn's hooks never
//! start before this one's (RMK-1914, the scenario
//! `acp/steer-isolates-running-rooms` on a store whose reads suspend).

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

#[path = "response_observers/store.rs"]
mod store;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use agent_client_protocol_schema::v1::{
    AuthenticateRequest, AuthenticateResponse, CancelNotification, CloseSessionRequest,
    CloseSessionResponse, InitializeRequest, InitializeResponse, NewSessionRequest,
    NewSessionResponse, PromptRequest, PromptResponse, SetSessionConfigOptionRequest,
    SetSessionConfigOptionResponse, StopReason,
};
use roomkit::model::hook::AllowOptions;
use roomkit::model::{
    ChannelId, ChannelType, EventType, HookResult, InboundMessage, RoomId, TextContent,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, Channel, CreateRoomOptions, HookExecution, HookOptions, HookTrigger,
    ProcessInboundOptions, RoomKit, RoomKitOptions, SendEventOptions,
};
use roomkit_acp::{
    AcpChannel, AcpChannelOptions, AcpClient, AcpConnection, AcpError, AcpTransport,
};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiGenerationEvent, AiResponseEvent, MockAiProvider,
    MockAiProviderOptions,
};
use store::{ContextGate, GatedStore};
use tokio::sync::mpsc;

const PATIENCE: Duration = Duration::from_secs(10);
/// Long enough, on the paused clock, for every task that can run to run.
const BRIEF: Duration = Duration::from_millis(50);

fn id(name: &str) -> ChannelId {
    ChannelId::new(name).unwrap()
}

fn room() -> RoomId {
    RoomId::new("r1").unwrap()
}

/// A kit over a store whose room-context reads `gate` may hold, with `agent`
/// and a websocket bound to `r1`, and an ASYNC `ON_AI_RESPONSE` hook that
/// tells the room of each response it observes.
async fn kit(
    gate: &Arc<ContextGate>,
    agent: impl Channel,
) -> (Arc<RoomKit<GatedStore>>, mpsc::UnboundedReceiver<String>) {
    let kit = Arc::new(RoomKit::new(RoomKitOptions::with_store(GatedStore::new(
        Arc::clone(gate),
    ))));
    kit.register_channel(RecordingTransport::new(
        id("ws"),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.register_channel(agent).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room()),
        ..Default::default()
    })
    .await
    .unwrap();
    for name in ["ws", "agent"] {
        kit.attach_channel(&room(), &id(name), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    let (observed, responses) = mpsc::unbounded_channel();
    kit.hook(
        HookTrigger::OnAiResponse,
        move |event: AiResponseEvent, _| {
            let observed = observed.clone();
            let room = event.room_id.map(|room| room.to_string());
            async move {
                observed.send(room.unwrap_or_default()).unwrap();
            }
        },
        HookOptions {
            execution: HookExecution::Async,
            ..HookOptions::default()
        },
    )
    .unwrap();
    (kit, responses)
}

/// A message into `r1`, whose turn `gate` holds once armed at its first room
/// read: the turn returns only once that read, its `ON_AI_RESPONSE` report's,
/// is let go, and its hook then observes it.
async fn the_turn_returns_after_its_report_read(
    kit: Arc<RoomKit<GatedStore>>,
    gate: &ContextGate,
    mut responses: mpsc::UnboundedReceiver<String>,
) {
    let mut turn = tokio::spawn(async move {
        kit.process_inbound(
            InboundMessage::new(id("ws"), "alice", TextContent::new("Hello")),
            ProcessInboundOptions {
                room_id: Some(room()),
                ..Default::default()
            },
        )
        .await
    });
    tokio::time::timeout(PATIENCE, gate.reached.notified())
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(BRIEF, &mut turn).await.is_err(),
        "the turn returned before its ON_AI_RESPONSE report read the room"
    );
    gate.release.notify_one();
    let result = tokio::time::timeout(PATIENCE, turn)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(result.error.is_none(), "{:?}", result.error);
    let observed = tokio::time::timeout(PATIENCE, responses.recv()).await;
    assert_eq!(observed.unwrap().as_deref(), Some("r1"));
}

#[tokio::test(start_paused = true)]
async fn an_ai_turn_returns_after_its_response_observers_read_the_room() {
    let gate = Arc::new(ContextGate::default());
    let agent = AiChannel::new(
        id("agent"),
        MockAiProvider::new(MockAiProviderOptions {
            responses: vec!["Hi.".to_owned()],
            ..MockAiProviderOptions::default()
        }),
        AiChannelOptions::default(),
    )
    .unwrap();
    let (kit, responses) = kit(&gate, agent).await;
    // The generation's room read is done once its hook runs: the turn's
    // next read is its report's.
    let arming = Arc::clone(&gate);
    kit.hook(
        HookTrigger::BeforeAiGeneration,
        move |_: AiGenerationEvent, _| {
            arming.arm();
            async { Ok(HookResult::allow(AllowOptions::default())) }
        },
        HookOptions::default(),
    )
    .unwrap();
    the_turn_returns_after_its_report_read(kit, &gate, responses).await;
}

#[tokio::test(start_paused = true)]
async fn an_acp_turn_returns_after_its_response_observers_read_the_room() {
    let gate = Arc::new(ContextGate::default());
    let agent = AcpChannel::new(
        id("agent"),
        "/srv",
        AcpChannelOptions {
            transport: Some(Arming::new(&gate)),
            ..Default::default()
        },
    )
    .unwrap();
    let (kit, responses) = kit(&gate, agent).await;
    the_turn_returns_after_its_report_read(kit, &gate, responses).await;
}

/// A standalone ACP turn reports before it closes its own session, as
/// Python's `_report_response` runs before `_close_turn_session`.
#[tokio::test(start_paused = true)]
async fn a_standalone_acp_turn_reports_before_its_session_closes() {
    let gate = Arc::new(ContextGate::default());
    let transport = Arming::new(&gate);
    let closed = Arc::clone(&transport.closed);
    let agent = AcpChannel::new(
        id("agent"),
        "/srv",
        AcpChannelOptions {
            transport: Some(transport),
            ..Default::default()
        },
    )
    .unwrap();
    let (kit, mut responses) = kit(&gate, agent).await;
    let sender = Arc::clone(&kit);
    let mut sent = tokio::spawn(async move {
        let options = SendEventOptions {
            event_type: EventType::Instruction,
            standalone: true,
            addressed_to: Some(vec![id("agent")]),
            ..Default::default()
        };
        sender
            .send_event(&room(), &id("ws"), TextContent::new("Summarize."), options)
            .await
    });
    tokio::time::timeout(PATIENCE, gate.reached.notified())
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(BRIEF, &mut sent).await.is_err(),
        "the standalone turn returned before its ON_AI_RESPONSE report read the room"
    );
    assert_eq!(
        closed.load(Ordering::SeqCst),
        0,
        "the standalone session closed before its turn's report read the room"
    );
    gate.release.notify_one();
    let observed = tokio::time::timeout(PATIENCE, responses.recv()).await;
    assert_eq!(observed.unwrap().as_deref(), Some("r1"));
    tokio::time::timeout(PATIENCE, sent)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(closed.load(Ordering::SeqCst), 1);
}

/// An agent that ends each prompt at once, having armed the gate: the
/// turn's own room reads are done by then, its next one is its report's.
/// It counts the sessions it was asked to close.
struct Arming {
    gate: Arc<ContextGate>,
    closed: Arc<AtomicUsize>,
}

impl Arming {
    fn new(gate: &Arc<ContextGate>) -> Self {
        Self {
            gate: Arc::clone(gate),
            closed: Arc::default(),
        }
    }
}

#[expect(clippy::unused_async_trait_impl, reason = "transport fixture")]
impl AcpTransport for Arming {
    type Connection = ArmingConnection;

    fn name(&self) -> &'static str {
        "arming"
    }

    async fn open(&self, _: Arc<dyn AcpClient>) -> Result<ArmingConnection, AcpError> {
        Ok(ArmingConnection {
            gate: Arc::clone(&self.gate),
            closed: Arc::clone(&self.closed),
            sessions: AtomicUsize::new(0),
        })
    }

    async fn close(&self) {}
}

struct ArmingConnection {
    gate: Arc<ContextGate>,
    closed: Arc<AtomicUsize>,
    sessions: AtomicUsize,
}

#[expect(clippy::unused_async_trait_impl, reason = "connection fixture")]
impl AcpConnection for ArmingConnection {
    async fn initialize(&self, _: InitializeRequest) -> Result<InitializeResponse, AcpError> {
        let announced = serde_json::json!({
            "protocolVersion": 1,
            "agentCapabilities": { "sessionCapabilities": { "close": {} } },
        });
        Ok(serde_json::from_value(announced).unwrap())
    }

    async fn authenticate(&self, _: AuthenticateRequest) -> Result<AuthenticateResponse, AcpError> {
        Ok(AuthenticateResponse::new())
    }

    async fn new_session(&self, _: NewSessionRequest) -> Result<NewSessionResponse, AcpError> {
        let session = self.sessions.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(NewSessionResponse::new(format!("session-{session}")))
    }

    async fn prompt(&self, _: PromptRequest) -> Result<PromptResponse, AcpError> {
        self.gate.arm();
        Ok(PromptResponse::new(StopReason::EndTurn))
    }

    async fn cancel(&self, _: CancelNotification) -> Result<(), AcpError> {
        Ok(())
    }

    async fn close_session(
        &self,
        _: CloseSessionRequest,
    ) -> Result<CloseSessionResponse, AcpError> {
        self.closed.fetch_add(1, Ordering::SeqCst);
        Ok(CloseSessionResponse::new())
    }

    async fn set_config_option(
        &self,
        _: SetSessionConfigOptionRequest,
    ) -> Result<SetSessionConfigOptionResponse, AcpError> {
        Ok(SetSessionConfigOptionResponse::new(Vec::new()))
    }
}

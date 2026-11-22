//! A turn's `ON_AI_RESPONSE` observers read the room and start before the
//! turn returns, on an AI channel and on an ACP channel: Python's turn awaits
//! its report, room read included, before its stream closes, so a later
//! turn's observers never start before this one's (RFC §9.3; RMK-1914, the
//! scenario `acp/steer-isolates-running-rooms` on a store whose reads
//! suspend).

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

#[path = "response_observers/store.rs"]
mod store;

use std::sync::Arc;
use std::time::Duration;

use agent_client_protocol_schema::v1::{
    AuthenticateRequest, AuthenticateResponse, CancelNotification, CloseSessionRequest,
    CloseSessionResponse, InitializeRequest, InitializeResponse, NewSessionRequest,
    NewSessionResponse, PromptRequest, PromptResponse, SetSessionConfigOptionRequest,
    SetSessionConfigOptionResponse, StopReason,
};
use roomkit::model::hook::AllowOptions;
use roomkit::model::{ChannelId, ChannelType, HookResult, InboundMessage, RoomId, TextContent};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, Channel, CreateRoomOptions, HookExecution, HookOptions, HookTrigger,
    ProcessInboundOptions, RoomKit, RoomKitOptions,
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
            transport: Some(Arming(Arc::clone(&gate))),
            ..Default::default()
        },
    )
    .unwrap();
    let (kit, responses) = kit(&gate, agent).await;
    the_turn_returns_after_its_report_read(kit, &gate, responses).await;
}

/// An agent that ends each prompt at once, having armed the gate: the
/// turn's own room reads are done by then, its next one is its report's.
struct Arming(Arc<ContextGate>);

#[expect(clippy::unused_async_trait_impl, reason = "transport fixture")]
impl AcpTransport for Arming {
    type Connection = ArmingConnection;

    fn name(&self) -> &'static str {
        "arming"
    }

    async fn open(&self, _: Arc<dyn AcpClient>) -> Result<ArmingConnection, AcpError> {
        Ok(ArmingConnection(Arc::clone(&self.0)))
    }

    async fn close(&self) {}
}

struct ArmingConnection(Arc<ContextGate>);

#[expect(clippy::unused_async_trait_impl, reason = "connection fixture")]
impl AcpConnection for ArmingConnection {
    async fn initialize(&self, _: InitializeRequest) -> Result<InitializeResponse, AcpError> {
        Ok(serde_json::from_value(serde_json::json!({"protocolVersion": 1})).unwrap())
    }

    async fn authenticate(&self, _: AuthenticateRequest) -> Result<AuthenticateResponse, AcpError> {
        Ok(AuthenticateResponse::new())
    }

    async fn new_session(&self, _: NewSessionRequest) -> Result<NewSessionResponse, AcpError> {
        Ok(NewSessionResponse::new("session-1"))
    }

    async fn prompt(&self, _: PromptRequest) -> Result<PromptResponse, AcpError> {
        self.0.arm();
        Ok(PromptResponse::new(StopReason::EndTurn))
    }

    async fn cancel(&self, _: CancelNotification) -> Result<(), AcpError> {
        Ok(())
    }

    async fn close_session(
        &self,
        _: CloseSessionRequest,
    ) -> Result<CloseSessionResponse, AcpError> {
        Ok(CloseSessionResponse::new())
    }

    async fn set_config_option(
        &self,
        _: SetSessionConfigOptionRequest,
    ) -> Result<SetSessionConfigOptionResponse, AcpError> {
        Ok(SetSessionConfigOptionResponse::new(Vec::new()))
    }
}

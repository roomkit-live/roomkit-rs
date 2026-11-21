//! A turn whose caller goes away while the agent's permission request is
//! pending answers the request `cancelled`, after `session/cancel` (RFC §6.4).
//! Python: `tests/test_channels/test_acp_handler_cancellation.py::
//! test_turn_cancellation_answers_a_pending_permission_cancelled`, whose turn's
//! caller is cancelled.
#![expect(clippy::unwrap_used, reason = "test fixtures and assertions")]
use agent_client_protocol_schema::v1::{
    AuthenticateRequest, AuthenticateResponse, CancelNotification, CloseSessionRequest,
    CloseSessionResponse, InitializeRequest, InitializeResponse, NewSessionRequest,
    NewSessionResponse, PromptRequest, PromptResponse, RequestPermissionOutcome,
    RequestPermissionRequest, RequestPermissionResponse, SetSessionConfigOptionRequest,
    SetSessionConfigOptionResponse,
};
use roomkit::model::{ChannelId, ChannelType, InboundMessage, Metadata, RoomId, TextContent};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, ProcessInboundOptions, RoomKit, RoomKitOptions,
};
use roomkit_acp::{
    AcpChannel, AcpChannelOptions, AcpClient, AcpConnection, AcpError, AcpTransport,
};
use roomkit_ai::{
    ExternalToolFuture, ExternalToolHandler, ExternalToolHandlerState, OnToolCancelledOptions,
    OnToolResultOptions, PolicyExternalToolHandler, ProcessToolCallOptions, ToolDecision,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

const PATIENCE: Duration = Duration::from_secs(5);

type Answer = JoinHandle<Result<RequestPermissionResponse, AcpError>>;

#[derive(Default)]
struct State {
    client: Mutex<Option<Arc<dyn AcpClient>>>,
    answer: Mutex<Option<Answer>>,
    /// What reached the agent, in order: the notification, the answer.
    wire: Arc<Mutex<Vec<&'static str>>>,
}

struct Transport(Arc<State>);
struct Connection(Arc<State>);

#[expect(
    clippy::unused_async_trait_impl,
    reason = "the transport fixture answers immediately"
)]
impl AcpTransport for Transport {
    type Connection = Connection;
    fn name(&self) -> &'static str {
        "pending-permission-cut"
    }
    async fn open(&self, client: Arc<dyn AcpClient>) -> Result<Connection, AcpError> {
        *self.0.client.lock().unwrap() = Some(client);
        Ok(Connection(Arc::clone(&self.0)))
    }
    async fn close(&self) {}
}

#[expect(
    clippy::unused_async_trait_impl,
    reason = "the connection fixture has immediate answers"
)]
impl AcpConnection for Connection {
    async fn initialize(&self, _: InitializeRequest) -> Result<InitializeResponse, AcpError> {
        Ok(serde_json::from_value(serde_json::json!({"protocolVersion":1,"agentCapabilities":{"sessionCapabilities":{"close":{}}}})).unwrap())
    }
    async fn authenticate(&self, _: AuthenticateRequest) -> Result<AuthenticateResponse, AcpError> {
        Ok(AuthenticateResponse::new())
    }
    async fn new_session(&self, _: NewSessionRequest) -> Result<NewSessionResponse, AcpError> {
        Ok(NewSessionResponse::new("session-1"))
    }
    /// Asks a permission for a call it never announces, from a task of its
    /// own as the connection's reader does, and never answers the prompt.
    async fn prompt(&self, request: PromptRequest) -> Result<PromptResponse, AcpError> {
        let client = self.0.client.lock().unwrap().clone().unwrap();
        let permission: RequestPermissionRequest = serde_json::from_value(serde_json::json!({
            "sessionId": request.session_id.0.as_ref(),
            "toolCall": {"toolCallId": "read", "title": "Read", "rawInput": {}},
            "options": [
                {"optionId":"a","name":"Allow","kind":"allow_once"},
                {"optionId":"r","name":"Reject","kind":"reject_once"}
            ]
        }))
        .unwrap();
        let wire = Arc::clone(&self.0.wire);
        let answer = tokio::spawn(async move {
            let answer = client.request_permission(permission).await;
            wire.lock().unwrap().push("answer");
            answer
        });
        *self.0.answer.lock().unwrap() = Some(answer);
        std::future::pending().await
    }
    async fn cancel(&self, _: CancelNotification) -> Result<(), AcpError> {
        // A write to the agent's pipe yields before it returns.
        tokio::task::yield_now().await;
        self.0.wire.lock().unwrap().push("session/cancel");
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
        Ok(SetSessionConfigOptionResponse::new(vec![]))
    }
}

/// A human approval that withdraws its pending decision once told the call
/// was cut, as a handler may (RFC §9.3).
#[derive(Default)]
struct Withdrawing {
    policy: PolicyExternalToolHandler,
    asked: Notify,
    resume: Notify,
}

impl ExternalToolHandler for Withdrawing {
    fn state(&self) -> &ExternalToolHandlerState {
        self.policy.state()
    }
    fn process_tool_call<'a>(
        &'a self,
        _: &'a str,
        _: &'a Metadata,
        _: ProcessToolCallOptions,
    ) -> ExternalToolFuture<'a, ToolDecision> {
        Box::pin(async move {
            self.asked.notify_one();
            self.resume.notified().await;
            Ok(ToolDecision {
                reason: "turn ended".to_owned(),
                ..ToolDecision::new(false)
            })
        })
    }
    fn on_tool_result<'a>(
        &'a self,
        name: &'a str,
        input: &'a Metadata,
        result: &'a str,
        options: OnToolResultOptions,
    ) -> ExternalToolFuture<'a, ()> {
        self.policy.on_tool_result(name, input, result, options)
    }
    fn on_tool_cancelled<'a>(
        &'a self,
        name: &'a str,
        input: &'a Metadata,
        options: OnToolCancelledOptions,
    ) -> ExternalToolFuture<'a, ()> {
        Box::pin(async move {
            self.resume.notify_one();
            self.policy.on_tool_cancelled(name, input, options).await
        })
    }
}

async fn dropped_turn() {
    let state = Arc::new(State::default());
    let handler = Arc::new(Withdrawing::default());
    let channel = AcpChannel::new(
        ChannelId::new("claude").unwrap(),
        "/srv",
        AcpChannelOptions {
            transport: Some(Transport(Arc::clone(&state))),
            external_tool_handler: Some(Arc::<Withdrawing>::clone(&handler)),
            ..AcpChannelOptions::default()
        },
    )
    .unwrap();
    let kit = Arc::new(RoomKit::new(RoomKitOptions::default()));
    kit.register_channel(RecordingTransport::new(
        ChannelId::new("ws").unwrap(),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.register_channel(channel).unwrap();
    let room = RoomId::new("r1").unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..CreateRoomOptions::default()
    })
    .await
    .unwrap();
    for name in ["ws", "claude"] {
        kit.attach_channel(
            &room,
            &ChannelId::new(name).unwrap(),
            AttachChannelOptions::default(),
        )
        .await
        .unwrap();
    }
    let caller = {
        let kit = Arc::clone(&kit);
        tokio::spawn(async move {
            kit.process_inbound(
                InboundMessage::new(
                    ChannelId::new("ws").unwrap(),
                    "alice",
                    TextContent::new("go"),
                ),
                ProcessInboundOptions {
                    room_id: Some(room),
                    ..ProcessInboundOptions::default()
                },
            )
            .await
        })
    };
    handler.asked.notified().await;
    caller.abort();
    let _aborted = caller.await;
    let answer = state.answer.lock().unwrap().take().unwrap();
    let answer = answer.await.unwrap().unwrap();
    assert!(matches!(
        answer.outcome,
        RequestPermissionOutcome::Cancelled
    ));
    assert_eq!(*state.wire.lock().unwrap(), ["session/cancel", "answer"]);
    kit.close().await.unwrap();
}

#[tokio::test]
async fn a_dropped_turn_answers_a_pending_permission_cancelled() {
    tokio::time::timeout(PATIENCE, dropped_turn())
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dropped_turn_answers_a_pending_permission_cancelled_on_several_workers() {
    tokio::time::timeout(PATIENCE, dropped_turn())
        .await
        .unwrap();
}

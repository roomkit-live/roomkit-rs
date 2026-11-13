//! A buffered old answer cannot acknowledge a replacement ACP session.
//! The result is obtained before the dead pipe is discovered by another room.
#![expect(clippy::unwrap_used, reason = "test fixture and assertions")]
use agent_client_protocol_schema::v1::{
    AuthenticateRequest, AuthenticateResponse, CancelNotification, CloseSessionRequest,
    CloseSessionResponse, InitializeRequest, InitializeResponse, NewSessionRequest,
    NewSessionResponse, PromptRequest, PromptResponse, SessionConfigOption,
    SetSessionConfigOptionRequest, SetSessionConfigOptionResponse, StopReason,
};
use roomkit::model::{ChannelId, ChannelType, InboundMessage, RoomId, TextContent};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, ProcessInboundOptions, RoomKit, RoomKitOptions,
};
use roomkit_acp::{
    AcpChannel, AcpChannelOptions, AcpClient, AcpConnection, AcpError, AcpTransport,
};
use serde_json::json;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::sync::Notify;

const PATIENCE: Duration = Duration::from_secs(10);
#[derive(Default)]
struct State {
    alive: AtomicBool,
    opened: AtomicUsize,
    received: Notify,
    return_received: Notify,
    prompts: Mutex<Vec<(usize, PromptRequest)>>,
}
struct Transport(Arc<State>);
struct Connection {
    state: Arc<State>,
    generation: usize,
}
#[expect(clippy::unused_async_trait_impl, reason = "transport fixture")]
impl AcpTransport for Transport {
    type Connection = Connection;
    fn name(&self) -> &'static str {
        "received-result-reconnect"
    }
    async fn open(&self, _: Arc<dyn AcpClient>) -> Result<Connection, AcpError> {
        let generation = self.0.opened.fetch_add(1, Ordering::SeqCst);
        self.0.alive.store(true, Ordering::SeqCst);
        Ok(Connection {
            state: Arc::clone(&self.0),
            generation,
        })
    }
    async fn close(&self) {
        self.0.alive.store(false, Ordering::SeqCst);
    }
    fn is_alive(&self) -> bool {
        self.0.alive.load(Ordering::SeqCst)
    }
}
fn options(model: &str) -> Vec<SessionConfigOption> {
    serde_json::from_value(json!([{"id":"model","name":"Model","type":"select","currentValue":model,"options":[{"name":"Opus","value":"opus"},{"name":"Sonnet","value":"sonnet"}]}])).unwrap()
}
#[expect(clippy::unused_async_trait_impl, reason = "connection fixture")]
impl AcpConnection for Connection {
    async fn initialize(&self, _: InitializeRequest) -> Result<InitializeResponse, AcpError> {
        Ok(serde_json::from_value(
            json!({"protocolVersion":1,"agentCapabilities":{"sessionCapabilities":{"close":{}}}}),
        )
        .unwrap())
    }
    async fn authenticate(&self, _: AuthenticateRequest) -> Result<AuthenticateResponse, AcpError> {
        Ok(AuthenticateResponse::new())
    }
    async fn new_session(
        &self,
        request: NewSessionRequest,
    ) -> Result<NewSessionResponse, AcpError> {
        let room = request.meta.as_ref().unwrap()["roomkit.live/roomId"]
            .as_str()
            .unwrap();
        Ok(NewSessionResponse::new(format!("session-{room}")).config_options(options("opus")))
    }
    async fn set_config_option(
        &self,
        _: SetSessionConfigOptionRequest,
    ) -> Result<SetSessionConfigOptionResponse, AcpError> {
        Ok(SetSessionConfigOptionResponse::new(options("sonnet")))
    }
    async fn close_session(
        &self,
        _: CloseSessionRequest,
    ) -> Result<CloseSessionResponse, AcpError> {
        Ok(CloseSessionResponse::new())
    }
    async fn cancel(&self, _: CancelNotification) -> Result<(), AcpError> {
        Ok(())
    }
    async fn prompt(&self, request: PromptRequest) -> Result<PromptResponse, AcpError> {
        self.state
            .prompts
            .lock()
            .unwrap()
            .push((self.generation, request));
        let already_received = PromptResponse::new(StopReason::EndTurn);
        if self.generation == 0 {
            self.state.received.notify_one();
            self.state.return_received.notified().await;
        }
        Ok(already_received)
    }
}
async fn say(kit: &RoomKit, room: &RoomId, text: &str) {
    let result = tokio::time::timeout(
        PATIENCE,
        kit.process_inbound(
            InboundMessage::new(
                ChannelId::new("ws").unwrap(),
                "alice",
                TextContent::new(text),
            ),
            ProcessInboundOptions {
                room_id: Some(room.clone()),
                ..Default::default()
            },
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(result.error.is_none(), "{:?}", result.error);
}
async fn check_late_ack(open_replacement_first: bool) {
    let state = Arc::new(State::default());
    let channel = AcpChannel::new(
        ChannelId::new("claude").unwrap(),
        "/srv",
        AcpChannelOptions {
            transport: Some(Transport(Arc::clone(&state))),
            ..Default::default()
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
    kit.register_channel(channel.clone()).unwrap();
    let a = RoomId::new("room-a").unwrap();
    let b = RoomId::new("room-b").unwrap();
    for room in [&a, &b] {
        kit.create_room(CreateRoomOptions {
            room_id: Some(room.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
        for name in ["ws", "claude"] {
            kit.attach_channel(
                room,
                &ChannelId::new(name).unwrap(),
                AttachChannelOptions::default(),
            )
            .await
            .unwrap();
        }
    }
    let old = {
        let kit = Arc::clone(&kit);
        let a = a.clone();
        tokio::spawn(async move { say(&kit, &a, "old request").await })
    };
    tokio::time::timeout(PATIENCE, state.received.notified())
        .await
        .unwrap();
    state.alive.store(false, Ordering::SeqCst);
    say(&kit, &b, "other room").await;
    assert_eq!(state.opened.load(Ordering::SeqCst), 2);
    if open_replacement_first {
        channel
            .set_config_option(&a, "model", json!("sonnet"))
            .await
            .unwrap();
        assert_eq!(channel.session_id(&a).as_deref(), Some("session-room-a"));
    }
    state.return_received.notify_one();
    tokio::time::timeout(PATIENCE, old).await.unwrap().unwrap();
    say(&kit, &a, "new request").await;
    let sent = {
        let prompts = state.prompts.lock().unwrap();
        let (generation, prompt) = prompts.last().unwrap();
        assert_eq!(*generation, 1);
        serde_json::to_value(prompt)
            .unwrap()
            .pointer("/prompt/0/text")
            .unwrap()
            .as_str()
            .unwrap()
            .to_owned()
    };
    kit.close().await.unwrap();
    assert!(
        sent.contains("old request"),
        "replacement lost history: {sent}"
    );
    assert!(sent.ends_with("new request"));
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_ack_does_not_seed_a_session_not_yet_reopened() {
    check_late_ack(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_ack_does_not_seed_a_reopened_session_with_the_same_wire_id() {
    check_late_ack(true).await;
}

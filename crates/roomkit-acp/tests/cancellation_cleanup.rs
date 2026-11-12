//! Public cancellation no longer targets a finished preparation during cleanup.
//! Python: `tests/test_channels/test_acp_recovery.py::test_finished_preparation_is_not_cancelled_again_during_release`
#![expect(clippy::unwrap_used, reason = "test fixtures and assertions")]
#![expect(
    clippy::panic,
    reason = "a remote cancellation here violates the contract"
)]
use agent_client_protocol_schema::v1::{
    AuthenticateRequest, AuthenticateResponse, CancelNotification, CloseSessionRequest,
    CloseSessionResponse, InitializeRequest, InitializeResponse, NewSessionRequest,
    NewSessionResponse, PromptRequest, PromptResponse, SetSessionConfigOptionRequest,
    SetSessionConfigOptionResponse,
};
use roomkit::Channel;
use roomkit::model::{ChannelId, ChannelType, InboundMessage, RoomId, TextContent};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, ProcessInboundOptions, RoomKit, RoomKitOptions,
};
use roomkit_acp::{
    AcpChannel, AcpChannelOptions, AcpClient, AcpConnection, AcpError, AcpSessionInvalidatedError,
    AcpSessionInvalidatedErrorOptions, AcpTransport,
};
use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::sync::Notify;
#[derive(Default)]
struct State {
    channel: Mutex<Weak<AcpChannel<Transport>>>,
    opened: AtomicUsize,
    reached: Notify,
    release: Notify,
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
        "cancel-cleanup"
    }
    async fn open(&self, _: Arc<dyn AcpClient>) -> Result<Connection, AcpError> {
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
        let n = self.0.opened.fetch_add(1, Ordering::SeqCst) + 1;
        if n == 2 {
            let channel = self.0.channel.lock().unwrap().upgrade().unwrap();
            assert!(channel.cancel(&RoomId::new("r1").unwrap()).await.unwrap());
        }
        Ok(NewSessionResponse::new(format!("session-{n}")))
    }
    async fn prompt(&self, _: PromptRequest) -> Result<PromptResponse, AcpError> {
        Err(AcpSessionInvalidatedError::new(
            "unusable",
            AcpSessionInvalidatedErrorOptions {
                recovery_authorized: true,
            },
        )
        .into())
    }
    async fn cancel(&self, _: CancelNotification) -> Result<(), AcpError> {
        panic!("no remote prompt to cancel")
    }
    async fn close_session(
        &self,
        r: CloseSessionRequest,
    ) -> Result<CloseSessionResponse, AcpError> {
        if r.session_id.0.as_ref() == "session-2" {
            self.0.reached.notify_one();
            self.0.release.notified().await;
        }
        Ok(CloseSessionResponse::new())
    }
    async fn set_config_option(
        &self,
        _: SetSessionConfigOptionRequest,
    ) -> Result<SetSessionConfigOptionResponse, AcpError> {
        Ok(SetSessionConfigOptionResponse::new(vec![]))
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn finished_preparation_is_not_cancelled_again_during_release() {
    let state = Arc::new(State::default());
    let channel = Arc::new(
        AcpChannel::new(
            ChannelId::new("claude").unwrap(),
            "/srv",
            AcpChannelOptions {
                transport: Some(Transport(Arc::clone(&state))),
                ..Default::default()
            },
        )
        .unwrap(),
    );
    *state.channel.lock().unwrap() = Arc::downgrade(&channel);
    let kit = Arc::new(RoomKit::new(RoomKitOptions::default()));
    kit.register_channel(RecordingTransport::new(
        ChannelId::new("ws").unwrap(),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.register_channel((*channel).clone()).unwrap();
    let room = RoomId::new("r1").unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
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
    let consuming = {
        let kit = Arc::clone(&kit);
        let room = room.clone();
        tokio::spawn(async move {
            kit.process_inbound(
                InboundMessage::new(
                    ChannelId::new("ws").unwrap(),
                    "alice",
                    TextContent::new("hello"),
                ),
                ProcessInboundOptions {
                    room_id: Some(room),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
        })
    };
    tokio::time::timeout(Duration::from_secs(5), state.reached.notified())
        .await
        .unwrap();
    let second = channel.cancel(&room).await.unwrap();
    assert!(channel.session_id(&room).is_none());
    assert_eq!(channel.active_turns(), 0);
    state.release.notify_one();
    let outcome = tokio::time::timeout(Duration::from_secs(5), consuming)
        .await
        .unwrap()
        .unwrap();
    assert!(outcome.error.is_none());
    kit.close().await.unwrap();
    assert!(
        !second,
        "Python returns false once its preparation ended and local session was discarded"
    );
}

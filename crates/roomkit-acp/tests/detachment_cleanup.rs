//! A cancelled channel close stops detached-session cleanup (RFC §6.4).
//! Python: `tests/test_channels/test_acp_backend.py::test_interrupted_close_stops_detached_session_cleanup`
#![expect(clippy::unwrap_used, reason = "test fixtures and assertions")]
#![expect(
    clippy::unused_async_trait_impl,
    reason = "the fake agent answers immediate calls"
)]
use agent_client_protocol_schema::v1::{
    AuthenticateRequest, AuthenticateResponse, CancelNotification, CloseSessionRequest,
    CloseSessionResponse, InitializeRequest, InitializeResponse, NewSessionRequest,
    NewSessionResponse, PromptRequest, PromptResponse, SetSessionConfigOptionRequest,
    SetSessionConfigOptionResponse, StopReason,
};
use roomkit::{
    Channel,
    model::{ChannelId, RoomId},
};
use roomkit_acp::{
    AcpChannel, AcpChannelOptions, AcpClient, AcpConnection, AcpError, AcpTransport,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::sync::Notify;
#[derive(Default)]
struct State {
    opened: AtomicUsize,
    first: Notify,
    second: Notify,
    first_dropped: Notify,
    transport_closed: Notify,
    release: Notify,
}
struct Transport(Arc<State>);
struct Connection(Arc<State>);
struct End(Arc<State>);
impl Drop for End {
    fn drop(&mut self) {
        self.0.first_dropped.notify_one();
    }
}
impl AcpTransport for Transport {
    type Connection = Connection;
    fn name(&self) -> &'static str {
        "detach-cleanup"
    }
    async fn open(&self, _: Arc<dyn AcpClient>) -> Result<Connection, AcpError> {
        Ok(Connection(Arc::clone(&self.0)))
    }
    async fn close(&self) {
        self.0.transport_closed.notify_one();
    }
}
impl AcpConnection for Connection {
    async fn initialize(&self, _: InitializeRequest) -> Result<InitializeResponse, AcpError> {
        Ok(serde_json::from_value(serde_json::json!({"protocolVersion":1,"agentCapabilities":{"sessionCapabilities":{"close":{}}}})).unwrap())
    }
    async fn authenticate(&self, _: AuthenticateRequest) -> Result<AuthenticateResponse, AcpError> {
        Ok(AuthenticateResponse::new())
    }
    async fn new_session(&self, _: NewSessionRequest) -> Result<NewSessionResponse, AcpError> {
        Ok(NewSessionResponse::new(format!(
            "session-{}",
            self.0.opened.fetch_add(1, Ordering::SeqCst) + 1
        )))
    }
    async fn prompt(&self, _: PromptRequest) -> Result<PromptResponse, AcpError> {
        Ok(PromptResponse::new(StopReason::EndTurn))
    }
    async fn cancel(&self, _: CancelNotification) -> Result<(), AcpError> {
        Ok(())
    }
    async fn close_session(
        &self,
        r: CloseSessionRequest,
    ) -> Result<CloseSessionResponse, AcpError> {
        if r.session_id.0.as_ref() == "session-1" {
            let _ended = End(Arc::clone(&self.0));
            self.0.first.notify_one();
            self.0.release.notified().await;
        } else {
            self.0.second.notify_one();
            std::future::pending::<()>().await;
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
#[tokio::test]
async fn test_interrupted_close_stops_detached_session_cleanup() {
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
    let first = RoomId::new("r1").unwrap();
    let second = RoomId::new("r2").unwrap();
    for room in [&first, &second] {
        channel
            .set_config_option(room, "model", serde_json::json!("sonnet"))
            .await
            .unwrap();
    }
    channel.on_room_detached(first).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), state.first.notified())
        .await
        .unwrap();
    let closing = tokio::spawn({
        let channel = channel.clone();
        async move { channel.close().await }
    });
    tokio::time::timeout(Duration::from_secs(5), state.second.notified())
        .await
        .unwrap();
    closing.abort();
    assert!(closing.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(5), state.transport_closed.notified())
        .await
        .unwrap();
    let cleaned = tokio::time::timeout(Duration::from_secs(5), state.first_dropped.notified())
        .await
        .is_ok();
    if !cleaned {
        state.release.notify_one();
        tokio::time::timeout(Duration::from_secs(5), state.first_dropped.notified())
            .await
            .unwrap();
    }
    drop(channel);
    assert!(
        cleaned,
        "Python stops detached closing tasks even when channel.close is cancelled"
    );
}

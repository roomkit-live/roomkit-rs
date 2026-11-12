//! A permission on the wire cannot arrive too late to prevent prompt replay.
//! Python: `tests/test_channels/test_acp_recovery.py::test_activity_makes_even_an_authorized_signal_terminal`
#![expect(clippy::unwrap_used, reason = "test fixtures and assertions")]
#![expect(clippy::indexing_slicing, reason = "raw protocol fixtures")]
#![expect(clippy::panic, reason = "unexpected requests fail the test agent")]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use agent_client_protocol_schema::v1::{
    PromptRequest, RequestPermissionOutcome, RequestPermissionRequest, RequestPermissionResponse,
    SessionNotification,
};
use roomkit_acp::{AcpClient, AcpConnection, AcpError, ClientSideConnection};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use agent_client_protocol_schema::v1::{
    AuthenticateRequest, AuthenticateResponse, CancelNotification, CloseSessionRequest,
    CloseSessionResponse, InitializeRequest, InitializeResponse, NewSessionRequest,
    NewSessionResponse, PromptResponse, SetSessionConfigOptionRequest,
    SetSessionConfigOptionResponse,
};
use roomkit::model::{ChannelId, ChannelType, InboundMessage, RoomId, TextContent};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, ProcessInboundOptions, RoomKit, RoomKitOptions,
};
use roomkit_acp::{
    AcpChannel, AcpChannelOptions, AcpSessionInvalidatedError, AcpSessionInvalidatedErrorOptions,
    AcpTransport,
};
use std::sync::{Mutex, atomic::AtomicUsize};

struct Client(Arc<AtomicBool>, bool);
impl AcpClient for Client {
    fn session_update(
        &self,
        _: SessionNotification,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async {})
    }
    fn request_permission(
        &self,
        _: RequestPermissionRequest,
    ) -> Pin<Box<dyn Future<Output = Result<RequestPermissionResponse, AcpError>> + Send + '_>>
    {
        Box::pin(async move {
            self.0.store(true, Ordering::Release);
            if self.1 {
                std::future::pending::<()>().await;
            }
            Ok(RequestPermissionResponse::new(
                RequestPermissionOutcome::Cancelled,
            ))
        })
    }
}

#[derive(Default)]
struct Counts {
    opens: AtomicUsize,
    prompts: AtomicUsize,
    connection: Mutex<Option<ClientSideConnection>>,
}
struct Transport(Arc<Counts>);
struct Connection {
    inner: ClientSideConnection,
    counts: Arc<Counts>,
}
impl AcpTransport for Transport {
    type Connection = Connection;
    fn name(&self) -> &'static str {
        "wire-recovery"
    }
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "the transport trait returns a future"
    )]
    async fn open(&self, client: Arc<dyn AcpClient>) -> Result<Connection, AcpError> {
        let (ours, theirs) = tokio::io::duplex(65536);
        let (read, write) = tokio::io::split(ours);
        let inner = ClientSideConnection::new(client, read, write);
        *self.0.connection.lock().unwrap() = Some(inner.clone());
        let counts = Arc::clone(&self.0);
        tokio::spawn(async move {
            let (read, mut write) = tokio::io::split(theirs);
            let mut lines = BufReader::new(read).lines();
            while let Some(line) = lines.next_line().await.unwrap() {
                let request: Value = serde_json::from_str(&line).unwrap();
                let Some(method) = request["method"].as_str() else {
                    continue;
                };
                let result = match method {
                    "initialize" => {
                        json!({"protocolVersion":1,"agentCapabilities":{"sessionCapabilities":{"close":{}}}})
                    }
                    "session/new" => {
                        let n = counts.opens.fetch_add(1, Ordering::SeqCst) + 1;
                        json!({"sessionId":format!("s{n}")})
                    }
                    "session/prompt" => {
                        let permission = json!({"jsonrpc":"2.0","id":"permission-1","method":"session/request_permission","params":{"sessionId":request["params"]["sessionId"],"toolCall":{"toolCallId":"t1","title":"Write"},"options":[{"optionId":"no","name":"No","kind":"reject_once"}]}});
                        let answer = json!({"jsonrpc":"2.0","id":request["id"],"result":{"stopReason":"end_turn"}});
                        if write
                            .write_all(format!("{permission}\n{answer}\n").as_bytes())
                            .await
                            .is_err()
                        {
                            break;
                        }
                        continue;
                    }
                    "session/cancel" => continue,
                    "session/close" => json!({}),
                    other => panic!("unexpected {other}"),
                };
                let answer = json!({"jsonrpc":"2.0","id":request["id"],"result":result});
                if write
                    .write_all(format!("{answer}\n").as_bytes())
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        Ok(Connection {
            inner,
            counts: Arc::clone(&self.0),
        })
    }
    async fn close(&self) {
        let connection = self.0.connection.lock().unwrap().take();
        if let Some(connection) = connection {
            connection.close().await;
        }
    }
}
macro_rules! forward {
    ($name:ident,$req:ty,$res:ty) => {
        async fn $name(&self, r: $req) -> Result<$res, AcpError> {
            self.inner.$name(r).await
        }
    };
}
impl AcpConnection for Connection {
    forward!(initialize, InitializeRequest, InitializeResponse);
    forward!(authenticate, AuthenticateRequest, AuthenticateResponse);
    forward!(new_session, NewSessionRequest, NewSessionResponse);
    forward!(cancel, CancelNotification, ());
    forward!(close_session, CloseSessionRequest, CloseSessionResponse);
    forward!(
        set_config_option,
        SetSessionConfigOptionRequest,
        SetSessionConfigOptionResponse
    );
    async fn prompt(&self, r: PromptRequest) -> Result<PromptResponse, AcpError> {
        let n = self.counts.prompts.fetch_add(1, Ordering::SeqCst);
        let answer = self.inner.prompt(r).await?;
        if n == 0 {
            Err(AcpSessionInvalidatedError::new(
                "unusable",
                AcpSessionInvalidatedErrorOptions {
                    recovery_authorized: true,
                },
            )
            .into())
        } else {
            Ok(answer)
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_permission_on_wire_prevents_authorized_replay_in_the_channel() {
    let mut replays = 0;
    for _ in 0..500 {
        let counts = Arc::new(Counts::default());
        let agent = AcpChannel::new(
            ChannelId::new("claude").unwrap(),
            "/srv",
            AcpChannelOptions {
                transport: Some(Transport(Arc::clone(&counts))),
                ..Default::default()
            },
        )
        .unwrap();
        let kit = RoomKit::new(RoomKitOptions::default());
        kit.register_channel(RecordingTransport::new(
            ChannelId::new("ws").unwrap(),
            ChannelType::WEBSOCKET,
            RecordingTransportOptions::default(),
        ))
        .unwrap();
        kit.register_channel(agent).unwrap();
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
        let result = kit
            .process_inbound(
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
            .unwrap();
        let prompted = counts.prompts.load(Ordering::SeqCst);
        if prompted > 1 {
            replays += 1;
            assert!(result.error.is_none());
        } else {
            assert!(result.error.unwrap().message.contains("activity"));
        }
        kit.close().await.unwrap();
    }
    assert_eq!(
        replays, 0,
        "native authorized refusals replayed despite preceding wire permission: {replays}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_permission_before_the_prompt_answer_is_seen_before_prompt_returns() {
    let mut late = 0;
    for _ in 0..1000 {
        if !permission_before_return(false).await {
            late += 1;
        }
    }
    assert_eq!(
        late, 0,
        "permission callbacks not observed at the prompt boundary: {late}"
    );
}

async fn permission_before_return(hold_decision: bool) -> bool {
    let seen = Arc::new(AtomicBool::new(false));
    let (ours, theirs) = tokio::io::duplex(65536);
    let (read, write) = tokio::io::split(ours);
    let connection = ClientSideConnection::new(
        Arc::new(Client(Arc::clone(&seen), hold_decision)),
        read,
        write,
    );
    let agent = tokio::spawn(async move {
        let (read, mut write) = tokio::io::split(theirs);
        let mut lines = BufReader::new(read).lines();
        let prompt: Value =
            serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
        assert_eq!(prompt["method"], "session/prompt");
        let permission = json!({"jsonrpc":"2.0","id":"permission-1","method":"session/request_permission","params":{"sessionId":"s1","toolCall":{"toolCallId":"t1","title":"Write"},"options":[{"optionId":"no","name":"No","kind":"reject_once"}]}});
        let answer = json!({"jsonrpc":"2.0","id":prompt["id"],"result":{"stopReason":"end_turn"}});
        write
            .write_all(format!("{permission}\n{answer}\n").as_bytes())
            .await
            .unwrap();
        drop(lines.next_line().await);
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        connection.prompt(PromptRequest::new("s1", vec!["hello".into()])),
    )
    .await
    .unwrap()
    .unwrap();
    let observed = seen.load(Ordering::Acquire);
    connection.close().await;
    agent.await.unwrap();
    observed
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_finished_prompt_does_not_wait_for_a_permission_decision() {
    assert!(permission_before_return(true).await);
}

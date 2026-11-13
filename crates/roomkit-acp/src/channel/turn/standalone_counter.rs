//! Stop the response reader after its report is scheduled and before cleanup.
// Python: tests/test_channels/test_acp_standalone_response.py::test_standalone_response_observer_sees_no_active_turn

use std::future::{Future, poll_fn, ready};
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::task::Poll;
use std::time::Duration;

use agent_client_protocol_schema::v1::{
    AuthenticateRequest, AuthenticateResponse, CancelNotification, CloseSessionRequest,
    CloseSessionResponse, InitializeRequest, InitializeResponse, NewSessionRequest,
    NewSessionResponse, PromptRequest, PromptResponse, SetSessionConfigOptionRequest,
    SetSessionConfigOptionResponse, StopReason,
};
use futures_core::Stream;
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, CreateRoomOptions, HookExecution,
    HookRegistration, HookTrigger, RoomKit, RoomKitOptions, SendEventOptions, current_turn_host,
    model::{
        ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, EventType,
        InboundMessage, RoomContext, RoomEvent, RoomId, TextContent,
    },
    testing::{RecordingTransport, RecordingTransportOptions},
};
use roomkit_ai::AiResponseEvent;
use tokio::sync::Notify;

use super::{Ask, TurnStream, response_metadata};
use crate::{AcpChannel, AcpChannelOptions, AcpClient, AcpConnection, AcpError, AcpTransport};

struct Direct;
struct Connection;
impl AcpTransport for Direct {
    type Connection = Connection;
    fn name(&self) -> &'static str {
        "standalone-counter"
    }
    fn open(&self, _: Arc<dyn AcpClient>) -> impl Future<Output = Result<Connection, AcpError>> {
        ready(Ok(Connection))
    }
    fn close(&self) -> impl Future<Output = ()> {
        ready(())
    }
}
impl AcpConnection for Connection {
    fn initialize(
        &self,
        _: InitializeRequest,
    ) -> impl Future<Output = Result<InitializeResponse, AcpError>> {
        ready(Ok(serde_json::from_value(serde_json::json!({
            "protocolVersion": 1,
            "agentCapabilities": {"sessionCapabilities": {"close": {}}}
        }))
        .unwrap()))
    }
    fn authenticate(
        &self,
        _: AuthenticateRequest,
    ) -> impl Future<Output = Result<AuthenticateResponse, AcpError>> {
        ready(Ok(AuthenticateResponse::new()))
    }
    fn new_session(
        &self,
        _: NewSessionRequest,
    ) -> impl Future<Output = Result<NewSessionResponse, AcpError>> {
        ready(Ok(NewSessionResponse::new("temporary")))
    }
    fn prompt(&self, _: PromptRequest) -> impl Future<Output = Result<PromptResponse, AcpError>> {
        ready(Ok(PromptResponse::new(StopReason::EndTurn)))
    }
    fn cancel(&self, _: CancelNotification) -> impl Future<Output = Result<(), AcpError>> {
        ready(Ok(()))
    }
    fn close_session(
        &self,
        _: CloseSessionRequest,
    ) -> impl Future<Output = Result<CloseSessionResponse, AcpError>> {
        ready(Ok(CloseSessionResponse::new()))
    }
    fn set_config_option(
        &self,
        _: SetSessionConfigOptionRequest,
    ) -> impl Future<Output = Result<SetSessionConfigOptionResponse, AcpError>> {
        ready(Ok(SetSessionConfigOptionResponse::new(vec![])))
    }
}

struct Reading {
    inner: AcpChannel<Direct>,
    observed: Arc<Notify>,
}
impl Channel for Reading {
    fn channel_id(&self) -> ChannelId {
        self.inner.channel_id()
    }
    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }
    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }
    async fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> Result<RoomEvent, ChannelError> {
        self.inner.handle_inbound(message, context).await
    }
    async fn deliver(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        self.inner.deliver(event, binding, context).await
    }
    async fn on_event(
        &self,
        event: RoomEvent,
        _: ChannelBinding,
        context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        assert_eq!(event.r#type, EventType::Instruction);
        let mut stream = TurnStream::new(
            Arc::clone(&self.inner.shared),
            Ask {
                standalone: true,
                room_id: context.room.id.clone(),
                event_id: event.id.to_string(),
                input: crate::channel::context::Input {
                    context,
                    trigger: event,
                    text: roomkit_core::rules::transcript::mark_instruction("Summarize."),
                    blocks: Vec::new(),
                },
                record: response_metadata(true),
                host: Some(current_turn_host().unwrap()),
                answers_room: true,
            },
        );
        // Poll once at a time. The report queues its actual observer before
        // begin_session_cleanup returns Pending. Do not poll cleanup until
        // that observer has measured the public channel counter.
        loop {
            let result = poll_fn(|cx| Poll::Ready(Pin::new(&mut stream).poll_next(cx))).await;
            if stream.reported {
                assert!(result.is_pending());
                self.observed.notified().await;
                break;
            }
            assert!(!matches!(result, Poll::Ready(None)));
            tokio::task::yield_now().await;
        }
        while let Some(item) = poll_fn(|cx| Pin::new(&mut stream).poll_next(cx)).await {
            item.unwrap();
        }
        Ok(ChannelOutput::empty())
    }
    async fn close(&self) {
        self.inner.close().await;
    }
}

#[tokio::test]
async fn test_standalone_response_observer_sees_no_active_turn() {
    tokio::time::timeout(Duration::from_secs(5), check_counter())
        .await
        .unwrap();
}

async fn check_counter() {
    let channel = AcpChannel::new(
        ChannelId::new("claude").unwrap(),
        "/srv",
        AcpChannelOptions {
            transport: Some(Direct),
            ..Default::default()
        },
    )
    .unwrap();
    let observed = Arc::new(Notify::new());
    let count = Arc::new(AtomicUsize::new(usize::MAX));
    let calls = Arc::new(AtomicUsize::new(0));
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(Reading {
        inner: channel.clone(),
        observed: Arc::clone(&observed),
    })
    .unwrap();
    kit.register_channel(RecordingTransport::new(
        ChannelId::new("ws").unwrap(),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
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
    kit.turn_executor()
        .hooks()
        .register(HookRegistration::new(
            HookTrigger::OnAiResponse,
            HookExecution::Async,
            {
                let count = Arc::clone(&count);
                let calls = Arc::clone(&calls);
                move |_: AiResponseEvent, _: RoomContext| {
                    let channel = channel.clone();
                    let count = Arc::clone(&count);
                    let calls = Arc::clone(&calls);
                    let observed = Arc::clone(&observed);
                    async move {
                        count.store(channel.active_turns(), Ordering::SeqCst);
                        calls.fetch_add(1, Ordering::SeqCst);
                        observed.notify_one();
                    }
                }
            },
        ))
        .unwrap();
    kit.send_event(
        &room,
        &ChannelId::new("ws").unwrap(),
        TextContent::new("Summarize."),
        SendEventOptions {
            event_type: EventType::Instruction,
            standalone: true,
            addressed_to: Some(vec![ChannelId::new("claude").unwrap()]),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    kit.close().await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(count.load(Ordering::SeqCst), 0);
}

//! Public ACP transport with observable service and session-close boundaries.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use agent_client_protocol_schema::v1::*;
use roomkit::model::{ChannelId, Metadata};
use roomkit_ai::{
    ExternalToolFuture, ExternalToolHandler, ExternalToolHandlerState, OnToolResultOptions,
    PolicyExternalToolHandler, ProcessToolCallOptions, ToolDecision, ToolError,
};
use serde_json::json;
use tokio::sync::Notify;

use crate::{AcpChannel, AcpChannelOptions, AcpClient, AcpConnection, AcpError, AcpTransport};

#[derive(Default)]
pub(super) struct State {
    pub(super) log: Mutex<Vec<&'static str>>,
    pub(super) alive: AtomicBool,
    pub(super) authentication_fails: AtomicBool,
    pub(super) hold_close: AtomicBool,
    pub(super) closing: Notify,
    pub(super) resume: Notify,
    pub(super) client: Mutex<Option<Arc<dyn AcpClient>>>,
    sessions: AtomicUsize,
}

impl State {
    pub(super) fn log(&self, item: &'static str) {
        self.log.lock().unwrap().push(item);
    }

    pub(super) async fn permission(&self, session: &str) -> RequestPermissionResponse {
        let client = self.client.lock().unwrap().clone().unwrap();
        client.request_permission(serde_json::from_value(json!({
            "sessionId":session,"toolCall":{"toolCallId":"read","title":"Read","rawInput":{}},
            "options":[{"optionId":"a","name":"Allow","kind":"allow_once"},
                {"optionId":"r","name":"Reject","kind":"reject_once"}]
        })).unwrap()).await.unwrap()
    }
}

pub(super) struct Direct(Arc<State>);
pub(super) struct Connection(Arc<State>);

#[expect(
    clippy::unused_async_trait_impl,
    reason = "the transport's API returns futures"
)]
impl AcpTransport for Direct {
    type Connection = Connection;
    fn name(&self) -> &'static str {
        "handler-test"
    }
    async fn open(&self, client: Arc<dyn AcpClient>) -> Result<Connection, AcpError> {
        self.0.log("open");
        self.0.alive.store(true, Ordering::SeqCst);
        *self.0.client.lock().unwrap() = Some(client);
        Ok(Connection(Arc::clone(&self.0)))
    }
    async fn close(&self) {
        self.0.log("transport-close");
        self.0.alive.store(false, Ordering::SeqCst);
    }
    fn is_alive(&self) -> bool {
        self.0.alive.load(Ordering::SeqCst)
    }
}

#[expect(
    clippy::unused_async_trait_impl,
    reason = "the connection's API returns futures"
)]
impl AcpConnection for Connection {
    async fn initialize(&self, _: InitializeRequest) -> Result<InitializeResponse, AcpError> {
        self.0.log("initialize");
        Ok(serde_json::from_value(json!({"protocolVersion":1,
            "agentCapabilities":{"sessionCapabilities":{"close":{}}}}))
        .unwrap())
    }
    async fn authenticate(
        &self,
        request: AuthenticateRequest,
    ) -> Result<AuthenticateResponse, AcpError> {
        assert_eq!(request.method_id.0.as_ref(), "test-auth");
        self.0.log("authenticate");
        if self.0.authentication_fails.load(Ordering::SeqCst) {
            return Err(AcpError::Runtime("authentication failed".to_owned()));
        }
        Ok(AuthenticateResponse::new())
    }
    async fn new_session(&self, _: NewSessionRequest) -> Result<NewSessionResponse, AcpError> {
        self.0.log("session");
        let id = self.0.sessions.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(NewSessionResponse::new(format!("session-{id}")))
    }
    async fn set_config_option(
        &self,
        _: SetSessionConfigOptionRequest,
    ) -> Result<SetSessionConfigOptionResponse, AcpError> {
        Ok(serde_json::from_value(
            json!({"configOptions":[{"id":"model","type":"select","name":"Model",
            "currentValue":"opus","options":[{"value":"opus","name":"Opus"}]}]}),
        )
        .unwrap())
    }
    async fn close_session(
        &self,
        _: CloseSessionRequest,
    ) -> Result<CloseSessionResponse, AcpError> {
        if self.0.hold_close.load(Ordering::SeqCst) {
            self.0.closing.notify_one();
            self.0.resume.notified().await;
        }
        Ok(CloseSessionResponse::new())
    }
    async fn cancel(&self, _: CancelNotification) -> Result<(), AcpError> {
        Ok(())
    }
    async fn prompt(&self, _: PromptRequest) -> Result<PromptResponse, AcpError> {
        Ok(serde_json::from_value(json!({"stopReason":"end_turn"})).unwrap())
    }
}

pub(super) struct Service {
    policy: PolicyExternalToolHandler,
    state: Arc<State>,
    pub(super) starts: AtomicUsize,
    pub(super) stops: AtomicUsize,
    start_failures: usize,
    stop_fails: bool,
}

impl ExternalToolHandler for Service {
    fn state(&self) -> &ExternalToolHandlerState {
        self.policy.state()
    }
    fn process_tool_call<'a>(
        &'a self,
        name: &'a str,
        input: &'a Metadata,
        options: ProcessToolCallOptions,
    ) -> ExternalToolFuture<'a, ToolDecision> {
        self.policy.process_tool_call(name, input, options)
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
    fn start(&self) -> ExternalToolFuture<'_, ()> {
        Box::pin(async move {
            assert_eq!(self.state.log.lock().unwrap().last(), Some(&"authenticate"));
            self.state.log("start");
            let count = self.starts.fetch_add(1, Ordering::SeqCst);
            if count < self.start_failures {
                return Err(ToolError::Raised {
                    kind: "ValueError".to_owned(),
                    message: "approval service down".to_owned(),
                });
            }
            Ok(())
        })
    }
    fn stop(&self) -> ExternalToolFuture<'_, ()> {
        Box::pin(async move {
            assert!(!self.state.alive.load(Ordering::SeqCst));
            self.state.log("stop");
            self.stops.fetch_add(1, Ordering::SeqCst);
            if self.stop_fails {
                return Err(ToolError::Failed("service cleanup failed".to_owned()));
            }
            Ok(())
        })
    }
}

pub(super) fn channel(
    start_failures: usize,
    stop_fails: bool,
) -> (AcpChannel<Direct>, Arc<State>, Arc<Service>) {
    let state = Arc::new(State::default());
    let service = Arc::new(Service {
        policy: PolicyExternalToolHandler::default(),
        state: Arc::clone(&state),
        starts: AtomicUsize::new(0),
        stops: AtomicUsize::new(0),
        start_failures,
        stop_fails,
    });
    let agent = AcpChannel::new(
        ChannelId::new("acp-agent").unwrap(),
        "/srv",
        AcpChannelOptions {
            transport: Some(Direct(Arc::clone(&state))),
            authentication_method: Some("test-auth".to_owned()),
            external_tool_handler: Some(Arc::<Service>::clone(&service)),
            ..AcpChannelOptions::default()
        },
    )
    .unwrap();
    (agent, state, service)
}

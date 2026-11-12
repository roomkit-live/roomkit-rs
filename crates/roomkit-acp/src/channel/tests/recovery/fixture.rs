//! A native transport refusal, with gates at the connection's boundaries.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use agent_client_protocol_schema::v1::*;
use roomkit::model::ChannelId;
use serde_json::json;
use tokio::sync::Notify;

use crate::{
    AcpChannel, AcpChannelOptions, AcpClient, AcpConnection, AcpError, AcpSessionInvalidatedError,
    AcpSessionInvalidatedErrorOptions, AcpTransport,
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::channel) enum Stage {
    Start,
    InitialOpen,
    InitialConfigured,
    Close,
    Open,
    Configured,
    Prompt,
    Permission,
    FailedPrompt,
    AnsweredPrompt,
}

pub(in crate::channel) struct State {
    pub(in crate::channel) stage: Stage,
    pub(in crate::channel) reached: Notify,
    pub(in crate::channel) resume: Notify,
    armed: AtomicBool,
    pub(in crate::channel) opened: AtomicUsize,
    pub(in crate::channel) prompts: std::sync::Mutex<Vec<PromptRequest>>,
    pub(in crate::channel) closed: std::sync::Mutex<Vec<String>>,
    pub(in crate::channel) cancelled: std::sync::Mutex<Vec<String>>,
}

impl State {
    async fn hold(&self, stage: Stage) {
        if self.stage == stage && self.armed.swap(false, Ordering::SeqCst) {
            self.reached.notify_one();
            self.resume.notified().await;
        }
    }
}

pub(in crate::channel) struct Direct(Arc<State>);
pub(in crate::channel) struct Connection {
    state: Arc<State>,
    client: Arc<dyn AcpClient>,
}

pub(in crate::channel) fn channel(boundary: Stage) -> (AcpChannel<Direct>, Arc<State>) {
    let state = Arc::new(State {
        stage: boundary,
        reached: Notify::new(),
        resume: Notify::new(),
        armed: AtomicBool::new(true),
        opened: AtomicUsize::new(0),
        prompts: std::sync::Mutex::new(Vec::new()),
        closed: std::sync::Mutex::new(Vec::new()),
        cancelled: std::sync::Mutex::new(Vec::new()),
    });
    let channel = AcpChannel::new(
        ChannelId::new("claude").unwrap(),
        "/srv/project",
        AcpChannelOptions {
            transport: Some(Direct(Arc::clone(&state))),
            session_config: matches!(boundary, Stage::Configured | Stage::InitialConfigured).then(
                || {
                    [("model".to_owned(), json!("sonnet"))]
                        .into_iter()
                        .collect()
                },
            ),
            ..Default::default()
        },
    )
    .unwrap();
    (channel, state)
}

impl AcpTransport for Direct {
    type Connection = Connection;
    fn name(&self) -> &'static str {
        "direct"
    }
    async fn open(&self, client: Arc<dyn AcpClient>) -> Result<Connection, AcpError> {
        self.0.hold(Stage::Start).await;
        Ok(Connection {
            state: Arc::clone(&self.0),
            client,
        })
    }
    async fn close(&self) {}
}

fn options(model: &str) -> Vec<SessionConfigOption> {
    serde_json::from_value(
        json!([{"id":"model","name":"Model","type":"select","currentValue":model,
        "options":[{"name":"Opus","value":"opus"},{"name":"Sonnet","value":"sonnet"}]}]),
    )
    .unwrap()
}

impl AcpConnection for Connection {
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "the connection trait returns a future"
    )]
    async fn initialize(&self, _: InitializeRequest) -> Result<InitializeResponse, AcpError> {
        Ok(serde_json::from_value(
            json!({"protocolVersion":1,"agentCapabilities":{"sessionCapabilities":{"close":{}}}}),
        )
        .unwrap())
    }
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "the connection trait returns a future"
    )]
    async fn authenticate(&self, _: AuthenticateRequest) -> Result<AuthenticateResponse, AcpError> {
        Ok(AuthenticateResponse::new())
    }
    async fn new_session(&self, _: NewSessionRequest) -> Result<NewSessionResponse, AcpError> {
        let n = self.state.opened.fetch_add(1, Ordering::SeqCst) + 1;
        if n == 1 {
            self.state.hold(Stage::InitialOpen).await;
        }
        if n == 2 {
            self.state.hold(Stage::Open).await;
        }
        Ok(NewSessionResponse::new(format!("session-{n}")).config_options(options("opus")))
    }
    async fn set_config_option(
        &self,
        request: SetSessionConfigOptionRequest,
    ) -> Result<SetSessionConfigOptionResponse, AcpError> {
        if request.session_id.0.as_ref() == "session-1" {
            self.state.hold(Stage::InitialConfigured).await;
        }
        if request.session_id.0.as_ref() == "session-2" {
            self.state.hold(Stage::Configured).await;
        }
        Ok(SetSessionConfigOptionResponse::new(options("sonnet")))
    }
    async fn close_session(
        &self,
        request: CloseSessionRequest,
    ) -> Result<CloseSessionResponse, AcpError> {
        self.state
            .closed
            .lock()
            .unwrap()
            .push(request.session_id.0.to_string());
        if request.session_id.0.as_ref() == "session-1" {
            self.state.hold(Stage::Close).await;
        }
        Ok(CloseSessionResponse::new())
    }
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "the connection trait returns a future"
    )]
    async fn cancel(&self, request: CancelNotification) -> Result<(), AcpError> {
        self.state
            .cancelled
            .lock()
            .unwrap()
            .push(request.session_id.0.to_string());
        Ok(())
    }
    async fn prompt(&self, request: PromptRequest) -> Result<PromptResponse, AcpError> {
        let count = {
            let mut prompts = self.state.prompts.lock().unwrap();
            prompts.push(request.clone());
            prompts.len()
        };
        if count == 1 {
            if self.state.stage == Stage::Permission {
                let request = serde_json::from_value(json!({"sessionId":request.session_id,"toolCall":{"toolCallId":"tool-1","title":"Write"},"options":[{"optionId":"no","name":"No","kind":"reject_once"}]})).unwrap();
                let reply = self.client.request_permission(request).await?;
                assert_eq!(
                    serde_json::to_value(reply).unwrap()["outcome"]["optionId"],
                    "no"
                );
            }
            return Err(AcpSessionInvalidatedError::new(
                "unusable",
                AcpSessionInvalidatedErrorOptions {
                    recovery_authorized: true,
                },
            )
            .into());
        }
        if count == 2
            && matches!(
                self.state.stage,
                Stage::Prompt | Stage::FailedPrompt | Stage::AnsweredPrompt
            )
        {
            let update = serde_json::from_value(json!({"sessionId":request.session_id,"update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"partial"}}})).unwrap();
            self.client.session_update(update).await;
            self.state.hold(Stage::Prompt).await;
            if self.state.stage == Stage::FailedPrompt {
                return Err(AcpSessionInvalidatedError::new(
                    "late refusal",
                    AcpSessionInvalidatedErrorOptions {
                        recovery_authorized: true,
                    },
                )
                .into());
            }
        }
        Ok(PromptResponse::new(StopReason::EndTurn))
    }
}

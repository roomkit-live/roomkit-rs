//! Transport callbacks at opening/configuration return and at prompt admission.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use agent_client_protocol_schema::v1::*;
use roomkit::model::{ChannelId, Metadata};
use tokio::io::DuplexStream;
use tokio::sync::mpsc;

use super::super::Piped;
use crate::{
    AcpChannel, AcpChannelOptions, AcpClient, AcpConnection, AcpError, AcpTransport,
    ClientSideConnection,
};

type Action = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

#[derive(Clone, Copy, PartialEq)]
pub(super) enum Boundary {
    Opened,
    Configured,
    Prompting,
}

#[derive(Default)]
pub(super) struct Actions(Mutex<Option<(Boundary, Action)>>);

impl Actions {
    pub(super) fn at(&self, boundary: Boundary, action: Action) {
        *self.0.lock().unwrap() = Some((boundary, action));
    }

    fn take(&self, boundary: Boundary) -> Option<Action> {
        let mut action = self.0.lock().unwrap();
        if action.as_ref().is_some_and(|(at, _)| *at == boundary) {
            action.take().map(|(_, action)| action)
        } else {
            None
        }
    }

    async fn run(&self, boundary: Boundary) {
        if let Some(action) = self.take(boundary) {
            action().await;
        }
    }
}

pub(super) struct Transport {
    piped: Piped,
    actions: Arc<Actions>,
}

pub(super) struct Connection {
    inner: ClientSideConnection,
    actions: Arc<Actions>,
}

impl AcpTransport for Transport {
    type Connection = Connection;

    fn name(&self) -> &'static str {
        "standalone-boundaries"
    }

    async fn open(&self, client: Arc<dyn AcpClient>) -> Result<Connection, AcpError> {
        Ok(Connection {
            inner: self.piped.open(client).await?,
            actions: Arc::clone(&self.actions),
        })
    }

    async fn close(&self) {
        self.piped.close().await;
    }
}

impl AcpConnection for Connection {
    async fn initialize(&self, request: InitializeRequest) -> Result<InitializeResponse, AcpError> {
        self.inner.initialize(request).await
    }

    async fn authenticate(
        &self,
        request: AuthenticateRequest,
    ) -> Result<AuthenticateResponse, AcpError> {
        self.inner.authenticate(request).await
    }

    async fn new_session(
        &self,
        request: NewSessionRequest,
    ) -> Result<NewSessionResponse, AcpError> {
        let response = self.inner.new_session(request).await?;
        self.actions.run(Boundary::Opened).await;
        Ok(response)
    }

    async fn set_config_option(
        &self,
        request: SetSessionConfigOptionRequest,
    ) -> Result<SetSessionConfigOptionResponse, AcpError> {
        let response = self.inner.set_config_option(request).await?;
        self.actions.run(Boundary::Configured).await;
        Ok(response)
    }

    async fn prompt(&self, request: PromptRequest) -> Result<PromptResponse, AcpError> {
        self.actions.run(Boundary::Prompting).await;
        self.inner.prompt(request).await
    }

    async fn cancel(&self, notification: CancelNotification) -> Result<(), AcpError> {
        self.inner.cancel(notification).await
    }

    async fn close_session(
        &self,
        request: CloseSessionRequest,
    ) -> Result<CloseSessionResponse, AcpError> {
        self.inner.close_session(request).await
    }
}

pub(super) fn channel(
    setup: Option<Metadata>,
) -> (
    AcpChannel<Transport>,
    mpsc::UnboundedReceiver<DuplexStream>,
    Arc<Actions>,
) {
    let (agents, incoming) = mpsc::unbounded_channel();
    let actions = Arc::new(Actions::default());
    let channel = AcpChannel::new(
        ChannelId::new("claude").unwrap(),
        "/srv",
        AcpChannelOptions {
            transport: Some(Transport {
                piped: Piped {
                    agents,
                    closed: Arc::default(),
                    log: Arc::default(),
                },
                actions: Arc::clone(&actions),
            }),
            session_config: setup,
            ..Default::default()
        },
    )
    .unwrap();
    (channel, incoming, actions)
}

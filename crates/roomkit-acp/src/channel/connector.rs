//! The channel's connection to its agent and the rooms' sessions: Python's
//! `ACPConnectionMixin` and the room half of `ACPSessionsMixin`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use agent_client_protocol_schema::ProtocolVersion;
use agent_client_protocol_schema::v1::{
    AuthenticateRequest, CancelNotification, ClientCapabilities, CloseSessionRequest,
    FileSystemCapabilities, Implementation, InitializeRequest, InitializeResponse,
    NewSessionRequest,
};
use roomkit::model::RoomId;
use serde_json::{Map, Value, json};

use super::client::{ChannelClient, Turns};
use crate::connection::AcpConnection;
use crate::error::AcpError;
use crate::transport::AcpTransport;

/// The protocol's version RoomKit speaks: the stable ACP v1.
pub(super) const PROTOCOL_VERSION: u16 = 1;

/// The connection, its agent's answer to `initialize`, and the rooms'
/// sessions on it.
pub(super) struct Connector<T: AcpTransport> {
    pub(super) transport: Arc<T>,
    authentication_method: Option<String>,
    pub(super) turns: Arc<Turns>,
    /// One connection at a time.
    connecting: Arc<tokio::sync::Mutex<()>>,
    state: Mutex<State<T::Connection>>,
    /// Each room's lock its session is opened under.
    openings: Mutex<HashMap<RoomId, Arc<tokio::sync::Mutex<()>>>>,
}

struct State<C> {
    connection: Option<Arc<C>>,
    agent_info: Option<Value>,
    closes_sessions: bool,
    sessions: HashMap<RoomId, String>,
    /// No turn connects any more.
    closed: bool,
}

impl<T: AcpTransport> Connector<T> {
    pub(super) fn new(transport: T, authentication_method: Option<String>) -> Self {
        Self {
            transport: Arc::new(transport),
            authentication_method,
            turns: Arc::new(Turns::default()),
            connecting: Arc::new(tokio::sync::Mutex::new(())),
            state: Mutex::new(State {
                connection: None,
                agent_info: None,
                closes_sessions: true,
                sessions: HashMap::new(),
                closed: false,
            }),
            openings: Mutex::new(HashMap::new()),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State<T::Connection>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The live connection, opened and initialized when there is none, or
    /// opened again, every session dropped, when the transport says its
    /// connection is dead: Python's `_ensure_connection`.
    ///
    /// # Errors
    ///
    /// `AcpError::Runtime` once the channel is closed or when the agent
    /// selects a protocol version that is not 1; the transport's or the
    /// handshake's error, the transport closed then.
    pub(super) async fn connection(&self) -> Result<Arc<T::Connection>, AcpError> {
        if let Some(connection) = self.live()? {
            return Ok(connection);
        }
        let connecting = Arc::clone(&self.connecting).lock_owned().await;
        if let Some(connection) = self.live()? {
            return Ok(connection);
        }
        let dead = self.lock().connection.take();
        if dead.is_some() {
            // The agent behind the old connection is gone, its sessions
            // with it: a reconnect never resumes them.
            self.transport.close().await;
            self.lock().sessions.clear();
        }
        let client = Arc::new(ChannelClient {
            turns: Arc::clone(&self.turns),
        });
        // A turn dropped part-way through the handshake closes what it
        // opened, as Python's closes on any exception, still under the
        // connecting lock, so that no turn opens before the close ran.
        let mut opening = Opening {
            transport: Some(Arc::clone(&self.transport)),
            connecting: Some(connecting),
        };
        let handshake = self.handshake(client).await;
        opening.transport = None;
        let handshake = match handshake {
            // A close that ran meanwhile leaves no connection open after it,
            // as Python's re-checks under its lock.
            Ok(_) if self.lock().closed => {
                self.transport.close().await;
                Err(AcpError::Runtime("ACPChannel is closed".to_owned()))
            }
            other => other,
        };
        match handshake {
            Ok((connection, answer)) => {
                let connection = Arc::new(connection);
                let mut state = self.lock();
                state.agent_info = answer
                    .agent_info
                    .as_ref()
                    .and_then(|info| serde_json::to_value(info).ok());
                state.closes_sessions = answer
                    .agent_capabilities
                    .session_capabilities
                    .close
                    .is_some();
                state.connection = Some(Arc::clone(&connection));
                Ok(connection)
            }
            Err(error) => {
                // A transport that never opened as well as a handshake that
                // failed on a live one: closing tolerates both.
                self.transport.close().await;
                Err(error)
            }
        }
    }

    /// The connection, when it is there and its transport alive.
    fn live(&self) -> Result<Option<Arc<T::Connection>>, AcpError> {
        let state = self.lock();
        if state.closed {
            return Err(AcpError::Runtime("ACPChannel is closed".to_owned()));
        }
        Ok(state
            .connection
            .as_ref()
            .filter(|_| self.transport.is_alive())
            .map(Arc::clone))
    }

    async fn handshake(
        &self,
        client: Arc<ChannelClient>,
    ) -> Result<(T::Connection, InitializeResponse), AcpError> {
        let connection = self.transport.open(client).await?;
        let capabilities = ClientCapabilities::new()
            .fs(FileSystemCapabilities::new()
                .read_text_file(false)
                .write_text_file(false))
            .terminal(false);
        let request = InitializeRequest::new(ProtocolVersion::from(PROTOCOL_VERSION))
            .client_capabilities(capabilities)
            .client_info(
                Implementation::new("roomkit", env!("CARGO_PKG_VERSION")).title("RoomKit"),
            );
        let answer = connection.initialize(request).await?;
        let version = answer.protocol_version.as_u16();
        if version != PROTOCOL_VERSION {
            return Err(AcpError::Runtime(format!(
                "ACP protocol negotiation failed: agent selected version {version}, expected \
                 {PROTOCOL_VERSION}"
            )));
        }
        if let Some(method) = &self.authentication_method {
            connection
                .authenticate(AuthenticateRequest::new(method.clone()))
                .await?;
        }
        Ok((connection, answer))
    }

    /// The room's session, opened on `connection` when it has none, one
    /// opening of the room's at a time: `session/new` with the channel's
    /// directory and the room in its `_meta`, scope `room`.
    pub(super) async fn session(
        &self,
        room_id: &RoomId,
        connection: &T::Connection,
        cwd: &std::path::Path,
    ) -> Result<String, AcpError> {
        if let Some(session) = self.lock().sessions.get(room_id) {
            return Ok(session.clone());
        }
        let opening = Arc::clone(
            self.openings
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .entry(room_id.clone())
                .or_default(),
        );
        let _opening = opening.lock().await;
        if let Some(session) = self.lock().sessions.get(room_id) {
            return Ok(session.clone());
        }
        let mut meta = Map::new();
        meta.insert("roomkit.live/roomId".to_owned(), json!(room_id.as_str()));
        meta.insert("roomkit.live/sessionScope".to_owned(), json!("room"));
        let answer = connection
            .new_session(NewSessionRequest::new(cwd).meta(meta))
            .await?;
        let session = answer.session_id.0.to_string();
        self.lock()
            .sessions
            .insert(room_id.clone(), session.clone());
        Ok(session)
    }

    /// The room's session, if it has one.
    pub(super) fn session_of(&self, room_id: &RoomId) -> Option<String> {
        self.lock().sessions.get(room_id).cloned()
    }

    /// What `info` reports of the connection: whether it is open, the
    /// agent's `agentInfo`, how many sessions it holds.
    pub(super) fn report(&self) -> (bool, Option<Value>, usize) {
        let state = self.lock();
        (
            state.connection.is_some(),
            state.agent_info.clone(),
            state.sessions.len(),
        )
    }

    /// Closes the channel, as Python's `close()`: the prompts in flight
    /// cancelled and the sessions closed where the agent takes
    /// `session/close`, at once and within [`SHUTDOWN_GRACE`], then the
    /// transport, whatever the agent answered; nothing opens after. A
    /// second close does nothing; a close cut short still closes the
    /// transport, in the background. Never fails.
    pub(super) async fn close(&self) {
        let (connection, sessions, closes) = {
            let mut state = self.lock();
            if state.closed {
                return;
            }
            state.closed = true;
            let sessions: Vec<String> =
                state.sessions.drain().map(|(_, session)| session).collect();
            (state.connection.take(), sessions, state.closes_sessions)
        };
        let mut teardown = Teardown {
            transport: Some(Arc::clone(&self.transport)),
        };
        if let Some(connection) = connection {
            let prompting = self.turns.sessions();
            let closing = if closes { sessions } else { Vec::new() };
            // An agent that stopped answering cannot hold the close.
            let _late =
                tokio::time::timeout(SHUTDOWN_GRACE, goodbye(connection, prompting, closing)).await;
        }
        teardown.transport = None;
        self.transport.close().await;
    }
}

/// The graceful half of a close: each prompt in flight cancelled, then
/// each session closed, at once, as Python's `_say_goodbye` gathers them.
async fn goodbye<C: AcpConnection>(
    connection: Arc<C>,
    prompting: Vec<String>,
    sessions: Vec<String>,
) {
    for session in prompting {
        // An agent already gone has nothing to cancel.
        let _gone = connection.cancel(CancelNotification::new(session)).await;
    }
    let mut closing = tokio::task::JoinSet::new();
    for session in sessions {
        let connection = Arc::clone(&connection);
        closing.spawn(async move {
            // An agent that refuses keeps the session until the connection
            // closes, which it does now.
            let _refused = connection
                .close_session(CloseSessionRequest::new(session))
                .await;
        });
    }
    while closing.join_next().await.is_some() {}
}

/// A close under way: dropped before its transport was closed, it closes
/// it in the background, as Python's `finally` tears it down when its
/// caller is cancelled.
struct Teardown<T: AcpTransport> {
    transport: Option<Arc<T>>,
}

impl<T: AcpTransport> Drop for Teardown<T> {
    fn drop(&mut self) {
        let Some(transport) = self.transport.take() else {
            return;
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move { transport.close().await });
        }
    }
}

/// How long the agent is given to acknowledge the sessions' closes:
/// Python's `_SHUTDOWN_TIMEOUT`.
const SHUTDOWN_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

/// A handshake under way: dropped before it ended, it closes the transport
/// it opened, holding the connecting lock until the close ran.
struct Opening<T: AcpTransport> {
    transport: Option<Arc<T>>,
    connecting: Option<tokio::sync::OwnedMutexGuard<()>>,
}

impl<T: AcpTransport> Drop for Opening<T> {
    fn drop(&mut self) {
        let Some(transport) = self.transport.take() else {
            return;
        };
        let connecting = self.connecting.take();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                transport.close().await;
                drop(connecting);
            });
        }
    }
}

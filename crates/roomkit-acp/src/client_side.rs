//! The client's side of an ACP connection over a byte stream: JSON-RPC 2.0,
//! one message per line, as the Python SDK's `ClientSideConnection` speaks
//! it.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};

use agent_client_protocol_schema::v1::{
    AuthenticateRequest, AuthenticateResponse, CancelNotification, CloseSessionRequest,
    CloseSessionResponse, CreateElicitationRequest, CreateTerminalRequest, InitializeRequest,
    InitializeResponse, KillTerminalRequest, NewSessionRequest, NewSessionResponse, PromptRequest,
    PromptResponse, ReadTextFileRequest, ReleaseTerminalRequest, RequestPermissionRequest,
    SessionNotification, SetSessionConfigOptionRequest, SetSessionConfigOptionResponse,
    TerminalOutputRequest, WaitForTerminalExitRequest, WriteTextFileRequest,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value, json};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::oneshot;
use tokio::task::{JoinHandle, JoinSet};

use crate::client::AcpClient;
use crate::connection::AcpConnection;
use crate::error::{AcpError, INTERNAL_ERROR, INVALID_PARAMS};

/// The write half of the connection, as its writer is boxed.
type Writer = Box<dyn AsyncWrite + Send + Unpin>;

/// The answer a request waits for: the response's `result`, or its error.
type Answer = Result<Value, AcpError>;

/// The client's side of an ACP connection over a byte stream: the Python
/// SDK's `ClientSideConnection` (HITL 3592a76c), JSON-RPC 2.0 with one
/// message per line.
///
/// [`new`](Self::new) starts the connection's reader, one task that reads
/// the agent's messages in the order it sent them: a response goes to the
/// request waiting for it, a `session/update` notification to the
/// [`AcpClient`] before the next message is read, and a request of the
/// agent's is answered by a task of its own while the reading goes on.
/// `session/request_permission` is the client's to answer; every other
/// request is answered `method_not_found`, the Python implementation's
/// refusal of the file system, the terminals, elicitation and extensions,
/// and every other notification is ignored. A line that is not JSON is
/// skipped, as the SDK skips it.
///
/// The connection's requests are numbered from 0. Once the agent's stream
/// ends, or the connection is [closed](Self::close), every request still
/// waiting fails with [`AcpError::ConnectionClosed`], and so does every
/// later one. A clone is the same connection; dropping the last one stops
/// its reader and the tasks answering the agent.
#[derive(Clone)]
pub struct ClientSideConnection {
    shared: Arc<Shared>,
    tasks: Arc<Tasks>,
}

/// What the connection's handles and its reader share.
struct Shared {
    /// The stream to the agent, none once the connection is closed.
    writer: tokio::sync::Mutex<Option<Writer>>,
    state: Mutex<State>,
    client: Arc<dyn AcpClient>,
}

/// The requests waiting for an answer, and whether more may be sent.
#[derive(Default)]
struct State {
    next_id: i64,
    pending: HashMap<i64, oneshot::Sender<Answer>>,
    /// The agent's stream ended, or the connection was closed.
    ended: bool,
}

/// The connection's reader, which holds the tasks answering the agent's
/// requests: stopped, with them, when the last handle goes.
struct Tasks {
    reader: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for Tasks {
    fn drop(&mut self) {
        if let Some(reader) = self
            .reader
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            reader.abort();
        }
    }
}

impl ClientSideConnection {
    /// The connection speaking to the agent over `reader`, what the agent
    /// writes, and `writer`, what it reads, its callbacks going to
    /// `client`: the Python SDK's `connect_to_agent(client, writer,
    /// reader)`. Its reader starts at once, on the current tokio runtime,
    /// which must exist.
    pub fn new<R, W>(client: Arc<dyn AcpClient>, reader: R, writer: W) -> Self
    where
        R: AsyncRead + Send + Unpin + 'static,
        W: AsyncWrite + Send + Unpin + 'static,
    {
        let shared = Arc::new(Shared {
            writer: tokio::sync::Mutex::new(Some(Box::new(writer))),
            state: Mutex::new(State::default()),
            client,
        });
        let reader = tokio::spawn(read(Arc::clone(&shared), reader));
        Self {
            shared,
            tasks: Arc::new(Tasks {
                reader: Mutex::new(Some(reader)),
            }),
        }
    }

    /// Closes the connection, as the SDK's `close()`: every request waiting
    /// fails with [`AcpError::ConnectionClosed`], the stream to the agent is
    /// shut, and the reader stops with the tasks answering the agent. A
    /// second call does nothing.
    pub async fn close(&self) {
        self.shared.end();
        let writer = self.shared.writer.lock().await.take();
        if let Some(mut writer) = writer {
            // A stream the agent closed already cannot be shut again.
            let _shut = writer.shutdown().await;
        }
        let reader = self
            .tasks
            .reader
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(reader) = reader {
            reader.abort();
        }
    }

    /// Sends the request `method` with `params` and waits for its answer,
    /// read as `T`; `from_object` reads an answer that is no object as an
    /// empty one, as the SDK reads the answers of `authenticate`,
    /// `session/close` and `session/set_config_option`.
    async fn request<P, T>(
        &self,
        method: &str,
        params: &P,
        from_object: bool,
    ) -> Result<T, AcpError>
    where
        P: Serialize + Sync,
        T: DeserializeOwned,
    {
        let params = to_params(method, params)?;
        let (id, answer) = self.shared.register()?;
        let waiting = Waiting {
            shared: &self.shared,
            id,
        };
        let message = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
        self.shared.send(&message).await?;
        let result = answer
            .await
            .map_err(|_closed| AcpError::ConnectionClosed)??;
        drop(waiting);
        let result = if from_object && !result.is_object() {
            Value::Object(Map::new())
        } else {
            result
        };
        serde_json::from_value(result).map_err(|error| AcpError::InvalidResponse {
            method: method.to_owned(),
            reason: error.to_string(),
        })
    }

    /// Sends the notification `method` with `params`.
    async fn notify<P: Serialize + Sync>(&self, method: &str, params: &P) -> Result<(), AcpError> {
        let params = to_params(method, params)?;
        if self.shared.has_ended() {
            return Err(AcpError::ConnectionClosed);
        }
        let message = json!({ "jsonrpc": "2.0", "method": method, "params": params });
        self.shared.send(&message).await
    }
}

impl AcpConnection for ClientSideConnection {
    async fn initialize(&self, request: InitializeRequest) -> Result<InitializeResponse, AcpError> {
        self.request("initialize", &request, false).await
    }

    async fn authenticate(
        &self,
        request: AuthenticateRequest,
    ) -> Result<AuthenticateResponse, AcpError> {
        self.request("authenticate", &request, true).await
    }

    async fn new_session(
        &self,
        request: NewSessionRequest,
    ) -> Result<NewSessionResponse, AcpError> {
        self.request("session/new", &request, false).await
    }

    async fn prompt(&self, request: PromptRequest) -> Result<PromptResponse, AcpError> {
        // The reader hands each update to the client before it reads the
        // next message: once the answer is read, every update the agent
        // sent before it was taken.
        self.request("session/prompt", &request, false).await
    }

    async fn cancel(&self, notification: CancelNotification) -> Result<(), AcpError> {
        self.notify("session/cancel", &notification).await
    }

    async fn close_session(
        &self,
        request: CloseSessionRequest,
    ) -> Result<CloseSessionResponse, AcpError> {
        self.request("session/close", &request, true).await
    }

    async fn set_config_option(
        &self,
        request: SetSessionConfigOptionRequest,
    ) -> Result<SetSessionConfigOptionResponse, AcpError> {
        self.request("session/set_config_option", &request, true)
            .await
    }
}

impl fmt::Debug for ClientSideConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        f.debug_struct("ClientSideConnection")
            .field("pending", &state.pending.len())
            .field("ended", &state.ended)
            .finish_non_exhaustive()
    }
}

/// A request waiting for its answer: dropped before the answer came (its
/// caller cancelled), it takes the request out of the waiting ones, as the
/// SDK forgets a request whose wait was cancelled; dropped after, it finds
/// it gone already.
struct Waiting<'connection> {
    shared: &'connection Shared,
    id: i64,
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.shared.lock().pending.remove(&self.id);
    }
}

impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A new request's id and the channel its answer comes on; an error
    /// once the connection has ended.
    fn register(&self) -> Result<(i64, oneshot::Receiver<Answer>), AcpError> {
        let mut state = self.lock();
        if state.ended {
            return Err(AcpError::ConnectionClosed);
        }
        let id = state.next_id;
        state.next_id += 1;
        let (sender, receiver) = oneshot::channel();
        state.pending.insert(id, sender);
        Ok((id, receiver))
    }

    fn has_ended(&self) -> bool {
        self.lock().ended
    }

    /// Ends the connection: every request waiting fails, and no other is
    /// sent.
    fn end(&self) {
        let pending = {
            let mut state = self.lock();
            state.ended = true;
            std::mem::take(&mut state.pending)
        };
        for (_, waiting) in pending {
            // A request whose caller went has no one to tell.
            let _gone = waiting.send(Err(AcpError::ConnectionClosed));
        }
    }

    /// Writes `message` to the agent, one line.
    async fn send(&self, message: &Value) -> Result<(), AcpError> {
        let mut line = message.to_string();
        line.push('\n');
        let mut writer = self.writer.lock().await;
        let Some(writer) = writer.as_mut() else {
            return Err(AcpError::ConnectionClosed);
        };
        writer
            .write_all(line.as_bytes())
            .await
            .map_err(|_failed| AcpError::ConnectionClosed)?;
        writer
            .flush()
            .await
            .map_err(|_failed| AcpError::ConnectionClosed)
    }

    /// Gives the response `message` to the request waiting for it, if one
    /// is: its `result`, or its `error`, or none when it holds neither.
    fn answer(&self, message: &Map<String, Value>) {
        let Some(id) = message.get("id").and_then(Value::as_i64) else {
            return;
        };
        let Some(waiting) = self.lock().pending.remove(&id) else {
            return;
        };
        let answer = if let Some(result) = message.get("result") {
            Ok(result.clone())
        } else if let Some(error) = message.get("error") {
            Err(error_of(error))
        } else {
            Ok(Value::Null)
        };
        // A request whose caller went has no one to tell.
        let _gone = waiting.send(answer);
    }
}

/// Reads the agent's messages to the end of its stream, then ends the
/// connection; the tasks answering the agent's requests are waited for.
async fn read<R: AsyncRead + Send + Unpin>(shared: Arc<Shared>, reader: R) {
    let mut reader = BufReader::new(reader);
    let mut answering = JoinSet::new();
    let mut line = Vec::new();
    loop {
        line.clear();
        match reader.read_until(b'\n', &mut line).await {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
        // The answers given since the last line are let go of.
        while answering.try_join_next().is_some() {}
        let Some(message) = message_of(&line) else {
            continue;
        };
        take(&shared, message, &mut answering).await;
    }
    shared.end();
    while answering.join_next().await.is_some() {}
}

/// A line of the agent's stream as a message: none for a blank line or one
/// that is no JSON object, which the SDK logs and skips.
fn message_of(line: &[u8]) -> Option<Map<String, Value>> {
    let line = line.trim_ascii();
    if line.is_empty() {
        return None;
    }
    if let Ok(Value::Object(message)) = serde_json::from_slice(line) {
        return Some(message);
    }
    tracing::debug!(target: "roomkit::acp", "an ACP agent's line is no JSON-RPC message");
    None
}

/// Takes one of the agent's messages: a request answered by a task of
/// `answering`, a notification handed to the client, a response given to
/// the request it answers.
async fn take(shared: &Arc<Shared>, message: Map<String, Value>, answering: &mut JoinSet<()>) {
    let Some(method) = message.get("method").and_then(Value::as_str) else {
        if message.contains_key("id") {
            shared.answer(&message);
        }
        return;
    };
    let method = method.to_owned();
    let params = message.get("params").cloned().unwrap_or(Value::Null);
    match message.get("id") {
        Some(id) => {
            let shared = Arc::clone(shared);
            let id = id.clone();
            answering.spawn(async move {
                let response = match serve(&shared, &method, params).await {
                    Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
                    Err(error) => {
                        json!({ "jsonrpc": "2.0", "id": id, "error": error.to_error_object() })
                    }
                };
                // An agent that went cannot be answered.
                let _unanswered = shared.send(&response).await;
            });
        }
        None => notified(shared, &method, params).await,
    }
}

/// The answer to the agent's request `method`: the client's for a
/// permission, `method_not_found` for any other. The parameters of a
/// method of the protocol are read first, as the SDK reads them before its
/// client is called: parameters it refuses are answered `invalid_params`.
async fn serve(shared: &Shared, method: &str, params: Value) -> Answer {
    match method {
        "session/request_permission" => {
            let request: RequestPermissionRequest = read_params(params)?;
            let response = shared.client.request_permission(request).await?;
            serde_json::to_value(response).map_err(|error| {
                AcpError::rpc(
                    INTERNAL_ERROR,
                    "Internal error",
                    Some(json!({ "details": error.to_string() })),
                )
            })
        }
        "fs/read_text_file" => refused::<ReadTextFileRequest>(method, params),
        "fs/write_text_file" => refused::<WriteTextFileRequest>(method, params),
        "terminal/create" => refused::<CreateTerminalRequest>(method, params),
        "terminal/output" => refused::<TerminalOutputRequest>(method, params),
        "terminal/release" => refused::<ReleaseTerminalRequest>(method, params),
        "terminal/wait_for_exit" => refused::<WaitForTerminalExitRequest>(method, params),
        "terminal/kill" => refused::<KillTerminalRequest>(method, params),
        // The Python implementation's client refuses elicitation under the
        // name it gives the method.
        "elicitation/create" => {
            refused::<CreateElicitationRequest>("session/request_input", params)
        }
        other => Err(AcpError::method_not_found(other)),
    }
}

/// A request of the protocol RoomKit's client does not serve: its
/// parameters read as `T`, then `method_not_found` naming `named`.
fn refused<T: DeserializeOwned>(named: &str, params: Value) -> Answer {
    let _read: T = read_params(params)?;
    Err(AcpError::method_not_found(named))
}

/// `params` read as `T`, or the `invalid_params` error naming why.
fn read_params<T: DeserializeOwned>(params: Value) -> Result<T, AcpError> {
    serde_json::from_value(params).map_err(|error| {
        AcpError::rpc(
            INVALID_PARAMS,
            "Invalid params",
            Some(json!({ "errors": error.to_string() })),
        )
    })
}

/// Takes the agent's notification `method`: a session update goes to the
/// client, any other is ignored.
async fn notified(shared: &Shared, method: &str, params: Value) {
    if method != "session/update" {
        tracing::debug!(target: "roomkit::acp", method, "an ACP agent's notification is ignored");
        return;
    }
    match serde_json::from_value::<SessionNotification>(params) {
        Ok(notification) => shared.client.session_update(notification).await,
        Err(error) => {
            tracing::debug!(
                target: "roomkit::acp",
                %error,
                "an ACP agent's session update is not valid"
            );
        }
    }
}

/// The error a response's `error` member reads as: its code, message and
/// data, a code that is no integer read as an internal error's and a
/// message that is no text as `Error`, as the SDK reads them.
fn error_of(error: &Value) -> AcpError {
    let code = error
        .get("code")
        .and_then(Value::as_i64)
        .and_then(|code| i32::try_from(code).ok())
        .unwrap_or(INTERNAL_ERROR);
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("Error");
    let data = error.get("data").filter(|data| !data.is_null()).cloned();
    AcpError::rpc(code, message, data)
}

/// `params` as the request's JSON.
fn to_params<P: Serialize>(method: &str, params: &P) -> Result<Value, AcpError> {
    serde_json::to_value(params).map_err(|error| {
        AcpError::Runtime(format!("cannot write the ACP request {method}: {error}"))
    })
}

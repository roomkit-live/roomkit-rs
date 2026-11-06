//! The client's side of an ACP connection over a byte stream: JSON-RPC 2.0,
//! one message per line, as the Python SDK's `ClientSideConnection` speaks
//! it.

mod reader;
mod unwind;
mod updates;
mod writer;

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};

use agent_client_protocol_schema::v1::{
    AuthenticateRequest, AuthenticateResponse, CancelNotification, ClientCapabilities,
    CloseSessionRequest, CloseSessionResponse, InitializeRequest, InitializeResponse,
    NewSessionRequest, NewSessionResponse, PromptRequest, PromptResponse,
    SetSessionConfigOptionRequest, SetSessionConfigOptionResponse,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value, json};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::JoinHandle;

use self::updates::Updates;
use self::writer::Outgoing;
use crate::client::AcpClient;
use crate::connection::AcpConnection;
use crate::error::AcpError;

/// The answer a request waits for: the response's `result`, or its error.
type Answer = Result<Value, AcpError>;

/// The client's side of an ACP connection over a byte stream: the Python
/// SDK's `ClientSideConnection` (HITL 3592a76c), JSON-RPC 2.0 with one
/// message per line.
///
/// [`new`](Self::new) starts the connection's reader, one task that reads
/// the agent's messages in the order it sent them and never waits on the
/// client: a response goes to the request waiting for it; a `session/update`
/// notification joins its session's queue, whose updates the [`AcpClient`]
/// takes one after the other, each session's apart from the others'; a
/// request of the agent's is answered by a task of its own.
/// `session/request_permission` is the client's to answer; every other
/// request is answered `method_not_found`, the Python implementation's
/// refusal of the file system, the terminals, elicitation and extensions,
/// and every other notification is ignored. A client's callback that
/// panics is taken as one that raised, as the SDK logs a handler's error
/// and goes on. A line that is not JSON is skipped, as the SDK skips it;
/// one that is JSON but no object ends the connection, as the SDK's reader
/// ends on it.
///
/// Each message is written whole by a task of its own, as the SDK's sender
/// writes it, so a request its caller drops mid-write is still written
/// whole. The connection's requests are numbered from 0. Once the agent's
/// stream ends, or the connection is [closed](Self::close), every request
/// still waiting fails with [`AcpError::ConnectionClosed`], and so does
/// every later one. A clone is the same connection; dropping the last one
/// stops its reader, its writer and the tasks answering the agent.
#[derive(Clone)]
pub struct ClientSideConnection {
    shared: Arc<Shared>,
    tasks: Arc<Tasks>,
}

/// What the connection's handles and its tasks share.
struct Shared {
    /// The queue of the messages the writer writes, none once the
    /// connection is closed.
    outbox: Mutex<Option<mpsc::UnboundedSender<Outgoing>>>,
    state: Mutex<State>,
    client: Arc<dyn AcpClient>,
    updates: Updates,
}

/// The requests waiting for an answer, and whether more may be sent.
#[derive(Default)]
struct State {
    next_id: i64,
    pending: HashMap<i64, oneshot::Sender<Answer>>,
    /// The agent's stream ended, or the connection was closed.
    ended: bool,
}

/// The connection's reader, which holds the tasks answering the agent, and
/// its writer: stopped when the last handle goes.
struct Tasks {
    reader: Mutex<Option<JoinHandle<()>>>,
    writer: Mutex<Option<JoinHandle<()>>>,
    /// Asks the reader to stop, the tasks it holds stopped and waited for.
    stop: Arc<Notify>,
}

impl Tasks {
    fn take(slot: &Mutex<Option<JoinHandle<()>>>) -> Option<JoinHandle<()>> {
        slot.lock().unwrap_or_else(PoisonError::into_inner).take()
    }
}

impl Drop for Tasks {
    fn drop(&mut self) {
        for task in [Self::take(&self.reader), Self::take(&self.writer)]
            .into_iter()
            .flatten()
        {
            task.abort();
        }
    }
}

impl ClientSideConnection {
    /// The connection speaking to the agent over `reader`, what the agent
    /// writes, and `writer`, what it reads, its callbacks going to
    /// `client`: the Python SDK's `connect_to_agent(client, writer,
    /// reader)`. Its reader and its writer start at once, on the current
    /// tokio runtime, which must exist.
    pub fn new<R, W>(client: Arc<dyn AcpClient>, reader: R, writer: W) -> Self
    where
        R: AsyncRead + Send + Unpin + 'static,
        W: AsyncWrite + Send + Unpin + 'static,
    {
        let (outbox, written) = mpsc::unbounded_channel();
        let shared = Arc::new(Shared {
            outbox: Mutex::new(Some(outbox)),
            state: Mutex::new(State::default()),
            client,
            updates: Updates::default(),
        });
        let stop = Arc::new(Notify::new());
        let reader = tokio::spawn(reader::read(Arc::clone(&shared), reader, Arc::clone(&stop)));
        let writer = tokio::spawn(writer::write(writer, written));
        Self {
            shared,
            tasks: Arc::new(Tasks {
                reader: Mutex::new(Some(reader)),
                writer: Mutex::new(Some(writer)),
                stop,
            }),
        }
    }

    /// Closes the connection, as the SDK's `close()`: every request waiting
    /// fails with [`AcpError::ConnectionClosed`], the reader stops, the
    /// tasks answering the agent and taking its updates stopped and waited
    /// for, then the writer goes, which shuts the stream to the agent; a
    /// message it could not write yet is dropped, so that an agent that
    /// stopped reading cannot hold the close. A second call does nothing.
    pub async fn close(&self) {
        self.shared.end();
        let outbox = self
            .shared
            .outbox
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        drop(outbox);
        if let Some(reader) = Tasks::take(&self.tasks.reader) {
            self.tasks.stop.notify_one();
            // A reader that panicked has ended all the same.
            let _ended = reader.await;
        }
        if let Some(writer) = Tasks::take(&self.tasks.writer) {
            writer.abort();
            let _ended = writer.await;
        }
    }

    /// Sends the request `method` with `params` and waits for its answer,
    /// read as `T`; `from_object` reads an answer that is no object as an
    /// empty one, as the SDK reads the answers of `authenticate`,
    /// `session/close` and `session/set_config_option`.
    async fn request<T: DeserializeOwned>(
        &self,
        method: &str,
        params: Value,
        from_object: bool,
    ) -> Result<T, AcpError> {
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
    async fn notify(&self, method: &str, params: Value) -> Result<(), AcpError> {
        if self.shared.has_ended() {
            return Err(AcpError::ConnectionClosed);
        }
        let message = json!({ "jsonrpc": "2.0", "method": method, "params": params });
        self.shared.send(&message).await
    }
}

impl AcpConnection for ClientSideConnection {
    async fn initialize(&self, request: InitializeRequest) -> Result<InitializeResponse, AcpError> {
        let mut params = to_params("initialize", &request)?;
        without_default_capabilities(&mut params);
        self.request("initialize", params, false).await
    }

    async fn authenticate(
        &self,
        request: AuthenticateRequest,
    ) -> Result<AuthenticateResponse, AcpError> {
        let params = to_params("authenticate", &request)?;
        self.request("authenticate", params, true).await
    }

    async fn new_session(
        &self,
        request: NewSessionRequest,
    ) -> Result<NewSessionResponse, AcpError> {
        let params = to_params("session/new", &request)?;
        self.request("session/new", params, false).await
    }

    async fn prompt(&self, request: PromptRequest) -> Result<PromptResponse, AcpError> {
        let session = request.session_id.0.to_string();
        let params = to_params("session/prompt", &request)?;
        let answer = self.request("session/prompt", params, false).await;
        // Its answer read, the prompt waits for the updates of its session
        // the agent sent before it, as the SDK's prompt does, its answer or
        // its error alike.
        self.shared
            .updates
            .taken(self.shared.address(), &session)
            .await;
        answer
    }

    async fn cancel(&self, notification: CancelNotification) -> Result<(), AcpError> {
        let params = to_params("session/cancel", &notification)?;
        self.notify("session/cancel", params).await
    }

    async fn close_session(
        &self,
        request: CloseSessionRequest,
    ) -> Result<CloseSessionResponse, AcpError> {
        let params = to_params("session/close", &request)?;
        self.request("session/close", params, true).await
    }

    async fn set_config_option(
        &self,
        request: SetSessionConfigOptionRequest,
    ) -> Result<SetSessionConfigOptionResponse, AcpError> {
        let params = to_params("session/set_config_option", &request)?;
        self.request("session/set_config_option", params, true)
            .await
    }
}

impl fmt::Debug for ClientSideConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.shared.lock();
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
    /// The connection's address, which tells it from another's.
    fn address(&self) -> usize {
        std::ptr::from_ref(self).addr()
    }

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

    /// Hands `message` to the writer as one line, and waits until it is
    /// written: a message once handed over is written whole, whether its
    /// caller still waits or not.
    async fn send(&self, message: &Value) -> Result<(), AcpError> {
        let mut line = message.to_string();
        line.push('\n');
        let (written, done) = oneshot::channel();
        {
            let outbox = self.outbox.lock().unwrap_or_else(PoisonError::into_inner);
            let Some(outbox) = outbox.as_ref() else {
                return Err(AcpError::ConnectionClosed);
            };
            outbox
                .send(Outgoing { line, written })
                .map_err(|_closed| AcpError::ConnectionClosed)?;
        }
        match done.await {
            Ok(true) => Ok(()),
            Ok(false) | Err(_) => Err(AcpError::ConnectionClosed),
        }
    }

    /// Gives the response `message` to the request waiting for it, if one
    /// is: its `result`, else its `error`, else none. An error that is not
    /// an object ends the connection, as it ends the SDK's reader, every
    /// request waiting failing with it (HITL db51d363).
    fn answer(&self, message: &Map<String, Value>) -> Result<(), Ended> {
        let Some(id) = message.get("id").and_then(request_id) else {
            return Ok(());
        };
        let answer = if let Some(result) = message.get("result") {
            Ok(result.clone())
        } else if let Some(error) = message.get("error") {
            Err(reader::error_of(error).ok_or(Ended)?)
        } else {
            Ok(Value::Null)
        };
        let Some(waiting) = self.lock().pending.remove(&id) else {
            return Ok(());
        };
        // A request whose caller went has no one to tell.
        let _gone = waiting.send(answer);
        Ok(())
    }
}

/// The agent sent what ends the connection.
#[derive(Debug)]
struct Ended;

/// The id of a response, as the SDK finds its request by it: an integer,
/// or a number or a boolean equal to one, as Python's `dict` reads a key.
fn request_id(id: &Value) -> Option<i64> {
    match id {
        Value::Number(number) => number.as_i64().or_else(|| {
            let float = number.as_f64()?;
            // An integral float within i64's range reads as that integer.
            #[expect(
                clippy::cast_possible_truncation,
                reason = "an integral float within i64's range converts exactly"
            )]
            let integer = float as i64;
            (float.fract() == 0.0 && float.abs() < 9.0e18).then_some(integer)
        }),
        Value::Bool(flag) => Some(i64::from(*flag)),
        Value::Null | Value::String(_) | Value::Array(_) | Value::Object(_) => None,
    }
}

/// `params` as the request's JSON.
fn to_params<P: Serialize>(method: &str, params: &P) -> Result<Value, AcpError> {
    serde_json::to_value(params).map_err(|error| {
        AcpError::Runtime(format!("cannot write the ACP request {method}: {error}"))
    })
}

/// `initialize`'s parameters without the client's capabilities that are
/// their defaults, as the SDK writes a request (`exclude_defaults`): a
/// client with neither file system nor terminal sends none.
fn without_default_capabilities(params: &mut Value) {
    let Ok(defaults) = serde_json::to_value(ClientCapabilities::default()) else {
        return;
    };
    let Some(params) = params.as_object_mut() else {
        return;
    };
    let Some(capabilities) = params.get_mut("clientCapabilities") else {
        return;
    };
    if without_defaults(capabilities, &defaults) {
        params.shift_remove("clientCapabilities");
    }
}

/// Takes out of `value` each member equal to its default in `defaults`, an
/// object's members recursively; whether `value` is left its default.
fn without_defaults(value: &mut Value, defaults: &Value) -> bool {
    if value == defaults {
        return true;
    }
    let (Some(members), Some(defaults)) = (value.as_object_mut(), defaults.as_object()) else {
        return false;
    };
    let names: Vec<String> = members.keys().cloned().collect();
    for name in names {
        let Some(default) = defaults.get(&name) else {
            continue;
        };
        if members
            .get_mut(&name)
            .is_some_and(|member| without_defaults(member, default))
        {
            members.shift_remove(&name);
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_response_id_reads_as_python_finds_its_request() {
        assert_eq!(request_id(&json!(0)), Some(0));
        assert_eq!(request_id(&json!(0.0)), Some(0));
        assert_eq!(request_id(&json!(2.5)), None);
        assert_eq!(request_id(&json!(true)), Some(1));
        assert_eq!(request_id(&json!("0")), None);
    }

    #[test]
    fn capabilities_at_their_defaults_are_left_out_and_the_others_kept() {
        let mut params = json!({
            "protocolVersion": 1,
            "clientCapabilities": serde_json::to_value(ClientCapabilities::default()).unwrap(),
        });
        without_default_capabilities(&mut params);
        assert_eq!(params, json!({ "protocolVersion": 1 }));
        let mut params = json!({
            "protocolVersion": 1,
            "clientCapabilities": { "fs": { "readTextFile": true, "writeTextFile": false }, "terminal": false },
        });
        without_default_capabilities(&mut params);
        assert_eq!(
            params,
            json!({ "protocolVersion": 1, "clientCapabilities": { "fs": { "readTextFile": true } } })
        );
    }
}

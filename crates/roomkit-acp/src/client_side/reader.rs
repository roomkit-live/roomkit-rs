//! The connection's reader: the agent's messages read in order, a response
//! given to its request, an update queued on its session, a request
//! answered by a task of its own.

use std::sync::Arc;

use agent_client_protocol_schema::v1::{
    CreateTerminalRequest, KillTerminalRequest, ReadTextFileRequest, ReleaseTerminalRequest,
    RequestPermissionRequest, SessionNotification, TerminalOutputRequest,
    WaitForTerminalExitRequest, WriteTextFileRequest,
};
use serde::de::DeserializeOwned;
use serde_json::{Map, Value, json};
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::sync::Notify;
use tokio::task::JoinSet;

use super::unwind::caught;
use super::updates::Updates;
use super::{Answer, Ended, Shared};
use crate::error::{AcpError, INTERNAL_ERROR, INVALID_PARAMS};

/// Ends the connection however the reader ends: at the end of the agent's
/// stream, stopped, aborted, or panicking.
struct EndOnDrop(Arc<Shared>);

impl Drop for EndOnDrop {
    fn drop(&mut self) {
        self.0.end();
        self.0.updates.close();
    }
}

/// Reads the agent's messages to the end of its stream, or until `stop`;
/// then ends the connection. At the end of the stream, the tasks answering
/// the agent and taking its updates are waited for; stopped, they are
/// stopped and waited for.
pub(super) async fn read<R: AsyncRead + Unpin>(shared: Arc<Shared>, reader: R, stop: Arc<Notify>) {
    let _end = EndOnDrop(Arc::clone(&shared));
    let mut reader = BufReader::new(reader);
    let mut working = JoinSet::new();
    let mut line = Vec::new();
    loop {
        line.clear();
        let read = tokio::select! {
            () = stop.notified() => {
                working.shutdown().await;
                return;
            }
            read = reader.read_until(b'\n', &mut line) => read,
        };
        match read {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
        // What was done since the last line is let go of.
        while working.try_join_next().is_some() {}
        match message_of(&line) {
            Line::Skipped => {}
            Line::Ends => break,
            Line::Message(message) => {
                if take(&shared, &message, &mut working).is_err() {
                    break;
                }
            }
        }
    }
    shared.end();
    loop {
        tokio::select! {
            () = stop.notified() => {
                working.shutdown().await;
                return;
            }
            done = working.join_next() => if done.is_none() {
                return;
            },
        }
    }
}

/// A line of the agent's stream, as the SDK's reader reads it.
enum Line {
    /// Blank, or no JSON at all: logged and skipped.
    Skipped,
    /// JSON that is no object (`null` among them): the SDK's reader ends on
    /// it.
    Ends,
    Message(Map<String, Value>),
}

fn message_of(line: &[u8]) -> Line {
    let line = line.trim_ascii();
    if line.is_empty() {
        return Line::Skipped;
    }
    match serde_json::from_slice(line) {
        Ok(Value::Object(message)) => Line::Message(message),
        Ok(_) => {
            tracing::debug!(target: "roomkit::acp", "an ACP agent's message is no JSON object: the connection ends");
            Line::Ends
        }
        Err(_) => {
            tracing::debug!(target: "roomkit::acp", "an ACP agent's line is no JSON: skipped");
            Line::Skipped
        }
    }
}

/// Takes one of the agent's messages: a request answered by a task of
/// `working`, a session update queued on its session, a response given to
/// the request it answers. `Ended` when it ends the connection.
fn take(
    shared: &Arc<Shared>,
    message: &Map<String, Value>,
    working: &mut JoinSet<()>,
) -> Result<(), Ended> {
    let Some(method) = message
        .get("method")
        .filter(|method| !method.is_null())
        .cloned()
    else {
        if message.contains_key("id") {
            shared.answer(message)?;
        }
        return Ok(());
    };
    let params = message.get("params").cloned().unwrap_or(Value::Null);
    match message.get("id") {
        Some(id) => {
            let shared = Arc::clone(shared);
            let id = id.clone();
            working.spawn(async move {
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
        None => notified(shared, &method, params, working),
    }
    Ok(())
}

/// The answer to the agent's request `method`: the client's for a
/// permission, `method_not_found` for any other. The parameters of a
/// method of the protocol the SDK routes are read first, as it reads them
/// before its client is called: parameters it refuses are answered
/// `invalid_params`. Elicitation is not routed: the SDK keeps it behind
/// its unstable protocol, which the Python implementation leaves off.
async fn serve(shared: &Shared, method: &Value, params: Value) -> Answer {
    let Some(name) = method.as_str() else {
        // The SDK's router looks the method up by it: a list or an object
        // it cannot look up, CPython's 3.12 and 3.13 words (those roomkit
        // supports), any other value it does not find.
        return Err(match method {
            Value::Array(_) => internal("unhashable type: 'list'"),
            Value::Object(_) => internal("unhashable type: 'dict'"),
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
                AcpError::method_not_found(method.clone())
            }
        });
    };
    match name {
        "session/request_permission" => {
            let request: RequestPermissionRequest = read_params(params)?;
            let response = caught(shared.client.request_permission(request))
                .await
                .ok_or_else(|| internal("the client's request_permission panicked"))??;
            serde_json::to_value(response).map_err(|error| internal(&error.to_string()))
        }
        "fs/read_text_file" => refused::<ReadTextFileRequest>(name, params),
        "fs/write_text_file" => refused::<WriteTextFileRequest>(name, params),
        "terminal/create" => refused::<CreateTerminalRequest>(name, params),
        "terminal/output" => refused::<TerminalOutputRequest>(name, params),
        "terminal/release" => refused::<ReleaseTerminalRequest>(name, params),
        "terminal/wait_for_exit" => refused::<WaitForTerminalExitRequest>(name, params),
        "terminal/kill" => refused::<KillTerminalRequest>(name, params),
        other => Err(AcpError::method_not_found(other)),
    }
}

/// A request of the protocol RoomKit's client does not serve: its
/// parameters read as `T`, then `method_not_found` naming it.
fn refused<T: DeserializeOwned>(method: &str, params: Value) -> Answer {
    let _read: T = read_params(params)?;
    Err(AcpError::method_not_found(method))
}

/// `params` read as `T`, or the `invalid_params` error naming why, its
/// `errors` a list as pydantic's, serde's message its one item's `msg`
/// (HITL db51d363).
fn read_params<T: DeserializeOwned>(params: Value) -> Result<T, AcpError> {
    serde_json::from_value(params).map_err(|error| {
        AcpError::rpc(
            INVALID_PARAMS,
            "Invalid params",
            Some(json!({ "errors": [{ "msg": error.to_string() }] })),
        )
    })
}

fn internal(details: &str) -> AcpError {
    AcpError::rpc(
        INTERNAL_ERROR,
        "Internal error",
        Some(json!({ "details": details })),
    )
}

/// Takes the agent's notification `method`: a session update is queued on
/// its session, a task of `working` taking the session's updates when none
/// does; any other is ignored.
fn notified(shared: &Arc<Shared>, method: &Value, params: Value, working: &mut JoinSet<()>) {
    if method.as_str() != Some("session/update") {
        tracing::debug!(target: "roomkit::acp", %method, "an ACP agent's notification is ignored");
        return;
    }
    let notification = match serde_json::from_value::<SessionNotification>(params) {
        Ok(notification) => notification,
        Err(error) => {
            tracing::debug!(target: "roomkit::acp", %error, "an ACP agent's session update is not valid");
            return;
        }
    };
    if let Some(session) = shared.updates.queue(notification) {
        working.spawn(take_updates(Arc::clone(shared), session));
    }
}

/// Hands `session`'s updates to the client one after the other, until its
/// queue is empty. One that panics is let go of, as the SDK logs a
/// handler's error.
async fn take_updates(shared: Arc<Shared>, session: String) {
    while let Some(notification) = shared.updates.next(&session) {
        let client = Arc::clone(&shared.client);
        let taken = Updates::taking(
            shared.address(),
            session.clone(),
            caught(client.session_update(notification)),
        )
        .await;
        if taken.is_none() {
            tracing::debug!(target: "roomkit::acp", "an ACP client's session update panicked");
        }
        shared.updates.took(&session);
    }
}

/// The error a response's `error` member reads as, as the SDK reads it: an
/// object's code, message and data, a code that is no integer read as an
/// internal error's (an `i32` holds no other) and a message that is no
/// text as `Error`; an error that reads as false (`null`, `""`, `0`, `[]`)
/// as an internal error. None for any other, which ends the connection.
pub(super) fn error_of(error: &Value) -> Option<AcpError> {
    let falsy = match error {
        Value::Null => true,
        Value::Bool(flag) => !flag,
        Value::Number(number) => number.as_f64() == Some(0.0),
        Value::String(text) => text.is_empty(),
        Value::Array(items) => items.is_empty(),
        Value::Object(members) => members.is_empty(),
    };
    if falsy {
        return Some(AcpError::rpc(INTERNAL_ERROR, "Error", None));
    }
    let error = error.as_object()?;
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
    Some(AcpError::rpc(code, message, data))
}

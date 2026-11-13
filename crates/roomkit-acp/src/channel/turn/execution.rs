//! At most two prompt attempts, serialized by the same room turn guard.

mod hold;
pub(super) use hold::Hold;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use agent_client_protocol_schema::v1::{PromptRequest, PromptResponse};
use roomkit::ChannelError;
use roomkit::model::ResponseMetadata;
use serde_json::{Map, json};
use tokio::sync::oneshot;
use tokio::time::Instant;

use super::{Answered, activity_error, answered, interrupted, provider_error, raised, usage_of};
use crate::channel::Shared;
use crate::channel::updates::Sink;
use crate::connection::AcpConnection;
use crate::error::AcpError;
use crate::transport::AcpTransport;
use hold::Prompt;

pub(super) async fn run<T: AcpTransport>(
    shared: Arc<Shared<T>>,
    ask: super::Ask,
    text: Sink,
    generation: u64,
) -> (Result<Option<Answered>, ChannelError>, Option<Hold<T>>) {
    let turn_id = ask.host.as_ref().map(|host| host.turn_id().to_owned());
    let mut hold = Hold::acquire(shared, ask.room_id, ask.record, turn_id).await;
    hold.generation = generation;
    let result = execute(&mut hold, &ask.event_id, &ask.input, &text).await;
    hold.preparation.finish();
    (result, Some(hold))
}

async fn execute<T: AcpTransport>(
    hold: &mut Hold<T>,
    event_id: &str,
    input: &crate::channel::context::Input,
    text: &Sink,
) -> Result<Option<Answered>, ChannelError> {
    let Some((connection, mut session)) = open(hold).await? else {
        return Ok(None);
    };
    for attempt in 0..2 {
        let seen = hold.shared.connector.seen(&hold.room_id);
        let (prompt, labelled) =
            input.prompt(&hold.shared.channel_id, seen, hold.shared.room_history);
        let started = Instant::now();
        let activity = Arc::new(AtomicBool::new(false));
        let outcome = one_prompt(
            hold,
            &connection,
            &session,
            event_id,
            &prompt,
            text,
            &activity,
        )
        .await;
        match outcome {
            Ok(Some(answer)) => {
                hold.shared.connector.acknowledged(
                    &hold.room_id,
                    &connection,
                    &session,
                    input.seen_index(),
                    labelled,
                );
                return Ok(Some(Answered {
                    usage: usage_of(&answer),
                    started,
                }));
            }
            Ok(None) => return Ok(None),
            Err(AcpError::SessionInvalidated(error)) => {
                if activity.load(Ordering::Acquire) {
                    return Err(activity_error(error));
                }
                if attempt != 0 || !error.recovery_authorized {
                    return Err(raised(error.into()));
                }
            }
            Err(error) => return Err(provider_error(&format!("ACP agent prompt failed: {error}"))),
        }
        let Some(rebuilt) = rebuild(hold, &connection).await? else {
            return Ok(None);
        };
        session = rebuilt;
    }
    unreachable!("the second attempt always returns")
}

async fn open<T: AcpTransport>(
    hold: &mut Hold<T>,
) -> Result<Option<(Arc<T::Connection>, String)>, ChannelError> {
    if hold.detached() {
        interrupted(&hold.record);
        return Ok(None);
    }
    let shared = Arc::clone(&hold.shared);
    let opened = tokio::select! {
        biased;
        () = shared.connector.closed() => return Err(Box::new(roomkit::RoomKitError::Cancelled)),
        result = hold.preparation.run(open_room_session(&shared, &hold.room_id)) => result,
    };
    let Some(opened) = opened else {
        interrupted(&hold.record);
        return Ok(None);
    };
    let opened = opened.map_err(raised)?;
    hold.session_generation = Some(hold.generation);
    if hold.detached() {
        interrupted(&hold.record);
        return Ok(None);
    }
    Ok(Some(opened))
}

/// Closing the transport cannot wake a host's local MCP supplier. The caller
/// selects the connector's close signal while this whole opening is pending.
async fn open_room_session<T: AcpTransport>(
    shared: &Shared<T>,
    room: &roomkit::model::RoomId,
) -> Result<(Arc<T::Connection>, String), AcpError> {
    let connection = shared.connector.connection().await?;
    let session = shared
        .connector
        .session(room, &connection, &shared.cwd)
        .await?;
    Ok((connection, session))
}

async fn rebuild<T: AcpTransport>(
    hold: &mut Hold<T>,
    connection: &Arc<T::Connection>,
) -> Result<Option<String>, ChannelError> {
    let shared = Arc::clone(&hold.shared);
    hold.recovering = true;
    let rebuilt = tokio::select! {
        biased;
        () = shared.connector.closed() => return Ok(None),
        result = hold.preparation.run(shared.connector.rebuild(&hold.room_id, connection, &shared.cwd)) => result,
    };
    if shared.connector.is_closed() {
        return Ok(None);
    }
    let Some(rebuilt) = rebuilt else {
        interrupted(&hold.record);
        return Ok(None);
    };
    let session = rebuilt.map_err(raised)?;
    hold.record.with_value("acp", |acp| {
        if let Some(acp) = acp.as_object_mut() {
            for name in ["interrupted", "stop_reason", "prompt_returned"] {
                acp.shift_remove(name);
            }
        }
    });
    Ok(Some(session))
}

/// Register before the task may call the transport, including on another worker.
/// The connection contract drains preceding updates before returning its result.
async fn one_prompt<T: AcpTransport>(
    hold: &mut Hold<T>,
    connection: &Arc<T::Connection>,
    session: &str,
    event_id: &str,
    prompt: &str,
    text: &Sink,
    activity: &Arc<AtomicBool>,
) -> Result<Option<PromptResponse>, AcpError> {
    let mut meta = Map::new();
    meta.insert("roomkit.live/eventId".to_owned(), json!(event_id));
    let request = PromptRequest::new(session.to_owned(), vec![prompt.to_owned().into()]).meta(meta);
    let (start, ready) = oneshot::channel();
    let task = tokio::spawn(prompt_task(
        ready,
        Arc::clone(connection),
        request,
        hold.record.clone(),
        Arc::clone(&hold.preparation),
    ));
    if !hold.shared.connector.register_prompt(
        session,
        text.clone(),
        task.abort_handle(),
        Arc::clone(activity),
    ) {
        task.abort();
        interrupted(&hold.record);
        return Ok(None);
    }
    let registered = Registered {
        shared: Arc::clone(&hold.shared),
        session: session.to_owned(),
        text: text.clone(),
    };
    hold.prompt = Some(Prompt {
        task: task.abort_handle(),
        connection: Arc::clone(connection),
        session: session.to_owned(),
    });
    let _gone = start.send(());
    let joined = task.await;
    hold.prompt = None;
    drop(registered);
    match joined {
        Ok(answer) => answer,
        Err(failed) if failed.is_cancelled() => {
            interrupted(&hold.record);
            Ok(None)
        }
        Err(failed) => {
            interrupted(&hold.record);
            Err(AcpError::Runtime(failed.to_string()))
        }
    }
}

async fn prompt_task<C: AcpConnection>(
    ready: oneshot::Receiver<()>,
    connection: Arc<C>,
    request: PromptRequest,
    record: ResponseMetadata,
    preparation: Arc<crate::channel::preparation::Preparation>,
) -> Result<Option<PromptResponse>, AcpError> {
    ready.await.map_err(|_| AcpError::ConnectionClosed)?;
    let session = request.session_id.0.to_string();
    let Some(answer) = preparation
        .prompt(session, connection.prompt(request))
        .await
    else {
        interrupted(&record);
        return Ok(None);
    };
    match &answer {
        Ok(answer) => answered(&record, answer),
        Err(_) => interrupted(&record),
    }
    answer.map(Some)
}

struct Registered<T: AcpTransport> {
    shared: Arc<Shared<T>>,
    session: String,
    text: Sink,
}

impl<T: AcpTransport> Drop for Registered<T> {
    fn drop(&mut self) {
        self.shared
            .connector
            .turns
            .unregister(&self.session, &self.text);
    }
}

#[cfg(test)]
mod tests;

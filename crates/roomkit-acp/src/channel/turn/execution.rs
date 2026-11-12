//! At most two prompt attempts, serialized by the same room turn guard.

mod hold;
pub(super) use hold::Hold;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use agent_client_protocol_schema::v1::{PromptRequest, PromptResponse};
use roomkit::ChannelError;
use roomkit::model::{ResponseMetadata, RoomId};
use serde_json::{Map, json};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use super::{Answered, activity_error, answered, interrupted, provider_error, raised, usage_of};
use crate::channel::Shared;
use crate::connection::AcpConnection;
use crate::error::AcpError;
use crate::transport::AcpTransport;
use hold::Prompt;

pub(super) async fn run<T: AcpTransport>(
    shared: Arc<Shared<T>>,
    room_id: RoomId,
    event_id: String,
    prompt: String,
    record: ResponseMetadata,
    text: mpsc::UnboundedSender<String>,
) -> (Result<Option<Answered>, ChannelError>, Option<Hold<T>>) {
    let mut hold = Hold::acquire(shared, room_id, record).await;
    let result = execute(&mut hold, &event_id, &prompt, &text).await;
    (result, Some(hold))
}

async fn execute<T: AcpTransport>(
    hold: &mut Hold<T>,
    event_id: &str,
    prompt: &str,
    text: &mpsc::UnboundedSender<String>,
) -> Result<Option<Answered>, ChannelError> {
    let shared = Arc::clone(&hold.shared);
    let connection = shared.connector.connection().await.map_err(raised)?;
    let mut session = shared
        .connector
        .session(&hold.room_id, &connection, &shared.cwd)
        .await
        .map_err(raised)?;
    for attempt in 0..2 {
        let started = Instant::now();
        let activity = Arc::new(AtomicBool::new(false));
        let outcome = one_prompt(
            hold,
            &connection,
            &session,
            event_id,
            prompt,
            text,
            &activity,
        )
        .await;
        match outcome {
            Ok(Some(answer)) => {
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
        hold.recovering = true;
        let rebuilt = tokio::select! {
            biased;
            () = shared.connector.closed() => return Ok(None),
            result = shared.connector.rebuild(&hold.room_id, &connection, &shared.cwd) => result,
        };
        if shared.connector.is_closed() {
            return Ok(None);
        }
        session = rebuilt.map_err(raised)?;
        hold.record.with_value("acp", |acp| {
            if let Some(acp) = acp.as_object_mut() {
                for name in ["interrupted", "stop_reason", "prompt_returned"] {
                    acp.shift_remove(name);
                }
            }
        });
    }
    unreachable!("the second attempt always returns")
}

/// Register before the task may call the transport, including on another worker.
/// The connection contract drains preceding updates before returning its result.
async fn one_prompt<T: AcpTransport>(
    hold: &mut Hold<T>,
    connection: &Arc<T::Connection>,
    session: &str,
    event_id: &str,
    prompt: &str,
    text: &mpsc::UnboundedSender<String>,
    activity: &Arc<AtomicBool>,
) -> Result<Option<PromptResponse>, AcpError> {
    let mut meta = Map::new();
    meta.insert("roomkit.live/eventId".to_owned(), json!(event_id));
    let request = PromptRequest::new(session.to_owned(), vec![prompt.to_owned().into()]).meta(meta);
    let (start, ready) = oneshot::channel();
    let task = {
        let connection = Arc::clone(connection);
        let record = hold.record.clone();
        tokio::spawn(async move {
            ready.await.map_err(|_| AcpError::ConnectionClosed)?;
            let answer = connection.prompt(request).await;
            match &answer {
                Ok(answer) => answered(&record, answer),
                Err(_) => interrupted(&record),
            }
            answer
        })
    };
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
        Ok(answer) => answer.map(Some),
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

struct Registered<T: AcpTransport> {
    shared: Arc<Shared<T>>,
    session: String,
    text: mpsc::UnboundedSender<String>,
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

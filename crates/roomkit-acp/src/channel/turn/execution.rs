//! At most two prompt attempts, serialized by the same room turn guard.

mod acknowledgement;
mod hold;
mod opening;
pub(super) use hold::Hold;
use opening::{open, rebuild};

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use agent_client_protocol_schema::v1::PromptRequest;
use roomkit::ChannelError;
use roomkit::model::ResponseMetadata;
use serde_json::{Map, json};
use tokio::sync::oneshot;
use tokio::time::Instant;

use super::{Answered, activity_error, answered, interrupted, provider_error, raised};
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
    hold.standalone = ask.standalone;
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
        let (prompt, seen) = prepared_prompt(hold, input);
        let started = Instant::now();
        let activity = Arc::new(AtomicBool::new(false));
        let outcome = one_prompt(
            hold,
            &connection,
            &session,
            PromptInput {
                event_id,
                text: &prompt,
                seen,
            },
            text,
            &activity,
        )
        .await;
        match outcome {
            Ok(Some(answer)) => {
                return Ok(Some(Answered {
                    usage: answer.usage,
                    started,
                }));
            }
            Ok(None) => return Ok(None),
            Err(AcpError::SessionInvalidated(error)) => {
                if activity.load(Ordering::Acquire) {
                    return Err(activity_error(error));
                }
                if hold.standalone || attempt != 0 || !error.recovery_authorized {
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

fn prepared_prompt<T: AcpTransport>(
    hold: &Hold<T>,
    input: &crate::channel::context::Input,
) -> (String, crate::channel::context::Seen) {
    if hold.standalone {
        return (
            input.standalone_prompt(hold.shared.instructions.as_deref()),
            crate::channel::context::Seen::default(),
        );
    }
    let (prompt, labelled, instructed) = input.prompt(
        &hold.shared.channel_id,
        hold.shared.connector.seen(&hold.room_id),
        hold.shared.room_history,
        hold.shared.instructions.as_deref(),
    );
    (
        prompt,
        crate::channel::context::Seen {
            index: Some(input.seen_index()),
            labelled,
            instructed,
        },
    )
}

/// One prompt's text and the session state its normal return acknowledges.
struct PromptInput<'a> {
    event_id: &'a str,
    text: &'a str,
    seen: crate::channel::context::Seen,
}

/// Register before the task may call the transport, including on another worker.
/// The connection contract drains preceding updates before returning its result.
async fn one_prompt<T: AcpTransport>(
    hold: &mut Hold<T>,
    connection: &Arc<T::Connection>,
    session: &str,
    input: PromptInput<'_>,
    text: &Sink,
    activity: &Arc<AtomicBool>,
) -> Result<Option<PromptAnswer>, AcpError> {
    let usage = prompt_usage(hold, session, input.event_id);
    let mut meta = Map::new();
    meta.insert("roomkit.live/eventId".to_owned(), json!(input.event_id));
    let request =
        PromptRequest::new(session.to_owned(), vec![input.text.to_owned().into()]).meta(meta);
    let (start, ready) = oneshot::channel();
    let task = tokio::spawn(prompt_task(
        ready,
        Arc::clone(connection),
        request,
        hold.record.clone(),
        Arc::clone(&hold.preparation),
        acknowledgement::on_return(hold, connection, session, input.seen),
        Arc::clone(&usage),
    ));
    if !hold.shared.connector.register_prompt(
        session,
        text.clone(),
        task.abort_handle(),
        Arc::clone(activity),
        usage,
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
    acknowledge: impl FnOnce() + Send,
    usage: Arc<crate::channel::usage::Usage>,
) -> Result<Option<PromptAnswer>, AcpError> {
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
        Ok(answer) => {
            acknowledge();
            answered(&record, answer);
        }
        Err(_) => interrupted(&record),
    }
    answer.map(|response| {
        Some(PromptAnswer {
            usage: usage.finish(&response),
        })
    })
}

struct PromptAnswer {
    usage: crate::channel::usage::Snapshot,
}

fn prompt_usage<T: AcpTransport>(
    hold: &Hold<T>,
    session: &str,
    event: &str,
) -> Arc<crate::channel::usage::Usage> {
    let connector = &hold.shared.connector;
    Arc::new(crate::channel::usage::Usage::new(
        session,
        event,
        connector.transport.name(),
        connector.transport.provides_usage_metadata(),
        connector.report().1,
        crate::channel::config::values(&connector.options(&hold.room_id))
            .get("model")
            .cloned(),
    ))
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

#[cfg(test)]
mod usage_tests;

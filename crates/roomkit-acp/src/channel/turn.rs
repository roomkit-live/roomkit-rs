//! One ACP turn: a prompt sent to the room's session, streamed until the
//! agent ends it (Python's `ACPTurnMixin`).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use agent_client_protocol_schema::v1::{
    CancelNotification, PromptRequest, PromptResponse, StopReason,
};
use futures_core::Stream;
use roomkit::model::{ChannelId, Metadata, ResponseMetadata, RoomId};
use roomkit::{ChannelError, ClassedError, ResponseItem, TurnHost};
use roomkit_ai::{AiResponseEvent, ProviderError, ProviderErrorOptions, response_transcript};
use serde_json::{Map, Value, json};
use tokio::sync::mpsc;
use tokio::time::Instant;

use super::Shared;
use crate::connection::AcpConnection;
use crate::transport::AcpTransport;

/// What a turn asks: its room, its event, its text, and the record it writes.
pub(super) struct Ask {
    pub(super) room_id: RoomId,
    pub(super) event_id: String,
    pub(super) text: String,
    pub(super) record: ResponseMetadata,
    pub(super) host: TurnHost,
}

/// How the turn's prompt ended, once it returned.
struct Answered {
    usage: Metadata,
    started: Instant,
}

/// The turn's work, as its stream polls it.
type Work = Pin<Box<dyn Future<Output = Result<Answered, ChannelError>> + Send>>;

/// The stream of a turn: nothing runs until it is read; the turn's work
/// then runs as it is read, the agent's text handed on as it comes.
pub(super) struct TurnStream {
    work: Option<Work>,
    text: mpsc::UnboundedReceiver<String>,
    said: Vec<String>,
    ending: Option<Result<Answered, ChannelError>>,
    channel_id: ChannelId,
    room_id: RoomId,
    record: ResponseMetadata,
    host: TurnHost,
    done: bool,
}

impl TurnStream {
    pub(super) fn new<T: AcpTransport>(shared: Arc<Shared<T>>, ask: Ask) -> Self {
        let (sender, text) = mpsc::unbounded_channel();
        let Ask {
            room_id,
            event_id,
            text: prompt,
            record,
            host,
        } = ask;
        let channel_id = shared.channel_id.clone();
        let work = run(
            shared,
            room_id.clone(),
            event_id,
            prompt,
            record.clone(),
            sender,
        );
        Self {
            work: Some(Box::pin(work)),
            text,
            said: Vec::new(),
            ending: None,
            channel_id,
            room_id,
            record,
            host,
            done: false,
        }
    }

    /// Reports the turn that ran to its end to `ON_AI_RESPONSE`'s hooks,
    /// then emits `ai_response` (RFC §8.2, §9.2), as Python's
    /// `_report_response`: its text, its end as its record names it, the
    /// counters the agent returned, unaltered.
    fn report(&self, answered: &Answered) {
        let (segments, content) = response_transcript(
            std::iter::once(self.said.concat()).filter(|text| !text.is_empty()),
        );
        let latency_ms = u64::try_from(answered.started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let end = roomkit_core::rules::turn_end::recorded_turn_end(&self.record.to_metadata());
        let event = AiResponseEvent {
            room_id: Some(self.room_id.clone()),
            usage: answered.usage.clone(),
            loop_end_reason: end.and_then(|end| end.as_str().map(str::to_owned)),
            latency_ms,
            streaming: true,
            segments,
            ..AiResponseEvent::new(self.channel_id.clone(), content)
        };
        let mut data = Metadata::new();
        data.insert("tool_calls_count".to_owned(), Value::from(0));
        data.insert("latency_ms".to_owned(), Value::from(latency_ms));
        data.insert("streaming".to_owned(), Value::Bool(true));
        let host = self.host.clone();
        self.host
            .observe_turn(&event, move || host.emit_turn_event("ai_response", data));
    }
}

impl Stream for TurnStream {
    type Item = Result<ResponseItem, ChannelError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        if let Some(work) = this.work.as_mut()
            && let Poll::Ready(ending) = work.as_mut().poll(cx)
        {
            this.work = None;
            this.ending = Some(ending);
        }
        match this.text.poll_recv(cx) {
            Poll::Ready(Some(text)) => {
                this.said.push(text.clone());
                return Poll::Ready(Some(Ok(ResponseItem::Text(text))));
            }
            // The work holds the sender: the text ends once the work did.
            Poll::Ready(None) => {}
            Poll::Pending => return Poll::Pending,
        }
        this.done = true;
        match this.ending.take() {
            Some(Ok(answered)) => {
                this.report(&answered);
                Poll::Ready(None)
            }
            Some(Err(failure)) => Poll::Ready(Some(Err(failure))),
            None => Poll::Ready(None),
        }
    }
}

/// The turn's work, under the room's turn lock: the connection, the room's
/// session, the prompt; the text the agent says goes to `text` as it comes.
async fn run<T: AcpTransport>(
    shared: Arc<Shared<T>>,
    room_id: RoomId,
    event_id: String,
    prompt: String,
    record: ResponseMetadata,
    text: mpsc::UnboundedSender<String>,
) -> Result<Answered, ChannelError> {
    let lock = shared.room_lock(&room_id);
    let _turn = lock.lock().await;
    let _active = shared.active();
    let connection = shared.connector.connection().await?;
    let session = shared
        .connector
        .session(&room_id, &connection, &shared.cwd)
        .await?;
    let registered = Registered {
        shared: Arc::clone(&shared),
        session: session.clone(),
    };
    shared.connector.turns.register(&session, text);
    let started = Instant::now();
    let mut meta = Map::new();
    meta.insert("roomkit.live/eventId".to_owned(), json!(event_id));
    let request = PromptRequest::new(session.clone(), vec![prompt.into()]).meta(meta);
    let mut prompting = Prompting {
        connection: Some(Arc::clone(&connection)),
        session,
    };
    let answer = connection.prompt(request).await;
    prompting.connection = None;
    drop(registered);
    match answer {
        Ok(answer) => {
            answered(&record, &answer);
            Ok(Answered {
                usage: usage_of(&answer),
                started,
            })
        }
        Err(error) => {
            record.with_value("acp", |acp| {
                acp["interrupted"] = Value::Bool(true);
            });
            Err(provider_error(&format!("ACP agent prompt failed: {error}")))
        }
    }
}

/// The prompt's end on the turn's record (RFC §6.4): it returned, and the
/// agent's stop reason when it is not `end_turn`.
fn answered(record: &ResponseMetadata, answer: &PromptResponse) {
    record.with_value("acp", |acp| {
        acp["prompt_returned"] = Value::Bool(true);
        if answer.stop_reason != StopReason::EndTurn
            && let Ok(reason) = serde_json::to_value(answer.stop_reason)
        {
            acp["stop_reason"] = reason;
        }
    });
}

/// The counters the agent returned with its answer, unaltered, under
/// Python's names; none when it returned none.
fn usage_of(answer: &PromptResponse) -> Metadata {
    let mut usage = Metadata::new();
    let Some(counters) = &answer.usage else {
        return usage;
    };
    usage.insert(
        "total_tokens".to_owned(),
        Value::from(counters.total_tokens),
    );
    usage.insert(
        "input_tokens".to_owned(),
        Value::from(counters.input_tokens),
    );
    usage.insert(
        "output_tokens".to_owned(),
        Value::from(counters.output_tokens),
    );
    let optional = [
        ("thought_tokens", counters.thought_tokens),
        ("cached_read_tokens", counters.cached_read_tokens),
        ("cached_write_tokens", counters.cached_write_tokens),
    ];
    for (name, value) in optional {
        if let Some(value) = value {
            usage.insert(name.to_owned(), Value::from(value));
        }
    }
    usage
}

/// The turn's failure as Python raises it: a `ProviderError` of the
/// provider `acp`.
fn provider_error(message: &str) -> ChannelError {
    let error = ProviderError::new(
        message,
        ProviderErrorOptions {
            provider: "acp".to_owned(),
            ..ProviderErrorOptions::default()
        },
    );
    let mut attributes = Metadata::new();
    attributes.insert("retryable".to_owned(), Value::from(error.retryable));
    attributes.insert("provider".to_owned(), Value::from(error.provider.clone()));
    attributes.insert("status_code".to_owned(), Value::from(error.status_code));
    attributes.insert(
        "context_overflow".to_owned(),
        Value::from(error.context_overflow),
    );
    Box::new(ClassedError::new("ProviderError", error).with_attributes(attributes))
}

/// The turn prompting its session, until its prompt ended.
struct Registered<T: AcpTransport> {
    shared: Arc<Shared<T>>,
    session: String,
}

impl<T: AcpTransport> Drop for Registered<T> {
    fn drop(&mut self) {
        self.shared.connector.turns.unregister(&self.session);
    }
}

/// A prompt out: dropped before it returned (its reader went), the agent is
/// told to cancel it, as Python's turn cancels it in its session.
struct Prompting<C: AcpConnection> {
    connection: Option<Arc<C>>,
    session: String,
}

impl<C: AcpConnection> Drop for Prompting<C> {
    fn drop(&mut self) {
        let Some(connection) = self.connection.take() else {
            return;
        };
        let session = std::mem::take(&mut self.session);
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                // An agent already gone has nothing to cancel.
                let _gone = connection.cancel(CancelNotification::new(session)).await;
            });
        }
    }
}

//! One ACP turn: a prompt sent to the room's session, streamed until the
//! agent ends it (Python's `ACPTurnMixin`).

mod execution;
use execution::{Hold, run};

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use agent_client_protocol_schema::v1::{PromptResponse, StopReason};
use futures_core::Stream;
use roomkit::model::{ChannelId, Metadata, ResponseMetadata, RoomId};
use roomkit::{ChannelError, ClassedError, ResponseItem, TurnHost};
use roomkit_ai::{AiResponseEvent, ProviderError, ProviderErrorOptions, response_transcript};
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio::time::Instant;

use super::Shared;
use super::tools::{Closing, Tools};
use super::updates::{Sink, ThinkingWindow};
use crate::error::AcpError;
use crate::transport::AcpTransport;

/// What a turn asks: its room, its event, its text, and the record it writes.
pub(super) struct Ask {
    pub(super) room_id: RoomId,
    pub(super) event_id: String,
    pub(super) input: super::context::Input,
    pub(super) record: ResponseMetadata,
    /// The kit's turn, which `ON_AI_RESPONSE` fires through; none outside a
    /// kit, where nothing fires.
    pub(super) host: Option<TurnHost>,
    pub(super) answers_room: bool,
}

/// How the turn's prompt ended, once it returned.
struct Answered {
    usage: Metadata,
    started: Instant,
}

/// The turn's work, as its stream polls it: its outcome, and what it holds
/// once it took the room.
type Work<T> =
    Pin<Box<dyn Future<Output = (Result<Option<Answered>, ChannelError>, Option<Hold<T>>)> + Send>>;

/// The stream of a turn: nothing runs until it is read; the turn's work
/// then runs as it is read, the agent's text handed on as it comes.
pub(super) struct TurnStream<T: AcpTransport> {
    work: Option<Work<T>>,
    text: mpsc::UnboundedReceiver<ResponseItem>,
    thinking: Vec<String>,
    thinking_window: Arc<ThinkingWindow>,
    tool_ends: Closing,
    said: Vec<String>,
    ending: Option<Result<Option<Answered>, ChannelError>>,
    hold: Option<Hold<T>>,
    cleanup: Option<Pin<Box<dyn Future<Output = Hold<T>> + Send>>>,
    channel_id: ChannelId,
    room_id: RoomId,
    record: ResponseMetadata,
    host: Option<TurnHost>,
    done: bool,
}

impl<T: AcpTransport> TurnStream<T> {
    pub(super) fn new(shared: Arc<Shared<T>>, ask: Ask) -> Self {
        let (sender, text) = mpsc::unbounded_channel();
        let room_id = ask.room_id.clone();
        let record = ask.record.clone();
        let host = ask.host.clone();
        let channel_id = shared.channel_id.clone();
        let generation = shared.connector.generation(&room_id);
        let thinking_window = Arc::new(ThinkingWindow::new(
            host.clone().filter(|_| ask.answers_room),
            room_id.clone(),
            channel_id.clone(),
            shared.publish_thinking_text,
        ));
        let tools = Arc::new(Tools::new(
            host.clone(),
            room_id.clone(),
            channel_id.clone(),
            ask.answers_room,
            shared.connector.handler.handler.clone(),
        ));
        let sender = Sink::new(sender, Arc::clone(&thinking_window), Arc::clone(&tools));
        let work = run(shared, ask, sender, generation);
        Self {
            work: Some(Box::pin(work)),
            text,
            thinking: Vec::new(),
            thinking_window,
            tool_ends: Closing::new(tools),
            said: vec![String::new()],
            ending: None,
            hold: None,
            cleanup: None,
            channel_id,
            room_id,
            record,
            host,
            done: false,
        }
    }

    fn observe_item(&mut self, item: &ResponseItem) {
        if let ResponseItem::Text(text) = item {
            if let Some(segment) = self.said.last_mut() {
                segment.push_str(text);
            }
        } else if let ResponseItem::Thinking(text) = item {
            self.thinking.push(text.clone());
        } else if matches!(item, ResponseItem::ToolCallStart(_))
            && self.said.last().is_some_and(|text| !text.is_empty())
        {
            self.said.push(String::new());
        }
    }

    fn poll_cleanup(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if let Some(cleanup) = &mut self.cleanup {
            match cleanup.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(hold) => {
                    self.cleanup = None;
                    self.hold = Some(hold);
                }
            }
        }
        Poll::Ready(())
    }

    fn begin_session_cleanup(&mut self, cx: &mut Context<'_>) -> bool {
        // Failed recovery closes only after the queued output was read, and
        // keeps the same room lock until its terminal item is handed over.
        if (!matches!(&self.ending, Some(Ok(Some(_))))
            || self.hold.as_ref().is_some_and(Hold::detached))
            && self.hold.as_ref().is_some_and(Hold::needs_cleanup)
            && let Some(mut hold) = self.hold.take()
        {
            if matches!(&self.ending, Some(Ok(Some(_)))) {
                hold.completed();
            }
            self.cleanup = Some(Box::pin(async move {
                hold.cleanup().await;
                hold
            }));
            cx.waker().wake_by_ref();
            return true;
        }
        false
    }

    /// Reports the turn that ran to its end to `ON_AI_RESPONSE`'s hooks,
    /// then emits `ai_response` (RFC §8.2, §9.2), as Python's
    /// `_report_response`: its text, its end as its record names it, the
    /// counters the agent returned, unaltered. Outside a kit nothing fires.
    fn report(&self, answered: &Answered) {
        let Some(host) = &self.host else {
            return;
        };
        let (segments, content) =
            response_transcript(self.said.iter().filter(|text| !text.is_empty()).cloned());
        let latency_ms = u64::try_from(answered.started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let end = roomkit_core::rules::turn_end::recorded_turn_end(&self.record.to_metadata());
        let event = AiResponseEvent {
            room_id: Some(self.room_id.clone()),
            usage: answered.usage.clone(),
            loop_end_reason: end.and_then(|end| end.as_str().map(str::to_owned)),
            latency_ms,
            streaming: true,
            thinking: self.thinking.concat(),
            tool_calls_count: u32::try_from(self.tool_ends.count()).unwrap_or(u32::MAX),
            segments,
            ..AiResponseEvent::new(self.channel_id.clone(), content)
        };
        let mut data = Metadata::new();
        data.insert(
            "tool_calls_count".to_owned(),
            Value::from(self.tool_ends.count()),
        );
        data.insert("latency_ms".to_owned(), Value::from(latency_ms));
        data.insert("streaming".to_owned(), Value::Bool(true));
        let emitter = host.clone();
        host.observe_turn(&event, move || emitter.emit_turn_event("ai_response", data));
    }
}

impl<T: AcpTransport> Stream for TurnStream<T> {
    type Item = Result<ResponseItem, ChannelError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        this.tool_ends.start_reports();
        if this.poll_cleanup(cx).is_pending() {
            return Poll::Pending;
        }
        if let Some(work) = this.work.as_mut()
            && let Poll::Ready((ending, hold)) = work.as_mut().poll(cx)
        {
            this.work = None;
            this.ending = Some(ending);
            this.hold = hold;
        }
        match this.text.poll_recv(cx) {
            Poll::Ready(Some(item)) => {
                this.observe_item(&item);
                return Poll::Ready(Some(Ok(item)));
            }
            // The work holds a sender until it ends: the text ends then.
            Poll::Ready(None) => {}
            Poll::Pending => return Poll::Pending,
        }
        this.thinking_window.finish();
        match this.tool_ends.poll_next(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Some(item)) => return Poll::Ready(Some(Ok(item))),
            Poll::Ready(None) => {}
        }
        if let Some(hold) = &this.hold {
            hold.retire_steering();
        }
        if this.begin_session_cleanup(cx) {
            return Poll::Pending;
        }
        this.done = true;
        let ending = match this.ending.take() {
            Some(Ok(Some(answered))) => {
                this.report(&answered);
                if let Some(hold) = &mut this.hold {
                    hold.completed();
                }
                Poll::Ready(None)
            }
            Some(Err(failure)) => Poll::Ready(Some(Err(failure))),
            // A turn stopped by the channel's close ends quietly.
            Some(Ok(None)) | None => Poll::Ready(None),
        };
        // The turn is over once its end is read: its room and its count
        // go, as Python's turn lets them go when its stream ends.
        this.hold = None;
        ending
    }
}

impl<T: AcpTransport> Drop for TurnStream<T> {
    fn drop(&mut self) {
        self.thinking_window.finish();
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

/// The turn ended on the way, its prompt never returned (RFC §6.4).
fn interrupted(record: &ResponseMetadata) {
    record.with_value("acp", |acp| {
        acp["interrupted"] = Value::Bool(true);
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
    reported_provider(provider_failure(message))
}

fn activity_error(cause: crate::AcpSessionInvalidatedError) -> ChannelError {
    reported_provider(
        provider_failure("ACP session invalidated after turn activity; recovery refused")
            .with_source(cause),
    )
}

fn provider_failure(message: &str) -> ProviderError {
    ProviderError::new(
        message,
        ProviderErrorOptions {
            provider: "acp".to_owned(),
            ..ProviderErrorOptions::default()
        },
    )
}

fn reported_provider(error: ProviderError) -> ChannelError {
    let attributes = provider_attributes(&error);
    Box::new(ClassedError::new("ProviderError", error).with_attributes(attributes))
}

fn provider_attributes(error: &ProviderError) -> Metadata {
    let mut attributes = Metadata::new();
    attributes.insert("retryable".to_owned(), Value::from(error.retryable));
    attributes.insert("provider".to_owned(), Value::from(error.provider.clone()));
    attributes.insert("status_code".to_owned(), Value::from(error.status_code));
    attributes.insert(
        "context_overflow".to_owned(),
        Value::from(error.context_overflow),
    );
    attributes.insert(
        "before_request".to_owned(),
        Value::Bool(error.before_request),
    );
    attributes
}

/// A failure before the prompt (the connection, the handshake, the
/// session's opening) under the Python class it stands for (the names
/// table, `AcpError`).
pub(super) fn raised(error: AcpError) -> ChannelError {
    let mut attributes = Metadata::new();
    let class = match &error {
        AcpError::Rpc { code, data, .. } => {
            attributes.insert("code".to_owned(), Value::from(*code));
            attributes.insert("data".to_owned(), data.clone().unwrap_or(Value::Null));
            "RequestError"
        }
        AcpError::ExternalTool(error) => super::permissions::error_parts(error).0,
        AcpError::ConnectionClosed => "ConnectionError",
        AcpError::SessionInvalidated(error) => {
            attributes = provider_attributes(error);
            attributes.insert(
                "recovery_authorized".to_owned(),
                Value::Bool(error.recovery_authorized),
            );
            attributes.insert("provider".to_owned(), json!("acp"));
            "ACPSessionInvalidatedError"
        }
        AcpError::Value(_) => "ValueError",
        AcpError::Type(_) => "TypeError",
        // Python's OSError subclass for the spawn's failure.
        AcpError::Spawn { source, .. } => {
            let kind = source.kind();
            if kind == std::io::ErrorKind::NotFound {
                "FileNotFoundError"
            } else if kind == std::io::ErrorKind::PermissionDenied {
                "PermissionError"
            } else {
                "OSError"
            }
        }
        AcpError::InvalidResponse { .. } | AcpError::InvalidRequest { .. } => "ValidationError",
        AcpError::Runtime(_) | AcpError::SetupRefused { .. } => "RuntimeError",
    };
    Box::new(ClassedError::new(class.to_owned(), error).with_attributes(attributes))
}

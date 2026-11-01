//! A generation read as it arrives (RFC §6.4 "One loop, streamed or not";
//! step 4, decision 11): the completion's server-sent events over
//! `reqwest`'s `chunk()`, each text delta handed to the loop as it comes,
//! the calls' fragments folded by their index and handed with the end.
//!
//! The stream is pulled: the answer is read only as the loop reads it, and
//! dropping the stream closes the connection. An endpoint that answers a
//! whole completion (`application/json`) is read whole, as a completion
//! not streamed.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use roomkit_ai::{AiResponse, ProviderError, ProviderStream, StreamEvent, TextStream};

use crate::request::{Request, push_end, push_text};

use crate::config::OpenAiApi;
use crate::error::OpenAiError;
use crate::provider::{ANSWER_LIMIT, OpenAiAiProvider, read_body};
use crate::sse::Events;
use crate::wire;

mod folding;
use folding::Folding;

/// What a generation hands the loop next.
type Item = Result<StreamEvent, ProviderError>;

/// The generation answering `request`, through `provider`, as a stream.
pub(crate) fn answer(provider: &OpenAiAiProvider, request: Request) -> ProviderStream<'_> {
    let reading = Reading {
        provider,
        request,
        phase: Phase::Asking,
        ready: VecDeque::new(),
        secret: String::new(),
    };
    Box::pin(Answer {
        step: Some(Box::pin(reading.next())),
    })
}

/// A generation's stream: the step that reads its next event, run only as
/// the stream is polled.
struct Answer<'a> {
    step: Option<Step<'a>>,
}

/// One step of the read: the next event, and the read that follows it.
type Step<'a> = Pin<Box<dyn Future<Output = Option<(Item, Reading<'a>)>> + Send + 'a>>;

impl Stream for Answer<'_> {
    type Item = Item;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Item>> {
        let this = self.get_mut();
        let Some(step) = this.step.as_mut() else {
            return Poll::Ready(None);
        };
        match step.as_mut().poll(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                this.step = None;
                Poll::Ready(None)
            }
            Poll::Ready(Some((item, reading))) => {
                this.step = Some(Box::pin(reading.next()));
                Poll::Ready(Some(item))
            }
        }
    }
}

/// Where a generation's read stands.
struct Reading<'a> {
    provider: &'a OpenAiAiProvider,
    request: Request,
    phase: Phase,
    /// The events read and not handed yet, in order.
    ready: VecDeque<StreamEvent>,
    /// Credential used for this response, independent of concurrent refreshes.
    secret: String,
}

/// A streamed answer being read: its body, its events, its completion as
/// folded so far.
struct Streamed {
    response: reqwest::Response,
    events: Events,
    folding: Folding,
}

enum Phase {
    /// The request is not sent yet.
    Asking,
    /// The answer's events are read as its bytes arrive.
    Streaming(Box<Streamed>),
    /// The answer is read: what is ready is handed, then nothing.
    Over,
}

impl<'a> Reading<'a> {
    /// The next event, and the read that follows it; `None` once the
    /// answer is over. A failure is the last event.
    async fn next(mut self) -> Option<(Item, Reading<'a>)> {
        loop {
            if let Some(event) = self.ready.pop_front() {
                return Some((Ok(event), self));
            }
            let advanced = match std::mem::replace(&mut self.phase, Phase::Over) {
                Phase::Over => return None,
                Phase::Asking => self.ask().await,
                Phase::Streaming(streamed) => self.read(*streamed).await,
            };
            if let Err(error) = advanced {
                self.phase = Phase::Over;
                self.ready.clear();
                return Some((Err(error.into()), self));
            }
        }
    }

    /// Sends the request: a streamed answer is read as it arrives, a whole
    /// completion read whole.
    async fn ask(&mut self) -> Result<(), OpenAiError> {
        let (response, secret) = self.provider.ask(&self.request).await?;
        self.secret = secret;
        if !is_json(&response) {
            self.phase = Phase::Streaming(Box::new(Streamed {
                response,
                events: Events::new(ANSWER_LIMIT),
                folding: Folding::new(self.provider.api(), self.provider.namespace()),
            }));
            return Ok(());
        }
        let body = read_body(response).await?;
        let whole = match self.provider.api() {
            OpenAiApi::ChatCompletions => wire::response(&body, &self.secret)?,
            OpenAiApi::Responses => {
                crate::responses::body(&body, &self.secret, self.provider.namespace())?
            }
        };
        push_text(&mut self.ready, whole.text);
        push_end(&mut self.ready, whole.calls, whole.done);
        Ok(())
    }

    /// Reads the answer's next bytes, until they end an event that adds
    /// text, or the answer is over: its calls and its end then.
    async fn read(&mut self, streamed: Streamed) -> Result<(), OpenAiError> {
        let Streamed {
            mut response,
            mut events,
            mut folding,
        } = streamed;
        let secret = &self.secret;
        loop {
            let Some(chunk) = response.chunk().await.map_err(OpenAiError::request)? else {
                if let Some(data) = events.finish()?
                    && folding.take(&data, secret, &mut self.ready)?
                {
                    return Ok(());
                }
                return folding.eof(&mut self.ready);
            };
            for data in events.feed(&chunk)? {
                if folding.take(&data, secret, &mut self.ready)? {
                    return Ok(());
                }
            }
            if !self.ready.is_empty() {
                self.phase = Phase::Streaming(Box::new(Streamed {
                    response,
                    events,
                    folding,
                }));
                return Ok(());
            }
        }
    }
}

/// Whether `response` holds a whole completion rather than events.
fn is_json(response: &reqwest::Response) -> bool {
    response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.trim_start().starts_with("application/json"))
}

/// The whole answer `events` give: their text, their calls, how it ended.
pub(crate) async fn whole(mut events: ProviderStream<'_>) -> Result<AiResponse, ProviderError> {
    let mut response = AiResponse::new("");
    while let Some(event) = roomkit_next(&mut events).await {
        match event? {
            StreamEvent::TextDelta(delta) => response.content.push_str(&delta.text),
            StreamEvent::ToolCall(call) => response.tool_calls.push(call.into()),
            StreamEvent::Done(done) => {
                response.finish_reason = done.finish_reason;
                response.usage = done.usage;
                response.metadata = done.metadata;
                return Ok(response);
            }
        }
    }
    Err("the provider's response ended before its done event".into())
}

/// The text `events` give, as it comes.
pub(crate) fn text(events: ProviderStream<'_>) -> TextStream<'_> {
    Box::pin(Texts { events })
}

/// The text events of a structured stream.
struct Texts<'a> {
    events: ProviderStream<'a>,
}

impl Stream for Texts<'_> {
    type Item = Result<String, ProviderError>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match this.events.as_mut().poll_next(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),
                Poll::Ready(Some(Ok(StreamEvent::TextDelta(delta)))) => {
                    return Poll::Ready(Some(Ok(delta.text)));
                }
                Poll::Ready(Some(Ok(StreamEvent::ToolCall(_) | StreamEvent::Done(_)))) => {}
            }
        }
    }
}

/// The next item of `stream`.
async fn roomkit_next(stream: &mut ProviderStream<'_>) -> Option<Item> {
    std::future::poll_fn(|context| stream.as_mut().poll_next(context)).await
}

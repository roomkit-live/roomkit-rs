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
use roomkit_ai::{AiRequest, ProviderError, ProviderEvent, ProviderStream};

use crate::agent::{ANSWER_LIMIT, OpenAiProvider, read_body};
use crate::config::OpenAiApi;
use crate::error::OpenAiError;
use crate::sse::Events;
use crate::wire;

mod folding;
use folding::Folding;

/// What a generation hands the loop next.
type Item = Result<ProviderEvent, ProviderError>;

/// The generation answering `request`, through `provider`, as a stream.
pub(crate) fn answer<'a>(
    provider: &'a OpenAiProvider,
    request: &'a AiRequest,
) -> ProviderStream<'a> {
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
    provider: &'a OpenAiProvider,
    request: &'a AiRequest,
    phase: Phase,
    /// The events read and not handed yet, in order.
    ready: VecDeque<ProviderEvent>,
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
        let (response, secret) = self.provider.ask(self.request).await?;
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
        if !whole.text.is_empty() {
            self.ready.push_back(ProviderEvent::Text(whole.text));
        }
        self.ready
            .extend(whole.calls.into_iter().map(ProviderEvent::ToolCall));
        self.ready.push_back(ProviderEvent::Done(whole.done));
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

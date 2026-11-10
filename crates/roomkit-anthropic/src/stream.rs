//! A pull-driven Messages stream; dropping it cancels the HTTP read.
use crate::{AnthropicAiProvider, error, folding::Folding};
use futures_core::Stream;
use roomkit_ai::{
    AiResponse, FirstToken, ProviderError, ProviderStream, StreamEvent, TextStream, ThinkingBlocks,
};
use roomkit_provider_http::sse::{Events, STREAM_LIMIT};
use serde_json::Value;
use std::{
    collections::VecDeque,
    future::{Future, poll_fn},
    pin::Pin,
    task::{Context, Poll},
};

type Item = Result<StreamEvent, ProviderError>;
type Step<'a> = Pin<Box<dyn Future<Output = Option<(Item, Reading<'a>)>> + Send + 'a>>;

struct Answer<'a> {
    step: Option<Step<'a>>,
}
struct Reading<'a> {
    provider: &'a AnthropicAiProvider,
    body: Value,
    key: String,
    response: Option<reqwest::Response>,
    asked: bool,
    events: Events,
    folding: Folding,
    ready: VecDeque<StreamEvent>,
    failure: Option<ProviderError>,
    first: FirstToken,
}

pub(crate) fn answer(
    provider: &AnthropicAiProvider,
    body: Value,
    key: String,
) -> ProviderStream<'_> {
    let reading = Reading {
        provider,
        body,
        key,
        response: None,
        asked: false,
        events: Events::new(STREAM_LIMIT),
        folding: Folding::new(&provider.config.model),
        ready: VecDeque::new(),
        failure: None,
        first: FirstToken::start("AnthropicAIProvider", &provider.config.model),
    };
    Box::pin(Answer {
        step: Some(Box::pin(reading.next())),
    })
}

impl Stream for Answer<'_> {
    type Item = Item;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Item>> {
        let this = self.get_mut();
        let Some(step) = this.step.as_mut() else {
            return Poll::Ready(None);
        };
        match step.as_mut().poll(cx) {
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

impl Reading<'_> {
    async fn next(mut self) -> Option<(Item, Self)> {
        loop {
            if let Some(event) = self.ready.pop_front() {
                if matches!(&event, StreamEvent::TextDelta(_))
                    || matches!(&event, StreamEvent::ThinkingDelta(delta) if !delta.thinking.is_empty())
                {
                    self.first.seen();
                }
                return Some((Ok(event), self));
            }
            if let Some(error) = self.failure.take() {
                self.folding.stopped = true;
                self.response = None;
                return Some((Err(error), self));
            }
            if self.folding.stopped {
                return None;
            }
            if let Err(error) = self.read().await {
                self.failure = Some(error);
            }
        }
    }

    async fn open(&mut self) -> Result<(), ProviderError> {
        self.asked = true;
        let response = self.provider.send(Some(&self.body), &self.key).await?;
        if response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("application/json"))
        {
            let value: Value = serde_json::from_slice(&crate::http::read_body(response).await?)
                .map_err(|_| error::failed("the response is not JSON"))?;
            return Err(if value.get("error").is_some() {
                error::response(&value, None, &self.provider.secrets(&self.key))
            } else {
                error::failed("the Messages endpoint returned JSON instead of an event stream")
            });
        }
        self.response = Some(response);
        Ok(())
    }

    async fn read(&mut self) -> Result<(), ProviderError> {
        if !self.asked {
            self.open().await?;
        }
        let response = self
            .response
            .as_mut()
            .ok_or_else(|| error::failed("no response stream"))?;
        if let Some(chunk) = response.chunk().await.map_err(error::transport)? {
            let events = self
                .events
                .feed(&chunk)
                .map_err(|cause| error::failed(cause.to_string()))?;
            for event in events {
                self.event(&event)?;
                if self.folding.stopped {
                    self.response = None;
                    break;
                }
            }
        } else {
            if let Some(event) = self
                .events
                .finish()
                .map_err(|cause| error::failed(cause.to_string()))?
            {
                self.event(&event)?;
            }
            if !self.folding.stopped {
                return Err(error::failed("the stream ended before message_stop"));
            }
        }
        Ok(())
    }

    fn event(&mut self, data: &str) -> Result<(), ProviderError> {
        let value: Value = serde_json::from_str(data)
            .map_err(|_| error::failed("the stream event is not JSON"))?;
        self.folding
            .event(&value, &mut self.ready, &self.provider.secrets(&self.key))
    }
}

struct Refused(Option<ProviderError>);
impl Stream for Refused {
    type Item = Item;
    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Item>> {
        Poll::Ready(self.get_mut().0.take().map(Err))
    }
}
pub(crate) fn refused<'a>(error: ProviderError) -> ProviderStream<'a> {
    Box::pin(Refused(Some(error)))
}

pub(crate) async fn whole(mut stream: ProviderStream<'_>) -> Result<AiResponse, ProviderError> {
    let mut response = AiResponse::new("");
    let mut thinking = ThinkingBlocks::new();
    while let Some(event) = poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
        match event? {
            StreamEvent::ToolCallDelta(_) => {}
            StreamEvent::TextDelta(delta) => response.content.push_str(&delta.text),
            StreamEvent::ThinkingDelta(delta) => {
                thinking.add(&delta);
            }
            StreamEvent::ToolCall(call) => response.tool_calls.push(call.into()),
            StreamEvent::Done(done) => {
                response.finish_reason = done.finish_reason;
                response.usage = done.usage;
                response.metadata = done.metadata;
            }
        }
    }
    response.thinking = thinking.seen().then(|| thinking.text());
    response.thinking_signature = thinking.last_signature().map(str::to_owned);
    response.thinking_parts = thinking.keyed().then(|| thinking.parts());
    Ok(response)
}

struct Text<'a>(ProviderStream<'a>);
impl Stream for Text<'_> {
    type Item = Result<String, ProviderError>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match this.0.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(StreamEvent::TextDelta(delta)))) => {
                    return Poll::Ready(Some(Ok(delta.text)));
                }
                Poll::Ready(Some(Ok(_))) => {}
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}
pub(crate) fn text(stream: ProviderStream<'_>) -> TextStream<'_> {
    Box::pin(Text(stream))
}

//! A real provider stream whose first item has been consumed before it waits.

use std::future::ready;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use futures_core::Stream;
use roomkit_ai::{
    AiContext, AiProvider, AiResponse, ProviderError, ProviderStream, StreamDone, StreamEvent,
    StreamTextDelta,
};
use tokio::sync::Notify;

#[derive(Clone)]
pub(super) struct WaitingModel {
    first: &'static str,
    warmup: Arc<AtomicBool>,
    pub(super) started: Arc<Notify>,
    pub(super) dropped: Arc<AtomicBool>,
}

impl WaitingModel {
    pub(super) fn new(first: &'static str, warmup: bool) -> Self {
        Self {
            first,
            warmup: Arc::new(AtomicBool::new(warmup)),
            started: Arc::default(),
            dropped: Arc::default(),
        }
    }
}

struct WaitingStream {
    first: Option<&'static str>,
    announced: bool,
    complete: bool,
    model: WaitingModel,
}

impl Stream for WaitingStream {
    type Item = Result<StreamEvent, ProviderError>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(first) = self.first.take() {
            return Poll::Ready(Some(Ok(StreamEvent::TextDelta(StreamTextDelta {
                text: first.to_owned(),
            }))));
        }
        if self.complete {
            if self.announced {
                return Poll::Ready(None);
            }
            self.announced = true;
            return Poll::Ready(Some(Ok(StreamEvent::Done(StreamDone::default()))));
        }
        // A second poll witnesses consumption of the first delta, rather
        // than announcing that delta before handing it to the reader.
        if !self.announced {
            self.announced = true;
            self.model.started.notify_one();
        }
        Poll::Pending
    }
}

impl Drop for WaitingStream {
    fn drop(&mut self) {
        if !self.complete {
            self.model.dropped.store(true, Ordering::SeqCst);
        }
    }
}

impl AiProvider for WaitingModel {
    fn model_name(&self) -> &'static str {
        "waiting-model"
    }

    fn generate<'a>(
        &'a self,
        _context: &'a AiContext,
    ) -> impl Future<Output = Result<AiResponse, ProviderError>> + Send + 'a {
        ready(Err("this model only streams".into()))
    }

    fn generate_structured_stream<'a>(&'a self, _context: &'a AiContext) -> ProviderStream<'a> {
        let complete = self.warmup.swap(false, Ordering::SeqCst);
        Box::pin(WaitingStream {
            first: Some(if complete { "Warmup." } else { self.first }),
            announced: false,
            complete,
            model: self.clone(),
        })
    }
}

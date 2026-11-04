//! The record of the turn the current task runs within (RFC §6.7): what
//! `roomkit_ai::current_response_metadata` answers, Python's loop context's
//! `response_metadata`. A task spawned with
//! [`keep_context`](crate::keep_context) carries it, as Python's
//! `asyncio.create_task` copies its `ContextVar`. The record's writers are a
//! channel's; the kit only carries it, as it carries the tool call.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};

use futures_core::Stream;
use roomkit_core::model::ResponseMetadata;

tokio::task_local! {
    static RECORD: TurnRecord;
}

/// A turn's one record, which a hook may replace with its own. For
/// `roomkit_ai`'s turns.
#[doc(hidden)]
#[derive(Clone, Debug, Default)]
pub struct TurnRecord(Arc<Mutex<ResponseMetadata>>);

impl TurnRecord {
    /// The record as the turn holds it now.
    pub fn current(&self) -> ResponseMetadata {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Makes `record` the turn's one record: a tool's writes then land where
    /// the reply reads, whichever record a hook left.
    pub fn adopt(&self, record: ResponseMetadata) {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = record;
    }

    /// Runs `work` within this turn.
    pub async fn scope<F: Future>(self, work: F) -> F::Output {
        RECORD.scope(self, work).await
    }

    /// `stream` read within this turn, each of its reads.
    pub fn scope_stream<S>(self, stream: S) -> Scoped<S> {
        Scoped {
            record: self,
            stream,
        }
    }
}

/// A stream read within a turn's record.
#[doc(hidden)]
#[derive(Debug)]
pub struct Scoped<S> {
    record: TurnRecord,
    stream: S,
}

impl<S: Stream + Unpin> Stream for Scoped<S> {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<S::Item>> {
        let this = self.get_mut();
        RECORD.sync_scope(this.record.clone(), || {
            Pin::new(&mut this.stream).poll_next(context)
        })
    }
}

/// The record of the turn the current task runs within; `None` outside a
/// turn. For `roomkit_ai::current_response_metadata`.
#[doc(hidden)]
pub fn current_turn_record() -> Option<ResponseMetadata> {
    RECORD.try_with(TurnRecord::current).ok()
}

/// `future` within the turn's record its caller runs within, if any: what
/// [`keep_context`](crate::keep_context) carries into a spawned task.
pub(crate) fn within_current_turn_record<F>(
    future: F,
) -> impl Future<Output = F::Output> + Send + 'static
where
    F: Future + Send + 'static,
{
    let record = RECORD.try_with(Clone::clone).ok();
    async move {
        match record {
            Some(record) => record.scope(future).await,
            None => future.await,
        }
    }
}

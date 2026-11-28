//! One owned writer sends legacy POSTs in order, independently of the GET reader.
//!
//! What waits for the writer waits in one queue, in the order it came: the
//! messages rmcp hands over and the cancellations of the requests callers
//! gave up on, as Python's zero-buffer stream serves its waiting writes.

use std::sync::Arc;

use reqwest::{Url, header::HeaderMap};
use rmcp::model::ClientJsonRpcMessage;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::JoinHandle;

use super::client::Client;
use super::request::{self, Cancellation};
use crate::http::client::{Failure, HttpFailure};

pub(super) struct Pending {
    message: ClientJsonRpcMessage,
    accepted: oneshot::Sender<()>,
}

/// What waits for the writer.
pub(super) enum Queued {
    Message(Box<Pending>),
    Cancellation(Cancellation),
}

/// The writer's queue, which callers' cancellations join.
#[derive(Clone)]
pub(crate) struct Queue(mpsc::UnboundedSender<Queued>);

impl Queue {
    /// A connection's queue: its senders' end and its writer's.
    pub(super) fn new() -> (Self, mpsc::UnboundedReceiver<Queued>) {
        let (sender, receiver) = mpsc::unbounded_channel();
        (Self(sender), receiver)
    }

    /// Queues `cancellation` behind what waits already; nothing once the
    /// writer ended, which has no server to tell.
    pub(super) fn cancel(&self, cancellation: Cancellation) {
        let _queued = self.0.send(Queued::Cancellation(cancellation));
    }
}

pub(super) struct Writer {
    queue: Queue,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
    failure: Failure,
}

impl Writer {
    pub(super) fn new(
        client: Client,
        url: Url,
        headers: HeaderMap,
        failure: Failure,
        interrupted: Arc<Notify>,
        (queue, queued): (Queue, mpsc::UnboundedReceiver<Queued>),
    ) -> Self {
        let (stop, stopped) = oneshot::channel();
        let errors = failure.clone();
        let task = tokio::spawn(async move {
            tokio::select! {
                biased;
                () = interrupted.notified() => {}
                _ = stopped => {}
                () = write(client, url, headers, queued, errors) => {}
            }
        });
        Self {
            queue,
            stop: Some(stop),
            task: Some(task),
            failure,
        }
    }

    /// Queues `message` at once, in the order rmcp hands messages over;
    /// sending completes when the writer accepts it, as Python's zero-buffer
    /// stream does, before that POST receives its HTTP response.
    pub(super) fn send(
        &self,
        message: ClientJsonRpcMessage,
    ) -> impl Future<Output = Result<(), HttpFailure>> + Send + 'static {
        let failure = self.failure.clone();
        let accepted = (!request::local_cancellation(&message)).then(|| {
            let (accepted, received) = oneshot::channel();
            self.queue
                .0
                .send(Queued::Message(Box::new(Pending { message, accepted })))
                .map(|()| received)
                .map_err(|_| ())
        });
        async move {
            let Some(queued) = accepted else {
                return Ok(());
            };
            let closed = |()| failure.record(None, "legacy MCP writer is closed");
            queued.map_err(closed)?.await.map_err(|_| closed(()))
        }
    }

    pub(super) async fn close(&mut self) {
        self.stop.take();
        if let Some(task) = self.task.take() {
            let _joined = task.await;
        }
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        self.stop.take();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn write(
    client: Client,
    url: Url,
    headers: HeaderMap,
    mut queued: mpsc::UnboundedReceiver<Queued>,
    failure: Failure,
) {
    while let Some(mut message) = next(&mut queued).await {
        client.capture.stamp(&mut message);
        client.capture.outgoing(&message);
        let Ok(message) = crate::wire::message(&message) else {
            failure.record(None, "invalid MCP request");
            return;
        };
        if client
            .legacy_post(url.clone(), headers.clone(), message)
            .await
            .is_err()
        {
            // Python stops its writer after a failed POST. Its separate GET
            // reader stays open, and a pending response retains its deadline.
            return;
        }
    }
}

/// The next message the writer posts, in the order messages and
/// cancellations came, a request no caller waits for skipped; `None` once
/// the queue closed.
async fn next(queued: &mut mpsc::UnboundedReceiver<Queued>) -> Option<ClientJsonRpcMessage> {
    loop {
        let message = match queued.recv().await? {
            Queued::Message(pending) => {
                request::accept(&pending.message, pending.accepted).then_some(pending.message)
            }
            Queued::Cancellation(cancellation) => cancellation.notification(),
        };
        if message.is_some() {
            return message;
        }
    }
}

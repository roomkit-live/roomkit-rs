//! One owned writer sends legacy POSTs in order, independently of the GET reader.

use std::sync::Arc;

use reqwest::{Url, header::HeaderMap};
use rmcp::model::ClientJsonRpcMessage;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::JoinHandle;

use super::client::Client;
use super::request::{self, Cancellation};
use crate::http::client::{Failure, HttpFailure};

struct Pending {
    message: ClientJsonRpcMessage,
    accepted: oneshot::Sender<()>,
}

pub(super) struct Writer {
    messages: mpsc::Sender<Pending>,
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
        cancelled: mpsc::UnboundedReceiver<Cancellation>,
    ) -> Self {
        let (messages, received) = mpsc::channel(1);
        let (stop, stopped) = oneshot::channel();
        let errors = failure.clone();
        let task = tokio::spawn(async move {
            tokio::select! {
                biased;
                () = interrupted.notified() => {}
                _ = stopped => {}
                () = write(client, url, headers, received, cancelled, errors) => {}
            }
        });
        Self {
            messages,
            stop: Some(stop),
            task: Some(task),
            failure,
        }
    }

    /// Sending completes when the writer accepts the message, as Python's
    /// zero-buffer stream does, before that POST receives its HTTP response.
    pub(super) fn send(
        &self,
        message: ClientJsonRpcMessage,
    ) -> impl Future<Output = Result<(), HttpFailure>> + Send + 'static {
        let messages = self.messages.clone();
        let failure = self.failure.clone();
        async move {
            if request::local_cancellation(&message) {
                return Ok(());
            }
            let (accepted, received) = oneshot::channel();
            messages
                .send(Pending { message, accepted })
                .await
                .map_err(|_| failure.record(None, "legacy MCP writer is closed"))?;
            received
                .await
                .map_err(|_| failure.record(None, "legacy MCP writer is closed"))
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
    mut received: mpsc::Receiver<Pending>,
    mut cancelled: mpsc::UnboundedReceiver<Cancellation>,
    failure: Failure,
) {
    while let Some(message) = next(&mut received, &mut cancelled).await {
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

/// The next message the writer posts: a caller's cancellation before
/// anything queued, then the next message queued, a request no caller waits
/// for skipped; `None` once rmcp's queue closed.
async fn next(
    received: &mut mpsc::Receiver<Pending>,
    cancelled: &mut mpsc::UnboundedReceiver<Cancellation>,
) -> Option<ClientJsonRpcMessage> {
    loop {
        tokio::select! {
            biased;
            Some(cancellation) = cancelled.recv() => {
                if let Some(notification) = cancellation.notification() {
                    return Some(notification);
                }
            }
            pending = received.recv() => {
                let pending = pending?;
                if request::accept(&pending.message, pending.accepted) {
                    return Some(pending.message);
                }
            }
        }
    }
}

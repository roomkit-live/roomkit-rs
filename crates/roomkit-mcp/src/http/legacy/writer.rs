//! One owned writer sends legacy POSTs in order, independently of the GET reader.

use std::sync::Arc;

use reqwest::{Url, header::HeaderMap};
use rmcp::model::ClientJsonRpcMessage;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::JoinHandle;

use super::client::Client;
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
    ) -> Self {
        let (messages, received) = mpsc::channel(1);
        let (stop, stopped) = oneshot::channel();
        let errors = failure.clone();
        let task = tokio::spawn(async move {
            tokio::select! {
                biased;
                () = interrupted.notified() => {}
                _ = stopped => {}
                () = write(client, url, headers, received, errors) => {}
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
            if super::request::local_cancellation(&message) {
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
    failure: Failure,
) {
    while let Some(pending) = received.recv().await {
        if !super::request::accept(&pending.message, pending.accepted) {
            continue;
        }
        client.capture.outgoing(&pending.message);
        let Ok(message) = crate::wire::message(&pending.message) else {
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

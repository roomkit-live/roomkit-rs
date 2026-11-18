//! Each destination has a FIFO writer. A posted reply outlives its caller.
use std::collections::HashMap;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio::time::Instant;

use super::RedisTurnBackend;
use super::stream::Message;
use crate::RoomKitError;
use crate::turns::lock;

type Receipt = oneshot::Receiver<Result<(), RoomKitError>>;

/// Keep a numbered write in flight while receiving from the other side.
#[derive(Default)]
pub(super) struct PendingWrite(Option<Receipt>);

impl PendingWrite {
    pub(super) fn idle(&self) -> bool {
        self.0.is_none()
    }

    pub(super) fn start(
        &mut self,
        backend: &RedisTurnBackend,
        peer: &str,
        message: &Message,
    ) -> Result<(), RoomKitError> {
        self.0 = Some(backend.enqueue(peer, message)?);
        Ok(())
    }

    pub(super) async fn finished(&mut self) -> Result<(), RoomKitError> {
        let Some(receipt) = self.0.as_mut() else {
            return std::future::pending().await;
        };
        // Borrow the receipt while polling: another select branch must not lose it.
        let result = receipt.await.map_err(|_| super::routes::closed())?;
        self.0 = None;
        result
    }
}

#[derive(Default)]
pub(super) struct Outbox {
    destinations: HashMap<String, mpsc::UnboundedSender<Pending>>,
    writers: JoinSet<()>,
    closed: bool,
}

struct Pending {
    message: Message,
    retry: Retry,
    receipt: oneshot::Sender<Result<(), RoomKitError>>,
}

impl RedisTurnBackend {
    fn enqueue(&self, destination: &str, message: &Message) -> Result<Receipt, RoomKitError> {
        let mut outbox = lock(&self.0.outbox);
        if outbox.closed || *self.0.stop.borrow() {
            return Err(super::routes::closed());
        }
        while let Some(joined) = outbox.writers.try_join_next() {
            if let Err(error) = joined {
                tracing::warn!(%error, "Redis turn writer exited");
            }
        }
        let (receipt, result) = oneshot::channel();
        let pending = Pending {
            message: message.clone(),
            retry: Retry::for_message(&message.kind),
            receipt,
        };
        if let Some(sender) = outbox.destinations.get(destination) {
            sender.send(pending).map_err(|_| super::routes::closed())?;
        } else {
            let (tx, rx) = mpsc::unbounded_channel();
            outbox.destinations.insert(destination.to_owned(), tx);
            let backend = self.clone();
            let destination = destination.to_owned();
            outbox.writers.spawn(async move {
                backend.write_queue(&destination, rx, pending).await;
            });
        }
        Ok(result)
    }

    pub(super) async fn post(
        &self,
        destination: &str,
        message: &Message,
    ) -> Result<(), RoomKitError> {
        self.enqueue(destination, message)?
            .await
            .map_err(|_| super::routes::closed())?
    }

    async fn write_queue(
        &self,
        destination: &str,
        mut rx: mpsc::UnboundedReceiver<Pending>,
        mut pending: Pending,
    ) {
        loop {
            let result = self.write_pending(destination, &pending).await;
            drop(pending.receipt.send(result));
            // The sender holds this same lock while enqueueing. Observing an empty
            // queue and removing its writer is therefore one operation.
            let mut outbox = lock(&self.0.outbox);
            if let Ok(next) = rx.try_recv() {
                pending = next;
            } else {
                outbox.destinations.remove(destination);
                return;
            }
        }
    }

    async fn write_pending(
        &self,
        destination: &str,
        pending: &Pending,
    ) -> Result<(), RoomKitError> {
        let mut stop = self.0.stop.subscribe();
        loop {
            if *stop.borrow() {
                return Err(super::routes::closed());
            }
            match self.write(destination, &pending.message).await {
                Ok(()) => return Ok(()),
                Err(error) if pending.retry.again() => {
                    tracing::warn!(%error, "Retrying a Redis turn message");
                    tokio::select! {
                        _ = stop.changed() => return Err(super::routes::closed()),
                        () = tokio::time::sleep(Duration::from_secs(1)) => {}
                    }
                }
                Err(error) => {
                    return Err(RoomKitError::TurnLink {
                        reason: "frame_lost".into(),
                        message: Some(format!("Not written: {error}")),
                    });
                }
            }
        }
    }

    pub(super) async fn drain_outbox(&self, bound: Duration) {
        let mut writers = {
            let mut outbox = lock(&self.0.outbox);
            outbox.closed = true;
            std::mem::take(&mut outbox.writers)
        };
        let drained = async { while writers.join_next().await.is_some() {} };
        if tokio::time::timeout(bound, drained).await.is_err() {
            writers.shutdown().await;
        }
        lock(&self.0.outbox).destinations.clear();
    }
}

enum Retry {
    Once,
    UntilClosed,
    Until(Instant),
}

impl Retry {
    fn for_message(kind: &str) -> Self {
        match kind {
            "answer" => Self::UntilClosed,
            "work" => Self::Until(Instant::now() + Duration::from_secs(300)),
            _ => Self::Once,
        }
    }

    fn again(&self) -> bool {
        match self {
            Self::Once => false,
            Self::UntilClosed => true,
            Self::Until(end) => Instant::now() < *end,
        }
    }
}

#[cfg(test)]
mod tests;

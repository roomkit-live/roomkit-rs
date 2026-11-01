//! A persistent delivery queue behind the kit (RFC §22.2): Python's
//! `DeliveryBackend`, which decouples a delivery's enqueue from its
//! execution, so that a request survives a restart and may be executed by
//! another worker.

use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use super::item::DeliveryItem;
use crate::kit::RoomKit;
use crate::store::InMemoryStore;

/// What a delivery backend's call failed with: a boxed error, Python's
/// `Exception`.
pub type DeliveryBackendError = Box<dyn Error + Send + Sync>;

/// What a backend's call gives: its answer, or what it failed with.
pub(crate) type Answer<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, DeliveryBackendError>> + Send + 'a>>;

/// What [`DeliveryBackend::dequeue`] takes beside the worker: Python's
/// `dequeue(worker_id, batch_size=1, timeout=5.0)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DequeueOptions {
    /// Python `batch_size=1`: the most items claimed at once.
    pub batch_size: usize,
    /// Python `timeout=5.0`: the longest wait for a first item.
    pub timeout: Duration,
}

impl Default for DequeueOptions {
    fn default() -> Self {
        Self {
            batch_size: 1,
            timeout: Duration::from_secs(5),
        }
    }
}

/// What [`DeliveryBackend::nack`] takes beside the item: Python's
/// `nack(item_id, error=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NackOptions {
    /// Python `error=None`: what the attempt failed with.
    pub error: Option<String>,
}

/// What [`DeliveryBackend::get_dead_letter_items`] takes: Python's
/// `get_dead_letter_items(limit=50)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GetDeadLetterItemsOptions {
    /// Python `limit=50`: the most items read.
    pub limit: usize,
}

impl Default for GetDeadLetterItemsOptions {
    fn default() -> Self {
        Self { limit: 50 }
    }
}

/// A durable queue of delivery requests with at-least-once execution, by
/// its `enqueue`, `dequeue`, `ack` and `nack` (RFC §22.2): Python's
/// `DeliveryBackend`. It keeps an item's address, key, session target and
/// intent in its serialized form and across retries; its worker tells a
/// refusal from a target to retry.
pub trait DeliveryBackend<S = InMemoryStore>: Send + Sync + 'static {
    /// Adds `item` to the queue: Python's `enqueue(item)`.
    fn enqueue(&self, item: DeliveryItem) -> Answer<'_, ()>;

    /// Claims up to `options.batch_size` items for `worker_id`, waiting up
    /// to `options.timeout` for a first one: Python's `dequeue`.
    fn dequeue<'a>(
        &'a self,
        worker_id: &'a str,
        options: DequeueOptions,
    ) -> Answer<'a, Vec<DeliveryItem>>;

    /// Consumes a terminal item, sent, blocked or unknown: Python's
    /// `ack(item_id)`.
    fn ack<'a>(&'a self, item_id: &'a str) -> Answer<'a, ()>;

    /// Gives back an item whose attempt failed: attempted again after its
    /// retry delay while it has retries left, dead-lettered once it has
    /// none. Python's `nack(item_id, error=None)`.
    fn nack<'a>(&'a self, item_id: &'a str, options: NackOptions) -> Answer<'a, ()>;

    /// Moves an item to the dead-letter queue: Python's
    /// `dead_letter(item_id, error)`.
    fn dead_letter<'a>(&'a self, item_id: &'a str, error: &'a str) -> Answer<'a, ()>;

    /// The number of pending items, those waiting out a retry delay
    /// included: Python's `get_queue_depth()`.
    fn get_queue_depth(&self) -> Answer<'_, usize>;

    /// Up to `options.limit` items of the dead-letter queue, the most
    /// recently dead-lettered first: Python's `get_dead_letter_items`.
    fn get_dead_letter_items(
        &self,
        options: GetDeadLetterItemsOptions,
    ) -> Answer<'_, Vec<DeliveryItem>>;

    /// Keeps what the worker's execution of `item` came to, its outcome,
    /// on the item the backend holds, as the Python implementation's
    /// worker writes it on the item both share: the outcome an ack reads,
    /// a retry carries and a dead letter keeps. Provided as nothing.
    /// Outside the public API.
    #[doc(hidden)]
    fn executed<'a>(&'a self, _item: &'a DeliveryItem) -> Answer<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    /// Starts the backend's work for `kit`, its worker loop for one that
    /// executes its items: Python's `start(kit)`, which
    /// [`RoomKit::start`] calls. Provided as Python provides it: nothing.
    fn start(&self, _kit: RoomKit<S>) -> Answer<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    /// Stops the backend's work: Python's `close()`, which the kit's close
    /// calls. Provided as Python provides it: nothing.
    fn close(&self) -> Answer<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

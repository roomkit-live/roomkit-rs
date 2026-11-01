//! The worker that executes a delivery queue (RFC §22.2): Python's
//! `run_worker_loop`, which tells a refusal from a target to retry.

use std::sync::Arc;
use std::time::Duration;

use super::backend::{DeliveryBackend, DequeueOptions, NackOptions};
use super::execute::{ExecuteDeliveryOptions, execute_delivery};
use super::item::DeliveryItem;
use super::outcome::{DeliveryOutcome, DeliveryOutcomeStatus};
use crate::kit::{RoomKit, WeakRoomKit};
use crate::store::ConversationStore;

/// How long the worker waits before it reads the queue again once a read,
/// or an item's settling, failed.
const PAUSE: Duration = Duration::from_secs(1);

/// What [`run_worker_loop`] takes beside its backend, its kit and its id:
/// Python's `run_worker_loop(backend, kit, worker_id, *, batch_size=1,
/// poll_timeout=5.0)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RunWorkerLoopOptions {
    /// Python `batch_size=1`: the most items claimed at once.
    pub batch_size: usize,
    /// Python `poll_timeout=5.0`: the longest wait for an item.
    pub poll_timeout: Duration,
}

impl Default for RunWorkerLoopOptions {
    fn default() -> Self {
        Self {
            batch_size: 1,
            poll_timeout: Duration::from_secs(5),
        }
    }
}

/// Claims and executes `backend`'s items through `kit` as `worker_id`,
/// until the task running it is cut: Python's `run_worker_loop`. An item
/// sent, blocked or unknown with no error is acked; one unavailable or
/// failed is given back to retry, or dead-lettered when its error says no
/// retry may succeed (RFC §22.2: an absent recipient is never acknowledged
/// as a transmission). A failed queue transition is tried again without
/// executing the item again.
pub async fn run_worker_loop<S: ConversationStore>(
    backend: Arc<dyn DeliveryBackend<S>>,
    kit: RoomKit<S>,
    worker_id: &str,
    options: RunWorkerLoopOptions,
) {
    work(&*backend, || Some(kit.clone()), worker_id, options).await;
}

/// [`run_worker_loop`] for the kit `kit` holds weakly, as its own backend's
/// worker: the kit holds the backend, so a worker holding the kit would
/// keep a kit no one holds alive, and running. It ends once the kit is
/// gone.
pub(crate) async fn run_for<S: ConversationStore>(
    backend: Arc<dyn DeliveryBackend<S>>,
    kit: WeakRoomKit<S>,
    worker_id: &str,
    options: RunWorkerLoopOptions,
) {
    work(&*backend, || kit.upgrade(), worker_id, options).await;
}

/// The worker's loop over the kit `kit` gives, until it gives none.
async fn work<S: ConversationStore>(
    backend: &dyn DeliveryBackend<S>,
    kit: impl Fn() -> Option<RoomKit<S>>,
    worker_id: &str,
    options: RunWorkerLoopOptions,
) {
    let dequeue = DequeueOptions {
        batch_size: options.batch_size,
        timeout: options.poll_timeout,
    };
    loop {
        if kit().is_none() {
            return;
        }
        let items = match backend.dequeue(worker_id, dequeue).await {
            Ok(items) => items,
            Err(error) => {
                tracing::error!(target: "roomkit::delivery", %error, "the delivery queue's read failed: retrying after 1s");
                tokio::time::sleep(PAUSE).await;
                continue;
            }
        };
        // Claimed as the kit went, its items stay claimed: the backend
        // queues them again when it starts its next worker or closes.
        let Some(kit) = kit() else {
            return;
        };
        for mut item in items {
            let outcome =
                execute_delivery(&kit, &mut item, ExecuteDeliveryOptions::default()).await;
            if let Err(error) = backend.executed(&item).await {
                tracing::warn!(target: "roomkit::delivery", item = %item.id, %error, "the item's outcome was not kept");
            }
            settle(backend, &item, &outcome).await;
            tracing::debug!(target: "roomkit::delivery", item = %item.id, status = ?outcome.status, reason = ?outcome.reason, "delivery executed");
        }
    }
}

/// Moves `item` to where `outcome` puts it, the transition tried again
/// until it lands, never the delivery.
async fn settle<S: ConversationStore>(
    backend: &dyn DeliveryBackend<S>,
    item: &DeliveryItem,
    outcome: &DeliveryOutcome,
) {
    loop {
        let settled = match transition(outcome) {
            Transition::Ack => backend.ack(&item.id).await,
            Transition::DeadLetter(error) => backend.dead_letter(&item.id, &error).await,
            Transition::Nack(error) => {
                let options = NackOptions { error: Some(error) };
                backend.nack(&item.id, options).await
            }
        };
        match settled {
            Ok(()) => return,
            Err(error) => {
                tracing::error!(target: "roomkit::delivery", item = %item.id, %error, "the item's queue transition failed: retrying after 1s");
                tokio::time::sleep(PAUSE).await;
            }
        }
    }
}

/// Where an executed item goes.
enum Transition {
    Ack,
    DeadLetter(String),
    Nack(String),
}

/// Where `outcome` puts its item, as Python's `_settle_item` reads it: a
/// failure is unavailable, failed, or unknown with an error; a failure
/// whose error is not retryable is dead-lettered, any other given back.
fn transition(outcome: &DeliveryOutcome) -> Transition {
    let failure = matches!(
        outcome.status,
        DeliveryOutcomeStatus::Unavailable | DeliveryOutcomeStatus::Failed
    ) || (outcome.status == DeliveryOutcomeStatus::Unknown
        && outcome.error.is_some());
    if !failure {
        return Transition::Ack;
    }
    match &outcome.error {
        Some(error) if !error.retryable => Transition::DeadLetter(error.message.clone()),
        Some(error) => Transition::Nack(error.message.clone()),
        None => Transition::Nack(
            outcome
                .reason
                .clone()
                .unwrap_or_else(|| status_name(outcome.status)),
        ),
    }
}

fn status_name(status: DeliveryOutcomeStatus) -> String {
    serde_json::to_value(status)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_default()
}

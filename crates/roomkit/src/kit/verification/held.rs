//! A held message's delivery, named by its key until its release is
//! answered (RFC §10.1 "Explicit delivery cancellation", §11.7.6): Python's
//! `_name_held`, `_record_held_cancellation` and
//! `_write_held_cancellation`.
//!
//! A cancellation of a held delivery no run delivers any more is recorded
//! on the held event's mark, so that its release, in any process sharing
//! the store, answers it with no agent. The write is made off the
//! canceller, in a task of its own: a cancellation returns at once, under
//! the room's lock too, and the write takes that lock in its turn.

use std::sync::{Arc, Weak};

use roomkit_core::rules::verification::HELD_KEY;
use serde_json::Value;

use crate::delivery_keys::{HeldEvent, Recorder};
use crate::kit::Inner;
use crate::store::ConversationStore;
use crate::tasks::Kind;

/// The mark's member recording the cancellation, Python's `"cancelled"`.
const CANCELLED: &str = "cancelled";

impl<S: ConversationStore> Inner<S> {
    /// Names the deliveries of held messages until their release, for at
    /// most the policy's `release_max_age`, when the policy releases them
    /// (RFC §11.7.6); nothing is held otherwise.
    pub(crate) fn name_held_deliveries(self: &Arc<Self>) {
        let policy = &self.verification.policy;
        if !policy.release_on_verify {
            return;
        }
        let recorder = recorder(Arc::downgrade(self));
        self.delivery_keys
            .releasing(policy.release_max_age, recorder);
    }
}

/// What records a held delivery's cancellation on its event, off the
/// canceller: a write the kit's close cuts with its other pending work.
fn recorder<S: ConversationStore>(kit: Weak<Inner<S>>) -> Recorder {
    Box::new(move |held, reason| {
        let Some(inner) = kit.upgrade() else {
            return;
        };
        let write = write_held_cancellation(Weak::clone(&kit), held, reason);
        drop(inner.tasks.spawn_alone(Kind::Observer, write));
    })
}

/// Writes `reason` on the mark of `held`, under its room's lock, if the
/// event is still held and its cancellation not recorded yet.
async fn write_held_cancellation<S: ConversationStore>(
    kit: Weak<Inner<S>>,
    (room_id, event_id): HeldEvent,
    reason: String,
) {
    let Some(inner) = kit.upgrade() else {
        return;
    };
    // Released at its drop, once the mark is written.
    let _lock = match inner.locks.lock(&room_id).await {
        Ok(lock) => lock,
        Err(error) => {
            log_failed(&event_id, &error);
            return;
        }
    };
    let current = match inner.store.get_event(&event_id).await {
        Ok(Some(current)) if current.room_id == room_id => current,
        // Released already, or deleted.
        Ok(_) => return,
        Err(error) => {
            log_failed(&event_id, &error);
            return;
        }
    };
    let Some(Value::Object(mark)) = current.metadata.get(HELD_KEY) else {
        return;
    };
    if mark.get(CANCELLED).is_some_and(truthy) {
        return;
    }
    let mut mark = mark.clone();
    mark.insert(CANCELLED.to_owned(), Value::String(reason));
    let mut updated = current;
    updated
        .metadata
        .insert(HELD_KEY.to_owned(), Value::Object(mark));
    if let Err(error) = inner.store.update_event(updated).await {
        log_failed(&event_id, &error);
    }
}

/// Whether `value` reads as Python's truthy.
fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(flag) => *flag,
        Value::String(text) => !text.is_empty(),
        Value::Number(number) => number.as_f64().is_some_and(|n| n != 0.0),
        Value::Array(items) => !items.is_empty(),
        Value::Object(members) => !members.is_empty(),
    }
}

fn log_failed(event_id: &roomkit_core::model::EventId, error: &impl std::fmt::Display) {
    tracing::error!(
        target: "roomkit::verification",
        %event_id,
        %error,
        "recording the cancellation of a held event failed"
    );
}

//! The record of a delivery set with a failure on its event (RFC §5.13,
//! §10.1 step 18, §14.3), Python's `_record_failed_deliveries`.
//!
//! A set that all succeeded needs no record: paying a write per event to say
//! that everything worked spends the message volume on a question nobody
//! asks. A set with a failure is the one an operator comes back to, once the
//! live `delivery_failed` has gone, so the whole map is written, successes
//! included. It is written alone, under the room's lock: the event as it
//! stood at its commit would undo an edit or a delete made during its
//! delivery. That holds for a set its own call runs while the call holds
//! the lock (`Inbound::under_lock`: a keyed publication's section, a call
//! run on a lease), a set of an entry it adopted among them: Python's set
//! runs in the room's lane, outside the lock's execution context, so the
//! record's lock and its release are asked of the room's lock itself,
//! never answered as a nested section's, and the record waits for the
//! section that holds it. A record that cannot be written is logged and
//! changes nothing the set delivered. Past the close's grace the lock is
//! refused: a set the close cut leaves no record, its caller reading what
//! it delivered (RFC §12.10.4, Python's `_report_cut`).

use std::collections::BTreeMap;

use crate::model::{
    ChannelId, DeliveryResult, DeliveryResultStatus, ErrorRecord, EventId, Metadata, RoomEvent,
    RoomId,
};
use crate::ops::protocol::{EffectId, HoldRequest, KIT_CLOSED, Outcome, StoreRequest};
use crate::ops::{Cx, Progress};

/// Where a record stands.
#[derive(Debug)]
enum State {
    /// It waits for the room's lock.
    Locking(EffectId),
    /// It waits for its write.
    Writing(EffectId),
    /// It waits for the lock's release.
    Releasing(EffectId),
}

/// The record of one delivery set, as a sub-machine of the set.
#[derive(Debug)]
pub(super) struct DeliveryRecord {
    room_id: RoomId,
    event_id: EventId,
    results: Metadata,
    state: State,
}

impl DeliveryRecord {
    /// Takes the room's lock to record `results` on `event`, or `None` for a
    /// set in which nothing failed.
    pub(super) fn start(
        cx: &mut Cx<'_>,
        event: &RoomEvent,
        results: &BTreeMap<ChannelId, DeliveryResult>,
    ) -> Option<Self> {
        let failed = |result: &DeliveryResult| result.status == DeliveryResultStatus::Failed;
        if !results.values().any(failed) {
            return None;
        }
        let results = results
            .iter()
            .filter_map(|(id, result)| Some((id.to_string(), serde_json::to_value(result).ok()?)))
            .collect();
        let room_id = event.room_id.clone();
        let request = HoldRequest::AcquireLockForRecord {
            room_id: room_id.clone(),
        };
        Some(Self {
            state: State::Locking(cx.io.issue_unnested(request)),
            room_id,
            event_id: event.id.clone(),
            results,
        })
    }

    /// Whether the record waits for `effect`.
    pub(super) fn awaits(&self, effect: EffectId) -> bool {
        match self.state {
            State::Locking(awaited) | State::Writing(awaited) | State::Releasing(awaited) => {
                awaited == effect
            }
        }
    }

    /// Takes the answer to the record's outstanding effect. A failure is
    /// logged, and a lock taken is released: the set is over either way.
    pub(super) fn accept(&mut self, cx: &mut Cx<'_>, outcome: Outcome) -> Progress<()> {
        match self.state {
            State::Locking(_) => match outcome.into_locked() {
                Ok(()) => self.write(cx),
                Err(error) => {
                    self.could_not_record(&error);
                    Progress::Done(())
                }
            },
            State::Writing(_) => {
                if let Err(error) = outcome.into_stored() {
                    self.could_not_record(&error);
                }
                self.release(cx)
            }
            State::Releasing(_) => {
                if let Err(error) = outcome.into_released() {
                    tracing::warn!(
                        target: "roomkit::core::delivery",
                        room_id = %self.room_id,
                        event_id = %self.event_id,
                        error_type = %error.error_type,
                        message = %error.message,
                        "releasing the room's lock after a delivery record failed; the record \
                         stands"
                    );
                }
                Progress::Done(())
            }
        }
    }

    fn write(&mut self, cx: &mut Cx<'_>) -> Progress<()> {
        let request = StoreRequest::RecordDeliveryResults {
            room_id: self.room_id.clone(),
            event_id: self.event_id.clone(),
            results: std::mem::take(&mut self.results),
        };
        let effect = cx.io.issue_locked(request, self.room_id.clone());
        self.state = State::Writing(effect);
        Progress::Waiting
    }

    fn release(&mut self, cx: &mut Cx<'_>) -> Progress<()> {
        let request = HoldRequest::ReleaseLock {
            room_id: self.room_id.clone(),
        };
        self.state = State::Releasing(cx.io.issue_unnested(request));
        Progress::Waiting
    }

    /// Logs why the record was not written: expected once the kit closes.
    fn could_not_record(&self, error: &ErrorRecord) {
        if error.error_type == KIT_CLOSED {
            tracing::debug!(
                target: "roomkit::core::delivery",
                room_id = %self.room_id,
                event_id = %self.event_id,
                "the kit closed before the delivery record was written"
            );
            return;
        }
        tracing::warn!(
            target: "roomkit::core::delivery",
            room_id = %self.room_id,
            event_id = %self.event_id,
            error_type = %error.error_type,
            message = %error.message,
            "Could not record delivery results for event {}",
            self.event_id
        );
    }
}

//! The write of a delivery set's room metadata updates (RFC §10.1 step 15),
//! Python's `_write_metadata_updates`: once the set's tasks and
//! observations are written, the updates its targets returned, merged in
//! the order they answered, are written on the room once, as a partial
//! update, under the room's lock. A call that holds the lock meanwhile (a
//! locked hook writing the room's metadata) writes first, and the set's
//! updates follow it. That holds for a set its own call runs while the
//! call holds the lock, a keyed publication's adopted entry among them:
//! Python's set runs in the room's lane, outside the lock's execution
//! context, so its lock and its release are asked of the room's lock
//! itself, never answered as a nested section's. A delegated turn writes
//! its agents' updates on its child room the same way, as an operation of
//! its own (`ops::delegation`), as Python's `_broadcast_and_collect` goes
//! through `_persist_side_effects`.
//!
//! A failure is logged once and the set stands, as Python's `except
//! Exception` around its `locked` block logs it: the lock refused, the
//! write the store failed, the lock's release that failed. Past the kit's
//! close grace the lock is refused, logged at DEBUG only.

use crate::model::{ErrorRecord, Metadata, RoomId, RoomUpdates};
use crate::ops::protocol::{EffectId, HoldRequest, Outcome, StoreRequest};
use crate::ops::{Cx, Progress};

use super::persist::metadata_not_written;

/// Where the write stands.
#[derive(Debug)]
enum State {
    /// It waits for the room's lock.
    Locking(EffectId),
    /// It waits for its write.
    Writing(EffectId),
    /// It waits for the lock's release.
    Releasing(EffectId),
}

/// The write of one set's room metadata updates, as a sub-machine of the
/// set's side effects.
#[derive(Debug)]
pub(crate) struct MetadataWrite {
    room_id: RoomId,
    /// The updates, taken once written.
    metadata: Metadata,
    /// Their keys, which a failure's line names.
    keys: Vec<String>,
    /// Whether a failure was logged: the block's failure is logged once.
    failed: bool,
    state: State,
}

impl MetadataWrite {
    /// Takes the room's lock to write `metadata` on `room_id`.
    pub(crate) fn start(cx: &mut Cx<'_>, room_id: &RoomId, metadata: Metadata) -> Self {
        let request = HoldRequest::AcquireLockForRecord {
            room_id: room_id.clone(),
        };
        Self {
            state: State::Locking(cx.io.issue_unnested(request)),
            room_id: room_id.clone(),
            keys: metadata.keys().cloned().collect(),
            metadata,
            failed: false,
        }
    }

    /// Whether the write waits for `effect`.
    pub(crate) fn awaits(&self, effect: EffectId) -> bool {
        match self.state {
            State::Locking(awaited) | State::Writing(awaited) | State::Releasing(awaited) => {
                awaited == effect
            }
        }
    }

    /// Takes the answer to the write's outstanding effect: done once the
    /// lock is released, or was never taken.
    pub(crate) fn accept(&mut self, cx: &mut Cx<'_>, outcome: Outcome) -> Progress<()> {
        match self.state {
            State::Locking(_) => match outcome.into_locked() {
                Ok(()) => self.write(cx),
                Err(error) => {
                    self.not_written(&error);
                    Progress::Done(())
                }
            },
            State::Writing(_) => {
                if let Err(error) = outcome.into_stored() {
                    self.not_written(&error);
                }
                self.release(cx)
            }
            State::Releasing(_) => {
                if let Err(error) = outcome.into_released() {
                    self.not_written(&error);
                }
                Progress::Done(())
            }
        }
    }

    /// Writes the updates, stamped now, under the lock taken.
    fn write(&mut self, cx: &mut Cx<'_>) -> Progress<()> {
        let request = StoreRequest::UpdateRoom {
            room_id: self.room_id.clone(),
            updates: RoomUpdates {
                updated_at: Some(cx.env.now()),
                metadata: std::mem::take(&mut self.metadata),
                ..RoomUpdates::default()
            },
        };
        self.state = State::Writing(cx.io.issue_locked(request, self.room_id.clone()));
        Progress::Waiting
    }

    fn release(&mut self, cx: &mut Cx<'_>) -> Progress<()> {
        let request = HoldRequest::ReleaseLock {
            room_id: self.room_id.clone(),
        };
        self.state = State::Releasing(cx.io.issue_unnested(request));
        Progress::Waiting
    }

    /// Logs the block's first failure.
    fn not_written(&mut self, error: &ErrorRecord) {
        if !self.failed {
            self.failed = true;
            metadata_not_written(&self.room_id, &self.keys, error);
        }
    }
}

//! What a delegated turn's agents returned beside their answers, stored
//! in the child room before those answers are committed (RFC §23.3 step
//! 5, written as a room's are, §10.1 step 15): their tasks and
//! observations, then their room metadata updates, under the room's lock,
//! as Python's `_broadcast_and_collect` stores them through
//! `_persist_side_effects`. A call that holds the child room's lock
//! meanwhile writes first, and the turn's updates follow it. A write the
//! store fails is logged, and the turn goes on.

use std::sync::Arc;

use super::DelegatedTurn;
use super::nested::Nested;
use crate::model::{Metadata, Observation, RoomId, Task};
use crate::ops::inbound::{MetadataWrite, stored_tasks};
use crate::ops::io::Io;
use crate::ops::protocol::{
    Effect, EffectId, Operation, Outcome, ProtocolError, Step, StoreRequest,
};
use crate::ops::{Cx, Environment, KitConfig, Progress};

impl DelegatedTurn {
    /// Asks for the turns' tasks and observations to be stored, those
    /// there are; their room metadata updates are written once they are.
    pub(super) fn persist(
        &mut self,
        tasks: Vec<Task>,
        observations: Vec<Observation>,
        metadata: Metadata,
        effects: &mut Vec<Effect>,
    ) {
        self.metadata = metadata;
        if tasks.is_empty() && observations.is_empty() {
            return;
        }
        let write = self.issue(StoreRequest::PersistSideEffects {
            room_id: self.room_id.clone(),
            tasks: tasks.clone(),
            observations,
        });
        self.persisting.push(write.id);
        self.filing = Some((write.id, tasks));
        effects.push(write);
    }

    /// Takes the answer of a write of tasks and observations, the turns'
    /// or an agent's not asked: each one the store failed is logged, as
    /// Python's `_persist_side_effects` logs it, and the turns' tasks
    /// stored are kept for their `ON_TASK_CREATED`.
    pub(super) fn persisted(&mut self, id: EffectId, outcome: Outcome) {
        self.persisting.retain(|write| *write != id);
        let tasks = self
            .filing
            .take_if(|(write, _)| *write == id)
            .map(|(_, tasks)| tasks)
            .unwrap_or_default();
        let stored = stored_tasks(&self.room_id, tasks, outcome.into_persisted());
        self.created.extend(stored);
    }

    /// Writes the turns' room metadata updates once their tasks and
    /// observations are stored, and advances that write.
    pub(super) fn advance_side_effects(
        &mut self,
        environment: &dyn Environment,
        effects: &mut Vec<Effect>,
    ) {
        if self.filing.is_none() && self.updating.is_none() && !self.metadata.is_empty() {
            let metadata = std::mem::take(&mut self.metadata);
            let write =
                RoomMetadataWrite::new(Arc::clone(&self.config), self.room_id.clone(), metadata);
            self.updating = Some(Nested::new(write));
        }
        let Some(updating) = self.updating.as_mut() else {
            return;
        };
        if updating
            .poll(environment, &mut self.next_id, effects, &mut self.notices)
            .is_some()
        {
            self.updating = None;
        }
    }

    /// Whether the turns' side effects are all stored, or none was asked
    /// for: their tasks' `ON_TASK_CREATED`, their failures and their
    /// answers come after them.
    pub(super) fn side_effects_stored(&self) -> bool {
        self.filing.is_none() && self.metadata.is_empty() && self.updating.is_none()
    }

    /// Whether the write of the room metadata updates asked for `id`.
    pub(super) fn updating_owns(&self, id: EffectId) -> bool {
        self.updating
            .as_ref()
            .is_some_and(|updating| updating.owns(id))
    }

    /// Hands the write of the room metadata updates the answer to `id`.
    pub(super) fn complete_update(
        &mut self,
        id: EffectId,
        outcome: Outcome,
    ) -> Result<(), ProtocolError> {
        match self.updating.as_mut() {
            Some(updating) => updating.complete(id, outcome),
            None => Err(ProtocolError::UnknownEffect(id)),
        }
    }
}

/// The write of the room metadata updates a delegated turn's agents
/// returned, as an operation the turn nests: the inbound pipeline's write
/// ([`MetadataWrite`]), on the child room, under its lock. Over once the
/// lock is released, or was never taken; a failure is logged.
#[derive(Debug)]
pub(super) struct RoomMetadataWrite {
    config: Arc<KitConfig>,
    io: Io,
    room_id: RoomId,
    /// The updates, until the write starts.
    metadata: Option<Metadata>,
    write: Option<MetadataWrite>,
}

impl RoomMetadataWrite {
    /// The write of `metadata` on `room_id`.
    pub(super) fn new(config: Arc<KitConfig>, room_id: RoomId, metadata: Metadata) -> Self {
        Self {
            config,
            io: Io::new(),
            room_id,
            metadata: Some(metadata),
            write: None,
        }
    }
}

impl Operation for RoomMetadataWrite {
    type Output = ();

    fn poll(&mut self, environment: &dyn Environment) -> Step<()> {
        let mut cx = Cx {
            config: &self.config,
            env: environment,
            io: &mut self.io,
        };
        if let Some(metadata) = self.metadata.take() {
            self.write = Some(MetadataWrite::start(&mut cx, &self.room_id, metadata));
        }
        while let Some((effect, outcome)) = cx.io.next_outcome() {
            if let Some(write) = self.write.as_mut().filter(|write| write.awaits(effect))
                && matches!(write.accept(&mut cx, outcome), Progress::Done(()))
            {
                self.write = None;
            }
        }
        let over = self.write.is_none();
        self.io.step(over.then_some(()))
    }

    fn complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError> {
        self.io.complete(id, outcome)
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;
    use crate::model::{RoomUpdates, Timestamp};
    use crate::ops::SequentialEnvironment;
    use crate::ops::protocol::{HoldOutcome, HoldRequest, Next, Request, StoreOutcome};

    fn room() -> RoomId {
        RoomId::new("room_1::task-1").unwrap()
    }

    /// The one effect `step` asks for.
    fn asked(step: Step<()>) -> Effect {
        let Next::Effects(mut effects) = step.next else {
            panic!("nothing asked: {:?}", step.next);
        };
        assert_eq!(effects.len(), 1, "{effects:?}");
        effects.remove(0)
    }

    // Python: core/mixins/_child_execution.py _broadcast_and_collect, which
    // writes a delegated turn's metadata updates as a room's turn does
    // (core/mixins/helpers.py _write_metadata_updates, whose
    // patch_room_metadata stamps updated_at, under the room's lock); no test
    // of its own (RMK-1188, RMK-1521).
    #[test]
    fn a_delegated_turn_s_metadata_updates_are_written_stamped_now() {
        let environment = SequentialEnvironment::default();
        let mut metadata = Metadata::new();
        metadata.insert("stage".to_owned(), json!("found"));
        let config = Arc::new(KitConfig::default());
        let mut write = RoomMetadataWrite::new(config, room(), metadata.clone());

        let lock = asked(write.poll(&environment));
        write.complete(lock.id, HoldOutcome::Locked.into()).unwrap();
        let update = asked(write.poll(&environment));
        write
            .complete(update.id, StoreOutcome::Stored.into())
            .unwrap();
        let release = asked(write.poll(&environment));
        write
            .complete(release.id, HoldOutcome::Released.into())
            .unwrap();
        let ended = write.poll(&environment).next;

        let Request::Store(StoreRequest::UpdateRoom { room_id, updates }) = update.request else {
            panic!("not a room's write: {:?}", update.request);
        };
        assert_eq!(
            (
                lock.request,
                room_id,
                updates,
                update.locked,
                release.request,
                matches!(ended, Next::Ready(())),
            ),
            (
                Request::Hold(HoldRequest::AcquireLockForRecord { room_id: room() }),
                room(),
                RoomUpdates {
                    updated_at: Some(Timestamp::UNIX_EPOCH),
                    metadata,
                    ..RoomUpdates::default()
                },
                Some(room()),
                Request::Hold(HoldRequest::ReleaseLock { room_id: room() }),
                true,
            )
        );
    }
}

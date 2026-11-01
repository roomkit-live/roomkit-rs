//! A host's update of a room's metadata (RFC §5.1), as the Python
//! implementation's `update_room_metadata` makes it: under the room's lock,
//! its keys merged into the room's, the room's due timers left as they are.

use std::collections::VecDeque;
use std::sync::Arc;

use super::LifecycleError;
use super::change::{Decision, SectionChange};
use super::locked::LockedChange;
use super::report::Reported;
use crate::model::{Metadata, Room, RoomContext, RoomId, RoomUpdates};
use crate::ops::protocol::{EffectId, Operation, Outcome, ProtocolError, Step, StoreRequest};
use crate::ops::{Cx, Environment, KitConfig};
use crate::rules::attachment;

/// The merge of a host's keys into the room's metadata, each replacing the
/// key's value, the room's other keys kept.
#[derive(Clone, Debug)]
pub(crate) struct MetadataChange(Metadata);

impl SectionChange for MetadataChange {
    type Done = Box<RoomContext>;
    type Refusal = LifecycleError;

    /// Writes the keys and the room's last modification, which the context
    /// then reflects.
    fn decide(
        &self,
        mut context: Box<RoomContext>,
        cx: &Cx<'_>,
    ) -> Result<Decision<Box<RoomContext>>, LifecycleError> {
        let updates = RoomUpdates {
            updated_at: Some(cx.env.now()),
            metadata: self.0.clone(),
            ..RoomUpdates::default()
        };
        updates.apply(&mut context.room);
        let write = StoreRequest::UpdateRoom {
            room_id: context.room.id.clone(),
            updates,
        };
        Ok(Decision {
            writes: VecDeque::from([write]),
            done: context,
        })
    }

    /// A metadata update reports nothing: no trigger and no framework event
    /// names it.
    fn reported(_done: &Box<RoomContext>) -> Option<(Reported<'_>, &RoomContext)> {
        None
    }

    /// The Python implementation applies no due timer before it writes.
    fn applies_timers(&self) -> bool {
        false
    }
}

/// Merges a host's keys into a room's metadata under the room's lock (RFC
/// §5.1), Python's `update_room_metadata(room_id, metadata, *,
/// organization_id=None)`. A key only the framework writes, under the
/// namespace the implementation reserves or of the room's private state, is
/// dropped with a warning, the rest written, so a read-modify-write leaves
/// the framework's keys as they are. Its output is the room as stored.
#[derive(Debug)]
pub struct UpdateRoomMetadata {
    change: LockedChange<MetadataChange>,
}

impl UpdateRoomMetadata {
    /// The update of `room_id` with `metadata`, for a caller acting for
    /// `organization_id`: a room of another organization, or of none, is
    /// not found, nothing written (RFC §17.2); `None` reads the room
    /// unscoped.
    pub fn new(
        config: Arc<KitConfig>,
        room_id: RoomId,
        metadata: Metadata,
        organization_id: Option<String>,
    ) -> Self {
        let kept = attachment::without_framework_keys(metadata, &room_id, "update_room_metadata");
        let change = MetadataChange(kept);
        Self {
            change: LockedChange::new(config, room_id, organization_id, change),
        }
    }
}

impl Operation for UpdateRoomMetadata {
    type Output = Result<Room, LifecycleError>;

    fn poll(&mut self, environment: &dyn Environment) -> Step<Self::Output> {
        self.change.poll(environment, |context, _| context.room)
    }

    fn complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError> {
        self.change.complete(id, outcome)
    }
}

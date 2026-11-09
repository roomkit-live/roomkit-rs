//! Changes of a channel's binding to a room: attaching, detaching, updating
//! (RFC §5.7, §7.2, §7.5 rules 6 and 7).

use std::sync::Arc;

use super::LifecycleError;
use super::change::{AttachChange, DetachChange, Naming, UpdateChange};
use super::locked::LockedChange;
use crate::model::{BindingUpdate, ChannelBinding, ChannelId, Metadata, RoomId, Visibility};
use crate::ops::protocol::{EffectId, Operation, Outcome, ProtocolError, Step};
use crate::ops::{Environment, KitConfig};
use crate::rules::chain_depth::Draw;

/// Attaches a channel to a room with its binding, under the room's lock,
/// then fires `ON_CHANNEL_ATTACHED` and emits `room_channel_attached`. Its
/// output is the stored binding.
///
/// An explicit attach clears the channel's revocation, if it was detached
/// from the room (RFC §7.5 rule 7). A binding of visibility `internal` is
/// refused before anything is read (gap G-041), and so is a host channel's
/// bound as an intelligence channel (RFC §10.5).
#[derive(Debug)]
pub struct AttachChannel {
    change: LockedChange<AttachChange>,
}

impl AttachChannel {
    /// The attachment of `binding`'s channel to `binding`'s room, for a
    /// caller acting for `organization_id`: a room of another
    /// organization, or of none, is not found (RFC §17.2); `None` reads the
    /// room unscoped.
    pub fn new(
        config: Arc<KitConfig>,
        binding: ChannelBinding,
        organization_id: Option<String>,
    ) -> Self {
        let room_id = binding.room_id.clone();
        let refusal = refusal(&binding);
        let change = AttachChange(Box::new(binding));
        let change = match refusal {
            Some(refusal) => LockedChange::refused(config, room_id, change, refusal),
            None => LockedChange::new(config, room_id, organization_id, change),
        };
        Self { change }
    }

    /// The same change, made in the room's work that `draw` names, its
    /// trigger's hook bodies drawing their calls on that work's budget, at
    /// least `floor` deep (RFC §8.3, §10.3): a turn's tool's change, its
    /// hooks at least as deep as the turn's response. Made from outside the
    /// room's work, they run outside every work.
    #[must_use]
    pub fn made_in(self, draw: Draw, floor: u32) -> Self {
        Self {
            change: self.change.made_in(draw, floor),
        }
    }
}

/// Why `binding` cannot be attached, before anything is read: the
/// visibility `internal`, the framework's alone (gap G-041), as the Python
/// implementation refuses it before it reads the room.
fn refusal(binding: &ChannelBinding) -> Option<LifecycleError> {
    (binding.visibility == Visibility::Internal).then_some(LifecycleError::InternalVisibility)
}

impl Operation for AttachChannel {
    type Output = Result<ChannelBinding, LifecycleError>;

    fn poll(&mut self, environment: &dyn Environment) -> Step<Self::Output> {
        self.change
            .poll(environment, |attached, _| *attached.binding)
    }

    fn complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError> {
        self.change.complete(id, outcome)
    }

    fn expire(&mut self) -> Vec<EffectId> {
        self.change.expire()
    }
}

/// Detaches a channel from a room under its lock: records the channel's
/// revocation in the room, which an automatic attach respects (RFC §7.5
/// rule 7), and removes its binding. Then fires `ON_CHANNEL_DETACHED`, with
/// the room without the binding, and emits `room_channel_detached`. Its
/// output is the removed binding.
///
/// Detaching a channel the room does not bind fails with nothing written
/// (gap G-047).
#[derive(Debug)]
pub struct DetachChannel {
    change: LockedChange<DetachChange>,
}

impl DetachChannel {
    /// The detachment of `channel_id` from `room_id`, for a caller acting
    /// for `organization_id`: a room of another organization, or of none,
    /// is not found (RFC §17.2); `None` reads the room unscoped.
    pub fn new(
        config: Arc<KitConfig>,
        room_id: RoomId,
        channel_id: ChannelId,
        organization_id: Option<String>,
    ) -> Self {
        let change = DetachChange(channel_id);
        Self {
            change: LockedChange::new(config, room_id, organization_id, change),
        }
    }

    /// The same change, made in the room's work that `draw` names, its
    /// trigger's hook bodies drawing their calls on that work's budget, at
    /// least `floor` deep (RFC §8.3, §10.3): a turn's tool's change, its
    /// hooks at least as deep as the turn's response. Made from outside the
    /// room's work, they run outside every work.
    #[must_use]
    pub fn made_in(self, draw: Draw, floor: u32) -> Self {
        Self {
            change: self.change.made_in(draw, floor),
        }
    }
}

impl Operation for DetachChannel {
    type Output = Result<ChannelBinding, LifecycleError>;

    fn poll(&mut self, environment: &dyn Environment) -> Step<Self::Output> {
        self.change
            .poll(environment, |detached, _| *detached.binding)
    }

    fn complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError> {
        self.change.complete(id, outcome)
    }

    fn expire(&mut self) -> Vec<EffectId> {
        self.change.expire()
    }
}

/// Changes a binding's mute, access, visibility or metadata under the
/// room's lock, carrying over every field the update does not name (RFC
/// §7.5 rule 6). Fires `ON_CHANNEL_MUTED` or `ON_CHANNEL_UNMUTED` when
/// `muted` changes, nothing otherwise; an update that changes nothing writes
/// nothing (gap G-047). Its output is the binding as it now stands.
///
/// An update to the visibility `internal` is refused before anything is
/// read (gap G-041), and so is an update of a channel the room does not
/// bind, once read.
#[derive(Debug)]
pub struct UpdateBinding {
    change: LockedChange<UpdateChange>,
}

impl UpdateBinding {
    /// The update of `channel_id`'s binding to `room_id`, for a caller
    /// acting for `organization_id`: a room of another organization, or of
    /// none, is not found (RFC §17.2); `None` reads the room unscoped.
    pub fn new(
        config: Arc<KitConfig>,
        room_id: RoomId,
        channel_id: ChannelId,
        update: BindingUpdate,
        organization_id: Option<String>,
    ) -> Self {
        let internal = update.visibility == Some(Visibility::Internal);
        let change = UpdateChange {
            channel_id,
            update: Box::new(update),
            naming: Naming::Kept,
        };
        let change = if internal {
            LockedChange::refused(config, room_id, change, LifecycleError::InternalVisibility)
        } else {
            LockedChange::new(config, room_id, organization_id, change)
        };
        Self { change }
    }

    /// The update of `metadata` on `channel_id`'s binding to `room_id`, which
    /// the channel then names as an attach does (RFC §5.7, §10.4): its
    /// recipient in the channel's form, a binding naming no one taking it as
    /// its correspondent. Python's `update_binding_metadata`.
    pub fn metadata(
        config: Arc<KitConfig>,
        room_id: RoomId,
        channel_id: ChannelId,
        metadata: Metadata,
        organization_id: Option<String>,
    ) -> Self {
        let update = BindingUpdate {
            metadata,
            ..BindingUpdate::default()
        };
        let change = UpdateChange {
            channel_id,
            update: Box::new(update),
            naming: Naming::Asked,
        };
        Self {
            change: LockedChange::new(config, room_id, organization_id, change),
        }
    }

    /// The same change, made in the room's work that `draw` names, its
    /// trigger's hook bodies drawing their calls on that work's budget, at
    /// least `floor` deep (RFC §8.3, §10.3): a turn's tool's change, its
    /// hooks at least as deep as the turn's response. Made from outside the
    /// room's work, they run outside every work.
    #[must_use]
    pub fn made_in(self, draw: Draw, floor: u32) -> Self {
        Self {
            change: self.change.made_in(draw, floor),
        }
    }
}

impl Operation for UpdateBinding {
    type Output = Result<ChannelBinding, LifecycleError>;

    fn poll(&mut self, environment: &dyn Environment) -> Step<Self::Output> {
        self.change.poll(environment, |updated, _| *updated.binding)
    }

    fn complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError> {
        self.change.complete(id, outcome)
    }

    fn expire(&mut self) -> Vec<EffectId> {
        self.change.expire()
    }
}

//! The changes a locked section makes (RFC §5.1, §5.7, §7.5 rules 6 and
//! 7): each is decided on the room read under its lock once its due timers
//! are applied, into the writes it makes, in order, and what it did. What a
//! change did is a type of its own, so an operation reads only what its
//! change can do, and a change refuses with its own refusal.
//!
//! The lifecycle's changes live here; the inbound pipeline's step 2 makes
//! changes of its own through the same section (`inbound::arrival_change`).

use std::collections::VecDeque;
use std::fmt::Debug;

use super::LifecycleError;
use super::report::Reported;
use crate::model::{BindingUpdate, ChannelBinding, ChannelId, RoomContext, RoomUpdates, Timestamp};
use crate::ops::protocol::StoreRequest;
use crate::rules::attachment;
use crate::rules::lifecycle::{self, Transition};

/// A change a locked section makes (`LockedSection`).
pub(crate) trait SectionChange: Debug {
    /// What the change did, the room as it left it among it.
    type Done: Debug;
    /// Why the change is refused under the lock, nothing written.
    type Refusal: Debug;

    /// Decides the change on `context`, the room as the gate left it, at
    /// `now`.
    ///
    /// # Errors
    ///
    /// Returns the change's refusal, nothing to write.
    fn decide(
        &self,
        context: Box<RoomContext>,
        now: Timestamp,
    ) -> Result<Decision<Self::Done>, Self::Refusal>;

    /// What `done` reports once the lock is released, and the context its
    /// hooks receive (gap G-027); `None` for a change that reports nothing.
    fn reported(done: &Self::Done) -> Option<(Reported<'_>, &RoomContext)>;
}

/// The writes a change makes, in order, and what it did once they are
/// stored.
#[derive(Debug)]
pub(crate) struct Decision<D> {
    pub(crate) writes: VecDeque<StoreRequest>,
    pub(crate) done: D,
}

impl<D> Decision<D> {
    /// A change that writes nothing.
    pub(crate) fn unwritten(done: D) -> Self {
        Self {
            writes: VecDeque::new(),
            done,
        }
    }

    /// The same writes, with what the change did read as `map` says.
    pub(crate) fn map<E>(self, map: impl FnOnce(D) -> E) -> Decision<E> {
        Decision {
            writes: self.writes,
            done: map(self.done),
        }
    }
}

/// A channel attached to the room: its binding, and the room as the attach
/// left it.
#[derive(Clone, Debug)]
pub(crate) struct Attached {
    pub(crate) binding: Box<ChannelBinding>,
    pub(crate) context: Box<RoomContext>,
}

impl Attached {
    /// `ON_CHANNEL_ATTACHED` and `room_channel_attached` (RFC §26.1).
    pub(crate) fn reported(&self) -> (Reported<'_>, &RoomContext) {
        (Reported::ChannelAttached(&self.binding), &self.context)
    }
}

/// A channel detached from the room: its binding removed, and the room as
/// the detach left it.
#[derive(Clone, Debug)]
pub(crate) struct Detached {
    pub(crate) binding: Box<ChannelBinding>,
    pub(crate) context: Box<RoomContext>,
}

/// A channel's binding as an update left it, changed or not.
#[derive(Clone, Debug)]
pub(crate) struct Updated {
    pub(crate) binding: Box<ChannelBinding>,
    /// The binding's `muted`, when the update changed it.
    pub(crate) muted: Option<bool>,
    pub(crate) context: Box<RoomContext>,
}

impl Updated {
    /// `ON_CHANNEL_MUTED` or `ON_CHANNEL_UNMUTED` when `muted` changed,
    /// nothing otherwise (gap G-047).
    pub(crate) fn reported(&self) -> Option<(Reported<'_>, &RoomContext)> {
        let reported = if self.muted? {
            Reported::ChannelMuted(&self.binding)
        } else {
            Reported::ChannelUnmuted(&self.binding)
        };
        Some((reported, &self.context))
    }
}

/// A room's transition: applied, or left undone in a room in the status it
/// leads to already.
#[derive(Clone, Debug)]
pub(crate) struct Transitioned {
    pub(crate) transition: Transition,
    pub(crate) applied: bool,
    pub(crate) context: Box<RoomContext>,
}

/// The integrator's attach, which replaces the room's binding of the
/// channel and clears its revocation (RFC §7.5 rule 7).
#[derive(Clone, Debug)]
pub(crate) struct AttachChange(pub(crate) Box<ChannelBinding>);

impl SectionChange for AttachChange {
    type Done = Attached;
    type Refusal = LifecycleError;

    /// Stores the binding in place of any the room has for its channel. An
    /// explicit attach clears the channel's revocation first.
    fn decide(
        &self,
        mut context: Box<RoomContext>,
        now: Timestamp,
    ) -> Result<Decision<Attached>, LifecycleError> {
        let binding = &self.0;
        let mut writes = VecDeque::new();
        if let Some(updates) = attachment::clear_revocation(&context.room, &binding.channel_id, now)
        {
            writes.push_back(update_room(&mut context, updates));
        }
        writes.push_back(StoreRequest::StoreBinding {
            binding: ChannelBinding::clone(binding),
        });
        context
            .bindings
            .retain(|bound| bound.channel_id != binding.channel_id);
        context.bindings.push(ChannelBinding::clone(binding));
        let done = Attached {
            binding: binding.clone(),
            context,
        };
        Ok(Decision { writes, done })
    }

    fn reported(done: &Attached) -> Option<(Reported<'_>, &RoomContext)> {
        Some(done.reported())
    }
}

/// The detach of a channel from the room (RFC §7.5 rule 7).
#[derive(Clone, Debug)]
pub(crate) struct DetachChange(pub(crate) ChannelId);

impl SectionChange for DetachChange {
    type Done = Detached;
    type Refusal = LifecycleError;

    /// Records the channel's revocation, then removes its binding. The
    /// revocation is written first: a detach cut short leaves the channel
    /// revoked and still bound, never unbound and free to be attached again
    /// automatically. A channel the room does not bind is refused (gap
    /// G-047).
    fn decide(
        &self,
        mut context: Box<RoomContext>,
        now: Timestamp,
    ) -> Result<Decision<Detached>, LifecycleError> {
        let channel_id = &self.0;
        let binding = bound(&context, channel_id)?;
        let revocation = attachment::revoke(&context.room, channel_id, now);
        let mut writes = VecDeque::from([update_room(&mut context, revocation)]);
        writes.push_back(StoreRequest::DeleteBinding {
            room_id: context.room.id.clone(),
            channel_id: channel_id.clone(),
        });
        context
            .bindings
            .retain(|bound| &bound.channel_id != channel_id);
        let done = Detached {
            binding: Box::new(binding),
            context,
        };
        Ok(Decision { writes, done })
    }

    fn reported(done: &Detached) -> Option<(Reported<'_>, &RoomContext)> {
        Some((Reported::ChannelDetached(&done.binding), &done.context))
    }
}

/// The update of a channel's binding (RFC §7.5 rule 6).
#[derive(Clone, Debug)]
pub(crate) struct UpdateChange {
    pub(crate) channel_id: ChannelId,
    pub(crate) update: Box<BindingUpdate>,
}

impl SectionChange for UpdateChange {
    type Done = Updated;
    type Refusal = LifecycleError;

    /// Writes the binding as the update leaves it, every field it does not
    /// name carried over; an update that changes nothing writes nothing
    /// (gap G-047). A channel the room does not bind is refused.
    fn decide(
        &self,
        context: Box<RoomContext>,
        _now: Timestamp,
    ) -> Result<Decision<Updated>, LifecycleError> {
        let current = bound(&context, &self.channel_id)?;
        let updated = self.update.apply(&current);
        if updated == current {
            return Ok(Decision::unwritten(Updated {
                binding: Box::new(current),
                muted: None,
                context,
            }));
        }
        Ok(store_updated(&current, updated, context))
    }

    fn reported(done: &Updated) -> Option<(Reported<'_>, &RoomContext)> {
        done.reported()
    }
}

/// A transition of the room's status (RFC §5.1).
#[derive(Clone, Copy, Debug)]
pub(crate) struct TransitionChange(pub(crate) Transition);

impl SectionChange for TransitionChange {
    type Done = Transitioned;
    type Refusal = LifecycleError;

    /// Applies the transition when it leaves from the room's status; leaves
    /// the room as it is when the transition is done in that status
    /// already, and refuses it otherwise (gap G-074).
    fn decide(
        &self,
        mut context: Box<RoomContext>,
        now: Timestamp,
    ) -> Result<Decision<Transitioned>, LifecycleError> {
        let transition = self.0;
        let status = context.room.status;
        if transition.refuses(status) {
            return Err(LifecycleError::TransitionRefused {
                room_id: context.room.id.clone(),
                transition,
                status,
            });
        }
        let mut room = context.room.clone();
        if !lifecycle::apply(&mut room, transition, now) {
            return Ok(Decision::unwritten(Transitioned {
                transition,
                applied: false,
                context,
            }));
        }
        room.updated_at = now;
        let write = StoreRequest::UpdateRoom {
            room_id: room.id.clone(),
            updates: RoomUpdates::lifecycle(&room),
        };
        context.room = room;
        let done = Transitioned {
            transition,
            applied: true,
            context,
        };
        Ok(Decision {
            writes: VecDeque::from([write]),
            done,
        })
    }

    /// What the integrator's transition reports (RFC §26.1): a pause fires
    /// `ON_ROOM_PAUSED` without `room_paused`, which only its timer emits;
    /// a resume and a reopen report nothing, and neither does a transition
    /// done already.
    fn reported(done: &Transitioned) -> Option<(Reported<'_>, &RoomContext)> {
        if !done.applied {
            return None;
        }
        let reported = match done.transition {
            Transition::Pause => Reported::RoomPaused(None),
            Transition::Close => Reported::RoomClosed(None),
            Transition::Archive => Reported::RoomArchived,
            Transition::Resume | Transition::Reopen => return None,
        };
        Some((reported, &done.context))
    }
}

/// Nothing beyond the due timer transitions the gate applies (gap G-045):
/// what it did is the room as they left it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct TimersChange;

impl SectionChange for TimersChange {
    type Done = Box<RoomContext>;
    type Refusal = LifecycleError;

    fn decide(
        &self,
        context: Box<RoomContext>,
        _now: Timestamp,
    ) -> Result<Decision<Box<RoomContext>>, LifecycleError> {
        Ok(Decision::unwritten(context))
    }

    fn reported(_done: &Box<RoomContext>) -> Option<(Reported<'_>, &RoomContext)> {
        None
    }
}

/// Stores `updated` in place of `current`, which `context` then reflects;
/// the change names `muted` when it changed.
pub(crate) fn store_updated(
    current: &ChannelBinding,
    updated: ChannelBinding,
    mut context: Box<RoomContext>,
) -> Decision<Updated> {
    let muted = (updated.muted != current.muted).then_some(updated.muted);
    for binding in &mut context.bindings {
        if binding.channel_id == updated.channel_id {
            binding.clone_from(&updated);
        }
    }
    Decision {
        writes: VecDeque::from([StoreRequest::StoreBinding {
            binding: updated.clone(),
        }]),
        done: Updated {
            binding: Box::new(updated),
            muted,
            context,
        },
    }
}

/// The room's binding of `channel_id`.
fn bound(context: &RoomContext, channel_id: &ChannelId) -> Result<ChannelBinding, LifecycleError> {
    context
        .binding(channel_id)
        .cloned()
        .ok_or_else(|| LifecycleError::ChannelNotBound {
            room_id: context.room.id.clone(),
            channel_id: channel_id.clone(),
        })
}

/// The write of `updates` on the room, which `context` then reflects.
fn update_room(context: &mut RoomContext, updates: RoomUpdates) -> StoreRequest {
    updates.apply(&mut context.room);
    StoreRequest::UpdateRoom {
        room_id: context.room.id.clone(),
        updates,
    }
}

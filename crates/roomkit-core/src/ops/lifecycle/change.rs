//! What a lifecycle change writes, decided on the room read under its lock
//! once its due timers are applied, and what it did.

use std::collections::VecDeque;

use super::LifecycleError;
use super::report::Reported;
use crate::model::{
    BindingUpdate, ChannelBinding, ChannelId, RoomContext, RoomStatus, RoomUpdates, Timestamp,
};
use crate::ops::protocol::Request;
use crate::rules::addresses::AddressForm;
use crate::rules::attachment;
use crate::rules::lifecycle::{self, Transition};
use crate::rules::routing::{self, Recording, Sender};

/// A change made under a room's lock.
#[derive(Clone, Debug)]
pub(crate) enum Change {
    /// The integrator's attach, which replaces the room's binding of the
    /// channel and clears its revocation (RFC §7.5 rule 7).
    Attach(Box<ChannelBinding>),
    /// The framework's own attach for a message arriving on the channel
    /// (RFC §10.1 step 2), made only where rule 7 allows it (gap G-053).
    AttachAutomatically(Box<ChannelBinding>),
    /// The record of a routed message's sender on the room's binding of the
    /// channel (RFC §10.1 step 2, gap G-061).
    RecordSender(Box<SenderRecord>),
    Detach(ChannelId),
    UpdateBinding {
        channel_id: ChannelId,
        update: Box<BindingUpdate>,
    },
    Transition(Transition),
    /// Nothing beyond the due timer transitions the gate applies.
    CheckTimers,
}

/// What a change did. A change made carries the room as it left it, which
/// its hooks receive.
#[derive(Clone, Debug)]
pub(crate) enum Changed {
    Attached {
        binding: Box<ChannelBinding>,
        context: Box<RoomContext>,
    },
    Detached {
        binding: Box<ChannelBinding>,
        context: Box<RoomContext>,
    },
    BindingUpdated {
        binding: Box<ChannelBinding>,
        /// The binding's `muted` when the update changed it.
        muted: Option<bool>,
        context: Box<RoomContext>,
    },
    Transitioned {
        transition: Transition,
        context: Box<RoomContext>,
    },
    /// The change did not apply: it wrote nothing.
    Unchanged(Box<RoomContext>),
    /// The room is no longer the routed message's: it is CLOSED or
    /// ARCHIVED, or PAUSED, neither bound to the channel as a group nor the
    /// sender's conversation by step 1; its binding names another sender by
    /// now; or it no longer admits the sender a record would name on a
    /// binding naming no one. Nothing was written, and the message is routed
    /// once more (gaps G-045, G-055).
    Rerouted(Box<RoomContext>),
}

/// What recording a routed sender needs (RFC §10.1 step 2, §5.7).
#[derive(Clone, Debug)]
pub(crate) struct SenderRecord {
    /// The binding attached when the room does not bind the channel: the
    /// channel's defaults, naming the sender.
    pub(crate) binding: ChannelBinding,
    /// The sender, as routing names them.
    pub(crate) sender: Sender,
    /// The form the channel's addresses are compared in.
    pub(crate) form: AddressForm,
}

/// The writes a change makes, in order, and what it did once they are
/// stored.
#[derive(Debug)]
pub(crate) struct Decision {
    pub(crate) writes: VecDeque<Request>,
    pub(crate) changed: Changed,
}

impl Change {
    /// Decides the change on `context`, the room as the gate left it, at
    /// `now`.
    pub(crate) fn decide(
        &self,
        context: Box<RoomContext>,
        now: Timestamp,
    ) -> Result<Decision, LifecycleError> {
        match self {
            Self::Attach(binding) => Ok(attach(binding, context, now)),
            Self::AttachAutomatically(binding) => Ok(attach_automatically(binding, context)),
            Self::RecordSender(record) => Ok(record_sender(record, context)),
            Self::Detach(channel_id) => detach(channel_id, context, now),
            Self::UpdateBinding { channel_id, update } => {
                update_binding(channel_id, update, context)
            }
            Self::Transition(transition) => transition_room(*transition, context, now),
            Self::CheckTimers => Ok(Decision {
                writes: VecDeque::new(),
                changed: Changed::Unchanged(context),
            }),
        }
    }
}

impl Changed {
    /// What the change reports after the release, and the context its hooks
    /// receive; `None` for a change that reports nothing.
    pub(crate) fn reported(&self) -> Option<(Reported<'_>, &RoomContext)> {
        match self {
            Self::Attached { binding, context } => {
                Some((Reported::ChannelAttached(binding), context.as_ref()))
            }
            Self::Detached { binding, context } => {
                Some((Reported::ChannelDetached(binding), context.as_ref()))
            }
            Self::BindingUpdated {
                binding,
                muted: Some(true),
                context,
            } => Some((Reported::ChannelMuted(binding), context.as_ref())),
            Self::BindingUpdated {
                binding,
                muted: Some(false),
                context,
            } => Some((Reported::ChannelUnmuted(binding), context.as_ref())),
            Self::Transitioned {
                transition,
                context,
            } => transition_reported(*transition).map(|reported| (reported, context.as_ref())),
            Self::BindingUpdated { muted: None, .. } | Self::Unchanged(_) | Self::Rerouted(_) => {
                None
            }
        }
    }

    /// The room as the change left it.
    pub(crate) fn into_context(self) -> Box<RoomContext> {
        match self {
            Self::Attached { context, .. }
            | Self::Detached { context, .. }
            | Self::BindingUpdated { context, .. }
            | Self::Transitioned { context, .. }
            | Self::Unchanged(context)
            | Self::Rerouted(context) => context,
        }
    }
}

/// What the integrator's transition reports (RFC §26.1): a pause fires
/// `ON_ROOM_PAUSED` without `room_paused`, which only its timer emits; a
/// resume and a reopen report nothing.
fn transition_reported(transition: Transition) -> Option<Reported<'static>> {
    match transition {
        Transition::Pause => Some(Reported::RoomPaused(None)),
        Transition::Close => Some(Reported::RoomClosed(None)),
        Transition::Archive => Some(Reported::RoomArchived),
        Transition::Resume | Transition::Reopen => None,
    }
}

/// Stores the binding in place of any the room has for its channel. An
/// explicit attach clears the channel's revocation first (RFC §7.5 rule 7).
fn attach(binding: &ChannelBinding, mut context: Box<RoomContext>, now: Timestamp) -> Decision {
    let mut writes = VecDeque::new();
    if let Some(updates) = attachment::clear_revocation(&context.room, &binding.channel_id, now) {
        writes.push_back(update_room(&mut context, updates));
    }
    writes.push_back(Request::StoreBinding {
        binding: binding.clone(),
    });
    context
        .bindings
        .retain(|bound| bound.channel_id != binding.channel_id);
    context.bindings.push(binding.clone());
    Decision {
        writes,
        changed: Changed::Attached {
            binding: Box::new(binding.clone()),
            context,
        },
    }
}

/// Stores the binding, unless the room binds the channel, revoked it or
/// refuses the message the attach is for (RFC §7.5 rule 7, gap G-053): the
/// framework never replaces a binding, and never undoes a detach.
fn attach_automatically(binding: &ChannelBinding, mut context: Box<RoomContext>) -> Decision {
    if !attachment::may_attach_automatically(&context, &binding.channel_id) {
        return Decision {
            writes: VecDeque::new(),
            changed: Changed::Unchanged(context),
        };
    }
    context.bindings.push(binding.clone());
    Decision {
        writes: VecDeque::from([Request::StoreBinding {
            binding: binding.clone(),
        }]),
        changed: Changed::Attached {
            binding: Box::new(binding.clone()),
            context,
        },
    }
}

/// Records a routed message's sender on the room as the lock finds it (RFC
/// §10.1 step 2, §10.4, gap G-061). A room whose status no longer keeps it
/// the message's ([`kept_by_status`]), whose binding names another sender
/// by now, or which no longer admits the sender a record would name on a
/// binding naming no one, is no longer the message's: the message is routed
/// once more (gap G-055). A room that does not bind the channel has it
/// attached naming the sender, unless rule 7 refuses; one that binds it has
/// the sender recorded through rule 6, unless the binding is declared
/// `group` or has them already.
fn record_sender(record: &SenderRecord, context: Box<RoomContext>) -> Decision {
    let current = bound(&context, &record.binding.channel_id).ok();
    if !kept_by_status(record, current.as_ref(), &context) {
        return rerouted(context);
    }
    let Some(current) = current else {
        return attach_automatically(&record.binding, context);
    };
    let recording = routing::recording(
        &current,
        &record.sender,
        &record.form,
        &context.participants,
    );
    match recording {
        Recording::Kept => Decision {
            writes: VecDeque::new(),
            changed: Changed::Unchanged(context),
        },
        Recording::Write(update) if still_admitted(record, &current, &context) => {
            let updated = update.apply(&current);
            store_updated(&current, updated, context)
        }
        Recording::Taken | Recording::Write(_) => rerouted(context),
    }
}

/// Whether the room's status, as its lock finds it, keeps the room the
/// routed message's, `current` being its binding of the channel (RFC §5.1,
/// §10.4, gaps G-045, G-055). An ACTIVE room does. A PAUSED room does when
/// it is the sender's conversation by step 1 (its binding names the sender,
/// or the sender is a participant reached through the channel), or when its
/// binding is declared `group`, told as the default strategy's steps 1 and
/// 3 tell them ([`routing::paused_room_takes`]); the message's commit then
/// resumes it. Any other PAUSED room is no longer the message's, whatever
/// strategy chose it. A CLOSED or ARCHIVED room never is.
///
/// A room the default strategy read ACTIVE is found PAUSED here when the
/// lock's gate applied a pause that fell due meanwhile, and is kept by the
/// same test.
fn kept_by_status(
    record: &SenderRecord,
    current: Option<&ChannelBinding>,
    context: &RoomContext,
) -> bool {
    match context.room.status {
        RoomStatus::Active => true,
        RoomStatus::Paused => routing::paused_room_takes(
            &record.sender,
            &record.form,
            &record.binding.channel_id,
            current,
            &context.participants,
        ),
        RoomStatus::Closed | RoomStatus::Archived => false,
    }
}

/// Whether the room, as its lock finds it, still admits the sender a record
/// is about to name on `current` (RFC §10.4 step 3, gap G-055): between the
/// router's read and the lock, a message from someone else may have been
/// committed on the channel, through a room its caller named (gap G-053),
/// or a member may have joined through it.
///
/// What the room has heard is read from the events the locked section
/// reads, the room's latest `context_window` (50 by default), refused ones
/// set aside. That window holds every event committed since the router's
/// read unless as many were committed in the room in between; the events
/// before the read are the router's (the default router reads the whole
/// timeline). A wider gap goes unseen (gap G-055).
fn still_admitted(record: &SenderRecord, current: &ChannelBinding, context: &RoomContext) -> bool {
    let heard = routing::heard(&context.recent_events, &current.channel_id);
    let admitted = routing::still_admits(
        current,
        &record.sender,
        &record.form,
        &context.participants,
        &heard,
    );
    if let Err(refusal) = admitted {
        tracing::debug!(
            target: "roomkit::core::inbound",
            room_id = %context.room.id,
            channel_id = %current.channel_id,
            refusal = ?refusal,
            "the routed room no longer admits the sender by its lock"
        );
    }
    admitted.is_ok()
}

/// A room that is no longer the routed message's: nothing is written (gap
/// G-055).
fn rerouted(context: Box<RoomContext>) -> Decision {
    Decision {
        writes: VecDeque::new(),
        changed: Changed::Rerouted(context),
    }
}

/// Records the channel's revocation, then removes its binding (RFC §7.5
/// rule 7). The revocation is written first: a detach cut short leaves the
/// channel revoked and still bound, never unbound and free to be attached
/// again automatically.
fn detach(
    channel_id: &ChannelId,
    mut context: Box<RoomContext>,
    now: Timestamp,
) -> Result<Decision, LifecycleError> {
    let binding = bound(&context, channel_id)?;
    let revocation = attachment::revoke(&context.room, channel_id, now);
    let mut writes = VecDeque::from([update_room(&mut context, revocation)]);
    writes.push_back(Request::DeleteBinding {
        room_id: context.room.id.clone(),
        channel_id: channel_id.clone(),
    });
    context
        .bindings
        .retain(|bound| &bound.channel_id != channel_id);
    Ok(Decision {
        writes,
        changed: Changed::Detached {
            binding: Box::new(binding),
            context,
        },
    })
}

/// Writes the binding as the update leaves it, every field it does not name
/// carried over (RFC §7.5 rule 6); an update that changes nothing writes
/// nothing (gap G-047).
fn update_binding(
    channel_id: &ChannelId,
    update: &BindingUpdate,
    context: Box<RoomContext>,
) -> Result<Decision, LifecycleError> {
    let current = bound(&context, channel_id)?;
    let updated = update.apply(&current);
    if updated == current {
        return Ok(Decision {
            writes: VecDeque::new(),
            changed: Changed::Unchanged(context),
        });
    }
    Ok(store_updated(&current, updated, context))
}

/// Stores `updated` in place of `current`, which `context` then reflects;
/// the change names `muted` when it changed.
fn store_updated(
    current: &ChannelBinding,
    updated: ChannelBinding,
    mut context: Box<RoomContext>,
) -> Decision {
    let muted = (updated.muted != current.muted).then_some(updated.muted);
    for binding in &mut context.bindings {
        if binding.channel_id == updated.channel_id {
            binding.clone_from(&updated);
        }
    }
    Decision {
        writes: VecDeque::from([Request::StoreBinding {
            binding: updated.clone(),
        }]),
        changed: Changed::BindingUpdated {
            binding: Box::new(updated),
            muted,
            context,
        },
    }
}

/// Applies the transition when it leaves from the room's status; leaves the
/// room as it is when the transition is done in that status already, and
/// refuses it otherwise (RFC §5.1, gap G-074).
fn transition_room(
    transition: Transition,
    mut context: Box<RoomContext>,
    now: Timestamp,
) -> Result<Decision, LifecycleError> {
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
        return Ok(Decision {
            writes: VecDeque::new(),
            changed: Changed::Unchanged(context),
        });
    }
    room.updated_at = now;
    let write = Request::UpdateRoom {
        room_id: room.id.clone(),
        updates: RoomUpdates::lifecycle(&room),
    };
    context.room = room;
    Ok(Decision {
        writes: VecDeque::from([write]),
        changed: Changed::Transitioned {
            transition,
            context,
        },
    })
}

/// The room's binding of `channel_id`.
fn bound(context: &RoomContext, channel_id: &ChannelId) -> Result<ChannelBinding, LifecycleError> {
    context
        .bindings
        .iter()
        .find(|binding| &binding.channel_id == channel_id)
        .cloned()
        .ok_or_else(|| LifecycleError::ChannelNotBound {
            room_id: context.room.id.clone(),
            channel_id: channel_id.clone(),
        })
}

/// The write of `updates` on the room, which `context` then reflects.
fn update_room(context: &mut RoomContext, updates: RoomUpdates) -> Request {
    updates.apply(&mut context.room);
    Request::UpdateRoom {
        room_id: context.room.id.clone(),
        updates,
    }
}

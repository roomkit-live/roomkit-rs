//! The changes step 2 makes under a room's lock (RFC §10.1 step 2, §5.7,
//! §7.5 rule 7, §10.4): the framework's own attach for a message arriving on
//! a channel the room does not bind, and the record of a routed message's
//! sender on the room's binding of the channel. Each runs through the
//! lifecycle's locked section, and refuses nothing: what does not apply
//! writes nothing, or sends the message to be routed once more.

use std::collections::VecDeque;
use std::convert::Infallible;

use crate::model::{ChannelBinding, RoomContext, Timestamp};
use crate::ops::lifecycle::change::{self, Attached, Decision, SectionChange, Updated};
use crate::ops::lifecycle::report::Reported;
use crate::ops::protocol::StoreRequest;
use crate::rules::addresses::AddressForm;
use crate::rules::attachment;
use crate::rules::routing::{self, Recording, Sender};

/// A change step 2 makes under the room's lock.
#[derive(Clone, Debug)]
pub(super) enum ArrivalChange {
    /// The framework's own attach for a message arriving on the channel,
    /// made only where rule 7 allows it (gap G-053).
    AttachAutomatically(Box<ChannelBinding>),
    /// The record of a routed message's sender on the room's binding of the
    /// channel (gap G-061).
    RecordSender(Box<SenderRecord>),
}

/// What recording a routed sender needs (RFC §10.1 step 2, §5.7).
#[derive(Clone, Debug)]
pub(super) struct SenderRecord {
    /// The binding attached when the room does not bind the channel: the
    /// channel's defaults, naming the sender.
    pub(super) binding: ChannelBinding,
    /// The sender, as routing names them.
    pub(super) sender: Sender,
    /// The form the channel's addresses are compared in.
    pub(super) form: AddressForm,
}

/// What a change of step 2 did.
#[derive(Clone, Debug)]
pub(super) enum Recorded {
    /// The channel was attached, its binding naming the sender when step 2
    /// created the room or recorded a routed sender.
    Attached(Attached),
    /// The routed sender was recorded on the binding (gap G-061).
    Updated(Updated),
    /// Nothing was written: the binding stands as it is.
    Kept(Box<RoomContext>),
    /// The room is no longer the routed message's: it is CLOSED or
    /// ARCHIVED, or PAUSED, neither bound to the channel as a group nor the
    /// sender's conversation by step 1; its binding names another sender by
    /// now; or it no longer admits the sender a record would name on a
    /// binding naming no one. Nothing was written, and the message is routed
    /// once more (gaps G-045, G-055).
    Rerouted(Box<RoomContext>),
}

impl Recorded {
    /// The room as the change left it.
    pub(super) fn into_context(self) -> Box<RoomContext> {
        match self {
            Self::Attached(Attached { context, .. }) | Self::Updated(Updated { context, .. }) => {
                context
            }
            Self::Kept(context) | Self::Rerouted(context) => context,
        }
    }
}

impl SectionChange for ArrivalChange {
    type Done = Recorded;
    type Refusal = Infallible;

    fn decide(
        &self,
        context: Box<RoomContext>,
        _now: Timestamp,
    ) -> Result<Decision<Recorded>, Infallible> {
        Ok(match self {
            Self::AttachAutomatically(binding) => attach_automatically(binding, context),
            Self::RecordSender(record) => record_sender(record, context),
        })
    }

    /// An attach reports `ON_CHANNEL_ATTACHED` and `room_channel_attached`;
    /// the record itself reports nothing (RFC §10.1 step 2).
    fn reported(done: &Recorded) -> Option<(Reported<'_>, &RoomContext)> {
        match done {
            Recorded::Attached(attached) => Some(attached.reported()),
            Recorded::Updated(updated) => updated.reported(),
            Recorded::Kept(_) | Recorded::Rerouted(_) => None,
        }
    }
}

/// Stores the binding, unless the room binds the channel, revoked it or
/// refuses the message the attach is for (RFC §7.5 rule 7, gap G-053): the
/// framework never replaces a binding, and never undoes a detach.
fn attach_automatically(
    binding: &ChannelBinding,
    mut context: Box<RoomContext>,
) -> Decision<Recorded> {
    if !attachment::may_attach_automatically(&context, &binding.channel_id) {
        return Decision::unwritten(Recorded::Kept(context));
    }
    context.bindings.push(binding.clone());
    Decision {
        writes: VecDeque::from([StoreRequest::StoreBinding {
            binding: binding.clone(),
        }]),
        done: Recorded::Attached(Attached {
            binding: Box::new(binding.clone()),
            context,
        }),
    }
}

/// Records a routed message's sender on the room as the lock finds it (RFC
/// §10.1 step 2, §10.4, gap G-061). A room whose status no longer keeps it
/// the message's ([`routing::kept_by_status`]), whose binding names another
/// sender by now, or which no longer admits the sender a record would name
/// on a binding naming no one, is no longer the message's: the message is
/// routed once more (gap G-055). A room that does not bind the channel has
/// it attached naming the sender, unless rule 7 refuses; one that binds it
/// has the sender recorded through rule 6, unless the binding is declared
/// `group` or has them already.
fn record_sender(record: &SenderRecord, context: Box<RoomContext>) -> Decision<Recorded> {
    let current = context.binding(&record.binding.channel_id).cloned();
    let kept = routing::kept_by_status(
        context.room.status,
        &record.sender,
        &record.form,
        &record.binding.channel_id,
        current.as_ref(),
        &context.participants,
    );
    if !kept {
        return Decision::unwritten(Recorded::Rerouted(context));
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
        Recording::Kept => Decision::unwritten(Recorded::Kept(context)),
        Recording::Write(update) if still_admitted(record, &current, &context) => {
            let updated = update.apply(&current);
            change::store_updated(&current, updated, context).map(Recorded::Updated)
        }
        Recording::Taken | Recording::Write(_) => Decision::unwritten(Recorded::Rerouted(context)),
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

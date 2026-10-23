//! Step 8a of a pass (RFC §10.1 step 8a, §10.3): an edit's or a delete's
//! target read under the room lock and checked by `rules::edits`, then what
//! the commit writes on it.
//!
//! Every pass checks its event, whoever brought it: the inbound message, a
//! direct injection, a channel's response, an event a hook injected (gap
//! G-031). A refused event is not stored: an entry's caller receives the
//! reason, a response's or an injected event's refusal is logged (gap
//! G-062).

use super::pass::PassOrigin;
use crate::model::{ChannelBinding, EventId, Participant, RoomEvent};
use crate::ops::protocol::TargetUpdate;
use crate::rules::edits::{self, EditRefusal, Origin};

/// An edit or a delete step 8a accepted, with its target as read under the
/// room lock.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct Accepted {
    target: Box<RoomEvent>,
}

/// The event step 8a reads for `event`: the target of an edit or a delete,
/// `None` for an event that changes none.
///
/// # Errors
///
/// Returns [`EditRefusal::TargetNotFound`] for an event whose type and
/// content disagree (gap G-062).
pub(super) fn target_of(event: &RoomEvent) -> Result<Option<EventId>, EditRefusal> {
    Ok(edits::mutation(event)?.map(|mutation| mutation.target_event_id().clone()))
}

/// Checks `event` against `target`, the event the store holds under the
/// targeted id, among the room's `participants` (RFC §10.1 step 8a).
///
/// # Errors
///
/// Returns the first check that fails, as an [`EditRefusal`].
pub(super) fn check(
    event: &RoomEvent,
    origin: &PassOrigin,
    target: Option<RoomEvent>,
    participants: &[Participant],
) -> Result<Accepted, EditRefusal> {
    let mutation = edits::mutation(event)?.ok_or(EditRefusal::TargetNotFound)?;
    let origin = match origin {
        PassOrigin::Trigger | PassOrigin::Adopted(_) | PassOrigin::Response => Origin::Channel,
        PassOrigin::Injected { .. } => Origin::Hook,
    };
    edits::validate(event, &mutation, target.as_ref(), origin, participants)?;
    let target = target.ok_or(EditRefusal::TargetNotFound)?;
    Ok(Accepted {
        target: Box::new(target),
    })
}

impl Accepted {
    /// The target's id.
    pub(super) fn target_id(&self) -> &EventId {
        &self.target.id
    }

    /// Narrows `event`'s visibility to its target's (gap G-062), among the
    /// room's `bindings`, which tell each channel's category.
    pub(super) fn narrow(&self, event: &mut RoomEvent, bindings: &[ChannelBinding]) {
        event.visibility = edits::visibility_within(&event.visibility, &self.target, |channel| {
            bindings
                .iter()
                .find(|binding| &binding.channel_id == channel)
                .map(|binding| binding.category)
        });
    }

    /// What the commit of `event` writes on the target, and the target as it
    /// leaves it (RFC §10.3 state updates). The edit's new content is the
    /// one the hooks left. `None` for an event that no longer changes its
    /// target, which the hooks' kept content rules out (gap G-062): the pass
    /// then refuses it rather than commit a change without its target's
    /// update.
    pub(super) fn write(self, event: &RoomEvent) -> Option<(TargetUpdate, RoomEvent)> {
        let mutation = edits::mutation(event).ok().flatten()?;
        if mutation.target_event_id() != &self.target.id {
            return None;
        }
        let updates = edits::target_updates(&mutation, &self.target);
        let mut changed = *self.target;
        updates.apply(&mut changed);
        let update = TargetUpdate {
            event_id: changed.id.clone(),
            updates,
        };
        Some((update, changed))
    }
}

/// Logs the refusal of an event other than an entry's, whose caller's
/// result reports it instead (gap G-062).
pub(super) fn log_refusal(event: &RoomEvent, origin: &PassOrigin, refusal: EditRefusal) {
    if origin.is_entry() {
        return;
    }
    tracing::warn!(
        target: "roomkit::core::inbound",
        room_id = %event.room_id,
        event_id = %event.id,
        channel_id = %event.source.channel_id,
        reason = refusal.reason(),
        "an edit or a delete was refused at step 8a; nothing of it is stored"
    );
}

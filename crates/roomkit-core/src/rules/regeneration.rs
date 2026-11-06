//! The message a regeneration answers again (RFC §13.6): the Python
//! implementation's `_regenerate_target`, one choice for both its readers,
//! `regenerate_response` under the room's lock and `regenerate_target`
//! outside it, so that a host asking for the trigger sees the primitive's
//! own.

use crate::model::{ChannelCategory, EventStatus, RoomContext, RoomEvent};
use crate::rules::permissions;
use crate::rules::verification::release;

/// The event a regeneration of `context`'s room answers again: the newest
/// of its recent events a transport binding wrote that the room accepted,
/// provided that binding can still write; `None` when none qualifies.
///
/// A message stored `BLOCKED` was never broadcast, a hook refused it or its
/// source could not write (RFC §10.1 step 10): the room never answered it,
/// and neither does a regeneration. Nor a message a delete withdrew, nor an
/// edit or a delete itself (RFC §10.3): replayed, either would hand the
/// agents what is no longer said. A muted or read-only source has no turn to
/// regenerate (RFC §7.5).
#[must_use]
pub fn target(context: &RoomContext) -> Option<&RoomEvent> {
    let transport = |event: &RoomEvent| {
        context
            .bindings
            .iter()
            .find(|binding| binding.channel_id == event.source.channel_id)
            .filter(|binding| binding.category == ChannelCategory::Transport)
    };
    let trigger = context.recent_events.iter().rev().find(|event| {
        transport(event).is_some()
            && event.status != EventStatus::Blocked
            && !release::withdrawn(event)
    })?;
    let source = transport(trigger)?;
    permissions::write_refusal(source)
        .is_none()
        .then_some(trigger)
}

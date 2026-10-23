//! The commit of a pass (RFC §10.1 steps 10 to 12, §14.3): the event and
//! the room's counters in one write, with what an accepted edit or delete
//! writes on its target (RFC §10.3).

use super::pass::PassResult;
use crate::model::{EventStatus, RoomContext, RoomEvent, RoomUpdates, Timestamp};
use crate::ops::protocol::{EffectId, Request, RoomCounters, TargetUpdate};
use crate::ops::{Cx, FrameworkEvent, Notice};
use crate::rules::lifecycle::{self, Transition};
use crate::rules::permissions::DeliveryPlan;

/// A commit asked for, and what the pass did once it is durable.
pub(super) struct Committing {
    pub(super) effect: EffectId,
    pub(super) result: PassResult,
}

/// Commits `event` delivered with the room's counters, its activity moved
/// (RFC §10.1 step 12). A paused room is resumed in the same commit (gap
/// G-045), and the delivery set reads it active.
///
/// An accepted edit or delete updates its target in the same commit, never
/// before (RFC §10.1 step 12, §10.3): `written` is the write on the target
/// and the target as it leaves it. The delivery set reads the target so,
/// and the result carries it for the mutation trigger.
pub(super) fn delivered(
    cx: &mut Cx<'_>,
    mut event: RoomEvent,
    plan: DeliveryPlan,
    mut context: Box<RoomContext>,
    written: Option<(TargetUpdate, RoomEvent)>,
) -> Committing {
    event.status = EventStatus::Delivered;
    let now = cx.env.now();
    let counters = RoomCounters {
        event_count: context.room.event_count.saturating_add(1),
        latest_index: event.index,
        last_activity_at: Some(now),
        room: resume(&mut context, now),
    };
    let (target, changed) = match written {
        Some((update, changed)) => {
            for recent in &mut context.recent_events {
                if recent.id == changed.id {
                    recent.clone_from(&changed);
                }
            }
            (Some(update), Some(Box::new(changed)))
        }
        None => (None, None),
    };
    let effect = cx.io.issue(Request::Commit {
        event: event.clone(),
        counters,
        target,
    });
    Committing {
        effect,
        result: PassResult::Delivered {
            event: Box::new(event),
            plan,
            context,
            target: changed,
        },
    }
}

/// Commits `event` blocked by `blocked_by` (RFC §10.1 steps 10 and 11): the
/// record takes its index and counts in the room, without moving its
/// activity. An edit or a delete blocked leaves its target as it is (RFC
/// §10.3).
pub(super) fn blocked(
    cx: &mut Cx<'_>,
    mut event: RoomEvent,
    blocked_by: String,
    reason: Option<String>,
    context: &RoomContext,
) -> Committing {
    event.status = EventStatus::Blocked;
    event.blocked_by = Some(blocked_by.clone());
    cx.io
        .notify(Notice::FrameworkEvent(FrameworkEvent::EventBlocked {
            room_id: event.room_id.clone(),
            event_id: event.id.clone(),
            channel_id: event.source.channel_id.clone(),
            reason: reason.clone(),
            blocked_by,
        }));
    let counters = RoomCounters {
        event_count: context.room.event_count.saturating_add(1),
        latest_index: event.index,
        last_activity_at: context.room.timers.last_activity_at,
        room: RoomUpdates::default(),
    };
    let effect = cx.io.issue(Request::Commit {
        event: event.clone(),
        counters,
        target: None,
    });
    Committing {
        effect,
        result: PassResult::Blocked {
            event: Box::new(event),
            reason,
            context: Box::new(context.clone()),
        },
    }
}

/// The write that resumes the room of `context` at `now` when it is paused,
/// applied to `context` too; empty for a room that is not (gap G-045).
fn resume(context: &mut RoomContext, now: Timestamp) -> RoomUpdates {
    if !lifecycle::apply(&mut context.room, Transition::Resume, now) {
        return RoomUpdates::default();
    }
    context.room.updated_at = now;
    RoomUpdates::lifecycle(&context.room)
}

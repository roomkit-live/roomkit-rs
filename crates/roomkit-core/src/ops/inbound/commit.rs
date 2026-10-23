//! The commit of a pass (RFC §10.1 steps 10 to 12, §14.3): the event and
//! the room's counters in one write, with what an accepted edit or delete
//! writes on its target (RFC §10.3).
//!
//! An instruction is never committed (RFC §10.1.1 rules 4 and 5): its pass
//! decides it, blocked or delivered, with no commit, so it consumes no index
//! and moves no counter, and a paused room stays paused until an event is
//! committed (gap G-065).

use super::pass::PassResult;
use crate::model::{
    EventStatus, EventType, RoomContext, RoomEvent, RoomUpdates, Timestamp, Visibility,
};
use crate::ops::protocol::{EffectId, Request, RoomCounters, TargetUpdate};
use crate::ops::{Cx, FrameworkEvent, Notice};
use crate::rules::lifecycle::{self, Transition};
use crate::rules::permissions::{self, DeliveryPlan};

/// A commit asked for, and what the pass did once it is durable; no effect
/// for an instruction, which the pass decides without writing.
pub(super) struct Committing {
    pub(super) effect: Option<EffectId>,
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
    if event.event_type == EventType::Instruction {
        // Delivered to the agents it names, with the index step 8 showed its
        // hooks, which the next event takes (RFC §10.1.1 rule 5).
        return Committing {
            effect: None,
            result: PassResult::Delivered {
                event: Box::new(event),
                plan,
                context,
                target: None,
            },
        };
    }
    let now = cx.env.now();
    let counters = RoomCounters::after_commit(&context.room, event.index)
        .moving_activity(now, resume(&mut context, now));
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
    let room_id = event.room_id.clone();
    let request = Request::Commit {
        event: event.clone(),
        counters,
        target,
    };
    let effect = cx.io.issue_locked(request, room_id);
    Committing {
        effect: Some(effect),
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
/// §10.3). A blocked instruction is not stored, and `event_blocked` is
/// emitted all the same (RFC §10.1.1 rule 4).
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
    let result = PassResult::Blocked {
        event: Box::new(event.clone()),
        reason,
        context: Box::new(context.clone()),
    };
    if event.event_type == EventType::Instruction {
        return Committing {
            effect: None,
            result,
        };
    }
    let counters = RoomCounters::after_commit(&context.room, event.index);
    let room_id = event.room_id.clone();
    let request = Request::Commit {
        event,
        counters,
        target: None,
    };
    let effect = cx.io.issue_locked(request, room_id);
    Committing {
        effect: Some(effect),
        result,
    }
}

/// The visibility a channel's record stored `BLOCKED` keeps: the one step
/// 12 would have given it, its own intersected with its source binding's
/// (RFC §7.3), when the room of `context` binds its source; its own when it
/// does not (gap G-036). A muted or read-only source's record is narrowed
/// so, and a reader that sees it later sees no more than it would have.
pub(super) fn stored_visibility(event: &RoomEvent, context: &RoomContext) -> Visibility {
    context.binding(&event.source.channel_id).map_or_else(
        || event.visibility.clone(),
        |source| permissions::effective_visibility(&event.visibility, source, &context.bindings),
    )
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

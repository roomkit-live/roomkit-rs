//! The commit of a pass (RFC §10.1 steps 10 to 12, §14.3): the event and
//! the room's counters in one write, the store giving the index, with the
//! target an accepted edit or delete changes (RFC §10.3).
//!
//! An instruction is never committed (RFC §10.1.1 rules 4 and 5): its pass
//! decides it, blocked or delivered, with no commit, so it consumes no index
//! and moves no counter, and a paused room stays paused until an event is
//! committed (gap G-065). Nor is an event of a type the kit's persistence
//! policy keeps out (RFC §14.3): it is delivered, and nothing of it is
//! stored but an edit's or a delete's target, written alone, and the room's
//! register of authors its author record extends.

use super::pass::{Changed, PassResult};
use crate::model::{
    EventStatus, EventType, Metadata, RoomContext, RoomEvent, Timestamp, Visibility,
};
use crate::ops::protocol::{EffectId, StoreRequest};
use crate::ops::{Cx, FrameworkEvent, Notice, RECENT_EVENTS_LIMIT};
use crate::rules::authors::AUTHOR;
use crate::rules::lifecycle::{self, Transition};
use crate::rules::permissions::{self, DeliveryPlan};

/// A commit asked for, and what the pass did once it is durable; no write
/// for an instruction, which the pass decides without writing, nor for an
/// event the kit's persistence policy keeps out that changes nothing it
/// stores.
pub(super) struct Committing {
    pub(super) write: Option<Write>,
    pub(super) result: PassResult,
}

/// What a pass writes before its result stands.
pub(super) enum Write {
    /// The event's commit, answered with the event as stored.
    Commit(EffectId),
    /// What an event the kit's persistence policy keeps out of the timeline
    /// changes: an edit's or a delete's target, the room's register of
    /// authors (RFC §14.3).
    Unstored(EffectId),
}

/// Commits `event` delivered, the room's activity (RFC §10.1 step 12): the
/// store moves its last activity and resumes it when paused, in the same
/// commit (gap G-045), and the delivery set reads it active.
///
/// An accepted edit or delete updates its target in the same commit, never
/// before (RFC §10.1 step 12, §10.3): `written` is the target as it leaves
/// it. The delivery set reads the target so, and the result carries it,
/// with the room as the commit leaves it, for the mutation trigger.
/// `register` is the room's register of authors as the event's author
/// record left it, when it changed (RFC §10.1 step 12, gap G-089).
///
/// `policy_aware` is whether the kit's persistence policy reads the pass at
/// all: a hook's injected event is stored whatever it says (RFC §14.3).
pub(super) fn delivered(
    cx: &mut Cx<'_>,
    mut event: RoomEvent,
    plan: DeliveryPlan,
    mut context: Box<RoomContext>,
    written: Option<RoomEvent>,
    register: Option<Metadata>,
    policy_aware: bool,
) -> Committing {
    event.status = EventStatus::Delivered;
    if event.r#type == EventType::Instruction {
        // Delivered to the agents it names, with the index step 8 showed its
        // hooks, which the next event takes (RFC §10.1.1 rule 5).
        return Committing {
            write: None,
            result: PassResult::Delivered {
                event: Box::new(event),
                plan,
                context,
                changed: None,
            },
        };
    }
    if policy_aware && !cx.config.persists(event.r#type) {
        return unstored(cx, event, plan, context, written, register);
    }
    let now = cx.env.now();
    resume(&mut context, now);
    let (target, changed) = match written {
        Some(changed) => {
            rewrite_target(&mut context, &changed);
            let target = Box::new(changed.clone());
            let changed = Changed {
                target: changed,
                context: committed(&context, &event, now),
            };
            (Some(target), Some(Box::new(changed)))
        }
        None => (None, None),
    };
    let room_id = event.room_id.clone();
    let request = StoreRequest::Commit {
        event: Box::new(event.clone()),
        target,
        activity: Some(now),
        author_register: register,
    };
    let effect = cx.io.issue_locked(request, room_id);
    Committing {
        write: Some(Write::Commit(effect)),
        result: PassResult::Delivered {
            event: Box::new(event),
            plan,
            context,
            changed,
        },
    }
}

/// Delivers `event`, of a type the kit's persistence policy keeps out,
/// without committing it (RFC §14.3): it takes no index, moves none of the
/// room's counters and is no activity, a paused room staying paused. It
/// keeps the index its hooks were shown, and joins its delivery set's
/// context as its newest event, as a committed event does (Python's
/// `_commit_to_lane`). An accepted edit or delete writes its target alone
/// (`written`, Python's `_write_unstored_change`), which its mutation
/// trigger then reads in the room as it stands, the change its newest
/// event; the event's author record extends the room's register of
/// authors (`register`) all the same, so that a later source never takes
/// the rank a reader saw it hold (RFC §10.1 step 12).
fn unstored(
    cx: &mut Cx<'_>,
    event: RoomEvent,
    plan: DeliveryPlan,
    mut context: Box<RoomContext>,
    written: Option<RoomEvent>,
    register: Option<Metadata>,
) -> Committing {
    if let Some(changed) = &written {
        rewrite_target(&mut context, changed);
    }
    let write = (written.is_some() || register.is_some()).then(|| {
        let request = StoreRequest::WriteUnstored {
            room_id: event.room_id.clone(),
            target: written.clone().map(Box::new),
            author_register: register,
        };
        Write::Unstored(cx.io.issue_locked(request, event.room_id.clone()))
    });
    join_newest(&mut context.recent_events, &event);
    let changed = written.map(|target| {
        Box::new(Changed {
            target,
            context: (*context).clone(),
        })
    });
    Committing {
        write,
        result: PassResult::Delivered {
            event: Box::new(event),
            plan,
            context,
            changed,
        },
    }
}

/// Writes `changed`, an edit's or a delete's target as the change leaves
/// it, in place of its copies in `context`'s history.
fn rewrite_target(context: &mut RoomContext, changed: &RoomEvent) {
    for recent in &mut context.recent_events {
        if recent.id == changed.id {
            recent.clone_from(changed);
        }
    }
}

/// The room of `context` as the commit of `event`, the room's activity at
/// `now`, leaves it (RFC §10.3 "Mutation triggers", gap G-062): one more
/// index given out, `event`'s the latest, its activity moved, and `event`
/// the newest event of its history ([`join_newest`]).
fn committed(context: &RoomContext, event: &RoomEvent, now: Timestamp) -> RoomContext {
    let mut committed = context.clone();
    let room = &mut committed.room;
    room.event_count = room.event_count.saturating_add(1);
    room.latest_index = Some(event.index);
    room.timers.last_activity_at = Some(now);
    join_newest(&mut committed.recent_events, event);
    committed
}

/// `history`, the room's events its pass read, with `event`, as committed,
/// its newest in place of a copy it holds, the oldest dropped past
/// [`RECENT_EVENTS_LIMIT`]: the Python implementation's `committed_in`,
/// which a delivery set's context and a change's mutation trigger read
/// (RFC §10.2, §10.3). The window the pass read is not the cap: a window of
/// 50 holds 51 events once `event` joins it.
pub(super) fn join_newest(history: &mut Vec<RoomEvent>, event: &RoomEvent) {
    history.retain(|recent| recent.id != event.id);
    let excess = history
        .len()
        .saturating_sub(RECENT_EVENTS_LIMIT.saturating_sub(1));
    history.drain(..excess);
    history.push(event.clone());
}

/// Commits `event` blocked by `blocked_by` (RFC §10.1 steps 10 and 11): the
/// record takes its index and counts in the room, without moving its
/// activity. It takes no author record, one it came with dropped (§10.1
/// step 12, gap G-089). An edit or a delete blocked leaves its target as it is (RFC
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
    event.metadata.shift_remove(AUTHOR);
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
    if event.r#type == EventType::Instruction {
        return Committing {
            write: None,
            result,
        };
    }
    let room_id = event.room_id.clone();
    let request = StoreRequest::Commit {
        event: Box::new(event),
        target: None,
        activity: None,
        author_register: None,
    };
    let effect = cx.io.issue_locked(request, room_id);
    Committing {
        write: Some(Write::Commit(effect)),
        result,
    }
}

/// The visibility a channel's record stored `BLOCKED` keeps: the one step
/// 12 would have given it, its own intersected with its source binding's
/// (RFC §7.3), when the room of `context` binds its source; its own when it
/// does not (gap G-036). A muted or read-only source's record is narrowed
/// so, and a reader that sees it later sees no more than it would have.
pub(super) fn stored_visibility(event: &RoomEvent, context: &RoomContext) -> Visibility {
    context.get_binding(&event.source.channel_id).map_or_else(
        || event.visibility.clone(),
        |source| permissions::effective_visibility(&event.visibility, source, &context.bindings),
    )
}

/// Resumes the room of `context` at `now` when it is paused, as the commit
/// resumes the stored room (gap G-045).
fn resume(context: &mut RoomContext, now: Timestamp) {
    if lifecycle::apply(&mut context.room, Transition::Resume, now) {
        context.room.updated_at = now;
    }
}

/// Writes `stored`, the event as the store committed it, in place of its
/// planned copy in `context`, a context the commit left: the index the
/// store gave it is the latest (RFC §8.1, Python's `commit_event`).
pub(super) fn adopt(context: &mut RoomContext, stored: &RoomEvent) {
    let Some(copy) = context
        .recent_events
        .iter_mut()
        .find(|recent| recent.id == stored.id)
    else {
        return;
    };
    copy.clone_from(stored);
    context.room.latest_index = Some(stored.index);
    context.room.event_count = context.room.event_count.max(stored.index.saturating_add(1));
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::model::{ChannelId, ChannelType, EventId, EventSource, RoomId, TextContent};

    fn events(count: usize) -> Vec<RoomEvent> {
        (0..count)
            .map(|index| {
                RoomEvent::at(
                    EventId::new(format!("evt_{index}")).unwrap(),
                    RoomId::new("r").unwrap(),
                    EventType::Message,
                    EventSource::new(ChannelId::new("sms").unwrap(), ChannelType::SMS),
                    TextContent::new("hi"),
                    Timestamp::UNIX_EPOCH,
                )
            })
            .collect()
    }

    // Python: tests/test_context_visibility.py::TestCommittedVisibility::test_a_detached_source_leaves_the_event_speaking_for_itself
    #[test]
    fn a_detached_source_leaves_the_event_speaking_for_itself() {
        let mut event = events(1).remove(0);
        let context = RoomContext::new(crate::model::Room::at(
            event.room_id.clone(),
            Timestamp::UNIX_EPOCH,
        ));
        for visibility in [Visibility::Transport, Visibility::Internal] {
            event.visibility = visibility.clone();
            assert_eq!(stored_visibility(&event, &context), visibility);
        }
    }

    fn ids(history: &[RoomEvent]) -> (usize, Option<String>, Option<String>) {
        (
            history.len(),
            history.first().map(|event| event.id.to_string()),
            history.last().map(|event| event.id.to_string()),
        )
    }

    /// Python's `committed_in` (`core/mixins/inbound_locked.py`): a window
    /// under the ceiling keeps every event it read, the committed one after
    /// them; at the ceiling the oldest gives way (RMK-1130).
    #[test]
    fn a_committed_event_joins_its_context_as_python_s_committed_in_holds_it() {
        let mut window = events(50);
        let mut ceiling = events(RECENT_EVENTS_LIMIT);
        let newest = events(RECENT_EVENTS_LIMIT + 1).pop().unwrap();
        join_newest(&mut window, &newest);
        join_newest(&mut ceiling, &newest);
        let mut again = events(3);
        let copy = again[1].clone();
        join_newest(&mut again, &copy);
        assert_eq!(
            [ids(&window), ids(&ceiling), ids(&again)],
            [
                (51, Some("evt_0".into()), Some("evt_2000".into())),
                (2000, Some("evt_1".into()), Some("evt_2000".into())),
                (3, Some("evt_0".into()), Some("evt_1".into())),
            ]
        );
    }
}

//! The timer transitions every locked section applies before it decides
//! (RFC §5.1, gap G-045).
//!
//! A locked section reads its room under the lock, then opens the gate.
//! When a timer transition fell due, the gate writes it, every due one in a
//! single write, each at the instant it fell due, and emits each one's
//! framework event as soon as the write is stored, before the section
//! decides anything else: a message refused because its room's close fell
//! due reports `room_closed` before `room_refused_event`. Each one's trigger
//! fires once the section releases the room's lock, as an ASYNC trigger
//! does (RFC §5.1 When, [`TimerHooks`]). The section then decides on the
//! room as the transitions left it, so a late message is refused exactly.

use crate::model::{ErrorRecord, Room, RoomContext, RoomUpdates};
use crate::ops::protocol::{EffectId, Notice, Outcome, StoreRequest};
use crate::ops::{Cx, TransitionReason};
use crate::rules::lifecycle::{self, Transition};
use crate::rules::timers::{self, DueTransition};

use super::report::{self, Reported};

/// What the gate finds on a room read under its lock.
#[derive(Debug)]
pub(crate) enum Gate {
    /// No timer transition is due: the section decides on the room as read.
    Clear(Box<RoomContext>),
    /// The due transitions are being written; the section waits for
    /// `effect`, then hands its outcome to `pending`.
    Writing {
        effect: EffectId,
        pending: Box<Pending>,
    },
}

/// Due transitions whose write is outstanding.
#[derive(Debug)]
pub(crate) struct Pending {
    /// Each transition, with the room as it left it.
    applied: Vec<(DueTransition, Room)>,
    /// The room as the last transition left it.
    context: Box<RoomContext>,
}

/// What the gate hands back once its write is stored.
#[derive(Debug)]
pub(crate) struct Opened {
    /// The room as the transitions left it.
    pub(crate) context: Box<RoomContext>,
    /// The transitions applied, in the order they fell due.
    pub(crate) transitions: Vec<DueTransition>,
    /// Their triggers' hooks, which the section fires once it released the
    /// room's lock.
    pub(crate) hooks: TimerHooks,
}

/// The hooks of the timer transitions a gate applied, which fire once the
/// section that holds the room's lock released it (RFC §5.1 When).
#[derive(Debug, Default)]
pub(crate) struct TimerHooks(Vec<Notice>);

impl TimerHooks {
    /// Fires the hooks, the section's lock released: at once, or, for a
    /// section nested under another's lock, once that one released it
    /// ([`Io::notify_after_release`](crate::ops::io::Io::notify_after_release)).
    pub(crate) fn fire(self, cx: &mut Cx<'_>) {
        for notice in self.0 {
            cx.io.notify_after_release(notice);
        }
    }
}

/// Opens the gate on `context`, read under the room's lock.
pub(crate) fn open(cx: &mut Cx<'_>, context: Box<RoomContext>) -> Gate {
    let now = cx.env.now();
    let due = timers::due(&context.room, now);
    if due.is_empty() {
        return Gate::Clear(context);
    }
    let mut room = context.room.clone();
    let mut applied = Vec::with_capacity(due.len());
    for transition in due {
        lifecycle::apply(&mut room, transition.transition, transition.at);
        room.updated_at = now;
        applied.push((transition, room.clone()));
    }
    let effect = cx.io.issue(StoreRequest::UpdateRoom {
        room_id: room.id.clone(),
        updates: RoomUpdates::lifecycle(&room),
    });
    let context = Box::new(RoomContext { room, ..*context });
    Gate::Writing {
        effect,
        pending: Box::new(Pending { applied, context }),
    }
}

impl Pending {
    /// Takes the write's outcome. Stored, each transition's framework event
    /// is emitted, and its hooks, which receive the room as that transition
    /// left it (gap G-027), are handed back to fire after the release.
    pub(crate) fn stored(self, cx: &mut Cx<'_>, outcome: Outcome) -> Result<Opened, ErrorRecord> {
        outcome.into_stored()?;
        let mut hooks = Vec::new();
        for (due, room) in &self.applied {
            let Some(reported) = reported(due.transition) else {
                continue;
            };
            let context = RoomContext {
                room: room.clone(),
                ..(*self.context).clone()
            };
            hooks.extend(report::report_after_release(cx, reported, &context));
        }
        Ok(Opened {
            context: self.context,
            transitions: self.applied.into_iter().map(|(due, _)| due).collect(),
            hooks: TimerHooks(hooks),
        })
    }
}

/// What a timer's transition reports: `room_paused` and `room_closed` with
/// the reason `timer` (RFC §8.2).
fn reported(transition: Transition) -> Option<Reported<'static>> {
    match transition {
        Transition::Pause => Some(Reported::RoomPaused(Some(TransitionReason::Timer))),
        Transition::Close => Some(Reported::RoomClosed(Some(TransitionReason::Timer))),
        Transition::Resume | Transition::Reopen | Transition::Archive => None,
    }
}

//! The timer transitions every locked section applies before it decides
//! (RFC §5.1, gap G-045).
//!
//! A locked section reads its room under the lock, then opens the gate.
//! When a timer transition fell due, the gate writes it, every due one in a
//! single write, each at the instant it fell due, and reports each as soon
//! as the write is stored, before the section decides anything else: a
//! message refused because its room's close fell due reports `room_closed`
//! before `room_refused_event`. The section then decides on the room as the
//! transitions left it, so a late message is refused exactly.

use crate::model::{ErrorRecord, Room, RoomContext, RoomUpdates};
use crate::ops::protocol::{EffectId, Outcome, Request};
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
    let effect = cx.io.issue(Request::UpdateRoom {
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
    /// Takes the write's outcome. Stored, each transition is reported, its
    /// hooks receiving the room as that transition left it (gap G-027).
    pub(crate) fn stored(self, cx: &mut Cx<'_>, outcome: Outcome) -> Result<Opened, ErrorRecord> {
        outcome.into_stored()?;
        for (due, room) in &self.applied {
            let Some(reported) = reported(due.transition) else {
                continue;
            };
            let context = RoomContext {
                room: room.clone(),
                ..(*self.context).clone()
            };
            report::report(cx, reported, &context);
        }
        Ok(Opened {
            context: self.context,
            transitions: self.applied.into_iter().map(|(due, _)| due).collect(),
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

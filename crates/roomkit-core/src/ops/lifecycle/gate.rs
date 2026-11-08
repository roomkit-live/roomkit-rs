//! The timer transitions every locked section applies before it decides
//! (RFC §5.1, gap G-045).
//!
//! A locked section reads its room under the lock, then opens the gate.
//! When a timer transition fell due, the gate writes it, every due one in a
//! single write, each at the instant it fell due. Then, for each one in the
//! order they fell due, it commits the transition's lifecycle record, which
//! a CLOSED room still takes (RFC §5.1, gap G-046), and emits its framework
//! event, before the section decides anything else: a message refused
//! because its room's close fell due reports `room_closed` before
//! `room_refused_event`. Each one's trigger fires once the section releases
//! the room's lock, as an ASYNC trigger does (RFC §5.1 When,
//! [`TimerHooks`]). The section then decides on the room as the transitions
//! and their records left it, so a late message is refused exactly, and an
//! event takes the index after the records.

use crate::model::{ErrorRecord, Room, RoomContext, RoomUpdates};
use crate::ops::Cx;
use crate::ops::protocol::{EffectId, Notice, Outcome, StoreRequest};
use crate::rules::lifecycle::{self, Transition};
use crate::rules::records::Record;
use crate::rules::timers::{self, DueTransition};

use super::record;
use super::report::{self, Reported, TimerDue};

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

/// Due transitions whose writes are outstanding: the room's update, then
/// each one's lifecycle record.
#[derive(Debug)]
pub(crate) struct Pending {
    /// Each transition, with the room as it left it.
    applied: Vec<(DueTransition, Room)>,
    /// The room as the last transition left it, and the records committed
    /// so far.
    context: Box<RoomContext>,
    /// The transition whose record's commit is outstanding; `None` while
    /// the room's update is.
    recording: Option<usize>,
    /// The hooks of the transitions announced so far.
    hooks: Vec<Notice>,
}

/// What the gate does once one of its writes is stored.
#[derive(Debug)]
pub(crate) enum Applied {
    /// Every write is stored: the section decides on the room as they left
    /// it.
    Opened(Opened),
    /// The next transition's record is being committed; the section waits
    /// for `effect`, then hands its outcome to `pending` again.
    Writing {
        effect: EffectId,
        pending: Box<Pending>,
    },
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
        pending: Box::new(Pending {
            applied,
            context,
            recording: None,
            hooks: Vec::new(),
        }),
    }
}

impl Pending {
    /// Takes the outcome of the gate's write outstanding. Once the room's
    /// update is stored, each transition's record is committed in turn
    /// (gap G-046); once a record is, its transition's framework event is
    /// emitted, and its hooks, which receive the room as that transition
    /// left it (gap G-027), are kept to fire after the release. A record
    /// the kit's persistence policy keeps out is not written, and its
    /// transition is announced all the same (RFC §14.3).
    pub(crate) fn stored(
        mut self: Box<Self>,
        cx: &mut Cx<'_>,
        outcome: Outcome,
    ) -> Result<Applied, ErrorRecord> {
        outcome.into_written()?;
        let mut next = match self.recording.take() {
            Some(index) => {
                self.announce(cx, index);
                index.saturating_add(1)
            }
            None => 0,
        };
        while let Some(record) = self.record_at(next) {
            if let Some(request) = record::commit(cx, &mut self.context, &record) {
                self.recording = Some(next);
                let effect = record::issue_write(cx, request);
                return Ok(Applied::Writing {
                    effect,
                    pending: self,
                });
            }
            self.announce(cx, next);
            next = next.saturating_add(1);
        }
        Ok(Applied::Opened((*self).opened()))
    }

    /// The lifecycle record of the transition at `index`, if any: the
    /// timer's threshold, and the instant it fell due.
    fn record_at(&self, index: usize) -> Option<Record> {
        let (due, _) = self.applied.get(index)?;
        let room = &self.context.room;
        let threshold = match due.transition {
            Transition::Pause => room.timers.inactive_after_seconds,
            Transition::Resume | Transition::Reopen | Transition::Close | Transition::Archive => {
                room.timers.closed_after_seconds
            }
        };
        Some(Record::Timer {
            room_id: room.id.clone(),
            transition: due.transition,
            threshold: threshold.unwrap_or_default(),
            at: due.at,
        })
    }

    /// Emits the framework event of the transition at `index`, whose record
    /// is committed or kept out, and keeps its hooks, with the room as the transition
    /// left it and the records committed so far.
    fn announce(&mut self, cx: &mut Cx<'_>, index: usize) {
        let Some((due, room)) = self.applied.get(index) else {
            return;
        };
        let Some(reported) = reported(due, &self.context.room) else {
            return;
        };
        let counted = &self.context.room;
        let context = RoomContext {
            room: Room {
                event_count: counted.event_count,
                latest_index: counted.latest_index,
                ..room.clone()
            },
            ..(*self.context).clone()
        };
        self.hooks
            .extend(report::report_after_release(cx, reported, &context));
    }

    /// What the gate hands back once every write is stored.
    fn opened(self) -> Opened {
        let Self {
            applied,
            context,
            hooks,
            ..
        } = self;
        Opened {
            context,
            transitions: applied.into_iter().map(|(due, _)| due).collect(),
            hooks: TimerHooks(hooks),
        }
    }
}

/// What a timer's transition of `room` reports: `room_paused` and
/// `room_closed` with the reason `timer` (RFC §8.2), its hooks' payload
/// naming the timer's threshold and the instant it fell due.
fn reported(due: &DueTransition, room: &Room) -> Option<Reported<'static>> {
    let threshold = match due.transition {
        Transition::Pause => room.timers.inactive_after_seconds,
        Transition::Resume | Transition::Reopen | Transition::Close | Transition::Archive => {
            room.timers.closed_after_seconds
        }
    };
    let timer = TimerDue {
        threshold: threshold.unwrap_or_default(),
        at: due.at,
    };
    match due.transition {
        Transition::Pause => Some(Reported::RoomPaused(Some(timer))),
        Transition::Close => Some(Reported::RoomClosed(Some(timer))),
        Transition::Resume | Transition::Reopen | Transition::Archive => None,
    }
}

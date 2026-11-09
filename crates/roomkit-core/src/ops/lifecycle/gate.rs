//! Apply due timers before a locked section decides (RFC §5.1).
//!
//! One conditional write applies every due transition at its due instant.
//! A concurrent commit makes the gate reread and recompute, never overwrite
//! activity. Each transition's record and framework event precede the section's
//! decision; both hooks receive the final context after the lock is released.

mod write;

use super::record;
use super::report::{self, Reported, TimerDue};
use crate::model::{ErrorRecord, Room, RoomContext};
use crate::ops::Cx;
use crate::ops::protocol::{EffectId, Notice, Outcome, StoreRequest};
use crate::rules::lifecycle::Transition;
use crate::rules::records::Record;
use crate::rules::timers::DueTransition;

#[derive(Debug)]
pub(crate) enum Gate {
    Clear(Box<RoomContext>),
    Writing {
        effect: EffectId,
        pending: Box<Pending>,
    },
}

#[derive(Debug)]
pub(crate) enum Pending {
    Room(write::Write),
    Records(Records),
    Context(Records),
}

#[derive(Debug)]
pub(crate) struct Records {
    context: Box<RoomContext>,
    due: Vec<DueTransition>,
    recording: usize,
}

#[derive(Debug)]
pub(crate) enum Applied {
    Opened(Opened),
    Writing {
        effect: EffectId,
        pending: Box<Pending>,
    },
}

#[derive(Debug)]
pub(crate) struct Opened {
    pub(crate) context: Box<RoomContext>,
    pub(crate) transitions: Vec<DueTransition>,
    pub(crate) hooks: TimerHooks,
}

/// Timer hooks fire once the section releases its room lock, including a
/// section nested under another lock (RFC §5.1 When).
#[derive(Debug, Default)]
pub(crate) struct TimerHooks(Vec<Notice>);

impl TimerHooks {
    pub(crate) fn fire(self, cx: &mut Cx<'_>) {
        for notice in self.0 {
            cx.io.notify_after_release(notice);
        }
    }
}

pub(crate) fn open(cx: &mut Cx<'_>, context: Box<RoomContext>) -> Gate {
    match write::Write::start(cx, context, 0) {
        write::Progress::Done { context, .. } => Gate::Clear(context),
        write::Progress::Waiting { effect, write } => Gate::Writing {
            effect,
            pending: Box::new(Pending::Room(write)),
        },
    }
}

impl Pending {
    pub(crate) fn stored(
        self: Box<Self>,
        cx: &mut Cx<'_>,
        outcome: Outcome,
    ) -> Result<Applied, ErrorRecord> {
        match *self {
            Self::Room(write) => match write.answered(cx, outcome)? {
                write::Progress::Waiting { effect, write } => Ok(Applied::Writing {
                    effect,
                    pending: Box::new(Self::Room(write)),
                }),
                write::Progress::Done { context, due } => Ok(Records {
                    context,
                    due,
                    recording: 0,
                }
                .next(cx)),
            },
            Self::Context(mut records) => {
                records.context = outcome.into_context()?.ok_or_else(|| {
                    ErrorRecord::new(
                        "RoomNotFoundError",
                        "the timer room was deleted before its context read",
                    )
                })?;
                Ok(records.opened(cx))
            }
            Self::Records(mut records) => {
                outcome.into_written()?;
                records.announce(cx);
                records.recording += 1;
                Ok(records.next(cx))
            }
        }
    }
}

impl Records {
    /// A kept-out record still reports its transition; a stored record reports
    /// only once its write is durable, before the next record or decision.
    fn next(mut self, cx: &mut Cx<'_>) -> Applied {
        while let Some(due) = self.due.get(self.recording) {
            let record = Record::Timer {
                room_id: self.context.room.id.clone(),
                transition: due.transition,
                threshold: threshold(due, &self.context.room),
                at: due.at,
            };
            if let Some(request) = record::commit(cx, &mut self.context, &record) {
                return Applied::Writing {
                    effect: record::issue_write(cx, request),
                    pending: Box::new(Pending::Records(self)),
                };
            }
            self.announce(cx);
            self.recording += 1;
        }
        if self.due.is_empty() {
            return self.opened(cx);
        }
        let effect = cx.io.issue(StoreRequest::LoadContext {
            room_id: self.context.room.id.clone(),
            recent_events: cx.config.recent_events(),
        });
        Applied::Writing {
            effect,
            pending: Box::new(Pending::Context(self)),
        }
    }

    fn announce(&self, cx: &mut Cx<'_>) {
        if let Some(due) = self.due.get(self.recording)
            && let Some(reported) = reported(due, &self.context.room)
        {
            report::announce(cx, reported, &self.context.room.id);
        }
    }

    /// Construct both hooks only after all records have reached the store.
    fn opened(self, cx: &mut Cx<'_>) -> Applied {
        let hooks = self
            .due
            .iter()
            .filter_map(|due| {
                report::hooks_after_release(cx, reported(due, &self.context.room)?, &self.context)
            })
            .collect();
        Applied::Opened(Opened {
            context: self.context,
            transitions: self.due,
            hooks: TimerHooks(hooks),
        })
    }
}

fn threshold(due: &DueTransition, room: &Room) -> u64 {
    match due.transition {
        Transition::Pause => room.timers.inactive_after_seconds,
        Transition::Resume | Transition::Reopen | Transition::Close | Transition::Archive => {
            room.timers.closed_after_seconds
        }
    }
    .unwrap_or_default()
}

fn reported(due: &DueTransition, room: &Room) -> Option<Reported<'static>> {
    let timer = TimerDue {
        threshold: threshold(due, room),
        at: due.at,
    };
    match due.transition {
        Transition::Pause => Some(Reported::RoomPaused(Some(timer))),
        Transition::Close => Some(Reported::RoomClosed(Some(timer))),
        Transition::Resume | Transition::Reopen | Transition::Archive => None,
    }
}

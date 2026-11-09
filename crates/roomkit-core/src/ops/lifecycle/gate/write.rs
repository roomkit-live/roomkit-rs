//! A timer's conditional write, retried from the latest room after an off-lock
//! commit. Python: `core/_timer_transitions.py::TimerTransitions._write_due`.

use crate::model::{ErrorRecord, RoomContext};
use crate::ops::Cx;
use crate::ops::protocol::{EffectId, Outcome, StoreRequest};
use crate::rules::lifecycle;
use crate::rules::timers::{self, DueTransition};

const ATTEMPTS: usize = 3;

#[derive(Debug)]
pub(crate) struct Write {
    context: Box<RoomContext>,
    due: Vec<DueTransition>,
    attempts: usize,
    reading: bool,
}

pub(super) enum Progress {
    Done {
        context: Box<RoomContext>,
        due: Vec<DueTransition>,
    },
    Waiting {
        effect: EffectId,
        write: Write,
    },
}

impl Write {
    /// Compute the transition afresh at every attempt, preserving the snapshot
    /// read if its room is deleted before the retry (Python's missing-room rule).
    pub(super) fn start(cx: &mut Cx<'_>, context: Box<RoomContext>, attempts: usize) -> Progress {
        let now = cx.env.now();
        let due = timers::due(&context.room, now);
        if due.is_empty() || attempts == ATTEMPTS {
            return Progress::Done {
                context,
                due: Vec::new(),
            };
        }
        let mut room = context.room.clone();
        for transition in &due {
            lifecycle::apply(&mut room, transition.transition, transition.at);
        }
        room.updated_at = now;
        let effect = cx.io.issue(StoreRequest::UpdateRoomIf {
            room: Box::new(room),
            status: context.room.status,
            paused_at: context.room.paused_at,
            last_activity_at: context.room.timers.last_activity_at,
        });
        Progress::Waiting {
            effect,
            write: Self {
                context,
                due,
                attempts: attempts + 1,
                reading: false,
            },
        }
    }

    pub(super) fn answered(
        mut self,
        cx: &mut Cx<'_>,
        outcome: Outcome,
    ) -> Result<Progress, ErrorRecord> {
        if self.reading {
            return Ok(match outcome.into_context()? {
                Some(context) => Self::start(cx, context, self.attempts),
                None => Progress::Done {
                    context: self.context,
                    due: Vec::new(),
                },
            });
        }
        if let Some(room) = outcome.into_room_updated()? {
            self.context.room = *room;
            return Ok(Progress::Done {
                context: self.context,
                due: self.due,
            });
        }
        self.reading = true;
        let effect = cx.io.issue(StoreRequest::LoadContext {
            room_id: self.context.room.id.clone(),
            recent_events: cx.config.recent_events(),
        });
        Ok(Progress::Waiting {
            effect,
            write: self,
        })
    }
}

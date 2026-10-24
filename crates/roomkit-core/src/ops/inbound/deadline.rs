//! The call's deadline (RFC §13.6, §10.4, §9.5.1): set from the clock at
//! the call's start, carried by the requests that precede its decision, and
//! what the call does once it passed.
//!
//! `process_timeout` bounds a call from its start to its decision: its step
//! 2 (the identity read, the router, routing's wait for the lock), the
//! event's parse, its off-lock check and the wait for its admission ticket,
//! the wait for the room lock and the locked section up to the commit. The
//! room's delivery lane comes after the commit, and nothing of it is
//! bounded. The driver gives up on a request past the deadline and answers
//! `DeadlinePassed`; the call then releases what it holds (the room lock,
//! its ticket), emits `process_timeout`, and returns
//! `InboundResult(blocked, process_timeout)`. Nothing of the event is
//! written: the injected events, tasks and observations of the hooks that
//! ran before are dropped with it (gap G-068).
//!
//! No write carries the deadline, a commit least of all: giving up on one
//! midway could leave half of it done. The commit and everything after it
//! are never bounded (RFC §13.6), nor are the creation of a room by step 2
//! and what completes it, its `ON_ROOM_CREATED` hooks (each bounded by its
//! own timeout) and its attach, so a room step 2 creates is attached
//! whatever comes next.

use super::{InboundError, InboundResult, Machine};
use crate::model::{EventId, Timestamp};
use crate::ops::{Cx, FrameworkEvent, KitConfig, Notice};

/// The reason a call past its deadline is refused with (RFC §13.6).
pub(super) const PROCESS_TIMEOUT: &str = "process_timeout";

/// Why a sub-machine before the commit stopped short of handing its work on.
#[derive(Debug)]
pub(super) enum Stopped {
    /// It failed, with nothing written.
    Failed(InboundError),
    /// The call's deadline passed (RFC §13.6).
    Expired,
    /// A message routed into the room whose delivery made the call, past
    /// the reentry budget of the cascade that delivers: refused with no
    /// event (gap G-070).
    PastBudget,
}

/// The deadline of a call starting at `now`: `now` plus the kit's
/// `process_timeout`, `None` when the kit sets none. A timeout that reaches
/// past the clock's range bounds nothing.
pub(super) fn of_call(config: &KitConfig, now: Timestamp) -> Option<Timestamp> {
    config
        .process_timeout
        .and_then(|timeout| now.checked_add(timeout))
}

impl Machine {
    /// The call's deadline passed before its decision (RFC §13.6), or its
    /// caller stopped waiting then (gap G-076): it reports it, then ends
    /// `blocked` with the reason `process_timeout` and no event, once its
    /// ticket is released, and the events it committed, an adopted entry's,
    /// are delivered. The pass that held the lock released it already.
    pub(super) fn expire(&mut self, cx: &mut Cx<'_>, locked_event: Option<EventId>) {
        self.report_expiry(cx, locked_event);
        // The result as the call left it, an error met already included.
        let refused = InboundResult {
            event: None,
            blocked: true,
            reason: Some(PROCESS_TIMEOUT.to_owned()),
            ..std::mem::take(&mut self.trigger.result)
        };
        self.end(cx, Ok(refused));
    }

    /// Reports the call's expiry: `process_timeout`, naming the message's
    /// channel before the room lock and `locked_event`, the event whose
    /// pass held the lock, under it (RFC §8.2); a caller that stopped
    /// waiting is logged only, no deadline having passed.
    fn report_expiry(&self, cx: &mut Cx<'_>, locked_event: Option<EventId>) {
        if cx.io.is_expired() {
            tracing::info!(
                target: "roomkit::core::inbound",
                room_id = ?self.call.room_id,
                event_id = ?locked_event,
                "the call's caller stopped waiting before its decision; nothing of its event is written"
            );
            return;
        }
        let timeout = cx
            .config
            .process_timeout
            .map_or(0.0, |timeout| timeout.as_secs_f64());
        let channel_id = match locked_event {
            Some(_) => None,
            None => self.call.channel_id.clone(),
        };
        tracing::info!(
            target: "roomkit::core::inbound",
            room_id = ?self.call.room_id,
            channel_id = ?channel_id,
            event_id = ?locked_event,
            timeout,
            "the call passed its process_timeout before its decision; nothing of its event is written"
        );
        cx.io
            .notify(Notice::FrameworkEvent(FrameworkEvent::ProcessTimeout {
                room_id: self.call.room_id.clone(),
                channel_id,
                event_id: locked_event,
                timeout,
            }));
    }
}

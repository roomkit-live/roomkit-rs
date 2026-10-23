//! A direct injection's way to the room lock (RFC §10.5, gap G-069).
//!
//! The event the integrator injects takes the inbound pipeline without the
//! channel's parse: it is refused before anything is read when it cannot be
//! one (an unregistered channel, the visibility `internal`, an instruction
//! that names no agent or carries a key), then stamped, its source the
//! channel it names. It runs none of step 2: a room that does not exist is
//! not created, and a channel the room does not bind is not attached. The
//! call reads the room before the lock only when an off-lock hook fires for
//! the event, whose check reads it (RFC §9.5.1); otherwise its pass starts
//! at once. Every request up to its decision carries the call's deadline
//! (RFC §13.6).
//!
//! A room of another organization than the one the caller acts for, and a
//! room that binds no channel the event names, refuse it with nothing
//! written (gap G-069): the host named the room and the channel itself, so
//! the call is its error, with nothing to audit. Both are checked under the
//! room lock, before the pass's hooks or the record of an injection past its
//! caller's reentry budget, and on the room an off-lock check reads, before
//! its hooks too.

use super::arrival::Arrived;
use super::deadline::Stopped;
use super::instructions::{self, Admission};
use super::{InboundError, InboundResult, Machine, Stage, stamp};
use crate::model::{DirectEvent, RoomContext, RoomEvent, RoomId, Timestamp};
use crate::ops::protocol::{EffectId, Outcome, Request};
use crate::ops::{Cx, Progress};
use crate::rules::organizations;
use crate::rules::permissions::WriteRefusal;

/// What a direct injection is checked for on the room, before its hooks
/// (RFC §10.5, §17.2, gap G-069).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct DirectCall {
    /// The organization its caller acts for; `None` reads the room
    /// unscoped.
    pub(super) organization_id: Option<String>,
}

impl DirectCall {
    /// Why the room as `context` holds it refuses `event`, if it does: it
    /// is another organization's than the caller's, or binds no channel the
    /// event names. Nothing is written for either.
    pub(super) fn refusal(&self, event: &RoomEvent, context: &RoomContext) -> Option<InboundError> {
        if !organizations::admits(self.organization_id.as_deref(), &context.room) {
            return Some(InboundError::OtherOrganization(context.room.id.clone()));
        }
        let bound = context
            .bindings
            .iter()
            .any(|binding| binding.channel_id == event.source.channel_id);
        (!bound).then(|| InboundError::Refused {
            reason: WriteRefusal::Unbound.blocked_by().to_owned(),
        })
    }
}

/// The read of a directly injected event's room for its off-lock check
/// (RFC §9.5.1).
#[derive(Debug)]
pub(super) struct DirectRead {
    event: RoomEvent,
    /// What the room read is checked for.
    call: DirectCall,
    /// The read, outstanding.
    effect: Option<EffectId>,
}

impl DirectRead {
    /// The read of the room of `event`, stamped already, for `call`.
    pub(super) fn new(event: RoomEvent, call: DirectCall) -> Self {
        Self {
            event,
            call,
            effect: None,
        }
    }

    /// Reads the room, within the call's `deadline`.
    pub(super) fn start(&mut self, cx: &mut Cx<'_>, deadline: Option<Timestamp>) {
        let request = Request::LoadContext {
            room_id: self.event.room_id.clone(),
            recent_events: cx.config.context_window,
        };
        self.effect = Some(cx.io.issue_within(request, deadline));
    }

    /// Takes the driver's answer to the read: the room as the off-lock check
    /// reads it, or why the call stops, nothing written. A missing room
    /// fails the call, and so does one that refuses the injection, before
    /// the check's hooks run (gap G-069).
    pub(super) fn accept(
        &mut self,
        effect: EffectId,
        outcome: Outcome,
    ) -> Progress<Result<Arrived, Stopped>> {
        if self.effect != Some(effect) {
            debug_assert!(false, "a direct injection received {effect:?}");
            return Progress::Waiting;
        }
        self.effect = None;
        if outcome == Outcome::DeadlinePassed {
            return Progress::Done(Err(Stopped::Expired));
        }
        let failed = |error| Progress::Done(Err(Stopped::Failed(error)));
        match outcome.into_context() {
            Ok(Some(context)) => match self.call.refusal(&self.event, &context) {
                Some(refusal) => failed(refusal),
                None => Progress::Done(Ok(Arrived {
                    event: self.event.clone(),
                    context,
                })),
            },
            Ok(None) => failed(InboundError::RoomNotFound(self.event.room_id.clone())),
            Err(error) => failed(InboundError::Store(error)),
        }
    }
}

/// A directly injected event once admitted: stamped, or refused as an
/// instruction that cannot be one (RFC §10.1.1 rules 1 and 2).
#[derive(Debug)]
pub(super) enum Admitted {
    Event(Box<RoomEvent>),
    Refused(Box<InboundResult>),
}

/// Refuses `event` before anything is read when it cannot be one, or
/// stamps it for `room_id` (RFC §10.5): its channel must be registered,
/// and it is admitted as a message is (RFC §10.1.1, gap G-041).
///
/// # Errors
///
/// Returns why the event is refused before anything is read.
pub(super) fn admit(
    cx: &mut Cx<'_>,
    room_id: RoomId,
    event: DirectEvent,
) -> Result<Admitted, InboundError> {
    let channel = cx
        .config
        .channel(&event.channel_id)
        .cloned()
        .ok_or_else(|| InboundError::UnknownChannel(event.channel_id.clone()))?;
    if let Admission::Refused(refusal) = instructions::admit(cx.config, (&event).into())? {
        return Ok(Admitted::Refused(Box::new(instructions::refused(refusal))));
    }
    let id = cx.env.new_event_id();
    let now = cx.env.now();
    let event = stamp::stamp_direct(event, &channel, room_id, id, now);
    Ok(Admitted::Event(Box::new(event)))
}

impl Machine {
    /// Starts a direct injection's way (RFC §10.5), for a caller acting for
    /// `organization_id` (RFC §17.2): refused before anything is read when
    /// it cannot be one, its room is read for its off-lock check when an
    /// off-lock hook fires for it, and its pass starts otherwise.
    pub(super) fn begin_direct(
        &mut self,
        cx: &mut Cx<'_>,
        room_id: RoomId,
        organization_id: Option<String>,
        event: DirectEvent,
    ) {
        self.channel_id = Some(event.channel_id.clone());
        self.room_id = Some(room_id.clone());
        let call = DirectCall { organization_id };
        self.direct = Some(call.clone());
        let event = match admit(cx, room_id, event) {
            Ok(Admitted::Event(event)) => *event,
            Ok(Admitted::Refused(refused)) => return self.end(cx, Ok(*refused)),
            Err(error) => return self.end(cx, Err(error)),
        };
        if cx.config.hooks.off_lock_checks(&event.source).is_empty() {
            return self.start_trigger(event, None);
        }
        let mut read = Box::new(DirectRead::new(event, call));
        read.start(cx, self.deadline);
        self.stage = Stage::Reading(read);
    }

    /// Takes the answer of the room's read; its off-lock check starts.
    pub(super) fn on_read(
        &mut self,
        cx: &mut Cx<'_>,
        mut read: Box<DirectRead>,
        effect: EffectId,
        outcome: Outcome,
    ) {
        match read.accept(effect, outcome) {
            Progress::Waiting => self.stage = Stage::Reading(read),
            Progress::Done(Ok(arrived)) => self.after_arrival(cx, arrived),
            Progress::Done(Err(stopped)) => self.stop(cx, stopped),
        }
    }
}

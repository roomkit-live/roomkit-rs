//! A direct injection's way to the room lock (RFC §10.5, gap G-069).
//!
//! The event the integrator injects takes the inbound pipeline without the
//! channel's parse: it is refused before anything is read when it cannot be
//! one (an unregistered channel, the visibility `internal`, an instruction
//! that names no agent or carries a key), then stamped, its source the
//! channel it names. It runs none of step 2: a room that does not exist is
//! not created, and a channel the room does not bind is not attached, so
//! its event is refused `source_unbound` at step 11. The call enters the
//! room's lane, then reads the room before the lock only when an off-lock
//! hook fires for the event, whose check reads it (RFC §9.5.1); otherwise
//! its pass starts at once. Every request up to its decision carries the
//! call's deadline (RFC §13.6).

use std::mem;

use super::arrival::Arrived;
use super::deadline::Stopped;
use super::instructions::{self, Admission};
use super::{InboundError, InboundResult, Machine, Stage, stamp};
use crate::model::{DirectEvent, RoomContext, RoomEvent, RoomId, Timestamp};
use crate::ops::protocol::{EffectId, Outcome, Request};
use crate::ops::{Cx, Progress};

/// A directly injected event's entry into its room's lane.
#[derive(Debug)]
pub(super) struct DirectArrival {
    event: RoomEvent,
    /// The call's deadline (RFC §13.6).
    deadline: Option<Timestamp>,
    /// Whether an off-lock hook fires for the event, whose check reads the
    /// room before the lock.
    checked: bool,
    /// Whether the call holds the room's lane.
    in_lane: bool,
    state: State,
}

#[derive(Debug)]
enum State {
    Start,
    Entering(EffectId),
    /// The room is read for the off-lock check.
    Reading(EffectId),
    Done,
}

/// The event in its room's lane, with the room as read before the lock when
/// its off-lock check needs it.
#[derive(Debug)]
pub(super) struct Entered {
    pub(super) event: RoomEvent,
    pub(super) context: Option<Box<RoomContext>>,
}

impl DirectArrival {
    /// The way of `event`, stamped already, within the call's `deadline`;
    /// `checked` when an off-lock hook fires for it.
    pub(super) fn new(event: RoomEvent, deadline: Option<Timestamp>, checked: bool) -> Self {
        Self {
            event,
            deadline,
            checked,
            in_lane: false,
            state: State::Start,
        }
    }

    /// The room whose lane the call holds, which its caller leaves.
    pub(super) fn lane(&self) -> Option<&RoomId> {
        self.in_lane.then_some(&self.event.room_id)
    }

    /// Enters the room's lane.
    pub(super) fn start(&mut self, cx: &mut Cx<'_>) {
        if matches!(self.state, State::Start) {
            let room_id = self.event.room_id.clone();
            let request = Request::EnterLane { room_id };
            self.state = State::Entering(cx.io.issue_within(request, self.deadline));
        }
    }

    /// Takes the driver's answer to the outstanding effect; done with the
    /// event in its lane, or why the call stops, nothing written.
    pub(super) fn accept(
        &mut self,
        cx: &mut Cx<'_>,
        effect: EffectId,
        outcome: Outcome,
    ) -> Progress<Result<Entered, Stopped>> {
        if outcome == Outcome::DeadlinePassed {
            self.state = State::Done;
            return Progress::Done(Err(Stopped::Expired));
        }
        match mem::replace(&mut self.state, State::Done) {
            State::Entering(expected) if expected == effect => self.on_entered(cx, outcome),
            State::Reading(expected) if expected == effect => self.on_read(outcome),
            state @ (State::Start | State::Entering(_) | State::Reading(_) | State::Done) => {
                debug_assert!(false, "a direct injection received {effect:?} in {state:?}");
                self.state = state;
                Progress::Waiting
            }
        }
    }

    /// The lane is held: the room is read for the off-lock check, or the
    /// pass starts at once.
    fn on_entered(
        &mut self,
        cx: &mut Cx<'_>,
        outcome: Outcome,
    ) -> Progress<Result<Entered, Stopped>> {
        if let Err(error) = outcome.into_entered() {
            return Progress::Done(Err(Stopped::Failed(InboundError::Lane(error))));
        }
        self.in_lane = true;
        if !self.checked {
            return Progress::Done(Ok(Entered {
                event: self.event.clone(),
                context: None,
            }));
        }
        let request = Request::LoadContext {
            room_id: self.event.room_id.clone(),
            recent_events: cx.config.context_window,
        };
        self.state = State::Reading(cx.io.issue_within(request, self.deadline));
        Progress::Waiting
    }

    /// The room as the off-lock check reads it; a missing room fails the
    /// call, nothing written.
    fn on_read(&self, outcome: Outcome) -> Progress<Result<Entered, Stopped>> {
        let failed = |error| Progress::Done(Err(Stopped::Failed(error)));
        match outcome.into_context() {
            Ok(Some(context)) => Progress::Done(Ok(Entered {
                event: self.event.clone(),
                context: Some(context),
            })),
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
    /// Starts a direct injection's way (RFC §10.5): refused before anything
    /// is read when it cannot be one, it enters the room's lane.
    pub(super) fn begin_direct(&mut self, cx: &mut Cx<'_>, room_id: RoomId, event: DirectEvent) {
        self.channel_id = Some(event.channel_id.clone());
        self.room_id = Some(room_id.clone());
        let event = match admit(cx, room_id, event) {
            Ok(Admitted::Event(event)) => *event,
            Ok(Admitted::Refused(refused)) => return self.end(cx, Ok(*refused)),
            Err(error) => return self.end(cx, Err(error)),
        };
        let checked = !cx.config.hooks.off_lock_checks(&event.source).is_empty();
        let mut entering = Box::new(DirectArrival::new(event, self.deadline, checked));
        entering.start(cx);
        self.stage = Stage::Entering(entering);
    }

    /// Takes an answer of the direct injection's way; once in the lane, its
    /// off-lock check or its pass starts.
    pub(super) fn on_entering(
        &mut self,
        cx: &mut Cx<'_>,
        mut entering: Box<DirectArrival>,
        effect: EffectId,
        outcome: Outcome,
    ) {
        let Progress::Done(entered) = entering.accept(cx, effect, outcome) else {
            self.stage = Stage::Entering(entering);
            return;
        };
        self.holds_lane = entering.lane().is_some();
        match entered {
            Ok(Entered {
                event,
                context: Some(context),
            }) => self.after_arrival(cx, Arrived { event, context }),
            Ok(Entered {
                event,
                context: None,
            }) => self.start_trigger(event, None),
            Err(stopped) => self.stop(cx, stopped),
        }
    }
}

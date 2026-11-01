//! What a direct injection from the room's work reads before it draws on
//! the budget of that work (RFC §9.5.1 Past the cap, §13.4, §17.2).
//!
//! A call that draws on the reentry budget of the room's work, an entry the
//! operation adopted or a new arrival from a delivery, reads its room as it
//! starts, before it draws a pass: a room the caller does not reach, or
//! that binds no channel the event names, refuses the call as its pass
//! would (RFC §17.2, gap G-069), and a refused call draws nothing, key or no
//! key, as Python reads the scoped binding first. Then the call's key, when
//! it carries one: a key the channel holds in the room makes it a replay,
//! which returns the event the key committed and draws nothing, even past
//! the budget, and is never handed to a caller its pass would refuse. Only
//! a direct injection reads its key first: a message past the budget stores
//! nothing, while an injection past it would store a record whose key
//! collides with the event it replays. The reads take no lock: the pass
//! reads the room and the key again under the lock (step 7), and so does
//! the record stored past the budget.

use super::InboundError;
use super::deadline::Stopped;
use super::direct::DirectCall;
use crate::model::{RoomContext, RoomEvent};
use crate::ops::io::Bound;
use crate::ops::protocol::{EffectId, KeyHeld, Outcome, StoreRequest};
use crate::ops::{Cx, Progress};

/// The reads of a direct injection's room, then of its key, before it
/// draws on the budget of the room's work.
#[derive(Debug)]
pub(super) struct WorkRead {
    event: RoomEvent,
    call: DirectCall,
    bound: Bound,
    reading: Reading,
}

/// The read outstanding.
#[derive(Debug)]
enum Reading {
    Room(EffectId),
    /// The key, the room read already.
    Key {
        effect: EffectId,
        context: Box<RoomContext>,
    },
}

/// What the reads found.
#[derive(Debug)]
pub(super) enum Read {
    /// The event the call's key committed: the call is a replay.
    Replay(Box<RoomEvent>),
    /// The room took the call, which carries no key its channel holds: it
    /// draws on the budget, and the room's context, as read, serves its
    /// off-lock check.
    Fresh(Box<RoomContext>),
}

impl WorkRead {
    /// Reads the room of `event`, stamped already, for `call`, within
    /// `bound`.
    pub(super) fn start(cx: &mut Cx<'_>, event: RoomEvent, call: DirectCall, bound: Bound) -> Self {
        let request = StoreRequest::LoadContext {
            room_id: event.room_id.clone(),
            recent_events: cx.config.recent_events(),
        };
        let reading = Reading::Room(cx.io.issue_within(request, bound));
        Self {
            event,
            call,
            bound,
            reading,
        }
    }

    /// Whether the read waits for `effect`.
    pub(super) fn awaits(&self, effect: EffectId) -> bool {
        match &self.reading {
            Reading::Room(waited) | Reading::Key { effect: waited, .. } => *waited == effect,
        }
    }

    /// Takes the driver's answer to the read outstanding: the room, whose
    /// refusal stops the call and whose admission reads the key, if any;
    /// then the event the key committed. A stop writes nothing.
    pub(super) fn accept(
        &mut self,
        cx: &mut Cx<'_>,
        effect: EffectId,
        outcome: Outcome,
    ) -> Progress<Result<Read, Stopped>> {
        if !self.awaits(effect) {
            debug_assert!(false, "a read before the budget received {effect:?}");
            return Progress::Waiting;
        }
        if outcome == Outcome::DeadlinePassed {
            return Progress::Done(Err(Stopped::Expired));
        }
        let reading = std::mem::replace(&mut self.reading, Reading::Room(effect));
        match reading {
            Reading::Room(_) => self.on_room(cx, outcome),
            Reading::Key { context, .. } => Progress::Done(self.on_key(outcome, context)),
        }
    }

    /// The event and what it is checked for, once it is no replay.
    pub(super) fn into_parts(self) -> (RoomEvent, DirectCall) {
        (self.event, self.call)
    }

    /// The room is read: a room the caller does not reach, or that binds no
    /// channel the event names, refuses it as its pass would; else its key
    /// is read, when it carries one.
    fn on_room(&mut self, cx: &mut Cx<'_>, outcome: Outcome) -> Progress<Result<Read, Stopped>> {
        let failed = |error| Progress::Done(Err(Stopped::Failed(error)));
        let context = match outcome.into_context() {
            Ok(Some(context)) => context,
            Ok(None) => return failed(InboundError::RoomNotFound(self.event.room_id.clone())),
            Err(error) => return failed(InboundError::Store(error)),
        };
        if let Some(refusal) = self.call.refusal(&self.event, &context) {
            return failed(refusal);
        }
        let Some(key) = self.event.idempotency_key.clone() else {
            return Progress::Done(Ok(Read::Fresh(context)));
        };
        let request = StoreRequest::GetEventByIdempotencyKey {
            room_id: self.event.room_id.clone(),
            channel_id: Some(self.event.source.channel_id.clone()),
            key,
        };
        let effect = cx.io.issue_within(request, self.bound);
        self.reading = Reading::Key { effect, context };
        Progress::Waiting
    }

    /// The key is read: the event it committed, a replay, or none.
    fn on_key(&self, outcome: Outcome, context: Box<RoomContext>) -> Result<Read, Stopped> {
        let found = outcome
            .into_idempotent()
            .map_err(|error| Stopped::Failed(InboundError::Store(error)))?;
        let original = match found {
            KeyHeld::Free => return Ok(Read::Fresh(context)),
            KeyHeld::Event(original) => original,
            // A key held whose event the store cannot read back: the event
            // itself, as Python's `_replay_of_key` returns it.
            KeyHeld::Seen => Box::new(self.event.clone()),
        };
        tracing::info!(
            target: "roomkit::core::inbound",
            room_id = %self.event.room_id,
            channel_id = %self.event.source.channel_id,
            "a direct injection from the room's work replays its key; no pass drawn"
        );
        Ok(Read::Replay(original))
    }
}

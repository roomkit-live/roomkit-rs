//! A direct injection from the room's work whose key its channel holds is
//! a replay (RFC §9.5.1 Past the cap, §13.4).
//!
//! A call that draws on the reentry budget of the room's work, an entry the
//! operation adopted or a new arrival from a delivery, reads its key as it
//! starts, before it draws a pass: a key the channel holds in the room
//! makes it a replay, which returns the event the key committed and draws
//! nothing, even past the budget. Only a direct injection reads its key
//! first: a message past the budget stores nothing, while an injection past
//! it would store a record whose key collides with the event it replays.
//! The room is read first, and a room the caller does not reach, or that
//! binds no channel the event names, refuses the call as its pass would,
//! before its key is read (RFC §17.2, gap G-069): a replay is never handed
//! to a caller its pass would refuse. The reads take no lock: the pass
//! reads the key again under the lock (step 7), and so does the record
//! stored past the budget.

use super::InboundError;
use super::deadline::Stopped;
use super::direct::DirectCall;
use crate::model::RoomEvent;
use crate::ops::io::Bound;
use crate::ops::protocol::{EffectId, Outcome, StoreRequest};
use crate::ops::{Cx, Progress};

/// The reads of a direct injection's room, then of its key, before its
/// budget.
#[derive(Debug)]
pub(super) struct KeyRead {
    event: RoomEvent,
    key: String,
    call: DirectCall,
    bound: Bound,
    reading: Reading,
}

/// The read outstanding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reading {
    Room(EffectId),
    Key(EffectId),
}

impl KeyRead {
    /// Reads the room of `event`, stamped already, then `key`, the key it
    /// carries, each within `bound`.
    pub(super) fn start(
        cx: &mut Cx<'_>,
        event: RoomEvent,
        key: String,
        call: DirectCall,
        bound: Bound,
    ) -> Self {
        let request = StoreRequest::LoadContext {
            room_id: event.room_id.clone(),
            recent_events: 0,
        };
        let reading = Reading::Room(cx.io.issue_within(request, bound));
        Self {
            event,
            key,
            call,
            bound,
            reading,
        }
    }

    /// Whether the read waits for `effect`.
    pub(super) fn awaits(&self, effect: EffectId) -> bool {
        match self.reading {
            Reading::Room(waited) | Reading::Key(waited) => waited == effect,
        }
    }

    /// Takes the driver's answer to the read outstanding: the room, whose
    /// refusal stops the call and whose admission reads the key; then the
    /// event the key committed, a replay, `None` for a key no event of the
    /// channel holds. A stop writes nothing.
    pub(super) fn accept(
        &mut self,
        cx: &mut Cx<'_>,
        effect: EffectId,
        outcome: Outcome,
    ) -> Progress<Result<Option<Box<RoomEvent>>, Stopped>> {
        if !self.awaits(effect) {
            debug_assert!(false, "a key read received {effect:?}");
            return Progress::Waiting;
        }
        if outcome == Outcome::DeadlinePassed {
            return Progress::Done(Err(Stopped::Expired));
        }
        match self.reading {
            Reading::Room(_) => self.on_room(cx, outcome),
            Reading::Key(_) => Progress::Done(self.on_key(outcome)),
        }
    }

    /// The room is read: a room the caller does not reach, or that binds no
    /// channel the event names, refuses it as its pass would; else the key
    /// is read.
    fn on_room(
        &mut self,
        cx: &mut Cx<'_>,
        outcome: Outcome,
    ) -> Progress<Result<Option<Box<RoomEvent>>, Stopped>> {
        let failed = |error| Progress::Done(Err(Stopped::Failed(error)));
        let context = match outcome.into_context() {
            Ok(Some(context)) => context,
            Ok(None) => return failed(InboundError::RoomNotFound(self.event.room_id.clone())),
            Err(error) => return failed(InboundError::Store(error)),
        };
        if let Some(refusal) = self.call.refusal(&self.event, &context) {
            return failed(refusal);
        }
        let request = StoreRequest::GetEventByIdempotencyKey {
            room_id: self.event.room_id.clone(),
            channel_id: Some(self.event.source.channel_id.clone()),
            key: self.key.clone(),
        };
        self.reading = Reading::Key(cx.io.issue_within(request, self.bound));
        Progress::Waiting
    }

    /// The key is read: the event it committed, a replay, or none.
    fn on_key(&self, outcome: Outcome) -> Result<Option<Box<RoomEvent>>, Stopped> {
        let found = outcome
            .into_idempotent()
            .map_err(|error| Stopped::Failed(InboundError::Store(error)))?;
        if found.is_some() {
            tracing::info!(
                target: "roomkit::core::inbound",
                room_id = %self.event.room_id,
                channel_id = %self.event.source.channel_id,
                "a direct injection from the room's work replays its key; no pass drawn"
            );
        }
        Ok(found)
    }

    /// The event and what it is checked for, once its key is no replay.
    pub(super) fn into_parts(self) -> (RoomEvent, DirectCall) {
        (self.event, self.call)
    }
}

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
//! The read takes no lock: the pass reads the key again under the lock
//! (step 7), and so does the record stored past the budget.

use super::InboundError;
use super::deadline::Stopped;
use super::direct::DirectCall;
use crate::model::RoomEvent;
use crate::ops::io::Bound;
use crate::ops::protocol::{EffectId, Outcome, StoreRequest};
use crate::ops::{Cx, Progress};

/// The read of a direct injection's key before its budget.
#[derive(Debug)]
pub(super) struct KeyRead {
    event: RoomEvent,
    call: DirectCall,
    effect: EffectId,
}

impl KeyRead {
    /// Reads `key`, the key `event`, stamped already, carries, within
    /// `bound`.
    pub(super) fn start(
        cx: &mut Cx<'_>,
        event: RoomEvent,
        key: String,
        call: DirectCall,
        bound: Bound,
    ) -> Self {
        let request = StoreRequest::GetEventByIdempotencyKey {
            room_id: event.room_id.clone(),
            channel_id: Some(event.source.channel_id.clone()),
            key,
        };
        let effect = cx.io.issue_within(request, bound);
        Self {
            event,
            call,
            effect,
        }
    }

    /// Whether the read waits for `effect`.
    pub(super) fn awaits(&self, effect: EffectId) -> bool {
        self.effect == effect
    }

    /// Takes the driver's answer to the read: the event the key committed,
    /// a replay, `None` for a key no event of the channel holds, or why
    /// the call stops, nothing written.
    pub(super) fn accept(
        &self,
        effect: EffectId,
        outcome: Outcome,
    ) -> Progress<Result<Option<Box<RoomEvent>>, Stopped>> {
        if effect != self.effect {
            debug_assert!(false, "a key read received {effect:?}");
            return Progress::Waiting;
        }
        if outcome == Outcome::DeadlinePassed {
            return Progress::Done(Err(Stopped::Expired));
        }
        match outcome.into_idempotent() {
            Ok(found) => {
                if found.is_some() {
                    tracing::info!(
                        target: "roomkit::core::inbound",
                        room_id = %self.event.room_id,
                        channel_id = %self.event.source.channel_id,
                        "a direct injection from the room's work replays its key; no pass drawn"
                    );
                }
                Progress::Done(Ok(found))
            }
            Err(error) => Progress::Done(Err(Stopped::Failed(InboundError::Store(error)))),
        }
    }

    /// The event and what it is checked for, once its key is no replay.
    pub(super) fn into_parts(self) -> (RoomEvent, DirectCall) {
        (self.event, self.call)
    }
}

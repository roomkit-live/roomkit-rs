//! Instructions in the inbound pipeline (RFC §10.1.1): what refuses one
//! before anything is read, and what the cascade drops because only the
//! application gives an instruction.
//!
//! An instruction goes through the pipeline as a message does. Its pass
//! applies the room's status gate, runs its hooks and checks its source
//! under the room lock, and its delivery set joins the room's lane as any
//! other, behind the room's latest committed event, since the operation
//! holds the lane for the whole call (RFC §10.1.1 rule 5). The rest is
//! written by `rules::instructions` where the pipeline writes the matching
//! fields of a message:
//!
//! - its stamp gives it the visibility `intelligence` and the caller's
//!   metadata (`stamp`);
//! - a hook's modify keeps its visibility, its address and its
//!   `standalone` word (`stamp::keep_identity`);
//! - its commit writes nothing: blocked, it emits `event_blocked`; allowed,
//!   it is planned and the lock released, no index consumed and no counter
//!   moved (`commit`);
//! - a standalone instruction's turn is handed no history (`delivery`);
//! - every response it produces records its fingerprint (`stamp`).

use super::{InboundError, InboundResult};
use crate::model::{
    ChannelCategory, ChannelId, EventType, InboundMessage, InjectedEvent, RoomEvent,
};
use crate::ops::KitConfig;
use crate::rules::instructions::{self, InstructionRefusal};

/// What the admission of a message decided, before anything is read.
#[derive(Debug)]
pub(super) enum Admission {
    /// The message goes on.
    Admitted,
    /// An instruction refused: the call ends, nothing read or written (RFC
    /// §10.1.1 rules 1 and 2).
    Refused(InstructionRefusal),
}

/// Admits `message`, or refuses it, before anything is read (RFC §10.1.1
/// rules 1, 2 and 7): an instruction must name a registered intelligence
/// channel and carry no idempotency key (gap G-065).
///
/// # Errors
///
/// Returns [`InboundError::StandaloneOutsideInstruction`] for `standalone`
/// on any other event type: it is raised, since the event would otherwise
/// land as a participant's line without the isolation it asked for.
pub(super) fn admit(
    config: &KitConfig,
    message: &InboundMessage,
) -> Result<Admission, InboundError> {
    if message.standalone && !instructions::may_stand_alone(message.event_type) {
        return Err(InboundError::StandaloneOutsideInstruction);
    }
    if message.event_type != EventType::Instruction {
        return Ok(Admission::Admitted);
    }
    let is_intelligence = |channel_id: &ChannelId| {
        config
            .channel(channel_id)
            .is_some_and(|channel| channel.category == ChannelCategory::Intelligence)
    };
    let refusal = instructions::refusal(
        message.addressed_to.as_deref(),
        message.idempotency_key.as_deref(),
        is_intelligence,
    );
    Ok(refusal.map_or(Admission::Admitted, Admission::Refused))
}

/// The result of an instruction refused before anything was read: blocked,
/// with the refusal's reason and no event (RFC §10.1.1 rules 1 and 2).
pub(super) fn refused(refusal: InstructionRefusal) -> InboundResult {
    InboundResult {
        blocked: true,
        reason: Some(refusal.reason().to_owned()),
        ..InboundResult::default()
    }
}

/// The events a hook injected while deciding on `trigger`, without the
/// instructions among them (gap G-042): an injected event is committed and
/// asks no agent (RFC §9.4), an instruction is never committed and asks
/// the agent it names (RFC §10.1.1). Each one dropped is logged; the rest
/// of the hook's result stands.
pub(super) fn without_instructions(
    injected: Vec<InjectedEvent>,
    trigger: &RoomEvent,
) -> Vec<InjectedEvent> {
    injected
        .into_iter()
        .filter(|injected| {
            let instruction = injected.event.event_type == EventType::Instruction;
            if instruction {
                tracing::warn!(
                    target: "roomkit::core::inbound",
                    room_id = %trigger.room_id,
                    event_id = %trigger.id,
                    "a hook injected an instruction; it is dropped, and the rest of its result \
                     stands (gap G-042)"
                );
            }
            !instruction
        })
        .collect()
}

/// Whether `response`, which `responder` returned to `trigger`, re-enters
/// the room: an instruction does not (gap G-066). An instruction is the
/// application's direction (RFC §10.1.1); a channel's output is its own
/// words, so an agent never directs another agent this way, and an
/// instruction never lands in the timeline as a channel's response.
pub(super) fn reenters(response: &RoomEvent, responder: &ChannelId, trigger: &RoomEvent) -> bool {
    if response.event_type != EventType::Instruction {
        return true;
    }
    tracing::warn!(
        target: "roomkit::core::inbound",
        room_id = %trigger.room_id,
        event_id = %trigger.id,
        responder = %responder,
        "a channel answered with an instruction; it is dropped (gap G-066)"
    );
    false
}

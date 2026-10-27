//! The suite's names of the errors roomkit-rs's calls fail with: each by
//! its meaning in the RFC (roomkit-specs `conformance/README.md`, "Errors"),
//! after the table of `docs/rfc-gaps.md` G-085.

use roomkit::{InboundError, KitError, LifecycleError, MutationError};
use serde_json::{Value, json};

pub(crate) fn named(name: &str) -> Value {
    json!({ "name": name })
}

/// The suite's name of `error`; `None` for an error it has no name for, a
/// defect to look at.
#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "an error the suite has no name for is reported, whichever variant it is"
)]
pub(crate) fn error_of(error: &KitError) -> Option<Value> {
    match error {
        KitError::Inbound(error) => inbound_error(error),
        KitError::Lifecycle(error) => lifecycle_error(error),
        KitError::Mutation(error) => mutation_error(error),
        KitError::Config(_) | KitError::Registration(_) => Some(named("invalid_argument")),
        KitError::UnknownChannel(_) => Some(named("channel_not_registered")),
        KitError::Reentrant { .. } => Some(named("reentrant")),
        _ => None,
    }
}

#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "an error the suite has no name for is reported, whichever variant it is"
)]
fn inbound_error(error: &InboundError) -> Option<Value> {
    Some(match error {
        InboundError::UnknownChannel(_) => named("channel_not_registered"),
        InboundError::RoomNotFound(_) => named("room_not_found"),
        InboundError::InboundOnHostChannel(_)
        | InboundError::NotAHostChannel(_)
        | InboundError::HostAsIntelligence(_)
        | InboundError::StandaloneOutsideInstruction
        | InboundError::InternalVisibility => named("invalid_argument"),
        InboundError::ParticipantNotFound { .. } => named("participant_not_found"),
        InboundError::Refused { reason } => match reason.as_str() {
            "room_closed" => named("room_closed"),
            "source_unbound" => named("channel_not_bound"),
            "process_timeout" => named("process_timeout"),
            "reentry_loop_cap" => named("reentry_loop_cap"),
            "instruction_unaddressed" | "instruction_not_idempotent" => named("invalid_argument"),
            "target_event_not_found"
            | "identity_required_for_edit"
            | "not_authorized"
            | "not_original_author" => json!({ "name": "change_refused", "reason": reason }),
            _ => return None,
        },
        _ => return None,
    })
}

#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "an error the suite has no name for is reported, whichever variant it is"
)]
fn lifecycle_error(error: &LifecycleError) -> Option<Value> {
    Some(match error {
        LifecycleError::RoomNotFound(_) => named("room_not_found"),
        LifecycleError::ChannelNotBound { .. } => named("channel_not_bound"),
        LifecycleError::InternalVisibility
        | LifecycleError::HostAsIntelligence(_)
        | LifecycleError::ReservedMetadataKey(_) => named("invalid_argument"),
        LifecycleError::TransitionRefused { .. } => named("transition_refused"),
        _ => return None,
    })
}

#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "an error the suite has no name for is reported, whichever variant it is"
)]
fn mutation_error(error: &MutationError) -> Option<Value> {
    Some(match error {
        MutationError::RoomNotFound(_) => named("room_not_found"),
        MutationError::OtherRoom { .. } => named("invalid_argument"),
        MutationError::Refused { .. } => named("room_closed"),
        MutationError::ReentryLoopCap(_) => named("reentry_loop_cap"),
        _ => return None,
    })
}

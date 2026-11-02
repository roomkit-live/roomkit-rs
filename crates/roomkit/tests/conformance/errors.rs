//! The suite's names of the errors roomkit-rs's calls fail with: each by
//! its meaning in the RFC (roomkit-specs `conformance/README.md`, "Errors"),
//! after the table of `docs/rfc-gaps.md` G-085.

use roomkit::RoomKitError;
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
pub(crate) fn error_of(error: &RoomKitError) -> Option<Value> {
    Some(match error {
        RoomKitError::RoomNotFound(_) => named("room_not_found"),
        RoomKitError::ChannelNotRegistered { .. } => named("channel_not_registered"),
        RoomKitError::ChannelNotFound { .. } => named("channel_not_bound"),
        RoomKitError::ParticipantNotFound { .. } => named("participant_not_found"),
        RoomKitError::IdentityNotFound(_) => named("identity_not_found"),
        RoomKitError::RoomClosed { .. } => named("room_closed"),
        RoomKitError::ProcessTimeout { .. } => named("process_timeout"),
        RoomKitError::ReentryLoopCap(_) => named("reentry_loop_cap"),
        RoomKitError::TransitionRefused { .. } => named("transition_refused"),
        RoomKitError::ChangeRefused { reason } => {
            json!({ "name": "change_refused", "reason": reason })
        }
        RoomKitError::ReentrantCall { .. } => named("reentrant"),
        RoomKitError::EventNotFound(_) => named("event_not_found"),
        RoomKitError::Channel { .. } => named("channel_failed"),
        RoomKitError::Value(_)
        | RoomKitError::ChannelAlreadyRegistered(_)
        | RoomKitError::ToolNameCollision(_) => named("invalid_argument"),
        RoomKitError::VerificationRefused { reason } => {
            json!({ "name": "verification_refused", "reason": reason })
        }
        _ => return None,
    })
}

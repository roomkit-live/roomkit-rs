//! The suite's names of the errors roomkit-rs's calls fail with: each by
//! its meaning in the RFC (roomkit-specs `conformance/README.md`, "Errors"),
//! after the table of `docs/rfc-gaps.md` G-085.

use roomkit::RoomKitError;
use roomkit::model::{MediaContent, ModelError, Visibility};
use serde_json::{Value, json};

pub(crate) fn named(name: &str) -> Value {
    json!({ "name": name })
}

/// Public inputs build a strict `Visibility` before calling the kit. A
/// refusal by that model constructor is the suite's `invalid_argument`,
/// just as an invalid channel id is refused before registration. Stored
/// events and hook payloads use their own, lenient event deserialization.
pub(crate) fn visibility_input_error(spec: &Value) -> Option<Value> {
    let (input, fields): (&Value, &[&str]) = match spec["call"].as_str() {
        Some("process_inbound") => (&spec["message"], &["visibility", "response_visibility"]),
        Some("send_event") => (spec, &["visibility", "response_visibility"]),
        Some("attach_channel" | "set_visibility") => (spec, &["visibility"]),
        _ => return None,
    };
    fields.iter().find_map(|field| {
        let value = input.get(*field)?.as_str()?;
        matches!(
            value.parse::<Visibility>(),
            Err(ModelError::InvalidVisibility { .. })
        )
        .then(|| named("invalid_argument"))
    })
}

/// A media URL the content model refuses (RFC §6.4), in the content of a
/// `process_inbound` or a `send_event`, through a composite's parts and an
/// edit's new content, is the suite's `invalid_argument`, as Python's
/// validator refusing it is; roomkit-rs refuses it as it deserializes the
/// content, which the runner would read as a scenario it cannot read.
pub(crate) fn media_url_input_error(spec: &Value) -> Option<Value> {
    let content = match spec["call"].as_str() {
        Some("process_inbound") => &spec["message"]["content"],
        Some("send_event") => &spec["content"],
        _ => return None,
    };
    refuses_media_url(content).then(|| named("invalid_argument"))
}

fn refuses_media_url(content: &Value) -> bool {
    match content["type"].as_str() {
        Some("media") => content["url"]
            .as_str()
            .is_some_and(|url| MediaContent::new(url, "image/png").is_err()),
        Some("composite") => content["parts"]
            .as_array()
            .is_some_and(|parts| parts.iter().any(refuses_media_url)),
        Some("edit") => refuses_media_url(&content["new_content"]),
        _ => false,
    }
}

/// The suite's name of `error`; `None` for an error it has no name for, a
/// defect to look at.
#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "an error the suite has no name for is reported, whichever variant it is"
)]
pub(crate) fn error_of(error: &RoomKitError) -> Option<Value> {
    Some(match error {
        RoomKitError::RemoteTurn { kind, .. } if kind == "RequestError" => {
            named("acp_request_refused")
        }
        RoomKitError::RemoteTurn { kind, .. } if kind == "ConnectionError" => {
            named("acp_connection_closed")
        }
        RoomKitError::RemoteTurn { kind, message, .. }
            if kind == "ValidationError"
                && message.starts_with("invalid ACP arguments for session/set_config_option:") =>
        {
            named("invalid_argument")
        }
        RoomKitError::RoomNotFound(_) => named("room_not_found"),
        RoomKitError::ChannelNotRegistered { .. } => named("channel_not_registered"),
        RoomKitError::ChannelNotFound { .. } => named("channel_not_bound"),
        RoomKitError::ParticipantNotFound { .. } => named("participant_not_found"),
        RoomKitError::IdentityNotFound(_) => named("identity_not_found"),
        RoomKitError::RoomClosed { .. } => named("room_closed"),
        RoomKitError::KitClosed => named("kit_closed"),
        RoomKitError::ProcessTimeout { .. } => named("process_timeout"),
        RoomKitError::ReentryLoopCap(_) => named("reentry_loop_cap"),
        RoomKitError::TransitionRefused { .. } => named("transition_refused"),
        RoomKitError::ChangeRefused { reason } => {
            json!({ "name": "change_refused", "reason": reason })
        }
        RoomKitError::ReentrantCall { .. } => named("reentrant"),
        RoomKitError::EventNotFound(_) => named("event_not_found"),
        RoomKitError::IdempotencyKeyTaken { .. } => named("idempotency_key_taken"),
        RoomKitError::Channel { .. } => named("channel_failed"),
        RoomKitError::RemoteTurn { kind, .. } if kind == "ScriptedFailureError" => {
            named("channel_failed")
        }
        RoomKitError::TurnLink { reason, .. } => json!({"name": "turn_link", "reason": reason}),
        RoomKitError::Value(_)
        | RoomKitError::ChannelAlreadyRegistered(_)
        | RoomKitError::ToolNameCollision(_) => named("invalid_argument"),
        RoomKitError::Runtime(message)
            if message.starts_with("ACP session for room ")
                && message
                    .contains("it is not prompted. Set another value with set_config_option") =>
        {
            named("acp_setup_refused")
        }
        RoomKitError::Runtime(message)
            if message.contains("setup_handoff() already called")
                || message.contains("setup_delegation() already called") =>
        {
            named("already_set_up")
        }
        RoomKitError::VerificationRefused { reason } => {
            json!({ "name": "verification_refused", "reason": reason })
        }
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::{media_url_input_error, visibility_input_error};
    use serde_json::json;

    #[test]
    fn a_refused_media_url_is_a_named_public_input_error() {
        let media = json!({ "type": "media", "url": "app://a/%2e%2e/b", "mime_type": "image/png" });
        let composite =
            json!({ "type": "composite", "parts": [{ "type": "text", "body": "x" }, media] });
        let inbound = json!({ "call": "process_inbound", "message": { "content": composite } });
        let sent = json!({ "call": "send_event", "content": media });
        for spec in [inbound, sent] {
            assert_eq!(
                media_url_input_error(&spec),
                Some(json!({ "name": "invalid_argument" }))
            );
        }
        let link =
            json!({ "type": "media", "url": "https://a/%2e%2e/b", "mime_type": "image/png" });
        assert_eq!(
            media_url_input_error(&json!({ "call": "send_event", "content": link })),
            None
        );
    }

    #[test]
    fn a_visibility_constructor_refusal_is_a_named_public_input_error() {
        for call in [
            "attach_channel",
            "set_visibility",
            "process_inbound",
            "send_event",
        ] {
            let input = json!({ "visibility": "a,,b" });
            let mut spec = input.clone();
            spec["call"] = json!(call);
            spec["message"] = input;
            assert_eq!(
                visibility_input_error(&spec),
                Some(json!({ "name": "invalid_argument" }))
            );
        }
    }

    #[test]
    fn unrelated_shape_errors_and_accepted_values_are_not_named_refusals() {
        for value in [
            json!(42),
            json!(null),
            json!("all"),
            json!("internal"),
            json!(" a,a "),
        ] {
            let spec = json!({ "call": "send_event", "visibility": value });
            assert_eq!(visibility_input_error(&spec), None);
        }
        let stored = json!({ "call": "commit_event", "event": { "visibility": "" } });
        assert_eq!(visibility_input_error(&stored), None);
    }
}

//! What a completed request releases (RFC §11.7.6): the held messages of
//! its window, and what each is restored to; the Python implementation's
//! `_held_window`, `_held_for` and `_restore_held`.

use serde_json::Value;

use super::HELD_KEY;
use crate::model::{
    EventType, EventUpdates, IdentityId, Metadata, RoomEvent, Timestamp, VerificationPolicy,
    VerificationRequest, VerificationStatus, Visibility,
};
use crate::rules::edits;

/// How many of the room's latest events a release reads to find its
/// window, for a policy releasing at most `release_max_messages`: Python's
/// `max(200, release_max_messages * 5)`.
pub fn window_scan(release_max_messages: u32) -> u64 {
    u64::from(release_max_messages).saturating_mul(5).max(200)
}

/// Whether `event` is held and belongs to `identity_id` in
/// `organization_id`: Python's `_held_for`. Its mark alone is not enough:
/// the pipeline drops one a sender supplies and labels what it holds, so a
/// held event carries both, the label saying its sender was not verified.
pub fn held_for(
    event: &RoomEvent,
    identity_id: &IdentityId,
    organization_id: Option<&str>,
) -> bool {
    let Some(Value::Object(mark)) = event.metadata.get(HELD_KEY) else {
        return false;
    };
    let labelled = event
        .source
        .verification
        .is_some_and(|label| label != VerificationStatus::Verified);
    let organization = mark
        .get("organization_id")
        .and_then(Value::as_str)
        .filter(|organization| !organization.is_empty());
    labelled
        && mark.get("identity_id").and_then(Value::as_str) == Some(identity_id.as_str())
        && organization == organization_id.filter(|organization| !organization.is_empty())
}

/// The held messages `request` releases at `now`, oldest first: from
/// `recent`, the room's latest events newest first, those held for its
/// identity in its organization, received from its `window_start` on and
/// no longer than the policy's `release_max_age` before `now`, neither
/// deleted nor themselves a change (RFC §10.3); the latest
/// `release_max_messages` by index (RFC §14.1: a newest-first page is
/// still read in index order). Python's `_held_window`.
pub fn held_window(
    recent: Vec<RoomEvent>,
    request: &VerificationRequest,
    policy: &VerificationPolicy,
    now: Timestamp,
) -> Vec<RoomEvent> {
    let organization_id = request.organization_id.as_deref();
    let in_time = |event: &RoomEvent| {
        event.created_at >= request.window_start
            && now
                .duration_since(event.created_at)
                .is_none_or(|age| age <= policy.release_max_age)
    };
    let mut held: Vec<RoomEvent> = recent
        .into_iter()
        .filter(|event| held_for(event, &request.identity_id, organization_id))
        .filter(|event| in_time(event) && !withdrawn(event))
        .collect();
    held.sort_by_key(|event| std::cmp::Reverse(event.index));
    held.truncate(usize::try_from(policy.release_max_messages).unwrap_or(usize::MAX));
    held.sort_by_key(|event| event.index);
    held
}

/// What releasing `current`, the held event as the store holds it now,
/// writes on it: the visibility its mark recorded, `all` when it recorded
/// none, and its metadata without the mark; with whether its delivery was
/// cancelled while it was held (RFC §10.1 "Explicit delivery
/// cancellation"). `None` when it is no longer held: released already, or
/// changed meanwhile. Python's `_restore_held`, for one event.
pub fn restoring(current: &RoomEvent) -> Option<(EventUpdates, bool)> {
    let Some(Value::Object(mark)) = current.metadata.get(HELD_KEY) else {
        return None;
    };
    let visibility = mark
        .get("visibility")
        .and_then(Value::as_str)
        .filter(|recorded| !recorded.is_empty())
        .and_then(|recorded| recorded.parse::<Visibility>().ok())
        .unwrap_or(Visibility::All);
    let cancelled = mark.get("cancelled").is_some_and(truthy);
    let metadata: Metadata = current
        .metadata
        .iter()
        .filter(|(key, _)| key.as_str() != HELD_KEY)
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let updates = EventUpdates {
        visibility: Some(visibility),
        metadata: Some(metadata),
        ..EventUpdates::default()
    };
    Some((updates, cancelled))
}

/// Whether `event` is no message an agent may answer (RFC §10.3): deleted
/// by an accepted delete, or itself an edit or a delete. Python's
/// `withdrawn`.
pub fn withdrawn(event: &RoomEvent) -> bool {
    edits::is_deleted(event) || matches!(event.r#type, EventType::Edit | EventType::Delete)
}

/// Whether Python reads `value` as true: what `marker.get("cancelled")`
/// tests.
fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(set) => *set,
        Value::Number(number) => number.as_f64().is_some_and(|number| number != 0.0),
        Value::String(text) => !text.is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::Object(members) => !members.is_empty(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::model::{ChannelId, ChannelType, EventId, EventSource, RoomId, TextContent};

    fn at(millis: i64) -> Timestamp {
        Timestamp::from_unix_millis(millis).unwrap()
    }

    fn alice() -> IdentityId {
        IdentityId::new("alice-id").unwrap()
    }

    fn held(id: &str, index: u64, millis: i64) -> RoomEvent {
        let mut source = EventSource::new(ChannelId::new("sms").unwrap(), ChannelType::SMS);
        source.verification = Some(VerificationStatus::Unverified);
        let mut event = RoomEvent::at(
            EventId::new(id).unwrap(),
            RoomId::new("room-1").unwrap(),
            EventType::Message,
            source,
            TextContent::new(id),
            at(millis),
        );
        event.index = index;
        event.visibility = Visibility::Transport;
        event.metadata.insert(
            HELD_KEY.to_owned(),
            json!({"visibility": "all", "identity_id": "alice-id", "organization_id": null}),
        );
        event
    }

    fn request(window_start: i64) -> VerificationRequest {
        VerificationRequest::new(
            "req".to_owned(),
            alice(),
            RoomId::new("room-1").unwrap(),
            ChannelId::new("sms").unwrap(),
            "link".to_owned(),
            "scrypt$00".to_owned(),
            at(window_start),
            at(window_start),
            at(window_start + 600_000),
            3,
        )
    }

    fn policy(release_max_messages: u32, release_max_age_ms: u64) -> VerificationPolicy {
        VerificationPolicy {
            release_max_messages,
            release_max_age: std::time::Duration::from_millis(release_max_age_ms),
            ..VerificationPolicy::default()
        }
    }

    fn ids(window: &[RoomEvent]) -> Vec<&str> {
        window.iter().map(|event| event.id.as_str()).collect()
    }

    // Python: tests/test_verification_release.py::test_the_release_keeps_to_its_bound,
    // and _held_window's window start, age bound and order.
    #[test]
    fn a_window_takes_the_latest_held_within_its_age_oldest_first() {
        let recent = vec![held("e3", 3, 30), held("e2", 2, 20), held("e1", 1, 10)];
        let started = held_window(recent.clone(), &request(15), &policy(20, 3_600_000), at(40));
        assert_eq!(ids(&started), ["e2", "e3"]);
        let aged = held_window(recent.clone(), &request(0), &policy(20, 20), at(40));
        assert_eq!(ids(&aged), ["e2", "e3"]);
        let bounded = held_window(recent, &request(0), &policy(1, 3_600_000), at(40));
        assert_eq!(ids(&bounded), ["e3"]);
    }

    // Python: tests/test_verification_release.py::test_only_a_message_the_pipeline_held_is_released,
    // test_a_marker_a_sender_supplies_releases_nothing.
    #[test]
    fn a_mark_without_the_pipeline_s_label_is_not_held() {
        let mut copied = held("e1", 1, 10);
        copied.source.verification = None;
        assert!(!held_for(&copied, &alice(), None));
        let mut verified = held("e1", 1, 10);
        verified.source.verification = Some(VerificationStatus::Verified);
        assert!(!held_for(&verified, &alice(), None));
        assert!(held_for(&held("e1", 1, 10), &alice(), None));
        assert!(!held_for(&held("e1", 1, 10), &alice(), Some("acme")));
    }

    // Python: tests/test_verification_release.py::test_a_held_message_deleted_before_the_release_is_not_answered.
    #[test]
    fn a_deleted_held_message_is_out_of_the_window() {
        let mut deleted = held("e1", 1, 10);
        deleted
            .metadata
            .insert(edits::DELETED.to_owned(), Value::Bool(true));
        let window = held_window(vec![deleted], &request(0), &policy(20, 3_600_000), at(40));
        assert_eq!(window, Vec::new());
    }

    // Python: core/mixins/verification.py::_restore_held.
    #[test]
    fn a_release_restores_the_recorded_scope_and_drops_the_mark() {
        let mut event = held("e1", 1, 10);
        event.metadata.insert("crm".to_owned(), json!("c_1"));
        let (updates, cancelled) = restoring(&event).unwrap();
        assert_eq!(updates.visibility, Some(Visibility::All));
        assert_eq!(
            updates.metadata,
            Some(Metadata::from_iter([("crm".to_owned(), json!("c_1"))]))
        );
        assert!(!cancelled);
        event.metadata.insert(
            HELD_KEY.to_owned(),
            json!({"visibility": "", "identity_id": "alice-id", "cancelled": "caller_cancelled"}),
        );
        let (updates, cancelled) = restoring(&event).unwrap();
        assert_eq!(updates.visibility, Some(Visibility::All));
        assert!(cancelled);
        event.metadata.remove(HELD_KEY);
        assert!(restoring(&event).is_none());
    }
}

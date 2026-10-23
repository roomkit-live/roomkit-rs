//! Attaching a channel is a decision, not a repair (RFC §7.5 rule 7).
//!
//! Detaching a channel revokes its access to the room: the room keeps a
//! record of the revocation, which an automatic attach respects, and which
//! only an explicit attach clears. The record lives in the room's metadata,
//! under a key of the namespace the implementation reserves (RFC §10.6.5:
//! "a detach's revocation"); no integrator write may touch that namespace
//! (gap G-049).
//!
//! The framework attaches a channel itself for a message arriving on it
//! (RFC §10.1 step 2): never once it was detached, never over a binding the
//! room has, and never to a room whose status refuses the message anyway
//! (gap G-053).
//!
//! The record maps each revoked channel to the instant it was detached:
//!
//! ```json
//! { "roomkit:detached": { "sms": "2026-10-22T14:03:00Z" } }
//! ```

use serde_json::Value;

use crate::model::{ChannelId, Metadata, Room, RoomContext, RoomUpdates, Timestamp};

/// The prefix of the room metadata keys the implementation reserves (gap
/// G-049).
pub const RESERVED_PREFIX: &str = "roomkit:";

/// The room metadata key holding the room's revocations.
pub const REVOCATIONS_KEY: &str = "roomkit:detached";

/// Whether a room metadata key belongs to the implementation's namespace,
/// which integrator writes refuse (RFC §10.6.5).
pub fn is_reserved_key(key: &str) -> bool {
    key.starts_with(RESERVED_PREFIX)
}

/// The first key of `metadata` an integrator may not write, if any.
pub fn reserved_key(metadata: &Metadata) -> Option<&str> {
    metadata
        .keys()
        .map(String::as_str)
        .find(|key| is_reserved_key(key))
}

/// Whether `channel` was detached from the room and not explicitly attached
/// since.
pub fn revoked(room: &Room, channel: &ChannelId) -> bool {
    revocations(room).contains_key(channel.as_str())
}

/// Whether the framework may attach `channel` to the room on its own, for a
/// message arriving on it (RFC §10.1 step 2): never once the channel was
/// detached from the room, never over a binding the room has (RFC §7.5 rule
/// 7), and never to a room whose status refuses the message (gap G-053).
pub fn may_attach_automatically(context: &RoomContext, channel: &ChannelId) -> bool {
    let bound = context
        .bindings
        .iter()
        .any(|binding| &binding.channel_id == channel);
    !bound && !revoked(&context.room, channel) && context.room.status.accepts_events()
}

/// The write that records `channel`'s revocation as of `at`, keeping the
/// room's other revocations.
pub fn revoke(room: &Room, channel: &ChannelId, at: Timestamp) -> RoomUpdates {
    let mut revocations = revocations(room);
    revocations.insert(channel.to_string(), Value::String(at.to_string()));
    let mut updates = RoomUpdates {
        updated_at: Some(at),
        ..RoomUpdates::default()
    };
    updates
        .metadata
        .insert(REVOCATIONS_KEY.to_owned(), Value::Object(revocations));
    updates
}

/// The write that clears `channel`'s revocation as of `at`, for an explicit
/// attach; `None` when the channel is not revoked. The record goes once it
/// holds no revocation.
pub fn clear_revocation(room: &Room, channel: &ChannelId, at: Timestamp) -> Option<RoomUpdates> {
    let mut revocations = revocations(room);
    revocations.remove(channel.as_str())?;
    let mut updates = RoomUpdates {
        updated_at: Some(at),
        ..RoomUpdates::default()
    };
    if revocations.is_empty() {
        updates.removed_metadata.push(REVOCATIONS_KEY.to_owned());
    } else {
        updates
            .metadata
            .insert(REVOCATIONS_KEY.to_owned(), Value::Object(revocations));
    }
    Some(updates)
}

/// The room's revocations; a record of another shape holds none.
fn revocations(room: &Room) -> Metadata {
    match room.metadata.get(REVOCATIONS_KEY) {
        Some(Value::Object(revocations)) => revocations.clone(),
        Some(_) | None => Metadata::new(),
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;
    use crate::model::{ChannelBinding, ChannelCategory, ChannelDirection, ChannelType, RoomId};

    fn sms() -> ChannelId {
        ChannelId::new("sms").unwrap()
    }

    fn at() -> Timestamp {
        Timestamp::from_unix_millis(1_792_677_780_000).unwrap()
    }

    fn room() -> Room {
        Room::new(RoomId::new("room_1").unwrap(), Timestamp::UNIX_EPOCH)
    }

    fn revoked_room() -> Room {
        let mut room = room();
        revoke(&room, &sms(), at()).apply(&mut room);
        room
    }

    #[test]
    fn a_revocation_names_the_channel_and_its_instant() {
        assert_eq!(
            revoked_room().metadata.get(REVOCATIONS_KEY),
            Some(&json!({ "sms": "2026-10-22T14:03:00Z" }))
        );
    }

    #[test]
    fn a_revocation_keeps_the_room_s_others() {
        let mut room = revoked_room();
        let ws = ChannelId::new("ws").unwrap();
        revoke(&room, &ws, at()).apply(&mut room);
        assert!(revoked(&room, &sms()) && revoked(&room, &ws));
    }

    #[test]
    fn a_detached_channel_is_never_attached_automatically() {
        let context = RoomContext::new(revoked_room());
        assert!(!may_attach_automatically(&context, &sms()));
    }

    #[test]
    fn a_bound_channel_is_never_attached_automatically_over_its_binding() {
        let mut context = RoomContext::new(room());
        context.bindings.push(ChannelBinding::new(
            sms(),
            context.room.id.clone(),
            ChannelType::Sms,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
            Timestamp::UNIX_EPOCH,
        ));
        assert!(!may_attach_automatically(&context, &sms()));
    }

    #[test]
    fn a_closed_room_takes_no_automatic_attach() {
        let mut context = RoomContext::new(room());
        context.room.status = crate::model::RoomStatus::Closed;
        assert!(!may_attach_automatically(&context, &sms()));
    }

    #[test]
    fn a_channel_never_bound_may_be_attached_automatically() {
        assert!(may_attach_automatically(&RoomContext::new(room()), &sms()));
    }

    #[test]
    fn clearing_the_last_revocation_removes_the_record() {
        let mut room = revoked_room();
        clear_revocation(&room, &sms(), at())
            .unwrap()
            .apply(&mut room);
        assert_eq!(room.metadata.get(REVOCATIONS_KEY), None);
    }

    #[test]
    fn clearing_a_channel_not_revoked_writes_nothing() {
        assert_eq!(clear_revocation(&room(), &sms(), at()), None);
    }

    #[test]
    fn a_key_of_the_reserved_namespace_is_refused() {
        let metadata: Metadata = [
            ("crm".to_owned(), json!("c_1")),
            ("roomkit:detached".to_owned(), json!({})),
        ]
        .into_iter()
        .collect();
        assert_eq!(reserved_key(&metadata), Some("roomkit:detached"));
    }

    #[test]
    fn an_integrator_key_is_not_reserved() {
        assert!(!is_reserved_key("roomkit_crm"));
    }

    /// Gap G-049: the namespace is `roomkit:`; a key written `roomkit.` is
    /// the integrator's.
    #[test]
    fn a_key_under_the_dotted_prefix_is_the_integrator_s() {
        assert!(!is_reserved_key("roomkit.detached"));
    }
}

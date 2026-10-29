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
//! The record is the list of the revoked channels' ids, in the form the
//! Python implementation writes, so that a room either implementation
//! detached a channel from refuses it to both (gap G-049):
//!
//! ```json
//! { "roomkit:detached_channels": ["sms"] }
//! ```
//!
//! A room may still hold the record roomkit-rs wrote before, a map of each
//! revoked channel to the instant it was detached, under
//! `roomkit:detached`: its channels are revoked too, and the next write of
//! the room's revocations folds them into the list.

use serde_json::Value;

use super::authors::AUTHOR_REGISTER;
use crate::model::{ChannelId, Metadata, Room, RoomContext, RoomId, RoomUpdates, Timestamp};

/// The prefix of the room metadata keys the implementation reserves (gap
/// G-049).
pub(crate) const RESERVED_PREFIX: &str = "roomkit:";

/// The room metadata key holding the room's revocations: the list of the
/// revoked channels' ids.
pub const REVOCATIONS_KEY: &str = "roomkit:detached_channels";

/// The key of the record roomkit-rs kept before, a map of each revoked
/// channel to the instant it was detached; read, and folded into
/// [`REVOCATIONS_KEY`] by the next write.
pub(crate) const FORMER_REVOCATIONS_KEY: &str = "roomkit:detached";

/// Whether a room metadata key belongs to the implementation's namespace,
/// which integrator writes refuse (RFC §10.6.5).
pub(crate) fn is_reserved_key(key: &str) -> bool {
    key.starts_with(RESERVED_PREFIX)
}

/// The Python implementation's key of a room's speak-queue results, a key
/// of the room's private state a store shared with it may hold (RFC
/// §14.1).
const SPEAK_QUEUE_RESULTS: &str = "_speak_queue_results";

/// Whether only the framework writes the room metadata key `key`: one of
/// the namespace the implementation reserves, or a key of the room's
/// private state, its register of authors among them (RFC §5.1, §14.1, gap
/// G-049).
pub(crate) fn the_framework_s(key: &str) -> bool {
    is_reserved_key(key) || [AUTHOR_REGISTER, SPEAK_QUEUE_RESULTS].contains(&key)
}

/// `metadata`, a write of room `room_id`'s metadata by `writer`, without the
/// keys only the framework writes, each dropped with a warning naming the
/// writer; the rest is written (RFC §5.1: "a room's creation ... that name
/// such a key have it dropped with a warning, the rest written").
pub(crate) fn without_framework_keys(
    mut metadata: Metadata,
    room_id: &RoomId,
    writer: &str,
) -> Metadata {
    metadata.retain(|key, _| {
        let the_framework_s = the_framework_s(key);
        if the_framework_s {
            tracing::warn!(
                target: "roomkit::core::lifecycle",
                %room_id,
                key = %key,
                writer,
                "the metadata key is dropped: only the framework writes it"
            );
        }
        !the_framework_s
    });
    metadata
}

/// Whether `channel` was detached from the room and not explicitly attached
/// since.
pub fn revoked(room: &Room, channel: &ChannelId) -> bool {
    revocations(room).iter().any(|item| names(item, channel))
}

/// Whether the framework may attach `channel` to the room on its own, for a
/// message arriving on it (RFC §10.1 step 2): never once the channel was
/// detached from the room, never over a binding the room has (RFC §7.5 rule
/// 7), and never to a room whose status refuses the message (gap G-053).
pub(crate) fn may_attach_automatically(context: &RoomContext, channel: &ChannelId) -> bool {
    let bound = context.get_binding(channel).is_some();
    !bound && !revoked(&context.room, channel) && context.room.status.accepts_events()
}

/// The write that records `channel`'s revocation as of `at`, keeping the
/// room's other revocations.
pub fn revoke(room: &Room, channel: &ChannelId, at: Timestamp) -> RoomUpdates {
    let mut revocations = revocations(room);
    if !revocations.iter().any(|item| names(item, channel)) {
        revocations.push(Value::String(channel.to_string()));
    }
    written(room, revocations, at)
}

/// The write that clears `channel`'s revocation as of `at`, for an explicit
/// attach; `None` when the channel is not revoked. The record goes once it
/// holds no revocation.
pub(crate) fn clear_revocation(
    room: &Room,
    channel: &ChannelId,
    at: Timestamp,
) -> Option<RoomUpdates> {
    let mut revocations = revocations(room);
    let before = revocations.len();
    revocations.retain(|item| !names(item, channel));
    (revocations.len() < before).then(|| written(room, revocations, at))
}

/// The room's revocations: the list, then the channels of the former record
/// the list does not name. A record of another shape holds none.
fn revocations(room: &Room) -> Vec<Value> {
    let mut revocations = match room.metadata.get(REVOCATIONS_KEY) {
        Some(Value::Array(list)) => list.clone(),
        Some(_) | None => Vec::new(),
    };
    if let Some(Value::Object(former)) = room.metadata.get(FORMER_REVOCATIONS_KEY) {
        for channel in former.keys() {
            if !revocations
                .iter()
                .any(|item| item.as_str() == Some(channel))
            {
                revocations.push(Value::String(channel.clone()));
            }
        }
    }
    revocations
}

/// The write of `revocations` as of `at`: the list, or no record once it is
/// empty, and never the former record.
fn written(room: &Room, revocations: Vec<Value>, at: Timestamp) -> RoomUpdates {
    let mut updates = RoomUpdates {
        updated_at: Some(at),
        ..RoomUpdates::default()
    };
    if revocations.is_empty() {
        updates.removed_metadata.push(REVOCATIONS_KEY.to_owned());
    } else {
        updates
            .metadata
            .insert(REVOCATIONS_KEY.to_owned(), Value::Array(revocations));
    }
    if room.metadata.contains_key(FORMER_REVOCATIONS_KEY) {
        updates
            .removed_metadata
            .push(FORMER_REVOCATIONS_KEY.to_owned());
    }
    updates
}

/// Whether an item of the list names `channel`.
fn names(item: &Value, channel: &ChannelId) -> bool {
    item.as_str() == Some(channel.as_str())
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
        Room::at(RoomId::new("room_1").unwrap(), Timestamp::UNIX_EPOCH)
    }

    fn revoked_room() -> Room {
        let mut room = room();
        revoke(&room, &sms(), at()).apply(&mut room);
        room
    }

    /// A room the Python implementation detached a channel from, as its
    /// store holds it (gap G-049).
    #[test]
    fn a_channel_python_detached_is_revoked() {
        let mut room = room();
        room.metadata
            .insert("roomkit:detached_channels".to_owned(), json!(["sms"]));
        assert!(revoked(&room, &sms()));
    }

    #[test]
    fn a_revocation_lists_the_channel_as_python_does() {
        assert_eq!(
            revoked_room().metadata.get("roomkit:detached_channels"),
            Some(&json!(["sms"]))
        );
    }

    /// The record roomkit-rs wrote before gap G-049 followed Python's form.
    fn formerly_revoked_room() -> Room {
        let mut room = room();
        room.metadata.insert(
            "roomkit:detached".to_owned(),
            json!({ "sms": "2026-10-22T14:03:00Z" }),
        );
        room
    }

    #[test]
    fn a_channel_of_the_former_record_is_revoked() {
        assert!(revoked(&formerly_revoked_room(), &sms()));
    }

    #[test]
    fn the_next_revocation_folds_the_former_record_into_the_list() {
        let mut room = formerly_revoked_room();
        revoke(&room, &ChannelId::new("ws").unwrap(), at()).apply(&mut room);
        assert_eq!(
            room.metadata,
            [("roomkit:detached_channels".to_owned(), json!(["sms", "ws"]))]
                .into_iter()
                .collect::<Metadata>()
        );
    }

    #[test]
    fn clearing_the_former_record_s_last_channel_removes_both_records() {
        let mut room = formerly_revoked_room();
        clear_revocation(&room, &sms(), at())
            .unwrap()
            .apply(&mut room);
        assert_eq!(room.metadata, Metadata::new());
    }

    #[test]
    fn revoking_a_revoked_channel_lists_it_once() {
        let mut room = revoked_room();
        revoke(&room, &sms(), at()).apply(&mut room);
        assert_eq!(room.metadata.get(REVOCATIONS_KEY), Some(&json!(["sms"])));
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
        context.bindings.push(ChannelBinding::at(
            sms(),
            context.room.id.clone(),
            ChannelType::SMS,
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

    /// RFC §5.1, §14.1: a key of the reserved namespace or of the room's
    /// private state is dropped; the rest is kept.
    #[test]
    fn the_framework_s_keys_are_dropped_and_the_rest_kept() {
        let metadata: Metadata = [
            ("crm".to_owned(), json!("c_1")),
            ("roomkit:detached_channels".to_owned(), json!([])),
            ("author_register".to_owned(), json!({})),
            ("_speak_queue_results".to_owned(), json!([])),
        ]
        .into_iter()
        .collect();
        let room_id = RoomId::new("room_1").unwrap();
        let kept = without_framework_keys(metadata, &room_id, "create_room");
        assert_eq!(kept.keys().collect::<Vec<_>>(), ["crm"]);
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

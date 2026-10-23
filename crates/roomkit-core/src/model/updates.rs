//! Partial updates of the records a store keeps: a room (RFC §14.1
//! `update_room(room_id, updates)`) and a binding (RFC §7.5 rule 6).

use super::{Access, ChannelBinding, Metadata, Room, RoomStatus, Timestamp, Visibility};

/// A partial update of a room (RFC §14.1 `update_room(room_id, updates)`):
/// the fields it names are written, every other one is kept as stored.
///
/// A store applies it to the room as it holds it, so a write made off the
/// room lock (a channel's metadata update) never loses the counters of an
/// event committed meanwhile. A field of type `Option<Option<_>>` is written
/// when it is `Some`, to the value it holds, `None` included.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RoomUpdates {
    /// The room's status.
    pub status: Option<RoomStatus>,
    /// The room's last modification.
    pub updated_at: Option<Timestamp>,
    /// When the room was closed.
    pub closed_at: Option<Option<Timestamp>>,
    /// When the room was paused.
    pub paused_at: Option<Option<Timestamp>>,
    /// The room's last activity (`timers.last_activity_at`).
    pub last_activity_at: Option<Option<Timestamp>>,
    /// Metadata keys to write, each replacing the key's stored value.
    pub metadata: Metadata,
    /// Metadata keys to remove.
    pub removed_metadata: Vec<String>,
}

impl RoomUpdates {
    /// The lifecycle fields of `room` as they stand: its status, its last
    /// modification, and the instants a transition writes (RFC §5.1). The
    /// write of a transition decided on a copy of the room.
    pub fn lifecycle(room: &Room) -> Self {
        Self {
            status: Some(room.status),
            updated_at: Some(room.updated_at),
            closed_at: Some(room.closed_at),
            paused_at: Some(room.paused_at),
            last_activity_at: Some(room.timers.last_activity_at),
            ..Self::default()
        }
    }

    /// Whether the update writes nothing.
    pub fn is_empty(&self) -> bool {
        self == &Self::default()
    }

    /// Writes the update on `room`, as a store applies it.
    pub fn apply(&self, room: &mut Room) {
        if let Some(status) = self.status {
            room.status = status;
        }
        if let Some(updated_at) = self.updated_at {
            room.updated_at = updated_at;
        }
        if let Some(closed_at) = self.closed_at {
            room.closed_at = closed_at;
        }
        if let Some(paused_at) = self.paused_at {
            room.paused_at = paused_at;
        }
        if let Some(last_activity_at) = self.last_activity_at {
            room.timers.last_activity_at = last_activity_at;
        }
        for key in &self.removed_metadata {
            room.metadata.remove(key);
        }
        room.metadata.extend(
            self.metadata
                .iter()
                .map(|(key, value)| (key.clone(), value.clone())),
        );
    }
}

/// A change to an existing binding: what it names changes, everything else
/// is carried over (RFC §7.5 rule 6).
///
/// Every write the framework makes to an existing binding goes through
/// [`BindingUpdate::apply`], so no write ever widens a binding by letting a
/// field fall back to its default: `access`, `visibility` and `muted` change
/// only when the update names them.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct BindingUpdate {
    /// The binding's new access.
    pub access: Option<Access>,
    /// Whether the channel is muted (RFC §7.2).
    pub muted: Option<bool>,
    /// The binding's new visibility (RFC §7.3).
    pub visibility: Option<Visibility>,
    /// Metadata keys to write, each replacing the key's value.
    pub metadata: Metadata,
}

impl BindingUpdate {
    /// An update that mutes the channel.
    pub fn mute() -> Self {
        Self {
            muted: Some(true),
            ..Self::default()
        }
    }

    /// An update that unmutes the channel.
    pub fn unmute() -> Self {
        Self {
            muted: Some(false),
            ..Self::default()
        }
    }

    /// An update that changes the binding's visibility.
    pub fn visibility(visibility: Visibility) -> Self {
        Self {
            visibility: Some(visibility),
            ..Self::default()
        }
    }

    /// `binding` with this update applied, every field it does not name
    /// carried over (RFC §7.5 rule 6).
    pub fn apply(&self, binding: &ChannelBinding) -> ChannelBinding {
        let mut updated = binding.clone();
        if let Some(access) = self.access {
            updated.access = access;
        }
        if let Some(muted) = self.muted {
            updated.muted = muted;
        }
        if let Some(visibility) = &self.visibility {
            updated.visibility = visibility.clone();
        }
        updated.metadata.extend(
            self.metadata
                .iter()
                .map(|(key, value)| (key.clone(), value.clone())),
        );
        updated
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;
    use crate::model::{ChannelCategory, ChannelDirection, ChannelId, ChannelType, RoomId};

    fn room() -> Room {
        let mut room = Room::new(RoomId::new("room_1").unwrap(), Timestamp::UNIX_EPOCH);
        room.event_count = 3;
        room.metadata.insert("crm".to_owned(), json!("c_1"));
        room
    }

    fn binding() -> ChannelBinding {
        let mut binding = ChannelBinding::new(
            ChannelId::new("ws_observer").unwrap(),
            RoomId::new("room_1").unwrap(),
            ChannelType::Websocket,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
            Timestamp::UNIX_EPOCH,
        );
        binding.access = Access::ReadOnly;
        binding.visibility = Visibility::None;
        binding
    }

    #[test]
    fn an_empty_room_update_writes_nothing() {
        let mut stored = room();
        RoomUpdates::default().apply(&mut stored);
        assert_eq!(stored, room());
    }

    #[test]
    fn a_room_update_keeps_the_counters_it_does_not_name() {
        let mut stored = room();
        let paused = RoomUpdates {
            status: Some(RoomStatus::Paused),
            ..RoomUpdates::default()
        };
        paused.apply(&mut stored);
        assert_eq!((stored.status, stored.event_count), (RoomStatus::Paused, 3));
    }

    #[test]
    fn a_room_update_writes_and_removes_metadata_keys() {
        let mut stored = room();
        let retagged = RoomUpdates {
            metadata: [("tier".to_owned(), json!("gold"))].into_iter().collect(),
            removed_metadata: vec!["crm".to_owned()],
            ..RoomUpdates::default()
        };
        retagged.apply(&mut stored);
        assert_eq!(
            serde_json::Value::Object(stored.metadata),
            json!({ "tier": "gold" })
        );
    }

    #[test]
    fn a_room_update_can_clear_an_instant() {
        let mut stored = Room {
            closed_at: Some(Timestamp::UNIX_EPOCH),
            ..room()
        };
        let reopened = RoomUpdates {
            closed_at: Some(None),
            ..RoomUpdates::default()
        };
        reopened.apply(&mut stored);
        assert_eq!(stored.closed_at, None);
    }

    #[test]
    fn the_lifecycle_update_of_a_room_rewrites_it_as_it_stands() {
        let mut changed = room();
        changed.status = RoomStatus::Closed;
        changed.closed_at = Some(Timestamp::UNIX_EPOCH);
        let mut stored = room();
        RoomUpdates::lifecycle(&changed).apply(&mut stored);
        assert_eq!(stored, changed);
    }

    #[test]
    fn a_binding_update_carries_over_what_it_does_not_name() {
        let updated = BindingUpdate::unmute().apply(&binding());
        assert_eq!(
            (updated.access, updated.visibility),
            (Access::ReadOnly, Visibility::None)
        );
    }

    #[test]
    fn a_binding_update_changes_what_it_names() {
        let updated = BindingUpdate::mute().apply(&binding());
        assert!(updated.muted);
    }

    #[test]
    fn a_binding_update_merges_its_metadata_keys() {
        let mut current = binding();
        current.metadata.insert("persona".to_owned(), json!("calm"));
        let update = BindingUpdate {
            metadata: [("locale".to_owned(), json!("fr"))].into_iter().collect(),
            ..BindingUpdate::default()
        };
        let updated = update.apply(&current);
        assert_eq!(
            serde_json::Value::Object(updated.metadata),
            json!({ "persona": "calm", "locale": "fr" })
        );
    }
}

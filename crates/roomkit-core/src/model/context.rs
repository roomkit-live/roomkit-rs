//! What a channel or a hook is told about its room (RFC §6.1, §10.1 step 3).

use serde::{Deserialize, Serialize};

use super::{ChannelBinding, ChannelId, ChannelType, Metadata, Participant, Room, RoomEvent};

/// A room's state as one reader receives it (RFC §10.1 step 3).
///
/// Hooks receive the room's recent events whole, refused ones included; a
/// channel receives only the events it may see (RFC §7.5 rule 8), so each
/// channel of a broadcast gets a context of its own.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RoomContext {
    /// The room.
    pub room: Room,
    /// The room's channel bindings.
    #[serde(default)]
    pub bindings: Vec<ChannelBinding>,
    /// The room's participants.
    #[serde(default)]
    pub participants: Vec<Participant>,
    /// The room's latest events, in timeline order.
    #[serde(default)]
    pub recent_events: Vec<RoomEvent>,
    /// Lower index from which the loaded conversation tail is complete.
    /// Zero means the whole conversation; a positive value is a bounded
    /// read's oldest retained conversation index. Visibility filtering keeps
    /// the boundary. None leaves it unknown, as in older/hand-built contexts.
    #[serde(default)]
    pub history_start_index: Option<u64>,
    /// The room's register of authors, which ranks the sources whose names
    /// read alike (RFC §6.4, §10.1 step 12, gap G-089): the room's private
    /// state under `author_register` (RFC §14.1 `get_private_state`), kept
    /// apart from its metadata, which clients may read. `None` when the
    /// room holds none.
    #[serde(default)]
    pub author_register: Option<Metadata>,
}

impl RoomContext {
    /// Keep a carried tail's lower boundary honest when its cap drops rows.
    pub(crate) fn trim_history(&mut self, limit: usize) {
        let excess = self.recent_events.len().saturating_sub(limit);
        if excess > 0 {
            self.recent_events.drain(..excess);
            self.history_start_index = self.recent_events.first().map(|event| event.index);
        }
        if limit == 0 {
            self.history_start_index = None;
        }
    }

    /// The context of `room` with no binding, participant or event.
    pub fn new(room: Room) -> Self {
        Self {
            room,
            bindings: Vec::new(),
            participants: Vec::new(),
            recent_events: Vec::new(),
            history_start_index: None,
            author_register: None,
        }
    }

    /// The room's binding of `channel_id`, if it binds the channel: a room
    /// binds a channel once at most (RFC §5.7).
    pub fn get_binding(&self, channel_id: &ChannelId) -> Option<&ChannelBinding> {
        self.bindings
            .iter()
            .find(|binding| &binding.channel_id == channel_id)
    }

    /// Every binding but `exclude_channel_id`'s.
    pub fn other_channels(&self, exclude_channel_id: &ChannelId) -> Vec<&ChannelBinding> {
        self.bindings
            .iter()
            .filter(|binding| &binding.channel_id != exclude_channel_id)
            .collect()
    }

    /// The bindings of `channel_type`'s channels.
    pub fn channels_by_type(&self, channel_type: &ChannelType) -> Vec<&ChannelBinding> {
        self.bindings
            .iter()
            .filter(|binding| &binding.channel_type == channel_type)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::model::{ChannelCategory, ChannelDirection, RoomId, Timestamp};

    fn context() -> RoomContext {
        let room_id = RoomId::new("room_1").unwrap();
        let binding = |id: &str, channel_type: ChannelType| {
            ChannelBinding::at(
                ChannelId::new(id).unwrap(),
                room_id.clone(),
                channel_type,
                ChannelCategory::Transport,
                ChannelDirection::Bidirectional,
                Timestamp::UNIX_EPOCH,
            )
        };
        RoomContext {
            bindings: vec![
                binding("sms", ChannelType::SMS),
                binding("ws", ChannelType::WEBSOCKET),
                binding("sms_2", ChannelType::SMS),
            ],
            ..RoomContext::new(Room::at(room_id.clone(), Timestamp::UNIX_EPOCH))
        }
    }

    fn ids(bindings: &[&ChannelBinding]) -> Vec<String> {
        bindings
            .iter()
            .map(|binding| binding.channel_id.to_string())
            .collect()
    }

    // Python: tests/test_context_history_boundary.py::test_the_optional_boundary_survives_serialization_and_rejects_negative_indices
    #[test]
    fn the_optional_boundary_survives_serialization_and_rejects_negative_indices() {
        let mut context = context();
        context.history_start_index = Some(7);
        let mut value = serde_json::to_value(&context).unwrap();
        assert_eq!(
            serde_json::from_value::<RoomContext>(value.clone()).unwrap(),
            context
        );
        value
            .as_object_mut()
            .unwrap()
            .shift_remove("history_start_index");
        assert_eq!(
            serde_json::from_value::<RoomContext>(value.clone())
                .unwrap()
                .history_start_index,
            None
        );
        value["history_start_index"] = serde_json::json!(-1);
        assert!(serde_json::from_value::<RoomContext>(value).is_err());
    }

    /// Python's `get_binding`, `other_channels` and `channels_by_type`.
    #[test]
    fn a_context_finds_its_bindings_as_python_s_does() {
        let context = context();
        let sms = ChannelId::new("sms").unwrap();
        assert_eq!(
            (
                context
                    .get_binding(&sms)
                    .map(|binding| binding.channel_id.to_string()),
                ids(&context.other_channels(&sms)),
                ids(&context.channels_by_type(&ChannelType::SMS)),
            ),
            (
                Some("sms".to_owned()),
                vec!["ws".to_owned(), "sms_2".to_owned()],
                vec!["sms".to_owned(), "sms_2".to_owned()],
            )
        );
    }
}

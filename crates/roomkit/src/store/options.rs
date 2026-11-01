//! What the store's methods take beside their positional arguments:
//! Python's keyword arguments, with its defaults.

use roomkit_core::model::{ChannelType, IdentityId, Metadata, RoomEvent, RoomStatus, TaskStatus};

use super::{EventFilter, RoomFilter};

/// What [`list_rooms`](super::ConversationStore::list_rooms) takes:
/// Python's `offset=0, limit=50`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ListRoomsOptions {
    /// How many rooms to skip.
    pub offset: u64,
    /// How many rooms at most.
    pub limit: u64,
}

impl Default for ListRoomsOptions {
    fn default() -> Self {
        Self {
            offset: 0,
            limit: 50,
        }
    }
}

/// What [`find_rooms`](super::ConversationStore::find_rooms) takes:
/// Python's `organization_id=None, status=None, metadata_filter=None, *,
/// limit=100, offset=0`.
#[derive(Clone, Debug, PartialEq)]
pub struct FindRoomsOptions {
    /// Only the rooms of this organization.
    pub organization_id: Option<String>,
    /// Only the rooms in this status.
    pub status: Option<RoomStatus>,
    /// Only the rooms whose metadata holds each of these keys and values.
    pub metadata_filter: Option<Metadata>,
    /// How many rooms at most.
    pub limit: u64,
    /// How many rooms to skip.
    pub offset: u64,
}

impl Default for FindRoomsOptions {
    fn default() -> Self {
        Self {
            organization_id: None,
            status: None,
            metadata_filter: None,
            limit: 100,
            offset: 0,
        }
    }
}

/// What [`find_latest_room`](super::ConversationStore::find_latest_room)
/// takes: Python's `channel_type=None, status=None`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FindLatestRoomOptions {
    /// Only a binding of this channel type names the participant; their
    /// participant record names them on any.
    pub channel_type: Option<ChannelType>,
    /// Only the rooms in this status.
    pub status: Option<RoomStatus>,
}

/// What [`find_room_id_by_channel`](super::ConversationStore::find_room_id_by_channel)
/// takes: Python's `status=None`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FindRoomIdByChannelOptions {
    /// Only the rooms in this status.
    pub status: Option<RoomStatus>,
}

/// What [`find_room_ids_by_channel`](super::ConversationStore::find_room_ids_by_channel)
/// takes: Python's `room_filter=None, limit=2, *, group=None`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FindRoomIdsByChannelOptions {
    /// Which rooms are read.
    pub room_filter: Option<RoomFilter>,
    /// How many rooms at most.
    pub limit: usize,
    /// Only the rooms whose binding of the channel is a group's, or not.
    pub group: Option<bool>,
}

impl Default for FindRoomIdsByChannelOptions {
    fn default() -> Self {
        Self {
            room_filter: None,
            limit: 2,
            group: None,
        }
    }
}

/// What [`find_room_id_by_binding`](super::ConversationStore::find_room_id_by_binding)
/// takes: Python's `identity_id=None, room_filter=None, *,
/// recipient_key=None`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FindRoomIdByBindingOptions {
    /// The identity the caller resolved the sender to.
    pub identity_id: Option<IdentityId>,
    /// Which rooms are read.
    pub room_filter: Option<RoomFilter>,
    /// The channel's recipient key, by which a binding names its
    /// recipient's conversation.
    pub recipient_key: Option<String>,
}

/// What [`find_room_id_by_participant`](super::ConversationStore::find_room_id_by_participant)
/// takes: Python's `identity_id=None, room_filter=None`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FindRoomIdByParticipantOptions {
    /// The identity the caller resolved the sender to.
    pub identity_id: Option<IdentityId>,
    /// Which rooms are read.
    pub room_filter: Option<RoomFilter>,
}

/// What [`advance_delivered_index`](super::ConversationStore::advance_delivered_index)
/// takes: Python's `*, force=False`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AdvanceDeliveredIndexOptions {
    /// The cursor moves from any lower value.
    pub force: bool,
}

/// What [`patch_room_metadata`](super::ConversationStore::patch_room_metadata)
/// takes: Python's `*, unset=()`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PatchRoomMetadataOptions {
    /// The keys removed.
    pub unset: Vec<String>,
}

/// What [`get_conversation`](super::ConversationStore::get_conversation)
/// takes: Python's `*, limit=50, after_index=None`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GetConversationOptions {
    /// How many messages at most.
    pub limit: u64,
    /// The first messages after this index, in place of the newest.
    pub after_index: Option<u64>,
}

impl Default for GetConversationOptions {
    fn default() -> Self {
        Self {
            limit: 50,
            after_index: None,
        }
    }
}

/// What [`get_timeline`](super::ConversationStore::get_timeline) takes:
/// Python's `*, event_filter=None, limit=50, after_index=None,
/// newest_first=False`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetTimelineOptions {
    /// Which events the page may hold; `None` for every one the room
    /// received.
    pub event_filter: Option<EventFilter>,
    /// How many events at most.
    pub limit: u64,
    /// The first events after this index.
    pub after_index: Option<u64>,
    /// Without a cursor, the newest `limit` events rather than the oldest.
    pub newest_first: bool,
}

impl Default for GetTimelineOptions {
    fn default() -> Self {
        Self {
            event_filter: None,
            limit: 50,
            after_index: None,
            newest_first: false,
        }
    }
}

/// What [`resolve_identity`](super::ConversationStore::resolve_identity)
/// takes: Python's `organization_id=None`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ResolveIdentityOptions {
    /// The organization whose identities are read; none for the identities
    /// of none.
    pub organization_id: Option<String>,
}

/// What [`link_address`](super::ConversationStore::link_address) takes:
/// Python's `organization_id=None`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LinkAddressOptions {
    /// The organization whose identity the address is linked to.
    pub organization_id: Option<String>,
}

/// What [`unlink_address`](super::ConversationStore::unlink_address)
/// takes: Python's `organization_id=None`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UnlinkAddressOptions {
    /// The organization whose identity the address is unlinked from.
    pub organization_id: Option<String>,
}

/// What [`commit_event`](super::ConversationStore::commit_event) takes:
/// Python's `*, target=None, activity=True`.
#[derive(Clone, Debug, PartialEq)]
pub struct CommitEventOptions {
    /// The event an accepted edit or delete targets, as the change leaves
    /// it, written in the same commit.
    pub target: Option<RoomEvent>,
    /// The commit is the room's activity: it moves the room's last
    /// activity and resumes a `PAUSED` room.
    pub activity: bool,
}

impl Default for CommitEventOptions {
    fn default() -> Self {
        Self {
            target: None,
            activity: true,
        }
    }
}

/// What [`delete_event`](super::ConversationStore::delete_event) takes:
/// Python's `*, cascade_replies=True`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeleteEventOptions {
    /// The event's replies go with it.
    pub cascade_replies: bool,
}

impl Default for DeleteEventOptions {
    fn default() -> Self {
        Self {
            cascade_replies: true,
        }
    }
}

/// What [`get_event_count`](super::ConversationStore::get_event_count)
/// takes: Python's `event_filter=None`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GetEventCountOptions {
    /// Which events are counted; the received ones without one.
    pub event_filter: Option<EventFilter>,
}

/// What [`list_tasks`](super::ConversationStore::list_tasks) takes:
/// Python's `status=None`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ListTasksOptions {
    /// Only the tasks in this status.
    pub status: Option<TaskStatus>,
}

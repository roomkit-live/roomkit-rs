//! [`SqliteStore`], the [`ConversationStore`] over a SQLite file.

use std::path::Path;

use roomkit::model::{
    ChannelBinding, ChannelId, ChannelType, EventId, EventUpdates, Identity, IdentityId, Metadata,
    Observation, Participant, ParticipantId, Room, RoomContext, RoomEvent, RoomId, RoomUpdates,
    Task,
};
use roomkit::{ConversationStore, EventFilter, RoomCounters, RoomFilter, StoreError, TargetUpdate};

use crate::connection::Database;
use crate::error::OpenError;
use crate::{events, identities, lookups, members, rooms, side_effects};

/// The key of a room's private state holding its register of authors (RFC
/// §10.1 step 12, §14.1), which a context carries.
const AUTHOR_REGISTER: &str = "author_register";

/// A conversation store in a SQLite file, on the Python implementation's
/// schema (see the [crate] documentation).
///
/// Every read returns copies and every write copies in (RFC §14.4). Each
/// method that writes is one transaction: a commit's event, its room's
/// counters and its target's update are written together or not at all
/// (RFC §14.3).
#[derive(Debug, Clone)]
pub struct SqliteStore {
    database: Database,
}

impl SqliteStore {
    /// Opens the file at `path`, creating it at schema version 12, as the
    /// Python implementation's `SQLiteStore` creates it, when it does not
    /// exist or holds no schema, and its directory when it is missing. The
    /// path is taken as written: unlike Python's store, a leading `~` is not
    /// read as the home directory.
    ///
    /// # Errors
    ///
    /// Returns [`OpenError::NewerSchema`] for a file of a newer version,
    /// which is left as it is, [`OpenError::OlderSchema`] for one of an
    /// older version, [`OpenError::Directory`] when the missing directory
    /// cannot be created, and [`OpenError::Database`] when SQLite fails.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, OpenError> {
        let database = Database::open(path.as_ref().to_path_buf()).await?;
        Ok(Self { database })
    }
}

impl ConversationStore for SqliteStore {
    async fn create_room(&self, room: Room) -> Result<(), StoreError> {
        self.database
            .write(move |connection| rooms::create(connection, &room))
            .await
    }

    async fn update_room(
        &self,
        room_id: &RoomId,
        updates: RoomUpdates,
    ) -> Result<Room, StoreError> {
        let room_id = room_id.clone();
        self.database
            .write(move |connection| rooms::update(connection, &room_id, updates))
            .await
    }

    async fn get_private_state(
        &self,
        room_id: &RoomId,
        key: &str,
    ) -> Result<Option<Metadata>, StoreError> {
        let (room_id, key) = (room_id.clone(), key.to_owned());
        self.database
            .read(move |connection| rooms::private_state(connection, &room_id, &key))
            .await
    }

    async fn set_private_state(
        &self,
        room_id: &RoomId,
        key: &str,
        value: Option<Metadata>,
    ) -> Result<(), StoreError> {
        let (room_id, key) = (room_id.clone(), key.to_owned());
        self.database
            .write(move |connection| rooms::set_private_state(connection, &room_id, &key, value))
            .await
    }

    async fn get_delivered_index(&self, room_id: &RoomId) -> Result<i64, StoreError> {
        let room_id = room_id.clone();
        self.database
            .read(move |connection| rooms::delivered_index(connection, &room_id))
            .await
    }

    async fn advance_delivered_index(
        &self,
        room_id: &RoomId,
        index: u64,
        force: bool,
    ) -> Result<bool, StoreError> {
        let room_id = room_id.clone();
        self.database
            .write(move |connection| rooms::advance(connection, &room_id, index, force))
            .await
    }

    async fn get_room(&self, room_id: &RoomId) -> Result<Option<Room>, StoreError> {
        let room_id = room_id.clone();
        self.database
            .read(move |connection| Ok(rooms::get(connection, &room_id)?.map(|stored| stored.room)))
            .await
    }

    async fn list_rooms(&self, filter: &RoomFilter) -> Result<Vec<Room>, StoreError> {
        let filter = filter.clone();
        self.database
            .read(move |connection| lookups::list(connection, &filter))
            .await
    }

    async fn find_room_id_by_binding(
        &self,
        channel_id: &ChannelId,
        participant_id: &ParticipantId,
        identity_id: Option<&IdentityId>,
        filter: &RoomFilter,
    ) -> Result<Option<RoomId>, StoreError> {
        let (channel_id, participant_id) = (channel_id.clone(), participant_id.clone());
        let (identity_id, filter) = (identity_id.cloned(), filter.clone());
        self.database
            .read(move |connection| {
                lookups::find_by_binding(
                    connection,
                    &channel_id,
                    &participant_id,
                    identity_id.as_ref(),
                    &filter,
                )
            })
            .await
    }

    async fn find_room_id_by_participant(
        &self,
        channel_id: &ChannelId,
        participant_id: &ParticipantId,
        identity_id: Option<&IdentityId>,
        filter: &RoomFilter,
    ) -> Result<Option<RoomId>, StoreError> {
        let (channel_id, participant_id) = (channel_id.clone(), participant_id.clone());
        let (identity_id, filter) = (identity_id.cloned(), filter.clone());
        self.database
            .read(move |connection| {
                lookups::find_by_participant(
                    connection,
                    &channel_id,
                    &participant_id,
                    identity_id.as_ref(),
                    &filter,
                )
            })
            .await
    }

    async fn store_binding(&self, binding: ChannelBinding) -> Result<(), StoreError> {
        self.database
            .write(move |connection| members::store_binding(connection, &binding))
            .await
    }

    async fn delete_binding(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<(), StoreError> {
        let (room_id, channel_id) = (room_id.clone(), channel_id.clone());
        self.database
            .write(move |connection| {
                rooms::check(connection, &room_id)?;
                members::delete_binding(connection, &room_id, &channel_id)
            })
            .await
    }

    async fn list_bindings(&self, room_id: &RoomId) -> Result<Vec<ChannelBinding>, StoreError> {
        let room_id = room_id.clone();
        self.database
            .read(move |connection| {
                rooms::check(connection, &room_id)?;
                members::bindings(connection, &room_id)
            })
            .await
    }

    async fn load_context(
        &self,
        room_id: &RoomId,
        recent_events: usize,
    ) -> Result<Option<RoomContext>, StoreError> {
        let room_id = room_id.clone();
        self.database
            .read(move |connection| {
                let Some(stored) = rooms::get(connection, &room_id)? else {
                    return Ok(None);
                };
                Ok(Some(RoomContext {
                    room: stored.room,
                    bindings: members::bindings(connection, &room_id)?,
                    participants: members::participants(connection, &room_id)?,
                    recent_events: events::recent(connection, &room_id, recent_events)?,
                    author_register: rooms::private_state(connection, &room_id, AUTHOR_REGISTER)?,
                }))
            })
            .await
    }

    async fn create_participant(&self, participant: Participant) -> Result<(), StoreError> {
        self.database
            .write(move |connection| members::create_participant(connection, &participant))
            .await
    }

    async fn get_participant(
        &self,
        room_id: &RoomId,
        participant_id: &ParticipantId,
    ) -> Result<Option<Participant>, StoreError> {
        let (room_id, participant_id) = (room_id.clone(), participant_id.clone());
        self.database
            .read(move |connection| members::participant(connection, &room_id, &participant_id))
            .await
    }

    async fn update_participant(&self, participant: Participant) -> Result<(), StoreError> {
        self.database
            .write(move |connection| members::update_participant(connection, &participant))
            .await
    }

    async fn delete_participant(
        &self,
        room_id: &RoomId,
        participant_id: &ParticipantId,
    ) -> Result<(), StoreError> {
        let (room_id, participant_id) = (room_id.clone(), participant_id.clone());
        self.database
            .write(move |connection| {
                members::delete_participant(connection, &room_id, &participant_id)
            })
            .await
    }

    async fn list_participants(&self, room_id: &RoomId) -> Result<Vec<Participant>, StoreError> {
        let room_id = room_id.clone();
        self.database
            .read(move |connection| {
                rooms::check(connection, &room_id)?;
                members::participants(connection, &room_id)
            })
            .await
    }

    async fn create_identity(&self, identity: Identity) -> Result<(), StoreError> {
        self.database
            .write(move |connection| identities::create(connection, &identity))
            .await
    }

    async fn get_identity(&self, identity_id: &IdentityId) -> Result<Option<Identity>, StoreError> {
        let identity_id = identity_id.clone();
        self.database
            .read(move |connection| identities::get(connection, &identity_id))
            .await
    }

    async fn resolve_identity(
        &self,
        channel_type: ChannelType,
        address: &str,
        organization_id: Option<&str>,
    ) -> Result<Option<Identity>, StoreError> {
        let (address, organization_id) = (address.to_owned(), organization_id.map(str::to_owned));
        self.database
            .read(move |connection| {
                identities::resolve(
                    connection,
                    &channel_type,
                    &address,
                    organization_id.as_deref(),
                )
            })
            .await
    }

    async fn link_address(
        &self,
        identity_id: &IdentityId,
        channel_type: ChannelType,
        address: &str,
        organization_id: Option<&str>,
    ) -> Result<(), StoreError> {
        let (identity_id, address) = (identity_id.clone(), address.to_owned());
        let organization_id = organization_id.map(str::to_owned);
        self.database
            .write(move |connection| {
                identities::link(
                    connection,
                    &identity_id,
                    &channel_type,
                    &address,
                    organization_id.as_deref(),
                )
            })
            .await
    }

    async fn unlink_address(
        &self,
        identity_id: &IdentityId,
        channel_type: ChannelType,
        address: &str,
        organization_id: Option<&str>,
    ) -> Result<(), StoreError> {
        let (identity_id, address) = (identity_id.clone(), address.to_owned());
        let organization_id = organization_id.map(str::to_owned);
        self.database
            .write(move |connection| {
                identities::unlink(
                    connection,
                    &identity_id,
                    &channel_type,
                    &address,
                    organization_id.as_deref(),
                )
            })
            .await
    }

    async fn get_event_by_idempotency_key(
        &self,
        room_id: &RoomId,
        channel_id: Option<&ChannelId>,
        key: &str,
    ) -> Result<Option<RoomEvent>, StoreError> {
        let (room_id, channel_id, key) = (room_id.clone(), channel_id.cloned(), key.to_owned());
        self.database
            .read(move |connection| {
                rooms::check(connection, &room_id)?;
                events::by_key(connection, &room_id, channel_id.as_ref(), &key)
            })
            .await
    }

    async fn commit(
        &self,
        event: RoomEvent,
        counters: RoomCounters,
        target: Option<TargetUpdate>,
    ) -> Result<(), StoreError> {
        self.database
            .write(move |connection| events::commit(connection, event, counters, target))
            .await
    }

    async fn get_event(&self, event_id: &EventId) -> Result<Option<RoomEvent>, StoreError> {
        let event_id = event_id.clone();
        self.database
            .read(move |connection| events::get(connection, &event_id))
            .await
    }

    async fn update_event(
        &self,
        event_id: &EventId,
        updates: EventUpdates,
    ) -> Result<RoomEvent, StoreError> {
        let event_id = event_id.clone();
        self.database
            .write(move |connection| events::update(connection, &event_id, &updates))
            .await
    }

    async fn delete_event(
        &self,
        room_id: &RoomId,
        event_id: &EventId,
        cascade_replies: bool,
    ) -> Result<Vec<RoomEvent>, StoreError> {
        let (room_id, event_id) = (room_id.clone(), event_id.clone());
        self.database
            .write(move |connection| {
                rooms::check(connection, &room_id)?;
                events::delete(connection, &room_id, &event_id, cascade_replies)
            })
            .await
    }

    async fn get_event_count(
        &self,
        room_id: &RoomId,
        filter: Option<EventFilter>,
    ) -> Result<u64, StoreError> {
        let room_id = room_id.clone();
        self.database
            .read(move |connection| {
                rooms::check(connection, &room_id)?;
                events::count(connection, &room_id, filter)
            })
            .await
    }

    async fn persist_side_effects(
        &self,
        room_id: &RoomId,
        tasks: Vec<Task>,
        observations: Vec<Observation>,
    ) -> Result<(), StoreError> {
        let room_id = room_id.clone();
        self.database
            .write(move |connection| {
                rooms::check(connection, &room_id)?;
                for task in &tasks {
                    side_effects::store_task(connection, task)?;
                }
                for observation in &observations {
                    side_effects::store_observation(connection, observation)?;
                }
                Ok(())
            })
            .await
    }

    async fn list_events(
        &self,
        room_id: &RoomId,
        include_blocked: bool,
    ) -> Result<Vec<RoomEvent>, StoreError> {
        let room_id = room_id.clone();
        self.database
            .read(move |connection| {
                rooms::check(connection, &room_id)?;
                events::list(connection, &room_id, include_blocked)
            })
            .await
    }

    async fn list_tasks(&self, room_id: &RoomId) -> Result<Vec<Task>, StoreError> {
        let room_id = room_id.clone();
        self.database
            .read(move |connection| {
                rooms::check(connection, &room_id)?;
                side_effects::tasks(connection, &room_id)
            })
            .await
    }

    async fn list_observations(&self, room_id: &RoomId) -> Result<Vec<Observation>, StoreError> {
        let room_id = room_id.clone();
        self.database
            .read(move |connection| {
                rooms::check(connection, &room_id)?;
                side_effects::observations(connection, &room_id)
            })
            .await
    }
}

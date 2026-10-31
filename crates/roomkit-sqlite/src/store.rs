//! [`SqliteStore`], the [`ConversationStore`] over a SQLite file.

use std::path::Path;

use roomkit::model::{
    ChannelBinding, ChannelId, ChannelType, EventId, Identity, IdentityId, Metadata, Observation,
    Participant, ParticipantId, Room, RoomEvent, RoomId, Task,
};
use roomkit::store::{
    AdvanceDeliveredIndexOptions, CommitEventOptions, DeleteEventOptions,
    FindRoomIdByBindingOptions, FindRoomIdByParticipantOptions, FindRoomIdsByChannelOptions,
    FindRoomsOptions, GetEventCountOptions, LinkAddressOptions, ListEventsOptions,
    ListRoomsOptions, ListTasksOptions, PatchRoomMetadataOptions, ResolveIdentityOptions,
    UnlinkAddressOptions,
};
use roomkit::{ConversationStore, StoreError, written};

use crate::connection::Database;
use crate::error::OpenError;
use crate::{events, identities, lookups, members, rooms, side_effects, timeline};

/// A conversation store in a SQLite file, on the Python implementation's
/// schema (see the [crate] documentation).
///
/// Every read returns copies and every write copies in (RFC §14.4). Each
/// method that writes is one transaction: a commit's event, its room's
/// counters and its target are written together or not at all (RFC
/// §14.3).
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

/// What [`ConversationStore::load_room_context`] reads: Python's `(room,
/// bindings, participants)`.
type RoomRead = (Option<Room>, Vec<ChannelBinding>, Vec<Participant>);

impl ConversationStore for SqliteStore {
    async fn create_room(&self, room: Room) -> Result<Room, StoreError> {
        self.database
            .write(move |connection| {
                rooms::create(connection, &room)?;
                Ok(room)
            })
            .await
    }

    async fn get_room(&self, room_id: &RoomId) -> Result<Option<Room>, StoreError> {
        let room_id = room_id.clone();
        self.database
            .read(move |connection| Ok(rooms::get(connection, &room_id)?.map(|stored| stored.room)))
            .await
    }

    async fn update_room(&self, room: Room) -> Result<Room, StoreError> {
        self.database
            .write(move |connection| rooms::update(connection, room))
            .await
    }

    /// One transaction, as Python's store patches it: no commit falls
    /// between the read and the write.
    async fn patch_room_metadata(
        &self,
        room_id: &RoomId,
        patch: Metadata,
        options: PatchRoomMetadataOptions,
    ) -> Result<Option<Room>, StoreError> {
        let room_id = room_id.clone();
        let now = written::now();
        self.database
            .write(move |connection| {
                rooms::patch_metadata(connection, &room_id, patch, &options.unset, now)
            })
            .await
    }

    async fn delete_room(&self, room_id: &RoomId) -> Result<bool, StoreError> {
        let room_id = room_id.clone();
        self.database
            .write(move |connection| rooms::delete(connection, &room_id))
            .await
    }

    async fn list_rooms(&self, options: ListRoomsOptions) -> Result<Vec<Room>, StoreError> {
        self.database
            .read(move |connection| lookups::list(connection, options.offset, options.limit))
            .await
    }

    async fn find_rooms(&self, options: FindRoomsOptions) -> Result<Vec<Room>, StoreError> {
        self.database
            .read(move |connection| lookups::find(connection, options))
            .await
    }

    async fn find_room_ids_by_channel(
        &self,
        channel_id: &ChannelId,
        options: FindRoomIdsByChannelOptions,
    ) -> Result<Vec<RoomId>, StoreError> {
        let channel_id = channel_id.clone();
        self.database
            .read(move |connection| lookups::find_ids_by_channel(connection, &channel_id, &options))
            .await
    }

    async fn find_room_id_by_binding(
        &self,
        channel_id: &ChannelId,
        participant_id: &str,
        options: FindRoomIdByBindingOptions,
    ) -> Result<Option<RoomId>, StoreError> {
        let (channel_id, participant_id) = (channel_id.clone(), participant_id.to_owned());
        self.database
            .read(move |connection| {
                lookups::find_by_binding(connection, &channel_id, &participant_id, &options)
            })
            .await
    }

    async fn find_room_id_by_participant(
        &self,
        channel_id: &ChannelId,
        participant_id: &str,
        options: FindRoomIdByParticipantOptions,
    ) -> Result<Option<RoomId>, StoreError> {
        let (channel_id, participant_id) = (channel_id.clone(), participant_id.to_owned());
        self.database
            .read(move |connection| {
                lookups::find_by_participant(connection, &channel_id, &participant_id, &options)
            })
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
        options: AdvanceDeliveredIndexOptions,
    ) -> Result<bool, StoreError> {
        let room_id = room_id.clone();
        self.database
            .write(move |connection| rooms::advance(connection, &room_id, index, options.force))
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
            .write(move |connection| {
                rooms::set_private_state(connection, &room_id, &key, value.as_ref())
            })
            .await
    }

    async fn add_binding(&self, binding: ChannelBinding) -> Result<ChannelBinding, StoreError> {
        self.put_binding(binding).await
    }

    async fn get_binding(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<Option<ChannelBinding>, StoreError> {
        let (room_id, channel_id) = (room_id.clone(), channel_id.clone());
        self.database
            .read(move |connection| members::binding(connection, &room_id, &channel_id))
            .await
    }

    async fn update_binding(&self, binding: ChannelBinding) -> Result<ChannelBinding, StoreError> {
        self.put_binding(binding).await
    }

    async fn remove_binding(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<bool, StoreError> {
        let (room_id, channel_id) = (room_id.clone(), channel_id.clone());
        self.database
            .write(move |connection| members::remove_binding(connection, &room_id, &channel_id))
            .await
    }

    async fn list_bindings(&self, room_id: &RoomId) -> Result<Vec<ChannelBinding>, StoreError> {
        let room_id = room_id.clone();
        self.database
            .read(move |connection| members::bindings(connection, &room_id))
            .await
    }

    /// One read of the room, its bindings and its participants.
    async fn load_room_context(&self, room_id: &RoomId) -> Result<RoomRead, StoreError> {
        let room_id = room_id.clone();
        self.database
            .read(move |connection| {
                let Some(stored) = rooms::get(connection, &room_id)? else {
                    return Ok((None, Vec::new(), Vec::new()));
                };
                Ok((
                    Some(stored.room),
                    members::bindings(connection, &room_id)?,
                    members::participants(connection, &room_id)?,
                ))
            })
            .await
    }

    async fn add_participant(&self, participant: Participant) -> Result<Participant, StoreError> {
        self.database
            .write(move |connection| {
                members::create_participant(connection, &participant)?;
                Ok(participant)
            })
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

    async fn update_participant(
        &self,
        participant: Participant,
    ) -> Result<Participant, StoreError> {
        self.database
            .write(move |connection| {
                members::update_participant(connection, &participant)?;
                Ok(participant)
            })
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
            .read(move |connection| members::participants(connection, &room_id))
            .await
    }

    async fn create_identity(&self, identity: Identity) -> Result<Identity, StoreError> {
        self.database
            .write(move |connection| {
                identities::create(connection, &identity)?;
                Ok(identity)
            })
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
        channel_type: &ChannelType,
        address: &str,
        options: ResolveIdentityOptions,
    ) -> Result<Option<Identity>, StoreError> {
        let (channel_type, address) = (channel_type.clone(), address.to_owned());
        self.database
            .read(move |connection| {
                identities::resolve(
                    connection,
                    &channel_type,
                    &address,
                    options.organization_id.as_deref(),
                )
            })
            .await
    }

    async fn link_address(
        &self,
        identity_id: &IdentityId,
        channel_type: &ChannelType,
        address: &str,
        options: LinkAddressOptions,
    ) -> Result<(), StoreError> {
        let (identity_id, channel_type) = (identity_id.clone(), channel_type.clone());
        let address = address.to_owned();
        self.database
            .write(move |connection| {
                identities::link(
                    connection,
                    &identity_id,
                    &channel_type,
                    &address,
                    options.organization_id.as_deref(),
                )
            })
            .await
    }

    async fn unlink_address(
        &self,
        identity_id: &IdentityId,
        channel_type: &ChannelType,
        address: &str,
        options: UnlinkAddressOptions,
    ) -> Result<(), StoreError> {
        let (identity_id, channel_type) = (identity_id.clone(), channel_type.clone());
        let address = address.to_owned();
        self.database
            .write(move |connection| {
                identities::unlink(
                    connection,
                    &identity_id,
                    &channel_type,
                    &address,
                    options.organization_id.as_deref(),
                )
            })
            .await
    }

    async fn check_idempotency(
        &self,
        room_id: &RoomId,
        key: &str,
        channel_id: Option<&ChannelId>,
    ) -> Result<bool, StoreError> {
        let found = self
            .get_event_by_idempotency_key(room_id, key, channel_id)
            .await?;
        Ok(found.is_some())
    }

    async fn get_event_by_idempotency_key(
        &self,
        room_id: &RoomId,
        key: &str,
        channel_id: Option<&ChannelId>,
    ) -> Result<Option<RoomEvent>, StoreError> {
        let (room_id, key, channel_id) = (room_id.clone(), key.to_owned(), channel_id.cloned());
        self.database
            .read(move |connection| events::by_key(connection, &room_id, channel_id.as_ref(), &key))
            .await
    }

    async fn commit_event(
        &self,
        room_id: &RoomId,
        mut event: RoomEvent,
        options: CommitEventOptions,
    ) -> Result<RoomEvent, StoreError> {
        let CommitEventOptions {
            mut target,
            activity,
        } = options;
        written::event(&mut event)?;
        if let Some(target) = &mut target {
            written::event(target)?;
        }
        let room_id = room_id.clone();
        let at = activity.then(written::now);
        self.database
            .write(move |connection| events::commit(connection, &room_id, event, target, at))
            .await
    }

    async fn get_event(&self, event_id: &EventId) -> Result<Option<RoomEvent>, StoreError> {
        let event_id = event_id.clone();
        self.database
            .read(move |connection| events::get(connection, &event_id))
            .await
    }

    async fn update_event(&self, mut event: RoomEvent) -> Result<RoomEvent, StoreError> {
        written::event(&mut event)?;
        self.database
            .write(move |connection| events::update(connection, event))
            .await
    }

    async fn delete_event(
        &self,
        room_id: &RoomId,
        event_id: &EventId,
        options: DeleteEventOptions,
    ) -> Result<Vec<RoomEvent>, StoreError> {
        let (room_id, event_id) = (room_id.clone(), event_id.clone());
        self.database
            .write(move |connection| {
                events::delete(connection, &room_id, &event_id, options.cascade_replies)
            })
            .await
    }

    async fn get_event_count(
        &self,
        room_id: &RoomId,
        options: GetEventCountOptions,
    ) -> Result<u64, StoreError> {
        let filter = options.event_filter;
        if let Some(filter) = &filter {
            filter.validate()?;
        }
        let room_id = room_id.clone();
        self.database
            .read(move |connection| timeline::count(connection, &room_id, filter.as_ref()))
            .await
    }

    async fn list_events(
        &self,
        room_id: &RoomId,
        options: ListEventsOptions,
    ) -> Result<Vec<RoomEvent>, StoreError> {
        options.validate()?;
        let room_id = room_id.clone();
        self.database
            .read(move |connection| timeline::page(connection, &room_id, &options))
            .await
    }

    async fn add_task(&self, task: Task) -> Result<Task, StoreError> {
        self.database
            .write(move |connection| {
                rooms::check(connection, &task.room_id)?;
                side_effects::store_task(connection, &task)?;
                Ok(task)
            })
            .await
    }

    async fn add_observation(&self, observation: Observation) -> Result<Observation, StoreError> {
        self.database
            .write(move |connection| {
                rooms::check(connection, &observation.room_id)?;
                side_effects::store_observation(connection, &observation)?;
                Ok(observation)
            })
            .await
    }

    async fn list_tasks(
        &self,
        room_id: &RoomId,
        options: ListTasksOptions,
    ) -> Result<Vec<Task>, StoreError> {
        let room_id = room_id.clone();
        let tasks = self
            .database
            .read(move |connection| side_effects::tasks(connection, &room_id))
            .await?;
        Ok(tasks
            .into_iter()
            .filter(|task| options.status.is_none_or(|status| task.status == status))
            .collect())
    }

    async fn list_observations(&self, room_id: &RoomId) -> Result<Vec<Observation>, StoreError> {
        let room_id = room_id.clone();
        self.database
            .read(move |connection| side_effects::observations(connection, &room_id))
            .await
    }
}

impl SqliteStore {
    /// Stores `binding` in place of the room's binding of its channel, as
    /// Python's store writes both `add_binding` and `update_binding`.
    async fn put_binding(&self, binding: ChannelBinding) -> Result<ChannelBinding, StoreError> {
        written::binding(&binding)?;
        self.database
            .write(move |connection| {
                members::store_binding(connection, &binding)?;
                Ok(binding)
            })
            .await
    }
}

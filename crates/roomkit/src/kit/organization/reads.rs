//! What an organization reads of its rooms (RFC §17.2): a room, the rooms
//! a filter admits, a room's timeline, bindings and participants, each from
//! the kit's store. Another organization's room, or one of none, reads as a
//! room that does not exist, as the scoped calls do not find it: `None`
//! where the store reads a room or an event, the store's `RoomNotFound`
//! where it reads a room's records, its roster included (RFC §17.2: the
//! reads of a room's roster fail as not found).

use roomkit_core::model::{
    ChannelBinding, EventId, Participant, ParticipantId, Room, RoomEvent, RoomId,
};
use roomkit_core::rules::organizations;

use super::OrganizationKit;
use crate::error::KitError;
use crate::kit::TimelineQuery;
use crate::store::{ConversationStore, RoomFilter, StoreError};

impl<S: ConversationStore> OrganizationKit<S> {
    /// The room `room_id`, when it is the organization's.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Store`] when the store fails the read.
    pub async fn get_room(&self, room_id: &RoomId) -> Result<Option<Room>, KitError> {
        let room = self.kit.store().get_room(room_id).await?;
        Ok(room.filter(|room| self.reaches(room)))
    }

    /// The organization's rooms `filter` admits, its `organization_id`
    /// replaced by the organization's.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Store`] when the store fails the read.
    pub async fn list_rooms(&self, filter: &RoomFilter) -> Result<Vec<Room>, KitError> {
        let filter = RoomFilter {
            organization_id: Some(self.organization_id.clone()),
            ..filter.clone()
        };
        Ok(self.kit.store().list_rooms(&filter).await?)
    }

    /// The stored event `event_id`, when its room is the organization's.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Store`] when the store fails a read.
    pub async fn get_event(&self, event_id: &EventId) -> Result<Option<RoomEvent>, KitError> {
        let Some(event) = self.kit.store().get_event(event_id).await? else {
            return Ok(None);
        };
        let own = self.get_room(&event.room_id).await?.is_some();
        Ok(own.then_some(event))
    }

    /// A page of the timeline of the organization's room `room_id`, as
    /// [`RoomKit::get_timeline`](crate::RoomKit::get_timeline) reads it: the
    /// Python implementation's `get_timeline` with an `organization_id`.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Store`] with [`StoreError::RoomNotFound`] when the
    /// room is not the organization's or does not exist, and as
    /// [`RoomKit::get_timeline`](crate::RoomKit::get_timeline) does.
    pub async fn get_timeline(
        &self,
        room_id: &RoomId,
        query: &TimelineQuery,
    ) -> Result<Vec<RoomEvent>, KitError> {
        self.own_room(room_id).await?;
        self.kit.get_timeline(room_id, query).await
    }

    /// The bindings of the organization's room `room_id`.
    ///
    /// # Errors
    ///
    /// As [`get_timeline`](Self::get_timeline).
    pub async fn list_bindings(&self, room_id: &RoomId) -> Result<Vec<ChannelBinding>, KitError> {
        self.own_room(room_id).await?;
        Ok(self.kit.store().list_bindings(room_id).await?)
    }

    /// The participants of the organization's room `room_id`, in the order
    /// they were created (RFC §14.1).
    ///
    /// # Errors
    ///
    /// As [`get_timeline`](Self::get_timeline).
    pub async fn list_participants(&self, room_id: &RoomId) -> Result<Vec<Participant>, KitError> {
        self.own_room(room_id).await?;
        Ok(self.kit.store().list_participants(room_id).await?)
    }

    /// The participant `participant_id` of the organization's room
    /// `room_id`; `None` when the room holds no such participant.
    ///
    /// # Errors
    ///
    /// As [`get_timeline`](Self::get_timeline): a read of the roster of a
    /// room that is not the organization's fails as not found, as on a room
    /// that does not exist (RFC §17.2).
    pub async fn get_participant(
        &self,
        room_id: &RoomId,
        participant_id: &ParticipantId,
    ) -> Result<Option<Participant>, KitError> {
        self.own_room(room_id).await?;
        Ok(self
            .kit
            .store()
            .get_participant(room_id, participant_id)
            .await?)
    }

    /// Refuses a room that is not the organization's as one that does not
    /// exist. A room's organization never changes, so a read made next
    /// reads a room the check let through.
    async fn own_room(&self, room_id: &RoomId) -> Result<(), KitError> {
        match self.get_room(room_id).await? {
            Some(_) => Ok(()),
            None => Err(StoreError::RoomNotFound(room_id.clone()).into()),
        }
    }

    /// Whether `room` is the organization's.
    fn reaches(&self, room: &Room) -> bool {
        organizations::admits(Some(&self.organization_id), room)
    }
}

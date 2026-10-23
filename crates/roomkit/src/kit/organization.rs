//! The kit acting for one organization (RFC §17.2, decision 2 of step 2b):
//! a handle whose every call is scoped to the organization.

use roomkit_core::model::{
    BindingUpdate, ChannelBinding, ChannelId, DirectEvent, EventId, EventUpdates, InboundMessage,
    Room, RoomEvent, RoomId,
};
use roomkit_core::ops::InboundResult;

use super::RoomKit;
use super::scope::Scope;
use crate::error::KitError;
use crate::store::{ConversationStore, InMemoryStore};

/// The kit acting for one organization (RFC §17.2), which
/// [`RoomKit::for_organization`] returns: each of its calls is the kit's
/// call of the same name, made for the organization.
///
/// A call reaches the organization's rooms only. A room of another
/// organization, or of none, is not the organization's: a lifecycle change
/// or a record's commit does not find it, an update or a delete of a
/// stored event finds no event in it, and a message or an injected event
/// is refused, each with nothing written, the room's due timers included.
/// A message is routed among the organization's rooms and identities only,
/// and a room a call creates belongs to the organization.
///
/// Cloning the handle is cheap; it shares its kit.
///
/// ```no_run
/// # async fn example(kit: roomkit::RoomKit) -> Result<(), Box<dyn std::error::Error>> {
/// use roomkit::model::{ChannelId, EventContent, InboundMessage, RoomId};
///
/// let bank = kit.for_organization("bank");
/// let room = RoomId::new("room_1")?;
/// let sms = ChannelId::new("sms")?;
/// bank.create_room(&room).await?;
/// bank.attach(&room, &sms).await?;
/// let message = InboundMessage::new(sms, "+15551234567", EventContent::text("Bonjour"));
/// bank.process_inbound(message, Some(&room)).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct OrganizationKit<S = InMemoryStore> {
    kit: RoomKit<S>,
    organization_id: String,
}

impl<S> Clone for OrganizationKit<S> {
    fn clone(&self) -> Self {
        Self {
            kit: self.kit.clone(),
            organization_id: self.organization_id.clone(),
        }
    }
}

impl<S> RoomKit<S> {
    /// The kit acting for `organization_id` (RFC §17.2): every call of the
    /// handle reaches the organization's rooms only, and a room it creates
    /// belongs to the organization. The kit's own calls act for no
    /// organization: they read rooms unscoped, and a room they create
    /// belongs to none.
    pub fn for_organization(&self, organization_id: impl Into<String>) -> OrganizationKit<S> {
        OrganizationKit {
            kit: self.clone(),
            organization_id: organization_id.into(),
        }
    }
}

impl<S> OrganizationKit<S> {
    /// The organization the handle acts for.
    pub fn organization_id(&self) -> &str {
        &self.organization_id
    }

    /// The kit the handle acts through.
    pub fn kit(&self) -> &RoomKit<S> {
        &self.kit
    }

    fn scope(&self) -> Scope<'_, S> {
        Scope::new(&self.kit, Some(&self.organization_id))
    }
}

impl<S: ConversationStore> OrganizationKit<S> {
    /// Creates the room `room_id`, belonging to the organization, as
    /// [`RoomKit::create_room`] does.
    ///
    /// # Errors
    ///
    /// As [`RoomKit::create_room`].
    pub async fn create_room(&self, room_id: &RoomId) -> Result<Room, KitError> {
        self.scope().create_room_with(room_id, |_| {}).await
    }

    /// Creates the room `room_id`, belonging to the organization, as
    /// [`RoomKit::create_room_with`] does.
    ///
    /// # Errors
    ///
    /// As [`RoomKit::create_room_with`].
    pub async fn create_room_with(
        &self,
        room_id: &RoomId,
        configure: impl FnOnce(&mut Room),
    ) -> Result<Room, KitError> {
        self.scope().create_room_with(room_id, configure).await
    }

    /// Attaches a registered channel to a room of the organization, as
    /// [`RoomKit::attach`] does.
    ///
    /// # Errors
    ///
    /// As [`RoomKit::attach`]; a room of another organization is not found.
    pub async fn attach(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<ChannelBinding, KitError> {
        self.scope().attach_with(room_id, channel_id, |_| {}).await
    }

    /// Attaches a registered channel to a room of the organization, as
    /// [`RoomKit::attach_with`] does.
    ///
    /// # Errors
    ///
    /// As [`RoomKit::attach_with`]; a room of another organization is not
    /// found.
    pub async fn attach_with(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        configure: impl FnOnce(&mut ChannelBinding),
    ) -> Result<ChannelBinding, KitError> {
        self.scope()
            .attach_with(room_id, channel_id, configure)
            .await
    }

    /// Detaches a channel from a room of the organization, as
    /// [`RoomKit::detach`] does.
    ///
    /// # Errors
    ///
    /// As [`RoomKit::detach`]; a room of another organization is not found.
    pub async fn detach(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<ChannelBinding, KitError> {
        self.scope().detach(room_id, channel_id).await
    }

    /// Changes a channel's binding to a room of the organization, as
    /// [`RoomKit::update_binding`] does.
    ///
    /// # Errors
    ///
    /// As [`RoomKit::update_binding`]; a room of another organization is
    /// not found.
    pub async fn update_binding(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        update: BindingUpdate,
    ) -> Result<ChannelBinding, KitError> {
        self.scope()
            .update_binding(room_id, channel_id, update)
            .await
    }

    /// Mutes a channel in a room of the organization, as [`RoomKit::mute`]
    /// does.
    ///
    /// # Errors
    ///
    /// As [`update_binding`](Self::update_binding).
    pub async fn mute(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<ChannelBinding, KitError> {
        self.update_binding(room_id, channel_id, BindingUpdate::mute())
            .await
    }

    /// Unmutes a channel in a room of the organization, as
    /// [`RoomKit::unmute`] does.
    ///
    /// # Errors
    ///
    /// As [`update_binding`](Self::update_binding).
    pub async fn unmute(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<ChannelBinding, KitError> {
        self.update_binding(room_id, channel_id, BindingUpdate::unmute())
            .await
    }

    /// Pauses a room of the organization, as [`RoomKit::pause_room`] does.
    ///
    /// # Errors
    ///
    /// As [`RoomKit::pause_room`]; a room of another organization is not
    /// found.
    pub async fn pause_room(&self, room_id: &RoomId) -> Result<Room, KitError> {
        self.scope().pause_room(room_id).await
    }

    /// Resumes a room of the organization, as [`RoomKit::resume_room`]
    /// does.
    ///
    /// # Errors
    ///
    /// As [`RoomKit::resume_room`]; a room of another organization is not
    /// found.
    pub async fn resume_room(&self, room_id: &RoomId) -> Result<Room, KitError> {
        self.scope().resume_room(room_id).await
    }

    /// Reopens a room of the organization, as [`RoomKit::reopen_room`]
    /// does.
    ///
    /// # Errors
    ///
    /// As [`RoomKit::reopen_room`]; a room of another organization is not
    /// found.
    pub async fn reopen_room(&self, room_id: &RoomId) -> Result<Room, KitError> {
        self.scope().reopen_room(room_id).await
    }

    /// Closes a room of the organization, as [`RoomKit::close_room`] does.
    ///
    /// # Errors
    ///
    /// As [`RoomKit::close_room`]; a room of another organization is not
    /// found.
    pub async fn close_room(&self, room_id: &RoomId) -> Result<Room, KitError> {
        self.scope().close_room(room_id).await
    }

    /// Archives a room of the organization, as [`RoomKit::archive_room`]
    /// does.
    ///
    /// # Errors
    ///
    /// As [`RoomKit::archive_room`]; a room of another organization is not
    /// found.
    pub async fn archive_room(&self, room_id: &RoomId) -> Result<Room, KitError> {
        self.scope().archive_room(room_id).await
    }

    /// Updates a stored event of a room of the organization, as
    /// [`RoomKit::update_event`] does: a room of another organization holds
    /// none, and the call returns `None`, nothing written or fired.
    ///
    /// # Errors
    ///
    /// As [`RoomKit::update_event`].
    pub async fn update_event(
        &self,
        room_id: &RoomId,
        event_id: &EventId,
        updates: EventUpdates,
    ) -> Result<Option<RoomEvent>, KitError> {
        self.scope().update_event(room_id, event_id, updates).await
    }

    /// Deletes a stored event of a room of the organization, as
    /// [`RoomKit::delete_event`] does: a room of another organization holds
    /// none, and the call removes no id, nothing written or fired.
    ///
    /// # Errors
    ///
    /// As [`RoomKit::delete_event`].
    pub async fn delete_event(
        &self,
        room_id: &RoomId,
        event_id: &EventId,
        cascade_replies: bool,
    ) -> Result<Vec<EventId>, KitError> {
        self.scope()
            .delete_event(room_id, event_id, cascade_replies)
            .await
    }

    /// Runs `message` as [`RoomKit::process_inbound`] does, for the
    /// organization: a room of another organization, or of none, fails the
    /// message with nothing written, a message is routed among the
    /// organization's rooms and identities only, and a room the call
    /// creates belongs to the organization.
    ///
    /// # Errors
    ///
    /// As [`RoomKit::process_inbound`], with
    /// [`InboundError::OtherOrganization`](crate::InboundError::OtherOrganization)
    /// for a room of another organization.
    pub async fn process_inbound(
        &self,
        message: InboundMessage,
        room_id: Option<&RoomId>,
    ) -> Result<InboundResult, KitError> {
        self.scope().process_inbound(message, room_id).await
    }

    /// Injects `event` into a room of the organization, as
    /// [`RoomKit::send_event`] does: a room of another organization, or of
    /// none, refuses it before its hooks, nothing written (gap G-069).
    ///
    /// # Errors
    ///
    /// As [`RoomKit::send_event`], with
    /// [`InboundError::OtherOrganization`](crate::InboundError::OtherOrganization)
    /// for a room of another organization.
    pub async fn send_event(
        &self,
        room_id: &RoomId,
        event: DirectEvent,
    ) -> Result<RoomEvent, KitError> {
        self.scope().send_event(room_id, event).await
    }

    /// Commits `event` to a room of the organization outside the pipeline,
    /// as [`RoomKit::commit_event`] does (RFC §10.5): a room of another
    /// organization, or of none, is not found, nothing written in it.
    ///
    /// # Errors
    ///
    /// As [`RoomKit::commit_event`].
    pub async fn commit_event(
        &self,
        room_id: &RoomId,
        event: RoomEvent,
    ) -> Result<RoomEvent, KitError> {
        self.scope().commit_event(room_id, event).await
    }
}

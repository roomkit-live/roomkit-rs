//! The room lifecycle and channel bindings (RFC §5.1, §5.7, §7.2): the
//! kit's calls for each lifecycle operation of the core.

use roomkit_core::model::{BindingUpdate, ChannelBinding, ChannelId, Room, RoomId};
use roomkit_core::ops::{
    ArchiveRoom, AttachChannel, CloseRoom, CreateRoom, DetachChannel, PauseRoom, ReopenRoom,
    ResumeRoom, UpdateBinding,
};

use super::RoomKit;
use crate::driver;
use crate::error::KitError;
use crate::store::ConversationStore;

impl<S: ConversationStore> RoomKit<S> {
    /// Creates the room `room_id`.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Lifecycle`] when the room exists already.
    pub async fn create_room(&self, room_id: &RoomId) -> Result<Room, KitError> {
        self.create_room_with(room_id, |_| {}).await
    }

    /// Creates the room `room_id`, with what `configure` sets: its
    /// organization, its timers (RFC §5.1), its agent response policy and
    /// its metadata. Its id, status, instants and counters stay those of a
    /// new room.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Lifecycle`] when the room exists already, or
    /// when its metadata names a key of the namespace the implementation
    /// reserves (`roomkit.`, RFC §10.6.5).
    pub async fn create_room_with(
        &self,
        room_id: &RoomId,
        configure: impl FnOnce(&mut Room),
    ) -> Result<Room, KitError> {
        let now = self.inner.environment().now();
        let mut room = Room::new(room_id.clone(), now);
        configure(&mut room);
        let Room {
            organization_id,
            mut timers,
            agent_response_policy,
            metadata,
            ..
        } = room;
        timers.last_activity_at = None;
        let room = Room {
            organization_id,
            timers,
            agent_response_policy,
            metadata,
            ..Room::new(room_id.clone(), now)
        };
        let created = driver::run(
            &self.inner,
            "create_room",
            CreateRoom::new(self.config(), room),
        )
        .await?;
        Ok(created?)
    }

    /// Attaches a registered channel to a room with the RFC's default
    /// binding: read-write, unmuted, visible to all (RFC §5.7).
    ///
    /// # Errors
    ///
    /// Returns [`KitError::UnknownChannel`] for a channel not registered, and
    /// [`KitError::Lifecycle`] when the room does not exist.
    pub async fn attach(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<ChannelBinding, KitError> {
        self.attach_with(room_id, channel_id, |_| {}).await
    }

    /// Attaches a registered channel to a room, with the binding `configure`
    /// adjusts from the default: its access, mute, visibility, category,
    /// capabilities and metadata. The channel, the room, and the channel's
    /// type and direction stay what they are. An attach is explicit: it
    /// clears the channel's revocation if the channel was detached from the
    /// room (RFC §7.5 rule 7).
    ///
    /// A transport that delivers to a recipient finds it in the binding's
    /// metadata, under the key its declaration names (`phone_number`,
    /// `email_address`, ...): a delivery to a binding without one is refused
    /// `no_recipient` (RFC §10.2 step 3d).
    ///
    /// # Errors
    ///
    /// Returns [`KitError::UnknownChannel`] for a channel not registered, and
    /// [`KitError::Lifecycle`] when the room does not exist or the binding's
    /// visibility is `internal`, which only the framework writes (gap
    /// G-041).
    pub async fn attach_with(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        configure: impl FnOnce(&mut ChannelBinding),
    ) -> Result<ChannelBinding, KitError> {
        let config = self.config();
        let declaration = config
            .channel(channel_id)
            .ok_or_else(|| KitError::UnknownChannel(channel_id.clone()))?;
        let mut binding = declaration.binding(room_id.clone(), self.inner.environment().now());
        configure(&mut binding);
        // `configure` adjusts the binding's permissions and data, never which
        // channel it binds to which room, nor what the channel is.
        binding.channel_id = channel_id.clone();
        binding.room_id = room_id.clone();
        binding.channel_type = declaration.channel_type;
        binding.direction = declaration.direction;
        let attached =
            driver::run(&self.inner, "attach", AttachChannel::new(config, binding)).await?;
        Ok(attached?)
    }

    /// Detaches a channel from a room: its binding is removed, and the room
    /// keeps the revocation, so the framework never attaches the channel
    /// again on its own (RFC §7.5 rule 7). Returns the removed binding.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Lifecycle`] when the room does not exist or does
    /// not bind the channel.
    pub async fn detach(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<ChannelBinding, KitError> {
        let detach = DetachChannel::new(self.config(), room_id.clone(), channel_id.clone());
        Ok(driver::run(&self.inner, "detach", detach).await??)
    }

    /// Changes a channel's binding to a room: what `update` names changes,
    /// everything else is kept (RFC §7.5 rule 6). Returns the binding as it
    /// now stands.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Lifecycle`] when the room does not exist, does
    /// not bind the channel, or the update names the visibility `internal`
    /// (gap G-041).
    pub async fn update_binding(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        update: BindingUpdate,
    ) -> Result<ChannelBinding, KitError> {
        let operation =
            UpdateBinding::new(self.config(), room_id.clone(), channel_id.clone(), update);
        Ok(driver::run(&self.inner, "update_binding", operation).await??)
    }

    /// Mutes a channel in a room (RFC §7.2): it still reads the room's
    /// events, and its own are stored `BLOCKED`. Fires `ON_CHANNEL_MUTED`
    /// unless the channel was muted already.
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

    /// Unmutes a channel in a room (RFC §7.2). Fires `ON_CHANNEL_UNMUTED`
    /// unless the channel was not muted.
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

    /// Pauses an active room (RFC §5.1); it still accepts events, the first
    /// one delivered resumes it. Returns the room as it now stands.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Lifecycle`] when the room does not exist.
    pub async fn pause_room(&self, room_id: &RoomId) -> Result<Room, KitError> {
        let pause = PauseRoom::new(self.config(), room_id.clone());
        Ok(driver::run(&self.inner, "pause_room", pause).await??)
    }

    /// Resumes a paused room (RFC §5.1), restarting its inactivity timer.
    /// Returns the room as it now stands.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Lifecycle`] when the room does not exist.
    pub async fn resume_room(&self, room_id: &RoomId) -> Result<Room, KitError> {
        let resume = ResumeRoom::new(self.config(), room_id.clone());
        Ok(driver::run(&self.inner, "resume_room", resume).await??)
    }

    /// Reopens a closed room: it accepts events again (RFC §5.1). Returns
    /// the room as it now stands.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Lifecycle`] when the room does not exist.
    pub async fn reopen_room(&self, room_id: &RoomId) -> Result<Room, KitError> {
        let reopen = ReopenRoom::new(self.config(), room_id.clone());
        Ok(driver::run(&self.inner, "reopen_room", reopen).await??)
    }

    /// Closes a room: it refuses every new event from now on (RFC §5.1).
    /// Returns the room as it now stands.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Lifecycle`] when the room does not exist.
    pub async fn close_room(&self, room_id: &RoomId) -> Result<Room, KitError> {
        let close = CloseRoom::new(self.config(), room_id.clone());
        Ok(driver::run(&self.inner, "close_room", close).await??)
    }

    /// Archives a room: a terminal, read-only state it never leaves (RFC
    /// §5.1). Returns the room as it now stands.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Lifecycle`] when the room does not exist.
    pub async fn archive_room(&self, room_id: &RoomId) -> Result<Room, KitError> {
        let archive = ArchiveRoom::new(self.config(), room_id.clone());
        Ok(driver::run(&self.inner, "archive_room", archive).await??)
    }
}

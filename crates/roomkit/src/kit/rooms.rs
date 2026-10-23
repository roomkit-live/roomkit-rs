//! The room lifecycle and channel bindings (RFC §5.1, §5.7, §7.2): the
//! kit's calls for each lifecycle operation of the core.

use roomkit_core::model::{
    BindingOptions, BindingUpdate, ChannelBinding, ChannelId, Room, RoomId, RoomOptions,
};
use roomkit_core::ops::{
    ArchiveRoom, AttachChannel, CloseRoom, CreateRoom, DetachChannel, PauseRoom, ReopenRoom,
    ResumeRoom, UpdateBinding,
};

use super::RoomKit;
use super::scope::Scope;
use crate::driver;
use crate::error::KitError;
use crate::store::ConversationStore;

impl<S: ConversationStore> RoomKit<S> {
    /// Creates the room `room_id`, then runs its `ON_ROOM_CREATED` hooks and
    /// waits for them, each within its timeout, before it emits
    /// `room_created` (RFC §10.1 step 2): what they set up, the agent they
    /// attach for instance, is in place when the call returns (RFC Appendix
    /// B.1, gap G-050). The room belongs to no organization; one created
    /// through [`for_organization`](Self::for_organization) belongs to that
    /// organization.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Lifecycle`] when the room exists already.
    pub async fn create_room(&self, room_id: &RoomId) -> Result<Room, KitError> {
        self.unscoped()
            .create_room_with(room_id, RoomOptions::default())
            .await
    }

    /// Creates the room `room_id` with `options`: its timers (RFC §5.1), its
    /// agent response policy and its metadata. It belongs to no
    /// organization, and its status, instants and counters are a new
    /// room's. Its `ON_ROOM_CREATED` hooks are awaited, as
    /// [`create_room`](Self::create_room)'s.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Lifecycle`] when the room exists already, or
    /// when its metadata names a key of the namespace the implementation
    /// reserves (`roomkit:`, RFC §10.6.5).
    pub async fn create_room_with(
        &self,
        room_id: &RoomId,
        options: RoomOptions,
    ) -> Result<Room, KitError> {
        self.unscoped().create_room_with(room_id, options).await
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
        self.unscoped()
            .attach_with(room_id, channel_id, BindingOptions::default())
            .await
    }

    /// Attaches a registered channel to a room with the binding `options`
    /// make of the channel's default: its access, mute, visibility,
    /// correspondent, category, capabilities, limits and metadata. An
    /// attach is explicit: it clears the channel's revocation if the channel
    /// was detached from the room (RFC §7.5 rule 7).
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
        options: BindingOptions,
    ) -> Result<ChannelBinding, KitError> {
        self.unscoped()
            .attach_with(room_id, channel_id, options)
            .await
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
        self.unscoped().detach(room_id, channel_id).await
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
        self.unscoped()
            .update_binding(room_id, channel_id, update)
            .await
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
    /// one delivered resumes it. A paused room is left as it is. Returns the
    /// room as it now stands.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Lifecycle`] when the room does not exist, and
    /// `LifecycleError::TransitionRefused`, nothing written, when it is
    /// CLOSED or ARCHIVED (gap G-074).
    pub async fn pause_room(&self, room_id: &RoomId) -> Result<Room, KitError> {
        self.unscoped().pause_room(room_id).await
    }

    /// Resumes a paused room (RFC §5.1), restarting its inactivity timer. An
    /// active room is left as it is. Returns the room as it now stands.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Lifecycle`] when the room does not exist, and
    /// `LifecycleError::TransitionRefused`, nothing written, when it is
    /// CLOSED or ARCHIVED (gap G-074).
    pub async fn resume_room(&self, room_id: &RoomId) -> Result<Room, KitError> {
        self.unscoped().resume_room(room_id).await
    }

    /// Reopens a closed room: it accepts events again (RFC §5.1). Returns
    /// the room as it now stands.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Lifecycle`] when the room does not exist, and
    /// `LifecycleError::TransitionRefused`, nothing written, when it is not
    /// CLOSED: an archived room is never left (gap G-074).
    pub async fn reopen_room(&self, room_id: &RoomId) -> Result<Room, KitError> {
        self.unscoped().reopen_room(room_id).await
    }

    /// Closes a room: it refuses every new event from now on (RFC §5.1). A
    /// closed room is left as it is. Returns the room as it now stands.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Lifecycle`] when the room does not exist, and
    /// `LifecycleError::TransitionRefused`, nothing written, when it is
    /// ARCHIVED: an archived room is never left (gap G-074).
    pub async fn close_room(&self, room_id: &RoomId) -> Result<Room, KitError> {
        self.unscoped().close_room(room_id).await
    }

    /// Archives a room: a terminal, read-only state it never leaves (RFC
    /// §5.1). An archived room is left as it is. Returns the room as it now
    /// stands.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Lifecycle`] when the room does not exist.
    pub async fn archive_room(&self, room_id: &RoomId) -> Result<Room, KitError> {
        self.unscoped().archive_room(room_id).await
    }
}

impl<S: ConversationStore> Scope<'_, S> {
    /// Creates the room, belonging to the scope's organization.
    pub(super) async fn create_room_with(
        &self,
        room_id: &RoomId,
        options: RoomOptions,
    ) -> Result<Room, KitError> {
        let inner = &self.kit.inner;
        let environment = inner.environment();
        let room = options.room(room_id.clone(), self.organization_id(), environment.now());
        let create = CreateRoom::new(self.kit.config(), room);
        Ok(driver::run_on(inner, environment, "create_room", create).await??)
    }

    /// Attaches the channel to a room the scope reaches.
    pub(super) async fn attach_with(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        options: BindingOptions,
    ) -> Result<ChannelBinding, KitError> {
        let config = self.kit.config();
        let declaration = config
            .channel(channel_id)
            .ok_or_else(|| KitError::UnknownChannel(channel_id.clone()))?;
        let environment = self.kit.inner.environment();
        let binding = options.apply(declaration.binding(room_id.clone(), environment.now()));
        let attach = AttachChannel::new(config, binding, self.organization_id());
        Ok(driver::run_on(&self.kit.inner, environment, "attach", attach).await??)
    }

    /// Detaches the channel from a room the scope reaches.
    pub(super) async fn detach(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<ChannelBinding, KitError> {
        let detach = DetachChannel::new(
            self.kit.config(),
            room_id.clone(),
            channel_id.clone(),
            self.organization_id(),
        );
        Ok(driver::run(&self.kit.inner, "detach", detach).await??)
    }

    /// Changes the channel's binding to a room the scope reaches.
    pub(super) async fn update_binding(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        update: BindingUpdate,
    ) -> Result<ChannelBinding, KitError> {
        let operation = UpdateBinding::new(
            self.kit.config(),
            room_id.clone(),
            channel_id.clone(),
            update,
            self.organization_id(),
        );
        Ok(driver::run(&self.kit.inner, "update_binding", operation).await??)
    }

    /// Pauses a room the scope reaches.
    pub(super) async fn pause_room(&self, room_id: &RoomId) -> Result<Room, KitError> {
        let pause = PauseRoom::new(self.kit.config(), room_id.clone(), self.organization_id());
        Ok(driver::run(&self.kit.inner, "pause_room", pause).await??)
    }

    /// Resumes a room the scope reaches.
    pub(super) async fn resume_room(&self, room_id: &RoomId) -> Result<Room, KitError> {
        let resume = ResumeRoom::new(self.kit.config(), room_id.clone(), self.organization_id());
        Ok(driver::run(&self.kit.inner, "resume_room", resume).await??)
    }

    /// Reopens a room the scope reaches.
    pub(super) async fn reopen_room(&self, room_id: &RoomId) -> Result<Room, KitError> {
        let reopen = ReopenRoom::new(self.kit.config(), room_id.clone(), self.organization_id());
        Ok(driver::run(&self.kit.inner, "reopen_room", reopen).await??)
    }

    /// Closes a room the scope reaches.
    pub(super) async fn close_room(&self, room_id: &RoomId) -> Result<Room, KitError> {
        let close = CloseRoom::new(self.kit.config(), room_id.clone(), self.organization_id());
        Ok(driver::run(&self.kit.inner, "close_room", close).await??)
    }

    /// Archives a room the scope reaches.
    pub(super) async fn archive_room(&self, room_id: &RoomId) -> Result<Room, KitError> {
        let archive = ArchiveRoom::new(self.kit.config(), room_id.clone(), self.organization_id());
        Ok(driver::run(&self.kit.inner, "archive_room", archive).await??)
    }
}

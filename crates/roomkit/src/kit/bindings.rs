//! A room's channel bindings and members (RFC §5.5, §5.7, §7.2): the kit's
//! calls for each, for the organization their options name (RFC §17.2).

use roomkit_core::model::{
    Access, BindingUpdate, ChannelBinding, ChannelCategory, ChannelId, Metadata, Participant,
    ParticipantStatus, RoomId, Visibility,
};
use roomkit_core::ops::{AttachChannel, DetachChannel, LifecycleError, UpdateBinding};

use super::RoomKit;
use super::events::room_work;
use super::options::{
    AttachChannelOptions, DetachChannelOptions, GetBindingOptions, ListBindingsOptions,
    ListMembersOptions, MuteOptions, SetAccessOptions, SetVisibilityOptions, UnmuteOptions,
    UpdateBindingMetadataOptions,
};
use crate::driver;
use crate::error::KitError;
use crate::store::ConversationStore;

impl<S: ConversationStore> RoomKit<S> {
    /// Attaches a registered channel to a room with the binding `options`
    /// make of the channel's default: its category and direction (the
    /// channel's unless named), access, visibility, mute, correspondent,
    /// read horizon, attach instant, limits and metadata (RFC §5.7). An
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
    /// [`KitError::Lifecycle`] when the room does not exist or is not of the
    /// organization `options` names, or the binding's visibility is
    /// `internal`, which only the framework writes (gap G-041).
    pub async fn attach_channel(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        options: AttachChannelOptions,
    ) -> Result<ChannelBinding, KitError> {
        let config = self.config();
        let declaration = config
            .channel(channel_id)
            .ok_or_else(|| KitError::UnknownChannel(channel_id.clone()))?;
        let environment = self.inner.environment();
        let organization_id = options.organization_id.clone();
        let binding = options.apply(declaration.binding(room_id.clone(), environment.now()));
        let attach = AttachChannel::new(config, binding, organization_id);
        let attach = match room_work(room_id) {
            Some((draw, floor)) => attach.made_in(draw, floor),
            None => attach,
        };
        let binding = driver::run_on(&self.inner, environment, "attach_channel", attach).await??;
        if binding.category == ChannelCategory::Intelligence {
            self.inner.turns.attached(&binding);
        }
        Ok(binding)
    }

    /// Detaches a channel from a room: its binding is removed, and the room
    /// keeps the revocation, so the framework never attaches the channel
    /// again on its own (RFC §7.5 rule 7). Returns whether the room bound
    /// the channel: `false` changes nothing, as in a room the store lacks
    /// when the call names no organization, as the Python implementation's
    /// `detach_channel` returns.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Lifecycle`] when the room is not of the
    /// organization `options` names, or does not exist and the call names
    /// one.
    pub async fn detach_channel(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        options: DetachChannelOptions,
    ) -> Result<bool, KitError> {
        let scoped = options.organization_id.is_some();
        let detach = DetachChannel::new(
            self.config(),
            room_id.clone(),
            channel_id.clone(),
            options.organization_id,
        );
        let detach = match room_work(room_id) {
            Some((draw, floor)) => detach.made_in(draw, floor),
            None => detach,
        };
        let binding = match driver::run(&self.inner, "detach_channel", detach).await? {
            Ok(binding) => binding,
            Err(LifecycleError::ChannelNotBound { .. }) => return Ok(false),
            Err(LifecycleError::RoomNotFound(_)) if !scoped => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        if binding.category == ChannelCategory::Intelligence {
            self.inner
                .turns
                .detached(&binding.channel_id, &binding.room_id);
        }
        Ok(true)
    }

    /// Mutes a channel in a room (RFC §7.2): it still reads the room's
    /// events, and its own are stored `BLOCKED`. Fires `ON_CHANNEL_MUTED`
    /// unless the channel was muted already. Returns the binding as it now
    /// stands.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Lifecycle`] when the room does not exist, is not
    /// of the organization `options` names, or does not bind the channel.
    pub async fn mute(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        options: MuteOptions,
    ) -> Result<ChannelBinding, KitError> {
        let update = BindingUpdate::mute();
        self.update_binding(room_id, channel_id, update, options.organization_id)
            .await
    }

    /// Unmutes a channel in a room (RFC §7.2). Fires `ON_CHANNEL_UNMUTED`
    /// unless the channel was not muted.
    ///
    /// # Errors
    ///
    /// As [`mute`](Self::mute).
    pub async fn unmute(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        options: UnmuteOptions,
    ) -> Result<ChannelBinding, KitError> {
        let update = BindingUpdate::unmute();
        self.update_binding(room_id, channel_id, update, options.organization_id)
            .await
    }

    /// Sets whether a channel reads, writes, both or neither in a room (RFC
    /// §7.1); the access the binding holds already writes nothing.
    ///
    /// # Errors
    ///
    /// As [`mute`](Self::mute).
    pub async fn set_access(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        access: Access,
        options: SetAccessOptions,
    ) -> Result<ChannelBinding, KitError> {
        let update = BindingUpdate {
            access: Some(access),
            ..BindingUpdate::default()
        };
        self.update_binding(room_id, channel_id, update, options.organization_id)
            .await
    }

    /// Sets who sees what a channel writes in a room (RFC §7.3).
    ///
    /// # Errors
    ///
    /// As [`mute`](Self::mute), and [`KitError::Lifecycle`] for the
    /// visibility `internal`, which only the framework writes (gap G-041).
    pub async fn set_visibility(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        visibility: Visibility,
        options: SetVisibilityOptions,
    ) -> Result<ChannelBinding, KitError> {
        let update = BindingUpdate::visibility(visibility);
        self.update_binding(room_id, channel_id, update, options.organization_id)
            .await
    }

    /// Merges `metadata` into a channel's binding to a room, key by key; the
    /// keys it does not name are kept (RFC §7.5 rule 6).
    ///
    /// # Errors
    ///
    /// As [`mute`](Self::mute).
    pub async fn update_binding_metadata(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        metadata: Metadata,
        options: UpdateBindingMetadataOptions,
    ) -> Result<ChannelBinding, KitError> {
        let update = BindingUpdate {
            metadata,
            ..BindingUpdate::default()
        };
        self.update_binding(room_id, channel_id, update, options.organization_id)
            .await
    }

    /// The channel's binding to a room, as the store holds it.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Store`] with `RoomNotFound` when the room is not
    /// of the organization `options` names, or does not exist and the call
    /// names one, and [`KitError::Lifecycle`] with
    /// [`LifecycleError::ChannelNotBound`] when the room does not bind the
    /// channel.
    pub async fn get_binding(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        options: GetBindingOptions,
    ) -> Result<ChannelBinding, KitError> {
        let bindings = self
            .list_bindings(
                room_id,
                ListBindingsOptions {
                    organization_id: options.organization_id,
                },
            )
            .await?;
        bindings
            .into_iter()
            .find(|binding| &binding.channel_id == channel_id)
            .ok_or_else(|| {
                KitError::Lifecycle(LifecycleError::ChannelNotBound {
                    room_id: room_id.clone(),
                    channel_id: channel_id.clone(),
                })
            })
    }

    /// The room's bindings, as the store holds them. The room is checked
    /// only for a call that names an organization, as the Python
    /// implementation's `list_bindings` does: otherwise the store answers.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Store`] with `RoomNotFound` when the room is not
    /// of the organization `options` names, or does not exist and the call
    /// names one, and when the store fails the read.
    pub async fn list_bindings(
        &self,
        room_id: &RoomId,
        options: ListBindingsOptions,
    ) -> Result<Vec<ChannelBinding>, KitError> {
        self.reached(room_id, options.organization_id.as_deref())
            .await?;
        Ok(self.store().list_bindings(room_id).await?)
    }

    /// The room's members, ACTIVE ones only unless `options` include those
    /// who left, in the order they joined (RFC §5.5, §14.1). The room is
    /// checked only for a call that names an organization, as
    /// [`list_bindings`](Self::list_bindings)'s.
    ///
    /// # Errors
    ///
    /// As [`list_bindings`](Self::list_bindings).
    pub async fn list_members(
        &self,
        room_id: &RoomId,
        options: ListMembersOptions,
    ) -> Result<Vec<Participant>, KitError> {
        self.reached(room_id, options.organization_id.as_deref())
            .await?;
        let mut members = self.store().list_participants(room_id).await?;
        if !options.include_left {
            members.retain(|member| member.status == ParticipantStatus::Active);
        }
        Ok(members)
    }

    /// Changes the channel's binding to a room the organization reaches:
    /// what `update` names changes, everything else is kept (RFC §7.5 rule
    /// 6).
    async fn update_binding(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        update: BindingUpdate,
        organization_id: Option<String>,
    ) -> Result<ChannelBinding, KitError> {
        let operation = UpdateBinding::new(
            self.config(),
            room_id.clone(),
            channel_id.clone(),
            update,
            organization_id,
        );
        let operation = match room_work(room_id) {
            Some((draw, floor)) => operation.made_in(draw, floor),
            None => operation,
        };
        Ok(driver::run(&self.inner, "update_binding", operation).await??)
    }
}

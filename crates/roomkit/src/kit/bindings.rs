//! A room's channel bindings and members (RFC §5.5, §5.7, §7.2): the kit's
//! calls for each, for the organization their options name (RFC §17.2).

use roomkit_core::model::{
    Access, BindingUpdate, ChannelBinding, ChannelId, Metadata, Participant, ParticipantStatus,
    RoomId, Visibility,
};
use roomkit_core::ops::{AttachChannel, DetachChannel, LifecycleError, UpdateBinding};

use super::RoomKit;
use super::addresses::named_correspondent;
use super::events::room_work;
use super::options::{
    AttachChannelOptions, DetachChannelOptions, GetBindingOptions, GetRoomOptions,
    ListBindingsOptions, ListMembersOptions, MuteOptions, SetAccessOptions, SetVisibilityOptions,
    UnmuteOptions, UpdateBindingMetadataOptions,
};
use crate::driver;
use crate::error::RoomKitError;
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
    /// Refuses in the Python implementation's order (`channel_ops.py`):
    /// [`RoomKitError::Value`] for a rate limit or a retry policy its model
    /// refuses, a value not above 0, as Python's model refuses it when built;
    /// then [`RoomKitError::Value`] for the visibility `internal`, which only the
    /// framework writes (gap G-041); then [`RoomKitError::RoomNotFound`] when
    /// the room does not exist or is not of the organization `options`
    /// names; then [`RoomKitError::ChannelNotRegistered`] for a channel not
    /// registered; then [`RoomKitError::Value`] for a host channel bound as
    /// an intelligence channel (RFC §6.2, §10.5); then
    /// [`RoomKitError::Channel`] for a transport that refused the attach in
    /// its [`on_room_attached`](crate::Channel::on_room_attached), the
    /// binding it replaced put back (RFC §6.1).
    pub async fn attach_channel(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        options: AttachChannelOptions,
    ) -> Result<ChannelBinding, RoomKitError> {
        // A rate limit or a retry policy a struct literal built past its
        // model's check is refused first, as Python's model refuses it when
        // built, before the call.
        if let Some(rate_limit) = &options.rate_limit {
            rate_limit.check()?;
        }
        if let Some(retry_policy) = &options.retry_policy {
            retry_policy.check()?;
        }
        if options.visibility == Visibility::Internal {
            return Err(LifecycleError::InternalVisibility.into());
        }
        let config = self.config();
        let Some(declaration) = config.channel(channel_id) else {
            let organization_id = options.organization_id;
            return Err(self
                .unregistered(room_id, channel_id, organization_id)
                .await);
        };
        if self.inner.discussions.get(room_id).is_some() {
            // The room in the caller's scope first: another tenant's reads as
            // not found, whatever it holds (RFC §17.2).
            let scope = GetRoomOptions {
                organization_id: options.organization_id.clone(),
            };
            self.get_room(room_id, scope).await?;
            self.refuse_discussion_binding(
                room_id,
                channel_id,
                options.category.unwrap_or(declaration.category),
                &declaration.channel_type,
            )?;
        }
        let environment = self.inner.environment();
        let organization_id = options.organization_id.clone();
        let binding = options.apply(declaration.binding(room_id.clone(), environment.now()));
        let binding = match self.inner.channel(channel_id) {
            Some(channel) => {
                named_correspondent(binding, &*channel, declaration.recipient_key.as_deref())
            }
            None => binding,
        };
        let attach = AttachChannel::new(config, binding, organization_id);
        let attach = match room_work(room_id) {
            Some((draw, floor)) => attach.made_in(draw, floor),
            None => attach,
        };
        Ok(driver::run_on(&self.inner, environment, "attach_channel", attach).await??)
    }

    /// Why an attach of `channel_id`, which no channel is registered under,
    /// into `room_id` fails: the room's absence first, for the organization
    /// `organization_id` names, then the channel's.
    async fn unregistered(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        organization_id: Option<String>,
    ) -> RoomKitError {
        match self
            .get_room(room_id, GetRoomOptions { organization_id })
            .await
        {
            Ok(_) => RoomKitError::channel_not_registered(channel_id.clone()),
            Err(error) => error,
        }
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
    /// Returns [`RoomKitError::RoomNotFound`] when the room is not of the
    /// organization `options` names, or does not exist and the call names
    /// one; [`RoomKitError::Channel`] when a transport's
    /// [`on_room_detached`](crate::Channel::on_room_detached) failed, the
    /// detach done and announced all the same (RFC §6.1).
    pub async fn detach_channel(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        options: DetachChannelOptions,
    ) -> Result<bool, RoomKitError> {
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
        match driver::run(&self.inner, "detach_channel", detach).await? {
            Ok(_) => Ok(true),
            Err(LifecycleError::ChannelNotBound { .. }) => Ok(false),
            Err(LifecycleError::RoomNotFound(_)) if !scoped => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    /// Mutes a channel in a room (RFC §7.2): it still reads the room's
    /// events, and its own are stored `BLOCKED`. Fires `ON_CHANNEL_MUTED`
    /// unless the channel was muted already. Returns the binding as it now
    /// stands.
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::RoomNotFound`] when the room does not exist or
    /// is not of the organization `options` names, and
    /// [`RoomKitError::ChannelNotFound`] when it does not bind the channel.
    pub async fn mute(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        options: MuteOptions,
    ) -> Result<ChannelBinding, RoomKitError> {
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
    ) -> Result<ChannelBinding, RoomKitError> {
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
    ) -> Result<ChannelBinding, RoomKitError> {
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
    /// As [`mute`](Self::mute), and [`RoomKitError::Value`] for the
    /// visibility `internal`, which only the framework writes (gap G-041).
    pub async fn set_visibility(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        visibility: Visibility,
        options: SetVisibilityOptions,
    ) -> Result<ChannelBinding, RoomKitError> {
        let update = BindingUpdate::visibility(visibility);
        self.update_binding(room_id, channel_id, update, options.organization_id)
            .await
    }

    /// Merges `metadata` into a channel's binding to a room, key by key; the
    /// keys it does not name are kept (RFC §7.5 rule 6). The channel then
    /// writes the binding's addresses as an attach does (RFC §5.7, §10.4):
    /// the recipient in its form, and a binding naming no one, not `group`,
    /// naming that recipient as its correspondent.
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
    ) -> Result<ChannelBinding, RoomKitError> {
        let operation = UpdateBinding::metadata(
            self.config(),
            room_id.clone(),
            channel_id.clone(),
            metadata,
            options.organization_id,
        );
        let operation = in_room_work(room_id, operation);
        Ok(driver::run(&self.inner, "update_binding_metadata", operation).await??)
    }

    /// The channel's binding to a room, as the store holds it.
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::RoomNotFound`] when the room is not
    /// of the organization `options` names, or does not exist and the call
    /// names one, and [`RoomKitError::ChannelNotFound`] when the room does
    /// not bind the channel.
    pub async fn get_binding(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        options: GetBindingOptions,
    ) -> Result<ChannelBinding, RoomKitError> {
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
            .ok_or_else(|| RoomKitError::ChannelNotFound {
                room_id: room_id.clone(),
                channel_id: channel_id.clone(),
            })
    }

    /// The room's bindings, as the store holds them. The room is checked
    /// only for a call that names an organization, as the Python
    /// implementation's `list_bindings` does: otherwise the store answers.
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::RoomNotFound`] when the room is not
    /// of the organization `options` names, or does not exist and the call
    /// names one, and when the store fails the read.
    pub async fn list_bindings(
        &self,
        room_id: &RoomId,
        options: ListBindingsOptions,
    ) -> Result<Vec<ChannelBinding>, RoomKitError> {
        self.reached(room_id, options.organization_id.as_deref())
            .await?;
        self.store()
            .list_bindings(room_id)
            .await
            .map_err(|error| self.read_failed(error))
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
    ) -> Result<Vec<Participant>, RoomKitError> {
        self.reached(room_id, options.organization_id.as_deref())
            .await?;
        let mut members = self
            .store()
            .list_participants(room_id)
            .await
            .map_err(|error| self.read_failed(error))?;
        if !options.include_left {
            members.retain(|member| member.status == ParticipantStatus::Active);
        }
        Ok(members)
    }

    /// Changes the channel's binding to a room the organization reaches:
    /// what `update` names changes, everything else is kept (RFC §7.5 rule
    /// 6).
    pub(super) async fn update_binding(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        update: BindingUpdate,
        organization_id: Option<String>,
    ) -> Result<ChannelBinding, RoomKitError> {
        let operation = UpdateBinding::new(
            self.config(),
            room_id.clone(),
            channel_id.clone(),
            update,
            organization_id,
        );
        let operation = in_room_work(room_id, operation);
        Ok(driver::run(&self.inner, "update_binding", operation).await??)
    }
}

/// `operation`, a change of a binding of `room_id`, made in the room's work
/// when the call comes from it (RFC §8.3, §10.3).
fn in_room_work(room_id: &RoomId, operation: UpdateBinding) -> UpdateBinding {
    match room_work(room_id) {
        Some((draw, floor)) => operation.made_in(draw, floor),
        None => operation,
    }
}

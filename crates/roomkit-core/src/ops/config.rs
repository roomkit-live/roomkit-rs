//! What a kit knows of itself: its channels, its hooks and its limits.

use std::collections::BTreeMap;
use std::time::Duration;

use thiserror::Error;

use crate::model::{
    ChannelBinding, ChannelCapabilities, ChannelCategory, ChannelDirection, ChannelId,
    ChannelMediaType, ChannelType, RoomId, Timestamp,
};
use crate::rules::addresses::{self, AddressForm, Addressing, CountryCode};
use crate::rules::chain_depth::DEFAULT_MAX_CHAIN_DEPTH;
use crate::rules::hook_registry::HookRegistry;
use crate::rules::host;

/// How many of a room's latest events a context holds by default.
const DEFAULT_CONTEXT_WINDOW: usize = 50;

/// A registered channel, as the core knows it: its declaration, never the
/// object behind it (RFC §10.6.2).
#[derive(Clone, Debug, PartialEq)]
pub struct ChannelDeclaration {
    /// The channel's id.
    pub channel_id: ChannelId,
    /// Its type, an open name (RFC §6.2): what it means for the channel is
    /// the rest of the declaration.
    pub channel_type: ChannelType,
    /// What it does.
    pub category: ChannelCategory,
    /// What it can do physically.
    pub direction: ChannelDirection,
    /// What it supports.
    pub capabilities: ChannelCapabilities,
    /// The provider or backend behind it, for event attribution (RFC §5.2).
    pub provider_name: Option<String>,
    /// How its senders are addressed (RFC §5.7, gap G-051): its type's
    /// default ([`Addressing::of`]), as written for a name the RFC does not
    /// list, unless the channel sets another.
    pub addressing: Addressing,
    /// The binding metadata key it delivers to (RFC §5.7, Appendix A):
    /// `phone_number`, `email_address`, ... A transport delivery whose
    /// binding holds no recipient under it is refused before any send (RFC
    /// §10.2 step 3d). `None` for a channel that delivers to a destination of
    /// its own, a webhook's URL or a WebSocket's connections, which needs no
    /// recipient.
    pub recipient_key: Option<String>,
    /// The country code a phone number written without one takes (RFC
    /// §5.7); `None` keeps such a number as written.
    pub country_code: Option<CountryCode>,
    /// The metadata key under which an inbound message carries the chat it
    /// was posted in, which routing addresses it by (RFC §5.7, §10.4, gaps
    /// G-051 and G-057): `chat_id` for Telegram, `conversation_id` for Teams
    /// and `group_jid` for WhatsApp Personal by default. On a channel
    /// carrying private chats and groups (WhatsApp Personal), a message
    /// without the key is addressed by its sender.
    pub chat_key: Option<String>,
}

impl ChannelDeclaration {
    /// A channel of `channel_type` with the RFC's defaults: the default
    /// capabilities, no provider name, no country code, and the addressing,
    /// recipient key and chat key of its type ([`Addressing::of`],
    /// `addresses::recipient_key`, `addresses::chat_key`, gaps G-051 and
    /// G-057), which a name the RFC does not list has none of: its senders
    /// are addressed as written, with no recipient and no chat key, unless
    /// the channel sets them.
    pub fn new(
        channel_id: ChannelId,
        channel_type: ChannelType,
        category: ChannelCategory,
        direction: ChannelDirection,
    ) -> Self {
        Self {
            channel_id,
            addressing: Addressing::of(&channel_type),
            recipient_key: addresses::recipient_key(&channel_type).map(str::to_owned),
            chat_key: addresses::chat_key(&channel_type).map(str::to_owned),
            channel_type,
            category,
            direction,
            capabilities: ChannelCapabilities::default(),
            provider_name: None,
            country_code: None,
        }
    }

    /// A host channel named `channel_id` (RFC §10.5, Appendix A.14.1): a
    /// transport of type `HOST`, `INBOUND`, never delivered to, declaring
    /// every media type, since a post may carry any content, and no
    /// recipient.
    pub fn host(channel_id: ChannelId) -> Self {
        let declaration = Self::new(
            channel_id,
            ChannelType::HOST,
            ChannelCategory::Transport,
            ChannelDirection::Inbound,
        );
        Self {
            capabilities: ChannelCapabilities {
                media_types: ChannelMediaType::all(),
                ..ChannelCapabilities::default()
            },
            provider_name: Some("HostChannel".to_owned()),
            ..declaration
        }
    }

    /// The form its bindings' and participants' addresses are compared in
    /// (RFC §5.7).
    pub fn form(&self) -> AddressForm {
        AddressForm {
            addressing: self.addressing,
            country_code: self.country_code.clone(),
            recipient_key: self.recipient_key.clone(),
        }
    }

    /// Its binding to `room_id` at the RFC's defaults: read-write, unmuted,
    /// visible to all, naming no correspondent and no recipient, with the
    /// channel's capabilities (RFC §5.7).
    pub fn binding(&self, room_id: RoomId, attached_at: Timestamp) -> ChannelBinding {
        let mut binding = ChannelBinding::new(
            self.channel_id.clone(),
            room_id,
            self.channel_type.clone(),
            self.category,
            self.direction,
            attached_at,
        );
        binding.capabilities = self.capabilities.clone();
        binding
    }
}

/// Why a kit configuration change was refused.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// Another channel is registered under the id.
    #[error("a channel {0} is already registered")]
    DuplicateChannel(ChannelId),
    /// No channel is registered under the id.
    #[error("no channel {0} is registered")]
    UnknownChannel(ChannelId),
    /// The channel claims a type reserved for what it does not do: `system`,
    /// or `host` for a channel other than a host channel (RFC §6.2, §10.5).
    #[error("channel {channel_id} claims the reserved type {channel_type}")]
    ReservedChannelType {
        /// The channel.
        channel_id: ChannelId,
        /// The type it claims.
        channel_type: ChannelType,
    },
}

/// A kit's configuration: what the operations read and never change.
#[derive(Clone, Debug)]
pub struct KitConfig {
    channels: BTreeMap<ChannelId, ChannelDeclaration>,
    /// The kit's hooks (RFC §9.1).
    pub hooks: HookRegistry,
    /// The chain-depth limit (RFC §8.3), 5 by default.
    pub max_chain_depth: u32,
    /// How many of a room's latest events a context holds.
    pub context_window: usize,
    /// How long a call may take from its start to its decision (RFC
    /// §13.6): past it the call is refused `process_timeout`, with nothing
    /// of its event written. `None`, the default, bounds nothing.
    pub process_timeout: Option<Duration>,
}

impl KitConfig {
    /// A kit with no channel, no hook and the default limits.
    pub fn new() -> Self {
        Self {
            channels: BTreeMap::new(),
            hooks: HookRegistry::new(),
            max_chain_depth: DEFAULT_MAX_CHAIN_DEPTH,
            context_window: DEFAULT_CONTEXT_WINDOW,
            process_timeout: None,
        }
    }

    /// Registers a channel.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::DuplicateChannel`] when the id is taken, and
    /// [`ConfigError::ReservedChannelType`] for a channel claiming `system`,
    /// or `host` without being a host channel, a transport whose direction is
    /// `INBOUND` (RFC §6.2, §10.5).
    pub fn register_channel(&mut self, declaration: ChannelDeclaration) -> Result<(), ConfigError> {
        if self.channels.contains_key(&declaration.channel_id) {
            return Err(ConfigError::DuplicateChannel(declaration.channel_id));
        }
        if host::claims_reserved_type(
            &declaration.channel_type,
            declaration.category,
            declaration.direction,
        ) {
            return Err(ConfigError::ReservedChannelType {
                channel_id: declaration.channel_id,
                channel_type: declaration.channel_type,
            });
        }
        self.channels
            .insert(declaration.channel_id.clone(), declaration);
        Ok(())
    }

    /// Unregisters the channel `id`, returning its declaration. Its
    /// bindings stay in their rooms, as the store holds them (gap G-048).
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::UnknownChannel`] when no channel has the id.
    pub fn unregister_channel(
        &mut self,
        id: &ChannelId,
    ) -> Result<ChannelDeclaration, ConfigError> {
        self.channels
            .remove(id)
            .ok_or_else(|| ConfigError::UnknownChannel(id.clone()))
    }

    /// The registered channel `id`.
    pub fn channel(&self, id: &ChannelId) -> Option<&ChannelDeclaration> {
        self.channels.get(id)
    }
}

impl Default for KitConfig {
    fn default() -> Self {
        Self::new()
    }
}

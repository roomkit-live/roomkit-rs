//! What a kit knows of itself: its channels, its hooks and its limits.

use std::collections::BTreeMap;

use thiserror::Error;

use crate::model::{
    ChannelBinding, ChannelCapabilities, ChannelCategory, ChannelDirection, ChannelId, ChannelType,
    RoomId, Timestamp,
};
use crate::rules::addresses::{self, Addressing, CountryCode};
use crate::rules::chain_depth::DEFAULT_MAX_CHAIN_DEPTH;
use crate::rules::hook_registry::HookRegistry;

/// How many of a room's latest events a context holds by default.
pub const DEFAULT_CONTEXT_WINDOW: usize = 50;

/// A registered channel, as the core knows it: its declaration, never the
/// object behind it (RFC §10.6.2).
#[derive(Clone, Debug, PartialEq)]
pub struct ChannelDeclaration {
    /// The channel's id.
    pub channel_id: ChannelId,
    /// Its type.
    pub channel_type: ChannelType,
    /// What it does.
    pub category: ChannelCategory,
    /// What it can do physically.
    pub direction: ChannelDirection,
    /// What it supports.
    pub capabilities: ChannelCapabilities,
    /// The provider or backend behind it, for event attribution (RFC §5.2).
    pub provider_name: Option<String>,
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
}

impl ChannelDeclaration {
    /// A channel of `channel_type` with the RFC's defaults: the default
    /// capabilities, no provider name, no country code, and the recipient
    /// key Appendix A gives its type (gap G-051).
    pub fn new(
        channel_id: ChannelId,
        channel_type: ChannelType,
        category: ChannelCategory,
        direction: ChannelDirection,
    ) -> Self {
        Self {
            channel_id,
            channel_type,
            category,
            direction,
            capabilities: ChannelCapabilities::default(),
            provider_name: None,
            recipient_key: addresses::recipient_key(channel_type).map(str::to_owned),
            country_code: None,
        }
    }

    /// How its senders are addressed, which their type decides (RFC §5.7,
    /// gap G-051).
    pub fn addressing(&self) -> Addressing {
        Addressing::of(self.channel_type)
    }

    /// Its binding to `room_id` at the RFC's defaults: read-write, unmuted,
    /// visible to all, naming no correspondent and no recipient, with the
    /// channel's capabilities (RFC §5.7).
    pub fn binding(&self, room_id: RoomId, attached_at: Timestamp) -> ChannelBinding {
        let mut binding = ChannelBinding::new(
            self.channel_id.clone(),
            room_id,
            self.channel_type,
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
}

impl KitConfig {
    /// A kit with no channel, no hook and the default limits.
    pub fn new() -> Self {
        Self {
            channels: BTreeMap::new(),
            hooks: HookRegistry::new(),
            max_chain_depth: DEFAULT_MAX_CHAIN_DEPTH,
            context_window: DEFAULT_CONTEXT_WINDOW,
        }
    }

    /// Registers a channel.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::DuplicateChannel`] when the id is taken.
    pub fn register_channel(&mut self, declaration: ChannelDeclaration) -> Result<(), ConfigError> {
        if self.channels.contains_key(&declaration.channel_id) {
            return Err(ConfigError::DuplicateChannel(declaration.channel_id));
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

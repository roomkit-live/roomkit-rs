//! What a kit knows of itself: its channels, its hooks and its limits.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use thiserror::Error;

use crate::model::{
    AgentResponsePolicy, ChannelBinding, ChannelCapabilities, ChannelCategory, ChannelDirection,
    ChannelId, ChannelMediaType, ChannelType, RoomId, Timestamp,
};
use crate::rules::addresses::{self, AddressForm, Addressing, CountryCode};
use crate::rules::chain_depth::DEFAULT_MAX_CHAIN_DEPTH;
use crate::rules::hook_registry::HookRegistry;
use crate::rules::host;

/// The most of a room's latest events a context holds, whatever its
/// channels declare (roomkit's `_RECENT_EVENTS_LIMIT`).
pub const RECENT_EVENTS_LIMIT: usize = 2_000;

/// The least of a room's latest events a context holds while a hook may
/// read them (roomkit's `_RECENT_EVENTS_FLOOR`).
pub const RECENT_EVENTS_FLOOR: usize = 50;

/// A registered channel, as the core knows it: its declaration, never the
/// object behind it (RFC §10.6.2).
#[derive(Clone, Debug, PartialEq)]
pub struct ChannelConfig {
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
    /// default (as [`addressing_of`] reads it), as written for a name the
    /// RFC does not list, unless the channel sets another.
    ///
    /// [`addressing_of`]: crate::rules::addresses::addressing_of
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
    /// Whether the channel, a transport, takes a streamed response as it is
    /// written (RFC §6.1 `supports_streaming_delivery`): the first transport
    /// a response's rows reach that does reads its stream through its
    /// `deliver_stream()`, the text as it comes and each row once the store
    /// holds it, and is not delivered those rows again (RFC §12.2 steps 13s
    /// and 14s). `false` by default.
    pub supports_streaming_delivery: bool,
    /// How many of a room's latest events its turns read (RFC §6.1
    /// `recent_events_window`): what a context of a room it is bound to
    /// holds at least. None by default: a transport reads no history.
    pub recent_events_window: usize,
    /// Whether the channel names its own senders: its inbound `sender_id`
    /// is a room's `Participant.id`, not an address, which no identity
    /// resolver is asked about (RFC §11.6). `false` by default.
    pub sender_is_participant: bool,
}

impl ChannelConfig {
    /// A channel of `channel_type` with the RFC's defaults: the default
    /// capabilities, no provider name, no country code, and the addressing,
    /// recipient key and chat key of its type ([`addressing_of`],
    /// `addresses::recipient_key`, `addresses::chat_key`, gaps G-051 and
    /// G-057), which a name the RFC does not list has none of: its senders
    /// are addressed as written, with no recipient and no chat key, unless
    /// the channel sets them.
    ///
    /// [`addressing_of`]: crate::rules::addresses::addressing_of
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
            supports_streaming_delivery: false,
            recent_events_window: 0,
            sender_is_participant: false,
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
        let mut binding = ChannelBinding::at(
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
    /// The channel claims the type reserved for the framework's own records,
    /// `system` (RFC §5.3).
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
    channels: BTreeMap<ChannelId, ChannelConfig>,
    /// The kit's hooks (RFC §9.1).
    pub hooks: HookRegistry,
    /// The chain-depth limit (RFC §8.3), 5 by default.
    pub max_chain_depth: u32,
    /// How long a call may take from its start to its decision (RFC
    /// §13.6): past it the call is refused `process_timeout`, with nothing
    /// of its event written. `None`, the default, bounds nothing.
    pub process_timeout: Option<Duration>,
    /// What an agent's own output solicits in a room created without a
    /// policy of its own, step 2's among them (RFC §19.3.1): the agent
    /// chain by default.
    pub agent_response_policy: AgentResponsePolicy,
    /// Identity resolution (RFC §10.1 step 5, §11): `None`, the default,
    /// when the kit has no identity resolver.
    pub identity: Option<IdentityConfig>,
}

/// How a kit resolves its senders' identities (RFC §11.4, §11.5).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdentityConfig {
    /// The channel types resolution runs for; `None`, every type (RFC
    /// §11.4).
    pub channel_types: Option<BTreeSet<ChannelType>>,
    /// How long the resolver may take before its answer is read as UNKNOWN
    /// (RFC §11.5).
    pub timeout: Duration,
}

impl KitConfig {
    /// A kit with no channel, no hook and the default limits.
    pub fn new() -> Self {
        Self {
            channels: BTreeMap::new(),
            hooks: HookRegistry::new(),
            max_chain_depth: DEFAULT_MAX_CHAIN_DEPTH,
            process_timeout: None,
            agent_response_policy: AgentResponsePolicy::default(),
            identity: None,
        }
    }

    /// Whether identity resolution runs for a channel of `channel_type`
    /// (RFC §11.4): the kit has a resolver, and no restriction or one that
    /// names the type. The Python implementation's
    /// `identity_enabled_for(channel_type)`.
    pub fn identity_enabled_for(&self, channel_type: &ChannelType) -> bool {
        self.identity.as_ref().is_some_and(|identity| {
            identity
                .channel_types
                .as_ref()
                .is_none_or(|types| types.contains(channel_type))
        })
    }

    /// Registers a channel.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::DuplicateChannel`] when the id is taken, and
    /// [`ConfigError::ReservedChannelType`] for a channel claiming `system`
    /// (RFC §5.3).
    pub fn register_channel(&mut self, declaration: ChannelConfig) -> Result<(), ConfigError> {
        if self.channels.contains_key(&declaration.channel_id) {
            return Err(ConfigError::DuplicateChannel(declaration.channel_id));
        }
        if host::claims_reserved_type(&declaration.channel_type) {
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
    pub fn unregister_channel(&mut self, id: &ChannelId) -> Result<ChannelConfig, ConfigError> {
        self.channels
            .remove(id)
            .ok_or_else(|| ConfigError::UnknownChannel(id.clone()))
    }

    /// The registered channel `id`.
    pub fn channel(&self, id: &ChannelId) -> Option<&ChannelConfig> {
        self.channels.get(id)
    }

    /// What a context's read of a room's latest events asks (RFC §10.1
    /// step 3): the window each registered channel declares, and the floor
    /// while a hook is registered, which the room's bindings, read with it,
    /// turn into a count ([`RecentEvents::limit`]).
    pub fn recent_events(&self) -> RecentEvents {
        let windows = self
            .channels
            .values()
            .filter(|channel| channel.recent_events_window > 0)
            .map(|channel| (channel.channel_id.clone(), channel.recent_events_window))
            .collect();
        let floor = if self.hooks.is_empty() {
            0
        } else {
            RECENT_EVENTS_FLOOR
        };
        RecentEvents { windows, floor }
    }

    /// How many of its latest events a context of a room bound to
    /// `bindings` holds: [`recent_events`](Self::recent_events) for them.
    pub fn recent_events_limit(&self, bindings: &[ChannelBinding]) -> usize {
        self.recent_events().limit(bindings)
    }
}

/// How many of a room's latest events its context holds (roomkit's
/// `_resolve_recent_events_limit`): the largest window a channel bound to
/// the room declares, at least the floor while a hook may read them, at most
/// [`RECENT_EVENTS_LIMIT`]; none on a room whose channels read no history
/// and with no hook to read it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecentEvents {
    windows: BTreeMap<ChannelId, usize>,
    floor: usize,
}

impl RecentEvents {
    /// The same read for a caller that reads the events itself: at least
    /// the floor, whether or not a hook is registered (roomkit's
    /// `reads_history`).
    #[must_use]
    pub fn read_by_the_caller(mut self) -> Self {
        self.floor = RECENT_EVENTS_FLOOR;
        self
    }

    /// The count for a room bound to `bindings`; a channel not registered
    /// declares none.
    pub fn limit(&self, bindings: &[ChannelBinding]) -> usize {
        let largest = bindings
            .iter()
            .filter_map(|binding| self.windows.get(&binding.channel_id))
            .copied()
            .max()
            .unwrap_or(0);
        RECENT_EVENTS_LIMIT.min(largest.max(self.floor))
    }
}

impl Default for KitConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::rules::hook_registry::{HookExecution, HookSpec, HookTrigger};

    fn binding(id: &str) -> ChannelBinding {
        let config = ChannelConfig::new(
            ChannelId::new(id).unwrap(),
            ChannelType::SMS,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
        );
        config.binding(RoomId::new("r1").unwrap(), Timestamp::UNIX_EPOCH)
    }

    fn reader(id: &str, window: usize) -> ChannelConfig {
        let mut config = ChannelConfig::new(
            ChannelId::new(id).unwrap(),
            ChannelType::AI,
            ChannelCategory::Intelligence,
            ChannelDirection::Bidirectional,
        );
        config.recent_events_window = window;
        config
    }

    /// Roomkit's `_resolve_recent_events_limit`: the largest window a bound
    /// channel declares, the floor while a hook is registered, the limit
    /// above all; a channel not registered, or not bound, counts for none.
    #[test]
    fn a_room_s_limit_is_its_largest_bound_window_floored_and_capped() {
        let mut config = KitConfig::new();
        config.register_channel(reader("small", 3)).unwrap();
        config.register_channel(reader("huge", 5_000)).unwrap();
        let limit = |config: &KitConfig, ids: &[&str]| {
            let bindings: Vec<ChannelBinding> = ids.iter().map(|id| binding(id)).collect();
            config.recent_events_limit(&bindings)
        };
        let without_hooks = [
            limit(&config, &[]),
            limit(&config, &["sms"]),
            limit(&config, &["sms", "small"]),
            limit(&config, &["small", "huge"]),
        ];
        let spec = HookSpec::new("audit", HookTrigger::BeforeBroadcast, HookExecution::Sync);
        config.hooks.register(spec).unwrap();
        let with_a_hook = [limit(&config, &["sms"]), limit(&config, &["small"])];
        assert_eq!(
            (without_hooks, with_a_hook),
            (
                [0, 0, 3, RECENT_EVENTS_LIMIT],
                [RECENT_EVENTS_FLOOR, RECENT_EVENTS_FLOOR]
            )
        );
        let read_by_the_caller = config.recent_events().read_by_the_caller();
        assert_eq!(
            read_by_the_caller.limit(&[binding("sms")]),
            RECENT_EVENTS_FLOOR
        );
    }
}

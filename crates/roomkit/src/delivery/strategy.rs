//! When a proactive delivery goes out (RFC §22.2): the strategies, Python's
//! `Immediate`, `WaitForIdle` and `Queued`, and the request they are given,
//! `DeliveryContext`.
//!
//! No channel of this implementation plays voice or hosts a realtime model:
//! `WaitForIdle` and `Queued` wait for idle on such a channel only, so here
//! each delivers at once, with no buffer, as RFC §22.2 reads them on any
//! other channel.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use roomkit_core::model::{ChannelId, Metadata, RoomId};

use super::outcome::DeliveryOutcome;
use super::targets::Prepared;
use crate::error::RoomKitError;
use crate::kit::RoomKit;
use crate::store::{ConversationStore, InMemoryStore};

/// What a strategy's [`deliver`](DeliveryStrategy::deliver) gives: its
/// outcome, `None` for one it leaves unknown, or the error it raised.
pub(crate) type Delivering<'a> =
    Pin<Box<dyn Future<Output = Result<Option<DeliveryOutcome>, RoomKitError>> + Send + 'a>>;

/// One delivery request, as a strategy is given it (RFC §22.1): Python's
/// `DeliveryContext(kit, room_id, content, channel_id=None, metadata=None,
/// addressed_to=None, idempotency_key=None, session_id=None,
/// chain_depth=0, instruction=False)`.
pub struct DeliveryContext<S = InMemoryStore> {
    /// The kit it delivers through.
    pub kit: RoomKit<S>,
    /// The room it delivers into.
    pub room_id: RoomId,
    /// Its text.
    pub content: String,
    /// The transport it names, `None` for the room's.
    pub channel_id: Option<ChannelId>,
    /// The metadata of the event it publishes.
    pub metadata: Metadata,
    /// The intelligence channels asked to act (RFC §19.3).
    pub addressed_to: Option<Vec<ChannelId>>,
    /// The key naming its publication in the room (RFC §22.1).
    pub idempotency_key: Option<String>,
    /// The realtime session it names.
    pub session_id: Option<String>,
    /// The chain it continues (RFC §8.3).
    pub chain_depth: u32,
    /// Whether it is the application's direction to an agent (RFC §10.1.1).
    pub instruction: bool,
    /// Whether its publication waits for the turn it asks for: a batch
    /// `Queued` delivers for others does not.
    pub(crate) wait_for_turn: bool,
}

impl<S> Clone for DeliveryContext<S> {
    fn clone(&self) -> Self {
        Self {
            kit: self.kit.clone(),
            room_id: self.room_id.clone(),
            content: self.content.clone(),
            channel_id: self.channel_id.clone(),
            metadata: self.metadata.clone(),
            addressed_to: self.addressed_to.clone(),
            idempotency_key: self.idempotency_key.clone(),
            session_id: self.session_id.clone(),
            chain_depth: self.chain_depth,
            instruction: self.instruction,
            wait_for_turn: self.wait_for_turn,
        }
    }
}

impl<S> fmt::Debug for DeliveryContext<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeliveryContext")
            .field("room_id", &self.room_id)
            .field("content", &self.content)
            .field("channel_id", &self.channel_id)
            .field("addressed_to", &self.addressed_to)
            .field("idempotency_key", &self.idempotency_key)
            .field("session_id", &self.session_id)
            .field("chain_depth", &self.chain_depth)
            .field("instruction", &self.instruction)
            .finish_non_exhaustive()
    }
}

impl<S: ConversationStore> DeliveryContext<S> {
    /// The request to deliver `content` into `room_id` through `kit`, with
    /// Python's defaults for the rest.
    pub fn new(kit: RoomKit<S>, room_id: RoomId, content: impl Into<String>) -> Self {
        Self {
            kit,
            room_id,
            content: content.into(),
            channel_id: None,
            metadata: Metadata::new(),
            addressed_to: None,
            idempotency_key: None,
            session_id: None,
            chain_depth: 0,
            instruction: false,
            wait_for_turn: true,
        }
    }

    /// The transport the request takes when it names none: a voice
    /// channel, else the first other transport bound to the room; an
    /// intelligence channel attached as a transport is none.
    ///
    /// # Errors
    ///
    /// The store's failure to read the room's bindings.
    pub async fn find_transport_channel_id(&self) -> Result<Option<ChannelId>, RoomKitError> {
        self.kit.find_transport_channel_id(&self.room_id).await
    }

    /// The transport the request names, else the one
    /// [`find_transport_channel_id`](Self::find_transport_channel_id)
    /// finds.
    ///
    /// # Errors
    ///
    /// The store's failure to read the room's bindings.
    pub async fn resolve_channel_id(&self) -> Result<Option<ChannelId>, RoomKitError> {
        match &self.channel_id {
            Some(channel_id) => Ok(Some(channel_id.clone())),
            None => self.find_transport_channel_id().await,
        }
    }
}

/// When a proactive delivery goes out (RFC §22.2): Python's
/// `DeliveryStrategy`. A strategy resolves the request's destination and
/// delivers it, now or at a moment the channel allows, and reports its
/// actual outcome; one that reports `None` leaves it unknown, never taken
/// for a success (RFC §22.1). What it raises is the delivery's failure.
pub trait DeliveryStrategy<S = InMemoryStore>: Send + Sync + 'static {
    /// Delivers `ctx`: Python's `async deliver(ctx)`.
    fn deliver(&self, ctx: DeliveryContext<S>) -> Delivering<'_>;

    /// The strategy in the serialized form a delivery backend keeps it in,
    /// `{"type": ..., "params": {...}}`; `None` for one that has none,
    /// which a backend runs as `immediate`. Outside the public API.
    #[doc(hidden)]
    fn serialized(&self) -> Option<serde_json::Value> {
        None
    }
}

/// Delivers now (RFC §22.2): Python's `Immediate`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Immediate;

impl<S: ConversationStore> DeliveryStrategy<S> for Immediate {
    fn deliver(&self, ctx: DeliveryContext<S>) -> Delivering<'_> {
        Box::pin(async move { deliver_now(&ctx).await.map(Some) })
    }

    fn serialized(&self) -> Option<serde_json::Value> {
        Some(serde_json::json!({"type": "immediate", "params": {}}))
    }
}

/// `ctx`'s channel resolved and checked, then its publication.
pub(crate) async fn deliver_now<S: ConversationStore>(
    ctx: &DeliveryContext<S>,
) -> Result<DeliveryOutcome, RoomKitError> {
    match ctx.kit.prepare_delivery(ctx, None).await? {
        Prepared::Channel(channel_id) => ctx.kit.deliver_to_channel(ctx, channel_id).await,
        Prepared::Done(outcome) => Ok(*outcome),
    }
}

/// What [`WaitForIdle::new`] takes: Python's `WaitForIdle(buffer=1.0,
/// playback_timeout=15.0)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WaitForIdleOptions {
    /// Python `buffer=1.0`: the wait after the channel went idle.
    pub buffer: Duration,
    /// Python `playback_timeout=15.0`: the longest wait for idle, past which
    /// it delivers anyway.
    pub playback_timeout: Duration,
}

impl Default for WaitForIdleOptions {
    fn default() -> Self {
        Self {
            buffer: Duration::from_secs(1),
            playback_timeout: Duration::from_secs(15),
        }
    }
}

/// Waits for the channel to go idle, then delivers (RFC §22.2): Python's
/// `WaitForIdle`. Idle is a voice channel's playback done, or a realtime
/// model's sessions idle; then it waits `buffer`. No channel of this
/// implementation plays voice or hosts a realtime model: it delivers at
/// once, with no buffer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WaitForIdle {
    options: WaitForIdleOptions,
}

impl WaitForIdle {
    /// The strategy waiting as `options` say.
    pub fn new(options: WaitForIdleOptions) -> Self {
        Self { options }
    }

    /// Python's `buffer`: the wait after the channel went idle.
    pub fn buffer(&self) -> Duration {
        self.options.buffer
    }

    /// Python's `playback_timeout`: the longest wait for idle.
    pub fn playback_timeout(&self) -> Duration {
        self.options.playback_timeout
    }
}

impl<S: ConversationStore> DeliveryStrategy<S> for WaitForIdle {
    fn deliver(&self, ctx: DeliveryContext<S>) -> Delivering<'_> {
        Box::pin(async move { deliver_now(&ctx).await.map(Some) })
    }

    fn serialized(&self) -> Option<serde_json::Value> {
        Some(serde_json::json!({
            "type": "wait_for_idle",
            "params": {
                "buffer": self.buffer().as_secs_f64(),
                "playback_timeout": self.playback_timeout().as_secs_f64(),
            },
        }))
    }
}

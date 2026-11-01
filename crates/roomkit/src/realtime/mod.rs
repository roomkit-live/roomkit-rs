//! Ephemeral events (RFC §8.4): typing, presence, read receipts, reactions
//! and a turn's tool calls, published to a room's subscribers and never
//! stored. Python's `roomkit.realtime`: the event, the backend that carries
//! it, and the in-memory backend a kit uses by default.

mod memory;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use roomkit_core::model::{ChannelId, Metadata, RoomId, Timestamp};
use serde::{Deserialize, Serialize};

pub use self::memory::{InMemoryRealtime, InMemoryRealtimeOptions};
use crate::environment::SystemEnvironment;
use roomkit_core::ops::Environment;

/// What an ephemeral event says: Python's `EphemeralEventType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum EphemeralEventType {
    /// A user started typing.
    TypingStart,
    /// A user stopped typing.
    TypingStop,
    /// A user is online.
    PresenceOnline,
    /// A user is away.
    PresenceAway,
    /// A user is offline.
    PresenceOffline,
    /// A user read an event, its id under `data.event_id`.
    ReadReceipt,
    /// A user reacted to an event.
    Reaction,
    /// A part of a tool call's arguments, as the model writes them.
    ToolCallDelta,
    /// Tool calls started.
    ToolCallStart,
    /// Tool calls ended.
    ToolCallEnd,
    /// A turn started thinking.
    ThinkingStart,
    /// A part of a turn's thinking.
    ThinkingDelta,
    /// A turn stopped thinking.
    ThinkingEnd,
    /// A part of a turn's answer.
    TextDelta,
    /// Anything else, a presence of another status among them.
    Custom,
}

/// An event a room's subscribers receive and no store keeps: Python's
/// `EphemeralEvent(room_id, type, user_id, id=<uuid>, channel_id=None,
/// data={}, timestamp=<now>)`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EphemeralEvent {
    /// The room it is about.
    pub room_id: RoomId,
    /// What it says.
    pub r#type: EphemeralEventType,
    /// Who it is about: a user, or the channel of a turn's tool calls.
    pub user_id: String,
    /// Its id, a fresh one by default.
    pub id: String,
    /// The channel it comes from, when it names one.
    #[serde(default)]
    pub channel_id: Option<ChannelId>,
    /// What else it carries.
    #[serde(default)]
    pub data: Metadata,
    /// When it was made.
    pub timestamp: Timestamp,
}

impl EphemeralEvent {
    /// An event of `type` about `user_id` in `room_id`, a fresh id, made
    /// now, no channel and no data. A caller sets the other fields by
    /// struct update.
    pub fn new(room_id: RoomId, r#type: EphemeralEventType, user_id: impl Into<String>) -> Self {
        Self {
            room_id,
            r#type,
            user_id: user_id.into(),
            id: uuid::Uuid::new_v4().simple().to_string(),
            channel_id: None,
            data: Metadata::new(),
            timestamp: SystemEnvironment.now(),
        }
    }

    /// The event in the serialized form, a JSON object: Python's
    /// `to_dict()`.
    pub fn to_dict(&self) -> Metadata {
        match serde_json::to_value(self) {
            Ok(serde_json::Value::Object(members)) => members,
            _ => Metadata::new(),
        }
    }

    /// The event `data` holds in the serialized form: Python's
    /// `EphemeralEvent.from_dict(data)`.
    ///
    /// # Errors
    ///
    /// Returns the reader's error for a member missing or of the wrong
    /// form, as Python's raises.
    pub fn from_dict(data: Metadata) -> Result<Self, serde_json::Error> {
        serde_json::from_value(serde_json::Value::Object(data))
    }
}

/// What a subscription runs for each event it receives: Python's
/// `EphemeralCallback`, an `async (event) -> None`. A callback that
/// panics is logged, and its subscription goes on.
pub type EphemeralCallback =
    Arc<dyn Fn(EphemeralEvent) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// What a realtime backend's methods return: a boxed future, since the kit
/// holds its backend as an object.
pub type RealtimeFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A publish-and-subscribe backend for ephemeral events: Python's
/// `RealtimeBackend`. The kit uses [`InMemoryRealtime`] unless
/// [`RoomKitOptions::realtime`](crate::RoomKitOptions::realtime) names
/// another, Redis pub/sub or NATS for several processes.
pub trait RealtimeBackend: Send + Sync + 'static {
    /// Hands `event` to every subscription of `channel`.
    fn publish<'a>(&'a self, channel: &'a str, event: EphemeralEvent) -> RealtimeFuture<'a, ()>;

    /// Runs `callback` for each event published to `channel` from now on;
    /// the id that [`unsubscribe`](Self::unsubscribe) takes.
    fn subscribe<'a>(
        &'a self,
        channel: &'a str,
        callback: EphemeralCallback,
    ) -> RealtimeFuture<'a, String>;

    /// Ends the subscription `subscription_id`; whether it existed.
    fn unsubscribe<'a>(&'a self, subscription_id: &'a str) -> RealtimeFuture<'a, bool>;

    /// Hands `event` to the room `room_id`'s subscriptions, those of the
    /// channel `room:<room_id>`.
    fn publish_to_room<'a>(
        &'a self,
        room_id: &'a RoomId,
        event: EphemeralEvent,
    ) -> RealtimeFuture<'a, ()> {
        Box::pin(async move { self.publish(&room_channel(room_id), event).await })
    }

    /// Subscribes `callback` to the room `room_id`, the channel
    /// `room:<room_id>`.
    fn subscribe_to_room<'a>(
        &'a self,
        room_id: &'a RoomId,
        callback: EphemeralCallback,
    ) -> RealtimeFuture<'a, String> {
        Box::pin(async move { self.subscribe(&room_channel(room_id), callback).await })
    }

    /// Releases what the backend holds; the kit's close calls it once its
    /// channels are closed. Nothing to release by default.
    fn close(&self) -> RealtimeFuture<'_, ()> {
        Box::pin(async {})
    }
}

/// The channel of the room `room_id`'s ephemeral events.
fn room_channel(room_id: &RoomId) -> String {
    format!("room:{}", room_id.as_str())
}

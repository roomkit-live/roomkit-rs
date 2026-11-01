//! Ephemeral events (RFC §8.4): what the kit publishes to a room's
//! subscribers and never stores, typing, presence, read receipts,
//! reactions and tool calls, and the subscriptions that receive them, each
//! for the organization a call names (RFC §17.2), as the Python
//! implementation's `RealtimeOpsMixin`.

use std::future::Future;
use std::sync::Arc;

use roomkit_core::model::{ChannelId, Metadata, RoomId};

use super::RoomKit;
use crate::error::RoomKitError;
use crate::realtime::{EphemeralEvent, EphemeralEventType, RealtimeBackend};
use crate::store::ConversationStore;

/// What [`RoomKit::publish_typing`] takes beside the room and the user:
/// Python's `publish_typing(room_id, user_id, is_typing=True, data=None, *,
/// organization_id=None)`.
#[derive(Clone, Debug, PartialEq)]
pub struct PublishTypingOptions {
    /// Python `is_typing=True`: `typing_start`, or `typing_stop` when
    /// `false`.
    pub is_typing: bool,
    /// Python `data=None`: what else the event carries (a display name).
    pub data: Option<Metadata>,
    /// Python `organization_id=None`: the organization the call acts for
    /// (RFC §17.2). The room is read with it first, and a room of another
    /// organization is not found, nothing published; `None` reads no room.
    pub organization_id: Option<String>,
}

impl Default for PublishTypingOptions {
    fn default() -> Self {
        Self {
            is_typing: true,
            data: None,
            organization_id: None,
        }
    }
}

/// What [`RoomKit::publish_presence`] takes beside the room, the user and
/// the status: Python's `publish_presence(room_id, user_id, status, *,
/// organization_id=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PublishPresenceOptions {
    /// Python `organization_id=None`, as
    /// [`PublishTypingOptions::organization_id`].
    pub organization_id: Option<String>,
}

/// What [`RoomKit::publish_read_receipt`] takes beside the room, the user
/// and the event: Python's `publish_read_receipt(room_id, user_id,
/// event_id, *, organization_id=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PublishReadReceiptOptions {
    /// Python `organization_id=None`, as
    /// [`PublishTypingOptions::organization_id`].
    pub organization_id: Option<String>,
}

/// What [`RoomKit::publish_reaction`] takes beside the room, the user, the
/// event and the emoji: Python's `publish_reaction(room_id, user_id,
/// target_event_id, emoji, action="add", *, organization_id=None)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishReactionOptions {
    /// Python `action="add"`: `add` or `remove`.
    pub action: String,
    /// Python `organization_id=None`, as
    /// [`PublishTypingOptions::organization_id`].
    pub organization_id: Option<String>,
}

impl Default for PublishReactionOptions {
    fn default() -> Self {
        Self {
            action: "add".to_owned(),
            organization_id: None,
        }
    }
}

/// What [`RoomKit::publish_tool_call`] takes beside the room, the channel
/// and the calls: Python's `publish_tool_call(room_id, channel_id,
/// tool_calls, event_type=EphemeralEventType.TOOL_CALL_START, *,
/// duration_ms=None, organization_id=None)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishToolCallOptions {
    /// Python `event_type=TOOL_CALL_START`: `tool_call_delta`,
    /// `tool_call_start` or `tool_call_end`.
    pub event_type: EphemeralEventType,
    /// Python `duration_ms=None`: how long the calls ran, for an end.
    pub duration_ms: Option<u64>,
    /// Python `organization_id=None`, as
    /// [`PublishTypingOptions::organization_id`].
    pub organization_id: Option<String>,
}

impl Default for PublishToolCallOptions {
    fn default() -> Self {
        Self {
            event_type: EphemeralEventType::ToolCallStart,
            duration_ms: None,
            organization_id: None,
        }
    }
}

/// What [`RoomKit::subscribe_room`] takes beside the room and the
/// callback: Python's `subscribe_room(room_id, callback, *,
/// organization_id=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SubscribeRoomOptions {
    /// Python `organization_id=None`: the organization the call acts for
    /// (RFC §17.2). The room is read with it first, and a room of another
    /// organization is not found, nothing subscribed; `None` reads no room.
    pub organization_id: Option<String>,
}

/// The type of a presence of `status`: `online`, `away` and `offline` each
/// their own, any other a custom event.
fn presence(status: &str) -> EphemeralEventType {
    match status {
        "online" => EphemeralEventType::PresenceOnline,
        "away" => EphemeralEventType::PresenceAway,
        "offline" => EphemeralEventType::PresenceOffline,
        _ => EphemeralEventType::Custom,
    }
}

impl<S: ConversationStore> RoomKit<S> {
    /// The backend the kit publishes its ephemeral events with: Python's
    /// `realtime` property, the backend
    /// [`RoomKitOptions::realtime`](crate::RoomKitOptions::realtime) named
    /// or an [`InMemoryRealtime`](crate::realtime::InMemoryRealtime).
    pub fn realtime(&self) -> Arc<dyn RealtimeBackend> {
        Arc::clone(&self.inner.realtime)
    }

    /// Publishes `event` to the room's subscribers, once the call is
    /// admitted and the organization `organization_id` reaches the room.
    async fn publish(
        &self,
        room_id: &RoomId,
        organization_id: Option<&str>,
        event: EphemeralEvent,
    ) -> Result<(), RoomKitError> {
        self.inner.open()?;
        self.reached(room_id, organization_id).await?;
        self.inner.realtime.publish_to_room(room_id, event).await;
        Ok(())
    }

    /// Publishes that `user_id` started typing in the room, or stopped
    /// with `is_typing` false: Python's `publish_typing`.
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::KitClosed`] once the kit's close started (RFC
    /// §12.10.4), [`RoomKitError::RoomNotFound`] for a room the
    /// organization `options` names does not reach, nothing published.
    pub async fn publish_typing(
        &self,
        room_id: &RoomId,
        user_id: &str,
        options: PublishTypingOptions,
    ) -> Result<(), RoomKitError> {
        let r#type = if options.is_typing {
            EphemeralEventType::TypingStart
        } else {
            EphemeralEventType::TypingStop
        };
        let event = EphemeralEvent {
            data: options.data.unwrap_or_default(),
            ..EphemeralEvent::new(room_id.clone(), r#type, user_id)
        };
        self.publish(room_id, options.organization_id.as_deref(), event)
            .await
    }

    /// Publishes `user_id`'s presence in the room: `online`, `away` and
    /// `offline` each their own type, any other status a custom event
    /// carrying it under `data.status`: Python's `publish_presence`.
    ///
    /// # Errors
    ///
    /// As [`publish_typing`](Self::publish_typing).
    pub async fn publish_presence(
        &self,
        room_id: &RoomId,
        user_id: &str,
        status: &str,
        options: PublishPresenceOptions,
    ) -> Result<(), RoomKitError> {
        let r#type = presence(status);
        let mut data = Metadata::new();
        if r#type == EphemeralEventType::Custom {
            data.insert("status".to_owned(), status.into());
        }
        let event = EphemeralEvent {
            data,
            ..EphemeralEvent::new(room_id.clone(), r#type, user_id)
        };
        self.publish(room_id, options.organization_id.as_deref(), event)
            .await
    }

    /// Publishes that `user_id` read the event `event_id`, under
    /// `data.event_id`: Python's `publish_read_receipt`.
    ///
    /// # Errors
    ///
    /// As [`publish_typing`](Self::publish_typing).
    pub async fn publish_read_receipt(
        &self,
        room_id: &RoomId,
        user_id: &str,
        event_id: &str,
        options: PublishReadReceiptOptions,
    ) -> Result<(), RoomKitError> {
        let data = Metadata::from_iter([("event_id".to_owned(), event_id.into())]);
        let event = EphemeralEvent {
            data,
            ..EphemeralEvent::new(room_id.clone(), EphemeralEventType::ReadReceipt, user_id)
        };
        self.publish(room_id, options.organization_id.as_deref(), event)
            .await
    }

    /// Publishes `user_id`'s reaction `emoji` to the event
    /// `target_event_id`, its `action` `add` or `remove`: Python's
    /// `publish_reaction`.
    ///
    /// # Errors
    ///
    /// As [`publish_typing`](Self::publish_typing).
    pub async fn publish_reaction(
        &self,
        room_id: &RoomId,
        user_id: &str,
        target_event_id: &str,
        emoji: &str,
        options: PublishReactionOptions,
    ) -> Result<(), RoomKitError> {
        let data = Metadata::from_iter([
            ("target_event_id".to_owned(), target_event_id.into()),
            ("emoji".to_owned(), emoji.into()),
            ("action".to_owned(), options.action.into()),
        ]);
        let event = EphemeralEvent {
            data,
            ..EphemeralEvent::new(room_id.clone(), EphemeralEventType::Reaction, user_id)
        };
        self.publish(room_id, options.organization_id.as_deref(), event)
            .await
    }

    /// Publishes the tool calls `tool_calls` of the channel `channel_id`,
    /// each an object (an id, a name, and its arguments or its result), of
    /// `event_type`, under `data.tool_calls` with the channel under
    /// `data.channel_id` and, when given, the duration under
    /// `data.duration_ms`: Python's `publish_tool_call`.
    ///
    /// # Errors
    ///
    /// As [`publish_typing`](Self::publish_typing).
    pub async fn publish_tool_call(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        tool_calls: Vec<Metadata>,
        options: PublishToolCallOptions,
    ) -> Result<(), RoomKitError> {
        let calls: Vec<serde_json::Value> = tool_calls.into_iter().map(Into::into).collect();
        let mut data = Metadata::from_iter([
            ("tool_calls".to_owned(), calls.into()),
            ("channel_id".to_owned(), channel_id.as_str().into()),
        ]);
        if let Some(duration) = options.duration_ms {
            data.insert("duration_ms".to_owned(), duration.into());
        }
        let event = EphemeralEvent {
            channel_id: Some(channel_id.clone()),
            data,
            ..EphemeralEvent::new(room_id.clone(), options.event_type, channel_id.as_str())
        };
        self.publish(room_id, options.organization_id.as_deref(), event)
            .await
    }

    /// Runs `callback` for each ephemeral event the room's subscribers
    /// receive from now on; the id [`unsubscribe_room`](Self::unsubscribe_room)
    /// takes: Python's `subscribe_room`. A callback that panics is logged,
    /// and its subscription goes on.
    ///
    /// # Errors
    ///
    /// As [`publish_typing`](Self::publish_typing), nothing subscribed.
    pub async fn subscribe_room<F, Fut>(
        &self,
        room_id: &RoomId,
        callback: F,
        options: SubscribeRoomOptions,
    ) -> Result<String, RoomKitError>
    where
        F: Fn(EphemeralEvent) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.inner.open()?;
        self.reached(room_id, options.organization_id.as_deref())
            .await?;
        let callback: crate::realtime::EphemeralCallback =
            Arc::new(move |event| Box::pin(callback(event)));
        Ok(self
            .inner
            .realtime
            .subscribe_to_room(room_id, callback)
            .await)
    }

    /// Ends the subscription `subscription_id`; whether it existed:
    /// Python's `unsubscribe_room`. A teardown call: once the kit's close
    /// started it returns `false` and does nothing, the close ending every
    /// subscription (RFC §12.10.4).
    pub async fn unsubscribe_room(&self, subscription_id: &str) -> bool {
        if self.inner.open().is_err() {
            return false;
        }
        self.inner.realtime.unsubscribe(subscription_id).await
    }
}

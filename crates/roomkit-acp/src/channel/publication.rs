//! ACP callbacks select their session's scope, never the connection reader's.

use roomkit::TurnHost;
use roomkit::channel_publication::ChannelPublication;
use roomkit::model::{ChannelId, RoomId};
use roomkit::realtime::{EphemeralEvent, EphemeralEventType};
use serde_json::{Value, json};

use super::config;

#[derive(Clone)]
pub(super) struct Publisher {
    pub(super) bridge: ChannelPublication,
    pub(super) channel_id: ChannelId,
    pub(super) publishes_text: bool,
}

impl Publisher {
    pub(super) fn new(channel_id: ChannelId, publishes_text: bool) -> Self {
        Self {
            bridge: ChannelPublication::default(),
            channel_id,
            publishes_text,
        }
    }
}

/// A suppressed turn must not fall back to the unscoped room publisher.
#[derive(Clone)]
pub(super) struct Scope {
    host: Option<TurnHost>,
    answers_room: bool,
}

impl Scope {
    pub(super) fn new(host: Option<TurnHost>, answers_room: bool) -> Self {
        Self { host, answers_room }
    }

    pub(super) async fn publish(&self, publisher: &ChannelPublication, event: EphemeralEvent) {
        if let Some(host) = &self.host {
            if self.answers_room {
                host.publish_from_channel(event);
            }
        } else {
            publisher.publish_outside(event).await;
        }
    }
}

pub(super) fn event(
    room: &RoomId,
    channel: &ChannelId,
    kind: EphemeralEventType,
    data: Value,
) -> EphemeralEvent {
    let mut data = match data {
        Value::Object(data) => data,
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) | Value::Array(_) => {
            serde_json::Map::new()
        }
    };
    data.insert("channel_id".to_owned(), json!(channel));
    EphemeralEvent {
        channel_id: Some(channel.clone()),
        data,
        ..EphemeralEvent::new(room.clone(), kind, channel.to_string())
    }
}

pub(super) fn configuration(session: &str, options: &[Value]) -> Value {
    json!({
        "type": "acp_config_options", "session_id": session,
        "values": config::values(options), "labels": config::labels(options),
        "config_options": options,
    })
}

//! ACP callbacks select their session's scope, never the connection reader's.

use roomkit::channel_publication::ChannelPublication;
use roomkit::model::{ChannelId, RoomId};
use roomkit::realtime::{EphemeralEvent, EphemeralEventType};
use roomkit::turns::ChannelTurnHooks;
use serde_json::{Value, json};

use super::config;

#[derive(Clone)]
pub(super) struct Publisher {
    pub(super) bridge: ChannelPublication,
    pub(super) channel_id: ChannelId,
    pub(super) publishes_text: bool,
    pub(super) publishes_tool_text: bool,
}

impl Publisher {
    pub(super) fn new(
        channel_id: ChannelId,
        publishes_text: bool,
        publishes_tool_text: bool,
    ) -> Self {
        Self {
            bridge: ChannelPublication::default(),
            channel_id,
            publishes_text,
            publishes_tool_text,
        }
    }
}

/// A suppressed turn must not fall back to the unscoped room publisher.
#[derive(Clone)]
pub(super) struct Scope {
    outlet: Option<ChannelTurnHooks>,
}

impl Scope {
    pub(super) fn new(outlet: Option<ChannelTurnHooks>) -> Self {
        Self { outlet }
    }

    pub(super) async fn publish(&self, event: EphemeralEvent) {
        if let Some(outlet) = &self.outlet {
            outlet.publish_now(event).await;
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

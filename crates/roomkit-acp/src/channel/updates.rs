//! A turn's text and thoughts: full stream items, bounded room publications.

use std::sync::{Arc, Mutex, PoisonError};

use roomkit::model::{ChannelId, Metadata, RoomEvent, RoomId, Visibility};
use roomkit::realtime::{EphemeralEvent, EphemeralEventType};
use roomkit::{ResponseItem, TurnHost};
use serde_json::Value;
use tokio::sync::mpsc;

use super::tools::Tools;

/// Only an unscoped answer may publish to the room's subscribers.
pub(super) fn answers_room(event: &RoomEvent) -> bool {
    event
        .response_visibility
        .as_ref()
        .is_none_or(|scope| *scope == Visibility::All)
}

/// The sender is held by the prompt, not by the stream consuming it.
#[derive(Clone)]
pub(super) struct Sink {
    sender: mpsc::UnboundedSender<ResponseItem>,
    thinking: Arc<ThinkingWindow>,
    tools: Arc<Tools>,
}

impl Sink {
    pub(super) fn new(
        sender: mpsc::UnboundedSender<ResponseItem>,
        thinking: Arc<ThinkingWindow>,
        tools: Arc<Tools>,
    ) -> Self {
        Self {
            sender,
            thinking,
            tools,
        }
    }

    pub(super) fn same_channel(&self, other: &Self) -> bool {
        self.sender.same_channel(&other.sender)
    }

    pub(super) fn text(&self, text: String) {
        let _gone = self.sender.send(ResponseItem::Text(text));
    }

    pub(super) fn thought(&self, text: String) {
        self.thinking.chunk(text, &self.sender);
    }

    pub(super) async fn tool(&self, update: Value) {
        if let Some(pending) = self.tools.update(&update, &self.sender) {
            let _gone = pending.await;
        }
    }
}

#[derive(Default)]
enum State {
    #[default]
    Waiting,
    Open,
    Closed,
}

/// A single window per turn, closed when its stream ends or is abandoned.
/// A late callback cannot reopen a window after the reader has gone.
pub(super) struct ThinkingWindow {
    state: Mutex<State>,
    host: Option<TurnHost>,
    room_id: RoomId,
    channel_id: ChannelId,
    publishes_text: bool,
}

impl ThinkingWindow {
    pub(super) fn new(
        host: Option<TurnHost>,
        room_id: RoomId,
        channel_id: ChannelId,
        publishes_text: bool,
    ) -> Self {
        Self {
            state: Mutex::default(),
            host,
            room_id,
            channel_id,
            publishes_text,
        }
    }

    fn chunk(&self, text: String, sender: &mpsc::UnboundedSender<ResponseItem>) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if matches!(*state, State::Closed) {
            return;
        }
        if matches!(*state, State::Waiting) {
            *state = State::Open;
            self.publish(EphemeralEventType::ThinkingStart, None);
        }
        self.publish(EphemeralEventType::ThinkingDelta, Some(&text));
        // Publication and the read stay ordered even if another worker drops
        // the stream as soon as it receives this item. Neither operation waits.
        let _gone = sender.send(ResponseItem::Thinking(text));
    }

    pub(super) fn finish(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if matches!(std::mem::replace(&mut *state, State::Closed), State::Open) {
            self.publish(EphemeralEventType::ThinkingEnd, None);
        }
    }

    fn publish(&self, kind: EphemeralEventType, delta: Option<&str>) {
        let Some(host) = &self.host else {
            return;
        };
        let mut data = Metadata::new();
        data.insert("round".to_owned(), Value::from(0));
        data.insert(
            "channel_id".to_owned(),
            Value::from(self.channel_id.to_string()),
        );
        if let Some(text) = delta {
            data.insert(
                "thinking_length".to_owned(),
                Value::from(text.chars().count()),
            );
            if self.publishes_text {
                data.insert(
                    "thinking".to_owned(),
                    Value::from(text.chars().take(1000).collect::<String>()),
                );
            }
        } else {
            // ACP's start/end carry an empty string, never the accumulated thought.
            data.insert("thinking".to_owned(), Value::from(""));
        }
        host.publish_ephemeral(EphemeralEvent {
            channel_id: Some(self.channel_id.clone()),
            data,
            ..EphemeralEvent::new(self.room_id.clone(), kind, self.channel_id.to_string())
        });
    }
}

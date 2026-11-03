//! A round's reasoning as the room's observers read it (RFC §6.7, §9.2):
//! `THINKING_START`, `THINKING_DELTA` and `THINKING_END` ephemeral events,
//! and `ON_AI_THINKING`. Python's `_StreamRound._add_thinking`,
//! `_end_thinking`, `_close_thinking_window`, `_publish_thinking_event` and
//! `_ThinkingCoalescer`.
//!
//! The reasoning repeats what the model read, and a room's realtime channel
//! has no visibility gate: its events carry its length, its round and the
//! channel, its text only for a channel that opted in
//! ([`AiChannelOptions::publish_thinking_text`](crate::AiChannelOptions::publish_thinking_text)).

use roomkit::TurnHost;
use roomkit::model::{ChannelId, Metadata, RoomId};
use roomkit::realtime::{EphemeralEvent, EphemeralEventType};
use serde_json::Value;
use tokio::time::Instant;

use crate::ThinkingEvent;

/// The most of a reasoning an ephemeral event carries: a flush past it goes
/// in several, a close cuts its block there. Python's
/// `THINKING_PREVIEW_LIMIT`.
const THINKING_PREVIEW_LIMIT: usize = 1000;

/// Batches a model's reasoning deltas into one publish per window, of time
/// or of size; a window of 0 ms publishes each as it comes. Python's
/// `_ThinkingCoalescer`.
#[derive(Debug)]
pub(crate) struct ThinkingCoalescer {
    flush_ms: f64,
    flush_chars: usize,
    pending: String,
    pending_chars: usize,
    last_publish: Instant,
}

impl ThinkingCoalescer {
    pub(crate) fn new(flush_ms: f64, flush_chars: usize) -> Self {
        Self {
            flush_ms,
            flush_chars,
            pending: String::new(),
            pending_chars: 0,
            last_publish: Instant::now(),
        }
    }

    /// Keeps `delta`; what to publish now, nothing while the window holds.
    pub(crate) fn add(&mut self, delta: &str) -> Vec<String> {
        self.pending.push_str(delta);
        self.pending_chars += delta.chars().count();
        let elapsed_ms = self.last_publish.elapsed().as_secs_f64() * 1000.0;
        if self.flush_ms <= 0.0
            || self.pending_chars >= self.flush_chars
            || elapsed_ms >= self.flush_ms
        {
            return self.flush();
        }
        Vec::new()
    }

    /// What is kept, cut at the preview limit so that no publish truncates
    /// it, and a fresh window.
    pub(crate) fn flush(&mut self) -> Vec<String> {
        if self.pending.is_empty() {
            return Vec::new();
        }
        let text = std::mem::take(&mut self.pending);
        self.pending_chars = 0;
        self.last_publish = Instant::now();
        let chars: Vec<char> = text.chars().collect();
        chars
            .chunks(THINKING_PREVIEW_LIMIT)
            .map(|piece| piece.iter().collect())
            .collect()
    }
}

/// How a turn's reasoning reaches the room's subscribers.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ThinkingPublication {
    /// How long a publish batches for, in milliseconds.
    pub(crate) coalesce_ms: f64,
    /// How many characters a publish batches.
    pub(crate) coalesce_chars: usize,
    /// Whether they read the reasoning's text.
    pub(crate) publishes_text: bool,
    /// Whether the turn's trigger scopes its answers nowhere: only then may
    /// it publish anything to them (RFC §8.4).
    pub(crate) answers_room: bool,
}

/// Who observes a turn's reasoning, a turn run by a kit's.
#[derive(Clone)]
struct Observers {
    turn: TurnHost,
    room_id: RoomId,
    channel_id: ChannelId,
    /// Whether the room's subscribers read the reasoning's text.
    publishes_text: bool,
    /// Whether the turn's trigger scopes its answers nowhere.
    answers_room: bool,
}

impl Observers {
    /// Queues `event_type` with `thinking`'s length, its round and the
    /// channel, and its text for a channel that opted in. A turn whose
    /// trigger scopes its answers publishes nothing (RFC §8.4).
    fn publish(&self, event_type: EphemeralEventType, thinking: &str, round: u32) {
        if !self.answers_room {
            return;
        }
        let mut data = Metadata::new();
        if self.publishes_text {
            let preview: String = thinking.chars().take(THINKING_PREVIEW_LIMIT).collect();
            data.insert("thinking".to_owned(), Value::from(preview));
        }
        data.insert(
            "thinking_length".to_owned(),
            Value::from(thinking.chars().count()),
        );
        data.insert("round".to_owned(), Value::from(round));
        data.insert(
            "channel_id".to_owned(),
            Value::from(self.channel_id.to_string()),
        );
        let event = EphemeralEvent {
            channel_id: Some(self.channel_id.clone()),
            data,
            ..EphemeralEvent::new(
                self.room_id.clone(),
                event_type,
                self.channel_id.to_string(),
            )
        };
        self.turn.publish_ephemeral(event);
    }
}

/// A round's reasoning, each block between what ends it a window of its own:
/// the text, the end of the generation, a failure, a stop. Each
/// `THINKING_END` carries its own block; `text` is the round's whole.
pub(super) struct ThinkingWindow {
    observers: Option<Observers>,
    round: u32,
    coalescer: ThinkingCoalescer,
    started: bool,
    chunks: Vec<String>,
    published: usize,
}

impl ThinkingWindow {
    /// The window of round `round` of a turn in `room_id`, on `turn`'s kit,
    /// none observing outside a kit.
    pub(super) fn new(
        turn: Option<&TurnHost>,
        (room_id, channel_id): (&RoomId, &ChannelId),
        round: u32,
        publication: ThinkingPublication,
    ) -> Self {
        Self {
            observers: turn.map(|turn| Observers {
                turn: turn.clone(),
                room_id: room_id.clone(),
                channel_id: channel_id.clone(),
                publishes_text: publication.publishes_text,
                answers_room: publication.answers_room,
            }),
            round,
            coalescer: ThinkingCoalescer::new(publication.coalesce_ms, publication.coalesce_chars),
            started: false,
            chunks: Vec::new(),
            published: 0,
        }
    }

    /// Adds a piece of reasoning, its window opening: `ON_AI_THINKING` and
    /// `THINKING_START`, then its deltas as the coalescer lets them go.
    pub(super) fn add(&mut self, thinking: &str) {
        self.chunks.push(thinking.to_owned());
        let Some(observers) = self.observers.as_ref() else {
            return;
        };
        if !self.started {
            self.started = true;
            let event = ThinkingEvent {
                round_index: self.round,
                ..ThinkingEvent::new(observers.room_id.clone(), observers.channel_id.clone(), "")
            };
            observers.turn.observe_turn(&event, || {});
            observers.publish(EphemeralEventType::ThinkingStart, "", self.round);
        }
        for delta in self.coalescer.add(thinking) {
            observers.publish(EphemeralEventType::ThinkingDelta, &delta, self.round);
        }
    }

    /// Closes the window: what the coalescer holds, then `THINKING_END`
    /// with its block. Nothing when none is open.
    pub(super) fn end(&mut self) {
        if !self.started || self.chunks.is_empty() {
            return;
        }
        self.started = false;
        let block = self
            .chunks
            .get(self.published..)
            .unwrap_or_default()
            .concat();
        self.published = self.chunks.len();
        let Some(observers) = self.observers.as_ref() else {
            return;
        };
        for delta in self.coalescer.flush() {
            observers.publish(EphemeralEventType::ThinkingDelta, &delta, self.round);
        }
        observers.publish(EphemeralEventType::ThinkingEnd, &block, self.round);
    }

    /// The round's reasoning, every block's.
    pub(super) fn text(&self) -> String {
        self.chunks.concat()
    }
}

impl Drop for ThinkingWindow {
    /// A reader that stopped drops the round mid-window: its close goes out
    /// all the same (Python's `close`, on every abnormal exit of a round).
    fn drop(&mut self) {
        self.end();
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    // Python: tests/test_ai_streaming_thinking.py::test_thinking_coalescer_batches_by_size_without_loss
    #[test]
    fn thinking_coalescer_batches_by_size_without_loss() {
        let mut coalescer = ThinkingCoalescer::new(1e9, 16);
        let mut published = Vec::new();
        for _ in 0..10 {
            published.extend(coalescer.add("aaaa"));
        }
        published.extend(coalescer.flush());
        assert_eq!(published.len(), 3);
        assert_eq!(published.concat(), "a".repeat(40));
    }

    // Python: tests/test_ai_streaming_thinking.py::test_thinking_coalescer_disabled_publishes_each_delta
    #[test]
    fn thinking_coalescer_disabled_publishes_each_delta() {
        let mut coalescer = ThinkingCoalescer::new(0.0, 256);
        assert_eq!(coalescer.add("a"), ["a"]);
        assert_eq!(coalescer.add("b"), ["b"]);
        assert_eq!(coalescer.flush(), Vec::<String>::new());
    }

    // Python: tests/test_ai_streaming_thinking.py::test_thinking_coalescer_flush_splits_at_preview_limit
    #[test]
    fn thinking_coalescer_flush_splits_at_preview_limit() {
        let mut coalescer = ThinkingCoalescer::new(60_000.0, 1_000_000);
        let text = "é".repeat(THINKING_PREVIEW_LIMIT * 2 + 7);
        assert_eq!(coalescer.add(&text), Vec::<String>::new());
        let published = coalescer.flush();
        let sizes: Vec<usize> = published.iter().map(|p| p.chars().count()).collect();
        assert_eq!(sizes, [THINKING_PREVIEW_LIMIT, THINKING_PREVIEW_LIMIT, 7]);
        assert_eq!(published.concat(), text);
    }

    // Python: tests/test_ai_streaming_thinking.py::test_thinking_coalescer_flush_empty_is_noop
    #[test]
    fn thinking_coalescer_flush_empty_is_noop() {
        let mut coalescer = ThinkingCoalescer::new(80.0, 256);
        assert_eq!(coalescer.flush(), Vec::<String>::new());
    }
}

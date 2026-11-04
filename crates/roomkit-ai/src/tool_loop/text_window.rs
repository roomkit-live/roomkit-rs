//! A turn's answer text, published as it is written: Python's
//! `_TextCoalescer`, `_publish_text`, `_flush_text` and
//! `_publish_text_event` (RFC §6.4).
//!
//! The channel publishes it only when the host asks
//! ([`AiChannelOptions::publish_text_deltas`](crate::AiChannelOptions::publish_text_deltas)),
//! and the kit lets it through only for a turn read as a delegated task's
//! trace: `BEFORE_BROADCAST` judges the message a room stores, never this
//! stream (RFC §23.3 step 5).

use roomkit::TurnHost;
use roomkit::model::{ChannelId, Metadata, RoomId};
use roomkit::realtime::{EphemeralEvent, EphemeralEventType};
use serde_json::Value;
use tokio::time::Instant;

use super::thinking::ThinkingWindow;

/// How a turn's answer text reaches the room's subscribers.
#[derive(Clone, Copy, Debug)]
pub(crate) struct TextPublication {
    /// `text_coalesce_ms` and `text_coalesce_chars`, when
    /// `publish_text_deltas` asks for it.
    pub(crate) window: Option<(f64, usize)>,
    /// Whether the turn's trigger scopes its answers nowhere.
    pub(crate) answers_room: bool,
}

/// Batches an answer's text into one publish per window, of time or of size;
/// a window of 0 ms publishes each piece as it comes. Python's
/// `_TextCoalescer`, which unlike the reasoning's cuts nothing: an event's
/// text has no preview limit.
#[derive(Debug)]
struct TextCoalescer {
    flush_ms: f64,
    flush_chars: usize,
    pending: Vec<String>,
    pending_chars: usize,
    last_publish: Instant,
}

impl TextCoalescer {
    fn new(flush_ms: f64, flush_chars: usize) -> Self {
        Self {
            flush_ms,
            flush_chars,
            pending: Vec::new(),
            pending_chars: 0,
            last_publish: Instant::now(),
        }
    }

    /// Keeps `delta`; what to publish now, nothing while the window holds.
    fn add(&mut self, delta: &str) -> Option<String> {
        self.pending.push(delta.to_owned());
        self.pending_chars += delta.chars().count();
        let elapsed_ms = self.last_publish.elapsed().as_secs_f64() * 1000.0;
        (self.flush_ms <= 0.0
            || self.pending_chars >= self.flush_chars
            || elapsed_ms >= self.flush_ms)
            .then(|| self.flush())
            .flatten()
    }

    /// What is kept, and a fresh window.
    fn flush(&mut self) -> Option<String> {
        if self.pending.is_empty() {
            return None;
        }
        let text = self.pending.concat();
        self.pending.clear();
        self.pending_chars = 0;
        self.last_publish = Instant::now();
        Some(text)
    }
}

/// Where a round's answer text goes, none when nothing publishes it.
struct Publisher {
    turn: TurnHost,
    room_id: RoomId,
    channel_id: ChannelId,
}

impl Publisher {
    /// Queues a `TEXT_DELTA` of the answer being written.
    fn publish(&self, text: String, round: u32) {
        let mut data = Metadata::new();
        data.insert("text".to_owned(), Value::from(text));
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
                EphemeralEventType::TextDelta,
                self.channel_id.to_string(),
            )
        };
        self.turn.publish_ephemeral(event);
    }
}

/// A round's answer text, on its window.
pub(super) struct TextWindow {
    publishing: Option<(Publisher, TextCoalescer)>,
    round: u32,
}

impl TextWindow {
    /// The window of round `round` of a turn in `room_id`, on `turn`'s kit;
    /// none publishes outside a kit, nor a channel that does not ask, nor a
    /// turn whose trigger scopes its answers (RFC §8.4).
    pub(super) fn new(
        turn: Option<&TurnHost>,
        (room_id, channel_id): (&RoomId, &ChannelId),
        round: u32,
        publication: TextPublication,
    ) -> Self {
        let window = publication.window.filter(|_| publication.answers_room);
        let publishing = turn.zip(window).map(|(turn, (flush_ms, flush_chars))| {
            let publisher = Publisher {
                turn: turn.clone(),
                room_id: room_id.clone(),
                channel_id: channel_id.clone(),
            };
            (publisher, TextCoalescer::new(flush_ms, flush_chars))
        });
        Self { publishing, round }
    }

    /// Adds the pieces the room was handed, each as the window lets it go.
    pub(super) fn add(&mut self, pieces: &[String]) {
        let Some((publisher, coalescer)) = self.publishing.as_mut() else {
            return;
        };
        for piece in pieces {
            if let Some(text) = coalescer.add(piece) {
                publisher.publish(text, self.round);
            }
        }
    }

    /// Publishes what the window holds: before a call, so that an observer
    /// reads text and calls in the order they came, and at the round's end.
    pub(super) fn flush(&mut self) {
        let Some((publisher, coalescer)) = self.publishing.as_mut() else {
            return;
        };
        if let Some(text) = coalescer.flush() {
            publisher.publish(text, self.round);
        }
    }
}

impl Drop for TextWindow {
    /// A round that ends drops its window, however it ended: a failure or a
    /// reader that stopped mid-window (Python's `close`).
    fn drop(&mut self) {
        self.flush();
    }
}

/// What a round publishes as it is read: its reasoning and its answer's text.
pub(super) struct RoundWindows {
    pub(super) thinking: ThinkingWindow,
    pub(super) text: TextWindow,
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    // Python: tests/test_ai_streaming_text_events.py::test_a_delegated_answer_is_published_as_its_text_on_a_window, by its size half.
    #[test]
    fn a_text_coalescer_batches_by_size_without_loss() {
        let mut coalescer = TextCoalescer::new(60_000.0, 4);
        assert_eq!(coalescer.add("ab"), None);
        assert_eq!(coalescer.add("cd"), Some("abcd".to_owned()));
        assert_eq!(coalescer.add("e"), None);
        assert_eq!(coalescer.flush(), Some("e".to_owned()));
        assert_eq!(coalescer.flush(), None);
    }

    // Python: channels/_ai_coalescers.py _Window.add: a window of 0 ms disables batching.
    #[test]
    fn a_text_window_of_zero_publishes_each_piece() {
        let mut coalescer = TextCoalescer::new(0.0, 256);
        assert_eq!(coalescer.add("a"), Some("a".to_owned()));
        assert_eq!(coalescer.add("b"), Some("b".to_owned()));
    }

    // Python: channels/_ai_coalescers.py _Window.add: past its time the window is spent.
    #[test]
    fn a_text_window_past_its_time_publishes_what_it_holds() {
        let mut coalescer = TextCoalescer::new(0.000_001, 256);
        std::thread::sleep(std::time::Duration::from_millis(1));
        assert_eq!(coalescer.add("a"), Some("a".to_owned()));
    }

    // Python: channels/_ai_coalescers.py _TextCoalescer.flush keeps no limit: an
    // answer's text goes whole, where the reasoning's is cut at its preview.
    #[test]
    fn a_text_flush_is_not_cut() {
        let long = "é".repeat(1500);
        let mut coalescer = TextCoalescer::new(60_000.0, 4096);
        assert_eq!(coalescer.add(&long), None);
        assert_eq!(coalescer.flush(), Some(long));
    }
}

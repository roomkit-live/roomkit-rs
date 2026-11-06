//! A discussion agent's streamed answer held back while it may still become
//! the silence token (RFC §19.7.5 rule 13): Python's `SegmentWriter`
//! `_release_text` and `_end_held` (`core/mixins/_streaming_segments.py`).
//! The streaming target never reads a silence, and reads a start that could
//! have been one in one piece once it cannot.

use roomkit_core::rules::discussion::names::SilentToken;

/// The longest start held back: one this long is no bare token, and is
/// released.
const HOLD_LIMIT: usize = 256;

/// One response's text as its streaming target is handed it, segment by
/// segment.
#[derive(Debug)]
pub(super) struct Hold {
    token: SilentToken,
    /// The segment's start, read and not handed yet.
    held: String,
    held_chars: usize,
    /// Whether the segment's start was released: the rest goes out as read.
    released: bool,
}

impl Hold {
    pub(super) fn new(token: SilentToken) -> Self {
        Self {
            token,
            held: String::new(),
            held_chars: 0,
            released: false,
        }
    }

    /// Whether the segment's start was released: the rest goes out as read.
    pub(super) fn is_released(&self) -> bool {
        self.released
    }

    /// What of `delta`, read while the segment's start is held, the target
    /// reads now, the segment's text read so far being `read`, `delta`
    /// included: nothing while the start may still become the token, else
    /// the held start in one piece.
    pub(super) fn release(&mut self, delta: &str, read: &str) -> Option<String> {
        self.held.push_str(delta);
        self.held_chars = self.held_chars.saturating_add(delta.chars().count());
        if self.held_chars <= HOLD_LIMIT && self.token.may_become(read) {
            return None;
        }
        self.released = true;
        self.held_chars = 0;
        Some(std::mem::take(&mut self.held))
    }

    /// The held start of a segment whose text, read whole, is `read`, once
    /// it ends: what the target reads, nothing when it is the silence token.
    pub(super) fn end(&mut self, read: &str) -> Option<String> {
        let held = std::mem::take(&mut self.held);
        self.held_chars = 0;
        (!held.is_empty() && !self.token.is_silent(read)).then_some(held)
    }

    /// The segment is written: the next one starts held.
    pub(super) fn next_segment(&mut self) {
        self.released = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read_through(hold: &mut Hold, deltas: &[&str]) -> (Vec<String>, String) {
        let mut read = String::new();
        let mut handed = Vec::new();
        for delta in deltas {
            read.push_str(delta);
            if hold.is_released() {
                handed.push((*delta).to_owned());
            } else {
                handed.extend(hold.release(delta, &read));
            }
        }
        (handed, read)
    }

    // Python: tests/orchestration/test_strategy_discussion.py::test_a_streamed_silence_never_reaches_a_live_transport
    #[test]
    fn a_silence_is_held_whole_and_another_start_released_in_one_piece() {
        let mut hold = Hold::new(SilentToken::default());
        let (handed, read) =
            read_through(&mut hold, &["(", "S", "i", "l", "e", "n", "t", ")", "."]);
        assert!(handed.is_empty());
        assert_eq!(hold.end(&read), None);

        hold.next_segment();
        let (handed, read) = read_through(&mut hold, &["(", "s", "i", ")", " is the symbol"]);
        assert_eq!(handed, ["(si)", " is the symbol"]);
        assert_eq!(hold.end(&read), None);
    }

    #[test]
    fn a_start_that_ends_as_no_silence_is_handed_at_its_end() {
        let mut hold = Hold::new(SilentToken::default());
        let (handed, read) = read_through(&mut hold, &["(", "si"]);
        assert!(handed.is_empty());
        assert_eq!(hold.end(&read).as_deref(), Some("(si"));
    }

    // Python: core/mixins/_streaming_segments.py `_HOLD_LIMIT`: blank text
    // may still become the token, and is held to the limit only.
    #[test]
    fn a_start_past_the_limit_is_released() {
        let mut hold = Hold::new(SilentToken::default());
        let (handed, _) = read_through(&mut hold, &[&" ".repeat(HOLD_LIMIT)]);
        assert!(handed.is_empty());
        let (handed, _) = read_through(&mut hold, &[" "]);
        assert_eq!(handed, [" ".repeat(HOLD_LIMIT + 1)]);
    }
}

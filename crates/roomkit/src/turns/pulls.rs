//! What the owner's pulls let a turn's stream read (RFC §10.6.4), on both
//! sides.
//!
//! The owner's reader grants a window of items, and grants it again once
//! half of it was read, saying how many items it read. The executor reads
//! text deltas ahead of the reader within what was granted, and never past
//! any other item (a call's start, its end, the loop's end) until a pull
//! its reader sent once it read that item: one that says fewer items read
//! than were read through such an item grants nothing.

use super::wire::Control;

/// How many items a pull grants at once (RFC §10.6.4): deltas run ahead of
/// their reader up to this many; any other item waits for a later pull.
pub(crate) const PULL_WINDOW: u32 = 64;

/// The executor's side: the reads its owner's pulls allow.
#[derive(Debug, Default)]
pub(crate) struct PullGate {
    credits: u64,
    /// The items read off the stream, and how many were read through the
    /// last one that is no delta.
    read: u64,
    barrier: u64,
    /// The reader closed the stream: nothing more is read.
    pub(crate) closing: bool,
    /// The stream is reading an item.
    pub(crate) reading: bool,
    /// The read under way follows a delta: it runs ahead of the reader.
    pub(crate) ahead: bool,
}

impl PullGate {
    /// Whether a pull lets the stream read its next item.
    pub(crate) fn granted(&self) -> bool {
        self.credits > 0
    }

    /// Takes a pull: one sent before its reader read the last item that is
    /// no delta grants nothing past that item.
    pub(crate) fn grant(&mut self, count: u32, read: u64) {
        if read >= self.barrier {
            self.credits = self.credits.saturating_add(u64::from(count));
        }
    }

    /// A read starts, on a pull's credit.
    pub(crate) fn start_read(&mut self) {
        self.reading = true;
    }

    /// `deltas` text deltas were read, each on a credit: the stream reads
    /// on ahead of its reader.
    pub(crate) fn read_deltas(&mut self, deltas: u64) {
        self.credits = self.credits.saturating_sub(deltas);
        self.read = self.read.saturating_add(deltas);
        self.ahead = true;
    }

    /// An item that is no delta was read: only a later pull reads on.
    pub(crate) fn read_other(&mut self) {
        self.read = self.read.saturating_add(1);
        self.credits = 0;
        self.barrier = self.read;
        self.ahead = false;
    }

    /// The read is over.
    pub(crate) fn end_read(&mut self) {
        self.reading = false;
    }

    /// The reader closed: whether the read under way runs ahead of it,
    /// which the close cuts; one only a pull let start (a call running past
    /// its start) finishes first.
    pub(crate) fn close(&mut self) -> bool {
        self.closing = true;
        self.reading && self.ahead
    }
}

/// The owner's side: what its reader was granted and read.
#[derive(Debug, Default)]
pub(crate) struct PullWindow {
    granted: u32,
    read: u64,
}

impl PullWindow {
    /// The pull granting the window again once half of it was read: one per
    /// many deltas, never one per item; `None` while enough remains.
    pub(crate) fn pull(&mut self) -> Option<Control> {
        if self.granted > PULL_WINDOW / 2 {
            return None;
        }
        let count = PULL_WINDOW - self.granted;
        self.granted = PULL_WINDOW;
        Some(Control::Pull {
            count,
            read: self.read,
        })
    }

    /// The reader read `deltas` text deltas, joined in one item.
    pub(crate) fn read_deltas(&mut self, deltas: u32) {
        self.granted = self.granted.saturating_sub(deltas);
        self.read = self.read.saturating_add(u64::from(deltas));
    }

    /// The reader read an item that is no delta: the executor waits for a
    /// pull past it.
    pub(crate) fn read_other(&mut self) {
        self.granted = 0;
        self.read = self.read.saturating_add(1);
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    /// RFC §10.6.4: deltas run ahead within a grant; past any other item,
    /// only a pull sent once the reader read it grants more.
    #[test]
    fn the_stream_stops_past_an_item_that_is_no_delta_until_a_later_pull() {
        let mut gate = PullGate::default();
        gate.grant(64, 0);
        gate.read_deltas(1);
        let ahead = gate.granted();
        gate.read_other();
        let past_the_start = gate.granted();
        // A pull sent before the reader read the start grants nothing.
        gate.grant(64, 1);
        let stale = gate.granted();
        gate.grant(64, 2);
        assert_eq!(
            (ahead, past_the_start, stale, gate.granted()),
            (true, false, false, true)
        );
    }

    /// The owner's pulls follow Python's: the window at once, then again
    /// once half of it was read, saying how far the reader is.
    #[test]
    fn the_owner_grants_its_window_again_once_half_of_it_was_read() {
        let mut window = PullWindow::default();
        let first = window.pull();
        window.read_deltas(1);
        let none = window.pull();
        window.read_other();
        let after_the_start = window.pull();
        assert_eq!(
            (first, none, after_the_start),
            (
                Some(Control::Pull { count: 64, read: 0 }),
                None,
                Some(Control::Pull { count: 64, read: 2 })
            )
        );
    }
}

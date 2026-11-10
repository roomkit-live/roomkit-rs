//! The events of a streamed completion, read from the bytes as they arrive
//! (server-sent events, step 4 decision 11: no SSE crate).
//!
//! A line ends at `\n`, a `\r` before it dropped; a blank line ends an
//! event, whose `data` lines join with `\n`; a comment (`:`) and the other
//! fields are skipped. Three bounds keep a broken or hostile server from
//! growing the memory: a line, an event's data and the whole stream, each
//! failing the turn once passed, so that a line that never ends, or events
//! that never stop, are cut.

use crate::SseError;

/// The longest line a stream may send, in bytes.
pub const LINE_LIMIT: usize = 1024 * 1024;

/// The most data one event may carry, in bytes.
pub const EVENT_LIMIT: usize = 1024 * 1024;

/// The most bytes a stream may send, envelopes included. A stream weighs far
/// more than what it says: GLM wraps each token of its reasoning in about 200
/// bytes of JSON, so that 4 MiB cut a turn after some 20,000 tokens. This
/// keeps the bound on events that never stop, past any answer a model writes.
pub const STREAM_LIMIT: usize = 256 * 1024 * 1024;

/// The events of one streamed answer, as its bytes arrive.
#[derive(Debug)]
pub struct Events {
    /// The bytes of the line being read.
    line: Vec<u8>,
    /// The data of the event being read, its lines joined.
    data: Option<String>,
    /// How many bytes the answer sent so far.
    read: usize,
    /// The most bytes the answer may send.
    limit: usize,
}

impl Events {
    /// The events of an answer of at most `limit` bytes.
    pub fn new(limit: usize) -> Self {
        Self {
            line: Vec::new(),
            data: None,
            read: 0,
            limit,
        }
    }

    /// Reads `chunk`, the answer's next bytes: the data of each event it
    /// ends, in order.
    ///
    /// # Errors
    ///
    /// [`SseError::AnswerTooLong`] past the stream's bound,
    /// [`SseError::StreamLineTooLong`] for a line past [`LINE_LIMIT`],
    /// [`SseError::StreamEventTooLong`] for an event past
    /// [`EVENT_LIMIT`], [`SseError::Malformed`] for a line that is no
    /// UTF-8.
    pub fn feed(&mut self, chunk: &[u8]) -> Result<Vec<String>, SseError> {
        self.read = self.read.saturating_add(chunk.len());
        if self.read > self.limit {
            return Err(SseError::AnswerTooLong { limit: self.limit });
        }
        let mut ended = Vec::new();
        let mut rest = chunk;
        while let Some(at) = rest.iter().position(|byte| *byte == b'\n') {
            let (head, tail) = rest.split_at(at);
            self.grow_line(head)?;
            rest = tail.get(1..).unwrap_or_default();
            let line = std::mem::take(&mut self.line);
            if let Some(event) = self.take_line(&line)? {
                ended.push(event);
            }
        }
        self.grow_line(rest)?;
        Ok(ended)
    }

    /// The data of the event the answer's end cut, if any: an answer that
    /// ends without its blank line.
    ///
    /// # Errors
    ///
    /// As [`feed`](Self::feed), for the last line.
    pub fn finish(&mut self) -> Result<Option<String>, SseError> {
        let line = std::mem::take(&mut self.line);
        if !line.is_empty() {
            self.take_line(&line)?;
        }
        Ok(self.data.take())
    }

    fn grow_line(&mut self, bytes: &[u8]) -> Result<(), SseError> {
        if self.line.len().saturating_add(bytes.len()) > LINE_LIMIT {
            return Err(SseError::StreamLineTooLong { limit: LINE_LIMIT });
        }
        self.line.extend_from_slice(bytes);
        Ok(())
    }

    /// Takes one whole line: the event it ends, at a blank line.
    fn take_line(&mut self, line: &[u8]) -> Result<Option<String>, SseError> {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if line.is_empty() {
            return Ok(self.data.take());
        }
        let Some(value) = line.strip_prefix(b"data:") else {
            // A comment, or a field other than data.
            return Ok(None);
        };
        let value = value.strip_prefix(b" ").unwrap_or(value);
        let value = std::str::from_utf8(value).map_err(|_| SseError::Malformed {
            reason: "an event's data is not UTF-8".to_owned(),
        })?;
        let data = self.data.get_or_insert_with(String::new);
        let joined = data.len() + value.len() + usize::from(!data.is_empty());
        if joined > EVENT_LIMIT {
            return Err(SseError::StreamEventTooLong { limit: EVENT_LIMIT });
        }
        if !data.is_empty() {
            data.push('\n');
        }
        data.push_str(value);
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn events_split_across_chunks_come_whole_and_in_order() {
        let mut events = Events::new(1024);
        let mut read = events.feed(b"data: {\"a\"").unwrap();
        read.extend(
            events
                .feed(b": 1}\r\n\r\n: keep-alive\n\ndata: [DONE]\n\n")
                .unwrap(),
        );
        assert_eq!(read, ["{\"a\": 1}", "[DONE]"]);
    }

    #[test]
    fn an_event_s_data_lines_join_and_other_fields_are_skipped() {
        let mut events = Events::new(1024);
        let read = events
            .feed(b"event: message\nid: 7\ndata: first\ndata:second\n\n")
            .unwrap();
        assert_eq!(read, ["first\nsecond"]);
    }

    #[test]
    fn an_event_the_answer_s_end_cuts_is_read() {
        let mut events = Events::new(1024);
        assert_eq!(events.feed(b"data: last").unwrap(), Vec::<String>::new());
        assert_eq!(events.finish().unwrap().as_deref(), Some("last"));
    }

    /// A server that sends a line without end is cut at the line's bound,
    /// whatever the answer's.
    #[test]
    fn a_line_that_never_ends_is_cut_at_its_bound() {
        let mut events = Events::new(usize::MAX);
        let chunk = vec![b'x'; 64 * 1024];
        let failed = (0..64).find_map(|_| events.feed(&chunk).err());
        assert_eq!(
            failed,
            Some(SseError::StreamLineTooLong { limit: LINE_LIMIT })
        );
    }

    /// An event whose data lines never stop is cut at the event's bound.
    #[test]
    fn an_event_that_never_ends_is_cut_at_its_bound() {
        let mut events = Events::new(usize::MAX);
        let line = format!("data: {}\n", "x".repeat(1023));
        let failed = (0..2048).find_map(|_| events.feed(line.as_bytes()).err());
        assert_eq!(
            failed,
            Some(SseError::StreamEventTooLong { limit: EVENT_LIMIT })
        );
    }

    /// Events that never stop are cut at the stream's bound.
    #[test]
    fn events_that_never_stop_are_cut_at_the_stream_s_bound() {
        let mut events = Events::new(4096);
        let failed = (0..1024).find_map(|_| events.feed(b"data: {}\n\n").err());
        assert_eq!(failed, Some(SseError::AnswerTooLong { limit: 4096 }));
    }
}

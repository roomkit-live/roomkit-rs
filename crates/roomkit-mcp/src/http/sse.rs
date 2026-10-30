//! Bound raw SSE events before the parser allocates a complete event.
use super::client::{Client, Failure};
use futures_util::{StreamExt, stream::try_unfold};
use rmcp::transport::common::client_side_sse::BoxedSseResponse;

pub(super) fn stream(
    response: reqwest::Response,
    lease: Client,
    failure: Failure,
    limit: usize,
) -> BoxedSseResponse {
    let bytes = try_unfold(
        (response, lease, failure, Bound::default()),
        move |(mut response, lease, failure, mut bound)| async move {
            let Some(chunk) = response
                .chunk()
                .await
                .map_err(|_| failure.record(None, "HTTP event stream interrupted"))?
            else {
                return Ok(None);
            };
            if !bound.accept(&chunk, limit) {
                return Err(failure.record(None, "MCP SSE event exceeds its size limit"));
            }
            Ok(Some((chunk, (response, lease, failure, bound))))
        },
    );
    sse_stream::SseStream::from_bytes_stream(bytes).boxed()
}
#[derive(Default)]
struct Bound {
    event: usize,
    line: usize,
    cr: bool,
}
impl Bound {
    fn accept(&mut self, bytes: &[u8], limit: usize) -> bool {
        for &byte in bytes {
            if byte == b'\n' && self.cr {
                self.cr = false;
                // Count CRLF as two bytes inside an event, without treating
                // its LF as a second line ending. The blank line reset has
                // already consumed a delimiter when event is zero.
                if self.event > 0 {
                    self.event = self.event.saturating_add(1);
                    if self.event > limit {
                        return false;
                    }
                }
                continue;
            }
            self.cr = byte == b'\r';
            self.event = self.event.saturating_add(1);
            if self.event > limit {
                return false;
            }
            if matches!(byte, b'\n' | b'\r') {
                if self.line == 0 {
                    self.event = 0;
                }
                self.line = 0;
            } else {
                self.line = self.line.saturating_add(1);
            }
        }
        true
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounds_events_across_chunks_and_line_endings() {
        for delimiter in ["\n", "\r", "\r\n"] {
            let event = format!("data: a{delimiter}{delimiter}");
            let mut bound = Bound::default();
            for _ in 0..20 {
                for byte in event.bytes() {
                    assert!(bound.accept(&[byte], 12));
                }
            }
        }
        let mut bound = Bound::default();
        assert!(bound.accept(b"data: 123", 12));
        assert!(!bound.accept(b"4567", 12));
        let mut bound = Bound::default();
        assert!(bound.accept(b"data: a\n", 12));
        assert!(!bound.accept(b"data: b", 12));
        let mut bound = Bound::default();
        assert!(bound.accept(b"data: a\r", 8));
        assert!(!bound.accept(b"\n", 8));
    }
}

//! The shared bounded SSE decoder, with the OpenAI provider's errors.
use crate::error::OpenAiError;
pub(crate) use roomkit_provider_http::sse::STREAM_LIMIT;

#[derive(Debug)]
pub(crate) struct Events(roomkit_provider_http::sse::Events);

impl Events {
    pub(crate) fn new(limit: usize) -> Self {
        Self(roomkit_provider_http::sse::Events::new(limit))
    }
    pub(crate) fn feed(&mut self, chunk: &[u8]) -> Result<Vec<String>, OpenAiError> {
        self.0.feed(chunk).map_err(error)
    }
    pub(crate) fn finish(&mut self) -> Result<Option<String>, OpenAiError> {
        self.0.finish().map_err(error)
    }
}

fn error(error: roomkit_provider_http::SseError) -> OpenAiError {
    use roomkit_provider_http::SseError;
    match error {
        SseError::AnswerTooLong { limit } => OpenAiError::AnswerTooLong { limit },
        SseError::StreamLineTooLong { limit } => OpenAiError::StreamLineTooLong { limit },
        SseError::StreamEventTooLong { limit } => OpenAiError::StreamEventTooLong { limit },
        SseError::Malformed { reason } => OpenAiError::Malformed { reason },
    }
}

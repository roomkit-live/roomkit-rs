//! What building an agent or running its turn fails with.

use std::error::Error;

/// What building an [`OpenAiAgent`](crate::OpenAiAgent) or running its turn
/// failed with.
///
/// A turn's error reaches the kit as the channel's error: the kit records
/// its message on the turn's `ON_ERROR` event and on the caller's result,
/// and nothing of it reaches the room (RFC §6.4, §9.2). Its message never
/// holds the API key.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum OpenAiError {
    /// The base URL is refused: it is no `http` or `https` URL once
    /// the selected API path follows it, or it holds a query or a fragment,
    /// which the path would follow instead of the URL's path (Azure's
    /// `?api-version=`). The error does not repeat the URL: a URL may carry
    /// credentials.
    #[error("the base URL is refused: {reason}")]
    BaseUrl {
        /// Why.
        reason: String,
    },
    /// The HTTP client could not be built.
    #[error("the HTTP client could not be built: {reason}")]
    Client {
        /// Why, with its causes.
        reason: String,
    },
    /// The completion's request failed before the provider answered it in
    /// full: the endpoint could not be reached, or the completion took
    /// longer than the configured timeout.
    #[error("the completion request failed: {reason}")]
    Request {
        /// Why, with its causes.
        reason: String,
    },
    /// The provider answered with an HTTP error.
    #[error("the provider answered HTTP {status}{}: {message}", detail(.error_type.as_deref(), .code.as_deref()))]
    Status {
        /// The HTTP status code.
        status: u16,
        /// The error's type, as the provider names it (`error.type`).
        error_type: Option<String>,
        /// The error's code, as the provider names it (`error.code`).
        code: Option<String>,
        /// The provider's message, or the status's reason when it gave
        /// none: on one line, within 500 characters, the API key removed.
        message: String,
    },
    /// The provider's answer is longer than a turn reads.
    #[error("the provider's answer is longer than {limit} bytes")]
    AnswerTooLong {
        /// How many bytes a turn reads at most.
        limit: usize,
    },
    /// The provider's answer does not match the selected API's wire format.
    #[error("the provider's answer is malformed: {reason}")]
    Malformed {
        /// What the answer lacks.
        reason: String,
    },
    /// A streamed answer sent a line longer than a turn reads: a server that
    /// never ends its line is cut there.
    #[error("the provider's stream sent a line longer than {limit} bytes")]
    StreamLineTooLong {
        /// How many bytes a line may hold.
        limit: usize,
    },
    /// A streamed answer sent an event longer than a turn reads.
    #[error("the provider's stream sent an event longer than {limit} bytes")]
    StreamEventTooLong {
        /// How many bytes an event's data may hold.
        limit: usize,
    },
    /// A streamed answer reported the provider's error mid-stream.
    #[error("the provider's stream failed: {message}")]
    StreamFailed {
        /// The provider's message: on one line, within 500 characters, the
        /// API key removed.
        message: String,
    },
}

impl OpenAiError {
    /// A request that failed with `error`, its URL left out: a base URL may
    /// carry credentials.
    pub(crate) fn request(error: reqwest::Error) -> Self {
        Self::Request {
            reason: with_causes(&error.without_url()),
        }
    }

    /// A client that could not be built because of `error`.
    pub(crate) fn client(error: reqwest::Error) -> Self {
        Self::Client {
            reason: with_causes(&error.without_url()),
        }
    }
}

/// The type and the code of a provider's error, in parentheses, as far as
/// it gives them.
fn detail(error_type: Option<&str>, code: Option<&str>) -> String {
    match (error_type, code) {
        (Some(error_type), Some(code)) => format!(" ({error_type}, {code})"),
        (Some(named), None) | (None, Some(named)) => format!(" ({named})"),
        (None, None) => String::new(),
    }
}

/// `error`'s message, then each of its causes', joined by `: `, so that the
/// kit, which records one message, records why.
fn with_causes(error: &dyn Error) -> String {
    let mut message = error.to_string();
    let mut cause = error.source();
    while let Some(error) = cause {
        message.push_str(": ");
        message.push_str(&error.to_string());
        cause = error.source();
    }
    message
}

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
    /// The base URL, with `/chat/completions` after it, is no `http` or
    /// `https` URL.
    #[error("the base URL {base_url:?} is not an http or https URL")]
    BaseUrl {
        /// The base URL as configured.
        base_url: String,
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
    #[error("the provider answered HTTP {status}: {message}")]
    Status {
        /// The HTTP status code.
        status: u16,
        /// The provider's message, or the start of its answer when it gave
        /// none.
        message: String,
    },
    /// The provider's answer is no chat completion.
    #[error("the provider's answer is not a chat completion: {reason}")]
    Malformed {
        /// What the answer lacks.
        reason: String,
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

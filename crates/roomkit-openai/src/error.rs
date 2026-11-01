//! What a provider's request fails with, before the loop reads it as a
//! [`ProviderError`].

use std::error::Error;

use roomkit_ai::{ProviderError, ProviderErrorOptions};

/// What building a provider or its request failed with: crate-private, the
/// loop reads each as a [`ProviderError`] (Python's rule of
/// `provider_error`). Its message never holds the API key.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub(crate) enum OpenAiError {
    /// The host could not provide an authorized bearer credential.
    #[error("{0}")]
    Credential(#[from] crate::TokenError),
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
    /// A tool's name is one the vendor refuses (RFC §6.7): checked before
    /// the request, which never goes.
    #[error("tool '{name}': {vendor} accepts tool names matching {rule}")]
    ToolName {
        /// The name.
        name: String,
        /// The vendor whose rule refuses it.
        vendor: String,
        /// The names the vendor accepts, as a pattern matched whole.
        rule: String,
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

/// The statuses a retry may get past, Python's `RETRYABLE_STATUS_CODES`.
fn retryable_status(status: u16) -> bool {
    matches!(status, 408 | 409 | 429 | 500..=599)
}

/// Whether `message`, which came with no status, names a transient failure:
/// a retried status, a rate limit or an overload, as whole words (Python's
/// `_names_transient`: `rate[ _-]?limit(ed|s)?`, `too many requests`,
/// `overloaded`, or a status number).
fn names_transient(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    let words: Vec<&str> = lower
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|word| !word.is_empty())
        .collect();
    let limit = |word: &str| matches!(word, "limit" | "limits" | "limited");
    let named = words.iter().enumerate().any(|(at, word)| {
        let next = words.get(at + 1).copied();
        (*word == "rate" && next.is_some_and(limit))
            || matches!(
                *word,
                "ratelimit" | "ratelimits" | "ratelimited" | "overloaded"
            )
            || (*word == "too" && next == Some("many") && words.get(at + 2) == Some(&"requests"))
    });
    named
        || words
            .iter()
            .filter(|word| word.len() == 3)
            .filter_map(|word| word.parse::<u16>().ok())
            .any(retryable_status)
}

impl From<OpenAiError> for ProviderError {
    /// The failure as every provider reads one (RFC §6.7, Python's
    /// `provider_error`): a status decides whether a retry may succeed, a
    /// lost connection may, an answer that could not be read may not; the
    /// error itself is the source.
    fn from(error: OpenAiError) -> Self {
        let (retryable, status_code, context_overflow) = match &error {
            OpenAiError::Status { status, code, .. } => (
                retryable_status(*status),
                Some(*status),
                (code.as_deref() == Some("context_length_exceeded")).then_some(true),
            ),
            OpenAiError::Request { .. } => (true, None, None),
            OpenAiError::StreamFailed { message } => (names_transient(message), None, None),
            OpenAiError::ToolName { .. } => (false, None, Some(false)),
            OpenAiError::Credential(_)
            | OpenAiError::BaseUrl { .. }
            | OpenAiError::Client { .. }
            | OpenAiError::AnswerTooLong { .. }
            | OpenAiError::Malformed { .. }
            | OpenAiError::StreamLineTooLong { .. }
            | OpenAiError::StreamEventTooLong { .. } => (false, None, None),
        };
        let options = ProviderErrorOptions {
            retryable,
            provider: "openai".to_owned(),
            status_code,
            context_overflow,
        };
        Self::new(error.to_string(), options).with_source(error)
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    /// Python's `provider_error`: a status decides, a lost connection is
    /// retried, an unreadable answer is not, a message is read for a
    /// transient failure; OpenAI's `context_length_exceeded` code is an
    /// overflow.
    #[test]
    fn a_failure_reads_as_python_s_provider_error() {
        let status = |status, code: Option<&str>| OpenAiError::Status {
            status,
            error_type: None,
            code: code.map(str::to_owned),
            message: "m".to_owned(),
        };
        let read = |error: OpenAiError| {
            let error = ProviderError::from(error);
            (error.retryable, error.status_code, error.context_overflow)
        };
        assert_eq!(
            [
                read(status(429, None)),
                read(status(503, None)),
                read(status(400, Some("context_length_exceeded"))),
                read(status(401, None)),
                read(OpenAiError::Request {
                    reason: "reset".to_owned()
                }),
                read(OpenAiError::Malformed {
                    reason: "x".to_owned()
                }),
                read(OpenAiError::StreamFailed {
                    message: "Rate limit reached".to_owned()
                }),
                read(OpenAiError::StreamFailed {
                    message: "token limit".to_owned()
                }),
            ],
            [
                (true, Some(429), None),
                (true, Some(503), None),
                (false, Some(400), Some(true)),
                (false, Some(401), None),
                (true, None, None),
                (false, None, None),
                (true, None, None),
                (false, None, None),
            ]
        );
    }
}

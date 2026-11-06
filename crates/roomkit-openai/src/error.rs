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
    /// The model listing's request failed before the endpoint answered it
    /// in full: the endpoint could not be reached, or it took longer than
    /// the configured timeout.
    #[error("the model listing request failed: {reason}")]
    ListingRequest {
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
        /// The HTTP status the error object names (Python's
        /// `_described_status`), if it names one.
        status: Option<u16>,
    },
    /// The provider answered a completion with an error object in its
    /// place, as a gateway answers 200 (Python's `error_body_failure`).
    #[error("the provider answered with an error: {message}")]
    Answered {
        /// The HTTP status the error object names, if it names one.
        status: Option<u16>,
        /// The error's message: on one line, within 500 characters, the
        /// API key removed.
        message: String,
    },
    /// An image part's `data:` URI does not read (Python's
    /// `image_part_payload`): checked before the request, which never goes.
    #[error("invalid image part: {0}")]
    Image(roomkit_core::rules::transcript::DataUriError),
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
    /// OpenAI's endpoint refuses the model this turn, as its catalogue
    /// states it (RFC §6.7): checked before the request, which never goes.
    #[error("{reason}")]
    ModelRefused {
        /// Python's words.
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

    /// This failure as a model listing's: a request that failed says so,
    /// any other reads the same.
    pub(crate) fn of_listing(self) -> Self {
        if let Self::Request { reason } = self {
            Self::ListingRequest { reason }
        } else {
            self
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

/// Whether `message`, which came with no status, names a transient failure,
/// as Python's `_names_transient` reads it: `rate[ _-]?limit(ed|s)?`, `too
/// many requests` or `overloaded`, in any case, or a retried status
/// (`[45]\d\d`), each a whole word, a word's letters including `_`.
fn names_transient(message: &str) -> bool {
    let chars: Vec<char> = message.chars().collect();
    let in_word = |at: usize| {
        chars
            .get(at)
            .is_some_and(|c| c.is_alphanumeric() || *c == '_')
    };
    let whole = |start: usize, end: usize| (start == 0 || !in_word(start - 1)) && !in_word(end);
    (0..chars.len()).any(|start| {
        transient_words_at(&chars, start)
            .into_iter()
            .any(|end| whole(start, end))
            || status_at(&chars, start)
                .is_some_and(|(end, status)| whole(start, end) && retryable_status(status))
    })
}

/// Where each of the transient words that start at `start` in `chars`
/// ends: each way `rate[ _-]?limit(ed|s)?` reads there, `too many
/// requests`, `overloaded`.
fn transient_words_at(chars: &[char], start: usize) -> Vec<usize> {
    let mut ends: Vec<usize> = ["too many requests", "overloaded"]
        .into_iter()
        .filter_map(|words| spelled_at(chars, start, words))
        .collect();
    if let Some(rate) = spelled_at(chars, start, "rate") {
        for separator in ["", " ", "_", "-"] {
            let Some(limit) = spelled_at(chars, rate, separator)
                .and_then(|after| spelled_at(chars, after, "limit"))
            else {
                continue;
            };
            ends.push(limit);
            ends.extend(
                ["ed", "s"]
                    .into_iter()
                    .filter_map(|suffix| spelled_at(chars, limit, suffix)),
            );
        }
    }
    ends
}

/// Where `text` ends when `chars` spell it at `at`, in any case.
fn spelled_at(chars: &[char], at: usize, text: &str) -> Option<usize> {
    let mut end = at;
    for expected in text.chars() {
        let found = chars.get(end)?;
        if !found.eq_ignore_ascii_case(&expected) {
            return None;
        }
        end += 1;
    }
    Some(end)
}

/// The status a number of three digits starting `4` or `5` at `start`
/// says, and where it ends.
fn status_at(chars: &[char], start: usize) -> Option<(usize, u16)> {
    let digits = chars.get(start..start + 3)?;
    if !matches!(digits.first(), Some('4' | '5')) {
        return None;
    }
    let number: String = digits.iter().collect();
    number.parse().ok().map(|status| (start + 3, status))
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
            OpenAiError::Request { .. } | OpenAiError::ListingRequest { .. } => (true, None, None),
            // An error object read inside a stream: the status it names
            // decides, else its words (Python's `provider_error`).
            OpenAiError::StreamFailed {
                status: Some(status),
                ..
            } => (retryable_status(*status), Some(*status), None),
            OpenAiError::StreamFailed {
                message,
                status: None,
            } => (names_transient(message), None, None),
            // An error object answered in place of a completion: only the
            // status it names decides (Python's `error_body_failure`).
            OpenAiError::Answered { status, .. } => {
                (status.is_some_and(retryable_status), *status, None)
            }
            OpenAiError::ToolName { .. } | OpenAiError::ModelRefused { .. } => {
                (false, None, Some(false))
            }
            OpenAiError::Credential(_)
            | OpenAiError::Image(_)
            | OpenAiError::BaseUrl { .. }
            | OpenAiError::Client { .. }
            | OpenAiError::AnswerTooLong { .. }
            | OpenAiError::Malformed { .. }
            | OpenAiError::StreamLineTooLong { .. }
            | OpenAiError::StreamEventTooLong { .. } => (false, None, None),
        };
        // Refused before the request, which never went (RFC §6.4).
        let before_request = matches!(
            error,
            OpenAiError::ToolName { .. } | OpenAiError::ModelRefused { .. }
        );
        let options = ProviderErrorOptions {
            retryable,
            provider: "openai".to_owned(),
            status_code,
            context_overflow,
            before_request,
        };
        Self::new(error.to_string(), options).with_source(error)
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    /// Python's `_names_transient`: its words read as Python's regular
    /// expressions read them, `_` a letter of a word and one separator at
    /// most (`tasks/probes/step5-review/mcp-openai/py_names_transient.out`).
    #[test]
    fn a_message_names_a_transient_failure_as_python_reads_it() {
        let messages = [
            "rate_limit_exceeded",
            "overloaded_error",
            "error_503",
            "rate  limit",
            "Rate limit reached",
            "HTTP 503",
            "ratelimited: retry",
            "Too many requests",
            "rate-limits",
            "HTTP 404",
        ];
        assert_eq!(
            messages.map(names_transient),
            [
                false, false, false, false, true, true, true, true, true, false
            ]
        );
    }

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
                    message: "Rate limit reached".to_owned(),
                    status: None,
                }),
                read(OpenAiError::StreamFailed {
                    message: "token limit".to_owned(),
                    status: None,
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

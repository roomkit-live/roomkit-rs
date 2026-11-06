//! What a provider's call fails with: Python's
//! `roomkit.providers.ai.ProviderError`.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

/// A provider's call that failed (RFC §6.7): Python's
/// `ProviderError(message, *, retryable=False, provider='', status_code=None,
/// context_overflow=None, before_request=False)`. The turn records its message; it never reaches
/// the room nor a model. The error it was raised from, when there is one,
/// is its [`source`](Error::source), as Python's `raise ... from`.
#[derive(Clone, Debug)]
pub struct ProviderError {
    /// What failed.
    pub message: String,
    /// Whether a retry may succeed.
    pub retryable: bool,
    /// The provider that failed, by its name.
    pub provider: String,
    /// The HTTP status the vendor answered, when it answered one.
    pub status_code: Option<u16>,
    /// Whether the request was past the model's context window, when the
    /// vendor said.
    pub context_overflow: Option<bool>,
    /// Whether the provider refused before sending any request: nothing
    /// reached the vendor, so the failure says nothing of what the request
    /// carried (RFC §6.4).
    pub before_request: bool,
    source: Option<Arc<dyn Error + Send + Sync>>,
}

/// What [`ProviderError::new`] takes beside the message: Python's
/// keywords, with its defaults.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProviderErrorOptions {
    /// Whether a retry may succeed.
    pub retryable: bool,
    /// The provider that failed, by its name.
    pub provider: String,
    /// The HTTP status the vendor answered.
    pub status_code: Option<u16>,
    /// Whether the request was past the model's context window.
    pub context_overflow: Option<bool>,
    /// Whether the provider refused before sending any request.
    pub before_request: bool,
}

impl ProviderError {
    /// The failure `message`, as `options` qualify it.
    pub fn new(message: impl Into<String>, options: ProviderErrorOptions) -> Self {
        let ProviderErrorOptions {
            retryable,
            provider,
            status_code,
            context_overflow,
            before_request,
        } = options;
        Self {
            message: message.into(),
            retryable,
            provider,
            status_code,
            context_overflow,
            before_request,
            source: None,
        }
    }

    /// The same failure, raised from `source`: Python's `raise ... from`.
    #[must_use]
    pub fn with_source(mut self, source: impl Error + Send + Sync + 'static) -> Self {
        self.source = Some(Arc::new(source));
        self
    }
}

impl fmt::Display for ProviderError {
    /// The message, led by the provider and the status it answered:
    /// `cerebras (402): Payment required`, as Python's `__str__`. A message
    /// that already names its provider, as a word in any case, or a failure
    /// with no provider, reads as given.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.provider.is_empty() || names_word(&self.message, &self.provider) {
            return formatter.write_str(&self.message);
        }
        formatter.write_str(&self.provider)?;
        if let Some(status) = self.status_code {
            write!(formatter, " ({status})")?;
        }
        write!(formatter, ": {}", self.message)
    }
}

/// Whether `text` holds `word` between word boundaries, case aside:
/// Python's `re.search(rf"\b{re.escape(word)}\b", text, re.IGNORECASE)`,
/// a word character a letter, a digit or `_`. Two of Python's Unicode
/// rules are not followed: its fold of `İ` to `i`, and its reading of a
/// combining vowel sign (`ि`) as no word character.
fn names_word(text: &str, word: &str) -> bool {
    let text: Vec<char> = text.chars().collect();
    let word: Vec<char> = word.chars().collect();
    if word.is_empty() {
        return false;
    }
    let is_word = |at: Option<usize>| {
        at.and_then(|at| text.get(at))
            .is_some_and(|&c| c.is_alphanumeric() || c == '_')
    };
    text.windows(word.len()).enumerate().any(|(start, found)| {
        let last = start + word.len() - 1;
        found
            .iter()
            .zip(&word)
            .all(|(&found, &wanted)| same_letter(found, wanted))
            && is_word(start.checked_sub(1)) != is_word(Some(start))
            && is_word(Some(last)) != is_word(Some(last + 1))
    })
}

/// Whether two characters are the same but for their case, by their
/// lowercase or their uppercase (`ſ` is `s`, `ς` is `σ`).
fn same_letter(found: char, wanted: char) -> bool {
    found == wanted
        || found.to_lowercase().eq(wanted.to_lowercase())
        || found.to_uppercase().eq(wanted.to_uppercase())
}

impl Error for ProviderError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        let source: &(dyn Error + 'static) = self.source.as_deref()?;
        Some(source)
    }
}

impl PartialEq for ProviderError {
    /// Two failures are the same when they say the same: their causes are
    /// not compared.
    fn eq(&self, other: &Self) -> bool {
        (
            &self.message,
            self.retryable,
            &self.provider,
            self.status_code,
            self.context_overflow,
            self.before_request,
        ) == (
            &other.message,
            other.retryable,
            &other.provider,
            other.status_code,
            other.context_overflow,
            other.before_request,
        )
    }
}

impl From<String> for ProviderError {
    /// A failure saying `message`, not retryable.
    fn from(message: String) -> Self {
        Self::new(message, ProviderErrorOptions::default())
    }
}

impl From<&str> for ProviderError {
    fn from(message: &str) -> Self {
        Self::from(message.to_owned())
    }
}

impl ProviderError {
    /// Whether the provider refused the request as past the model's
    /// context window (RFC §6.4): its structural classification, believed
    /// either way, else its message read by
    /// [`is_context_overflow_message`]. Python's
    /// `AIChannel._is_context_overflow`.
    pub(crate) fn is_context_overflow(&self) -> bool {
        self.context_overflow
            .unwrap_or_else(|| is_context_overflow_message(&self.message))
    }
}

/// The wordings observed across providers for a context-window refusal,
/// lowercase: Python's `_CONTEXT_OVERFLOW_PHRASES`. "request too large" is
/// left out: it is OpenAI's wording for a tokens-per-minute rate limit, a
/// 429 worth retrying.
const CONTEXT_OVERFLOW_PHRASES: &[&str] = &[
    "context length",
    "context_length",
    "maximum context",
    "context window",
    "token limit",
    "too many tokens",
    "prompt is too long",
    "exceeds the model",
    "range of input length",
    "exceeds the maximum number of tokens",
];

/// Whether a provider's error `text` describes a context-window overflow:
/// Python's `roomkit.providers.ai.is_context_overflow_message`. The words
/// are the fallback: an error classified structurally carries
/// [`ProviderError::context_overflow`] instead.
pub fn is_context_overflow_message(text: &str) -> bool {
    let haystack = text.to_lowercase();
    CONTEXT_OVERFLOW_PHRASES
        .iter()
        .any(|phrase| haystack.contains(phrase))
}

#[cfg(test)]
mod tests {
    use super::*;

    const TPM_RATE_LIMIT: &str = "Request too large for gpt-4o in organization org-x on tokens \
        per min (TPM): Limit 30000, Requested 31000.";

    fn refused(message: &str, context_overflow: Option<bool>) -> ProviderError {
        ProviderError::new(
            message,
            ProviderErrorOptions {
                context_overflow,
                ..ProviderErrorOptions::default()
            },
        )
    }

    // Python: tests/test_channels/test_ai_streaming_compaction.py::test_is_context_overflow_reads_the_typed_flag_first
    #[test]
    fn is_context_overflow_reads_the_typed_flag_first() {
        let read = [
            refused(
                r#"{"code": "refused", "detail": "window blown"}"#,
                Some(true),
            ),
            refused("prompt is too long: 210000 tokens > 200000 maximum", None),
            refused("boom", None),
            refused("maximum context length exceeded", Some(false)),
        ]
        .map(|error| error.is_context_overflow());
        assert_eq!(read, [true, true, false, false]);
    }

    // Python: tests/test_channels/test_ai_streaming_compaction.py::test_the_shared_phrase_list_covers_both_packages_wordings
    #[test]
    fn the_shared_phrase_list_covers_both_packages_wordings() {
        for text in [
            "This model's maximum context length is 128000 tokens",
            "error code: context_length_exceeded",
            "input exceeds the model capacity",
            "the request rode past the token limit",
            "prompt is too long: 210000 tokens > 200000 maximum",
            "Range of input length should be [1, 30720]",
            "400 INVALID_ARGUMENT. {'error': {'code': 400, 'message': 'The input token count \
             exceeds the maximum number of tokens allowed (8192).', 'status': 'INVALID_ARGUMENT'}}",
            "The input token count (1200000) exceeds the maximum number of tokens allowed (1048576).",
        ] {
            assert!(is_context_overflow_message(text), "{text}");
        }
        for text in [
            "invalid api key",
            "429 RESOURCE_EXHAUSTED. Quota exceeded for metric: \
             generativelanguage.googleapis.com/generate_content_free_tier_input_token_count",
            TPM_RATE_LIMIT,
        ] {
            assert!(!is_context_overflow_message(text), "{text}");
        }
    }

    // Python: tests/test_channels/test_ai_tool_loop.py::TestContextOverflowRecovery::test_is_context_overflow_matches_known_patterns
    #[test]
    fn is_context_overflow_matches_known_patterns() {
        for text in [
            "context length exceeded",
            "maximum context length is 200000",
            "token limit reached",
            "too many tokens in the request",
            "prompt is too long for model",
        ] {
            assert!(refused(text, None).is_context_overflow(), "{text}");
        }
    }

    // Python: tests/test_channels/test_ai_tool_loop.py::TestContextOverflowRecovery::test_is_context_overflow_rejects_unrelated_errors
    #[test]
    fn is_context_overflow_rejects_unrelated_errors() {
        assert!(!refused("Invalid API key", None).is_context_overflow());
        assert!(
            !refused("Request too large for gpt-4o on tokens per min (TPM)", None)
                .is_context_overflow()
        );
    }

    fn failed(message: &str, provider: &str, status_code: Option<u16>) -> ProviderError {
        ProviderError::new(
            message,
            ProviderErrorOptions {
                provider: provider.to_owned(),
                status_code,
                ..ProviderErrorOptions::default()
            },
        )
    }

    // Python: tests/test_provider_error_names_provider.py::test_the_message_leads_with_the_provider_and_its_status
    #[test]
    fn the_message_leads_with_the_provider_and_its_status() {
        let error = failed(
            "Error code: 402 - {'message': 'Payment required to access this resource.'}",
            "cerebras",
            Some(402),
        );
        assert_eq!(
            error.to_string(),
            "cerebras (402): Error code: 402 - {'message': 'Payment required to access this \
             resource.'}"
        );
        // The provider's own text is untouched for whoever reads it.
        assert!(error.message.starts_with("Error code: 402"));
    }

    // Python: tests/test_provider_error_names_provider.py::test_without_a_status_the_provider_still_leads
    #[test]
    fn without_a_status_the_provider_still_leads() {
        assert_eq!(
            failed("connection reset", "anthropic", None).to_string(),
            "anthropic: connection reset"
        );
    }

    // Python: tests/test_provider_error_names_provider.py::test_an_error_without_a_provider_reads_as_given
    #[test]
    fn an_error_without_a_provider_reads_as_given() {
        assert_eq!(ProviderError::from("boom").to_string(), "boom");
    }

    // Python: tests/test_provider_error_names_provider.py::test_a_rewrapped_error_is_not_prefixed_twice
    #[test]
    fn a_rewrapped_error_is_not_prefixed_twice() {
        let inner = failed("Payment required", "xai", Some(402));
        let outer = failed(&inner.to_string(), "xai", None);
        assert_eq!(outer.to_string(), "xai (402): Payment required");
    }

    // Python: tests/test_provider_error_names_provider.py::test_a_message_that_names_its_provider_is_left_as_it_is
    // and Python's `\b...\b` under IGNORECASE: a word of the message, in
    // any case, names the provider; a part of a longer word does not.
    #[test]
    fn a_message_that_names_its_provider_is_left_as_it_is() {
        let messages = [
            ("tool 'X': acme accepts tool names matching [a-z]+", "acme"),
            ("ACME refused", "acme"),
            ("see api.openai.com", "openai"),
            ("acmecorp refused", "acme"),
            ("acme_x refused", "acme"),
            ("x-acme2 refused", "acme"),
            ("Überlastet", "über"),
            ("küber refused", "über"),
            ("ÜBER refused", "über"),
            ("ſ refused", "s"),
            ("σ refused", "ς"),
            ("ς refused", "Σ"),
        ];
        assert_eq!(
            messages.map(|(message, provider)| failed(message, provider, None).to_string()),
            [
                "tool 'X': acme accepts tool names matching [a-z]+",
                "ACME refused",
                "see api.openai.com",
                "acme: acmecorp refused",
                "acme: acme_x refused",
                "acme: x-acme2 refused",
                "über: Überlastet",
                "über: küber refused",
                "ÜBER refused",
                "ſ refused",
                "σ refused",
                "ς refused",
            ]
        );
    }
}

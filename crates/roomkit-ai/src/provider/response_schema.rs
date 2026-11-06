//! Constrained JSON output (RFC §6.7): what a provider fails with when a
//! response schema cannot be honoured, and the checks every provider shares,
//! Python's `roomkit.providers.ai.response_schema`.

use std::error::Error;
use std::fmt;

use serde_json::{Map, Value};

use super::json_schema::{SchemaMismatchOptions, check_portable_schema, schema_mismatch};
use super::response::{is_malformed_call, ran_out_of_room};
use super::{
    AiContext, ProviderError, ProviderErrorOptions, ProviderStream, StreamDone, StreamEvent,
};
use crate::stream::{Yielder, items, next};

/// Why a response schema could not be honoured (RFC §6.7): Python's
/// `ResponseSchemaFailure`, the reason of a [`ResponseSchemaError`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ResponseSchemaFailure {
    /// The call cannot carry a schema: the provider takes none, or none
    /// beside the turn's tools. Raised before any request is sent.
    Unsupported,
    /// The model declined to answer.
    Refusal,
    /// The output cap or the context window cut the document.
    Truncated,
    /// The text is not a JSON document satisfying the schema, from a server
    /// that took the constraint and did not apply it.
    InvalidJson,
}

impl ResponseSchemaFailure {
    /// Python's value: `unsupported`, `refusal`, `truncated`,
    /// `invalid_json`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unsupported => "unsupported",
            Self::Refusal => "refusal",
            Self::Truncated => "truncated",
            Self::InvalidJson => "invalid_json",
        }
    }
}

/// A response schema was set and no answer satisfying it can be returned
/// (RFC §6.7): Python's `ResponseSchemaError(message, *, reason,
/// provider='')`, a `ProviderError` never retryable. A provider's stream and
/// `generate` fail with the [`ProviderError`] it turns into, which carries
/// it as its [`source`](Error::source):
///
/// ```
/// use std::error::Error;
/// use roomkit_ai::{
///     ProviderError, ResponseSchemaError, ResponseSchemaErrorOptions, ResponseSchemaFailure,
/// };
///
/// let error = ProviderError::from(ResponseSchemaError::new(
///     "the answer is not a JSON document",
///     ResponseSchemaFailure::InvalidJson,
///     ResponseSchemaErrorOptions { provider: "openai".into() },
/// ));
/// let reason = error
///     .source()
///     .and_then(|source| source.downcast_ref::<ResponseSchemaError>())
///     .map(|failure| failure.reason);
/// assert_eq!(reason, Some(ResponseSchemaFailure::InvalidJson));
/// assert!(!error.retryable);
/// assert_eq!(error.to_string(), "openai: the answer is not a JSON document");
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResponseSchemaError {
    /// What failed.
    pub message: String,
    /// Why.
    pub reason: ResponseSchemaFailure,
    /// The provider that failed, by its name.
    pub provider: String,
}

/// What [`ResponseSchemaError::new`] takes beside the message and the
/// reason: Python's `provider=''`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ResponseSchemaErrorOptions {
    /// The provider that failed, by its name.
    pub provider: String,
}

impl ResponseSchemaError {
    /// The failure `message`, for `reason`.
    pub fn new(
        message: impl Into<String>,
        reason: ResponseSchemaFailure,
        options: ResponseSchemaErrorOptions,
    ) -> Self {
        Self {
            message: message.into(),
            reason,
            provider: options.provider,
        }
    }

    /// The failure `error` carries, when a response schema failed.
    pub(crate) fn of(error: &ProviderError) -> Option<&Self> {
        error.source()?.downcast_ref::<Self>()
    }
}

impl fmt::Display for ResponseSchemaError {
    /// As the [`ProviderError`] it turns into reads.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let options = ProviderErrorOptions {
            provider: self.provider.clone(),
            ..ProviderErrorOptions::default()
        };
        fmt::Display::fmt(
            &ProviderError::new(self.message.clone(), options),
            formatter,
        )
    }
}

impl Error for ResponseSchemaError {}

impl From<ResponseSchemaError> for ProviderError {
    /// The provider's failure, never retryable, its message and its
    /// provider the schema's failure's, which is its source; an
    /// [`Unsupported`](ResponseSchemaFailure::Unsupported) one is refused
    /// before any request.
    fn from(failure: ResponseSchemaError) -> Self {
        let options = ProviderErrorOptions {
            provider: failure.provider.clone(),
            before_request: failure.reason == ResponseSchemaFailure::Unsupported,
            ..ProviderErrorOptions::default()
        };
        Self::new(failure.message.clone(), options).with_source(failure)
    }
}

/// The failure `message` for `reason`, of `provider`, as a provider fails.
fn failed(message: String, reason: ResponseSchemaFailure, provider: &str) -> ProviderError {
    let options = ResponseSchemaErrorOptions {
        provider: provider.to_owned(),
    };
    ResponseSchemaError::new(message, reason, options).into()
}

/// The schema a call on `context` sends, `None` when there is none: Python's
/// `schema_for_generate(context, *, supported, provider, with_tools=False)`,
/// `supported` and `with_tools` the provider's
/// [`supports_response_schema`](super::AiProvider::supports_response_schema)
/// and
/// [`supports_response_schema_with_tools`](super::AiProvider::supports_response_schema_with_tools).
/// A schema outside the portable subset, which a context built by hand can
/// hold, is refused first, as Python's context refuses it. For a provider.
///
/// # Errors
///
/// The schema's refusal, then [`ResponseSchemaFailure::Unsupported`] when
/// the provider cannot honour a schema, or none beside the turn's tools:
/// before any request.
#[doc(hidden)]
pub fn schema_for_generate<'a>(
    context: &'a AiContext,
    supported: bool,
    with_tools: bool,
    provider: &str,
) -> Result<Option<&'a Map<String, Value>>, ProviderError> {
    let Some(schema) = &context.response_schema else {
        return Ok(None);
    };
    if let Err(refused) = check_portable_schema(&Value::Object(schema.clone())) {
        let options = ProviderErrorOptions {
            provider: provider.to_owned(),
            before_request: true,
            ..ProviderErrorOptions::default()
        };
        return Err(ProviderError::new(refused.to_string(), options).with_source(refused));
    }
    if !supported {
        let message = format!(
            "{provider} does not support a response schema; check supports_response_schema \
             before setting one"
        );
        return Err(failed(
            message,
            ResponseSchemaFailure::Unsupported,
            provider,
        ));
    }
    if !context.tools.is_empty() && !with_tools {
        let message = format!(
            "{provider} cannot combine a response schema with tools in the same turn; check \
             supports_response_schema_with_tools before setting both"
        );
        return Err(failed(
            message,
            ResponseSchemaFailure::Unsupported,
            provider,
        ));
    }
    Ok(Some(schema))
}

/// Refuses a constrained answer that did not deliver its JSON document:
/// Python's `check_schema_answer(content, *, schema, provider, refusal=None,
/// finish_reason=None)`. `content` is the answer's text, its reasoning
/// split out; `refusal` why the model declined, when it did; an answer that
/// ran out of room is truncated, under every provider's word for it. The
/// document is checked against the schema itself, not only parsed: a
/// server that takes the constraint and ignores it answers JSON of
/// another shape. For a provider.
///
/// # Errors
///
/// [`ResponseSchemaFailure::Refusal`], [`ResponseSchemaFailure::Truncated`]
/// or [`ResponseSchemaFailure::InvalidJson`].
#[doc(hidden)]
pub fn check_schema_answer(
    content: &str,
    schema: &Map<String, Value>,
    provider: &str,
    refusal: Option<&str>,
    finish_reason: Option<&str>,
) -> Result<(), ProviderError> {
    if let Some(refusal) = refusal {
        let message = format!("the model declined to answer: {refusal}");
        return Err(failed(message, ResponseSchemaFailure::Refusal, provider));
    }
    if ran_out_of_room(finish_reason) {
        let message = format!(
            "the JSON answer was cut ({}): raise max_tokens, or shorten the context",
            finish_reason.unwrap_or_default()
        );
        return Err(failed(message, ResponseSchemaFailure::Truncated, provider));
    }
    let document: Value = serde_json::from_str(content).map_err(|error| {
        let message = format!("the answer is not a JSON document ({error})");
        failed(message, ResponseSchemaFailure::InvalidJson, provider)
    })?;
    let schema = Value::Object(schema.clone());
    match schema_mismatch(&schema, &document, SchemaMismatchOptions::default()) {
        None => Ok(()),
        Some(mismatch) => {
            let message = format!("the answer does not satisfy the response schema ({mismatch})");
            Err(failed(
                message,
                ResponseSchemaFailure::InvalidJson,
                provider,
            ))
        }
    }
}

/// A provider's stream `events`, a constrained answer checked before its
/// done event (RFC §6.7): Python's `checked_stream(events, context, *,
/// provider, refusal)`. The text goes out as it comes, partial JSON,
/// provisional until the end; at a done event with no call before it, and
/// that does not end on a call the provider could not hand over, the whole
/// text is checked as [`check_schema_answer`] checks an answer, its refusal
/// read by `refusal`, and the error replaces the done event when it fails.
/// Without a schema, the stream is untouched. For a provider.
#[doc(hidden)]
pub fn checked_stream<'a>(
    mut events: ProviderStream<'a>,
    context: &'a AiContext,
    provider: &'a str,
    refusal: fn(&StreamDone) -> Option<String>,
) -> ProviderStream<'a> {
    let Some(schema) = &context.response_schema else {
        return events;
    };
    Box::pin(items(
        move |out: Yielder<Result<StreamEvent, ProviderError>>| async move {
            let mut text = String::new();
            let mut called = false;
            while let Some(event) = next(&mut events).await {
                match &event {
                    Ok(StreamEvent::TextDelta(delta)) => text.push_str(&delta.text),
                    Ok(StreamEvent::ToolCall(_)) => called = true,
                    Ok(StreamEvent::Done(done))
                        if !called && !is_malformed_call(done.finish_reason.as_deref()) =>
                    {
                        let declined = refusal(done);
                        let finish = done.finish_reason.as_deref();
                        if let Err(error) = check_schema_answer(
                            &text,
                            schema,
                            provider,
                            declined.as_deref(),
                            finish,
                        ) {
                            return out.give(Err(error)).await;
                        }
                    }
                    _ => {}
                }
                out.give(event).await;
            }
        },
    ))
}

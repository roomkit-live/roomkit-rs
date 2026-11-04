//! The `store.<operation>` spans: Python's `PostgresStore._query_span`, reported
//! to the provider the kit gives the store.

use std::error::Error;
use std::future::Future;
use std::sync::{Arc, PoisonError, RwLock};

use roomkit::StoreError;
use roomkit::model::Metadata;
use roomkit::telemetry::{
    Attr, EndSpanOptions, SpanKind, StartSpanOptions, TelemetryProvider, get_current_span,
};
use serde_json::Value;

/// Where a store's spans go.
#[derive(Clone)]
struct Reporting {
    provider: Arc<dyn TelemetryProvider>,
    sensitive: bool,
}

/// The kit's telemetry, none until a kit is built on the store.
#[derive(Default)]
pub(crate) struct QuerySpans {
    reporting: RwLock<Option<Reporting>>,
}

impl QuerySpans {
    /// What the store's spans go to from now on.
    pub(crate) fn set(&self, provider: Arc<dyn TelemetryProvider>, sensitive: bool) {
        *self
            .reporting
            .write()
            .unwrap_or_else(PoisonError::into_inner) = Some(Reporting {
            provider,
            sensitive,
        });
    }

    /// `work` under a `store.<operation>` span, a child of the current one,
    /// ended with its outcome. A `work` dropped before its end leaves its span
    /// open, as Python's, whose `except Exception` a cancellation passes through.
    pub(crate) async fn around<T>(
        &self,
        operation: &str,
        table: &str,
        work: impl Future<Output = Result<T, StoreError>>,
    ) -> Result<T, StoreError> {
        let reporting = self
            .reporting
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let Some(Reporting {
            provider,
            sensitive,
        }) = reporting
        else {
            return work.await;
        };
        let mut attributes = Metadata::new();
        attributes.insert(Attr::STORE_OPERATION.to_owned(), Value::from(operation));
        attributes.insert(Attr::STORE_TABLE.to_owned(), Value::from(table));
        let span = provider.start_span(
            SpanKind::StoreQuery,
            &format!("store.{operation}"),
            StartSpanOptions {
                parent_id: get_current_span(),
                attributes,
                ..StartSpanOptions::default()
            },
        );
        let outcome = work.await;
        let end = match &outcome {
            Ok(_) => EndSpanOptions::default(),
            Err(error) => EndSpanOptions {
                status: "error".to_owned(),
                error_message: Some(error_message(error, sensitive)),
                ..EndSpanOptions::default()
            },
        };
        provider.end_span(&span, end);
        outcome
    }
}

/// What a span says of `error`: its class, unless the kit records what can hold
/// personal data, its text: a driver's can quote a row (RFC §15.7, Python's
/// `span_error`).
fn error_message(error: &StoreError, sensitive: bool) -> String {
    if !sensitive {
        return class_of(error).to_owned();
    }
    let text = match error {
        StoreError::Unavailable(cause) => with_causes(&**cause),
        other => other.to_string(),
    };
    if text.is_empty() {
        class_of(error).to_owned()
    } else {
        text
    }
}

/// `error`'s text, each of its causes' after it: a driver's own says "db error",
/// and its cause what the server said, as asyncpg's text does.
fn with_causes(error: &(dyn Error + 'static)) -> String {
    let mut text = error.to_string();
    let mut cause = error.source();
    while let Some(current) = cause {
        text.push_str(": ");
        text.push_str(&current.to_string());
        cause = current.source();
    }
    text
}

/// The class Python's store raises for `error`; a driver's failure is the
/// `ChannelError` the kit names a store's failure, its class being the driver's.
fn class_of(error: &StoreError) -> &'static str {
    match error {
        StoreError::RoomAlreadyExists(_) => "RoomAlreadyExistsError",
        StoreError::RoomNotFound(_) => "RoomNotFoundError",
        StoreError::ParticipantExists { .. } => "ParticipantExistsError",
        StoreError::ParticipantNotFound { .. } => "ParticipantNotFoundError",
        StoreError::IdentityNotFound(_) => "IdentityNotFoundError",
        StoreError::AddressLinked { .. } => "AddressLinkedError",
        StoreError::EventNotFound(_) => "EventNotFoundError",
        StoreError::TaskNotFound(_) => "TaskNotFoundError",
        StoreError::IdempotencyKeyTaken { .. } => "IdempotencyKeyTakenError",
        StoreError::Value(_) => "ValueError",
        _ => "ChannelError",
    }
}

//! The `store.<operation>` spans: Python's `PostgresStore._query_span`, reported
//! to the provider the kit gives the store.

use std::error::Error;
use std::future::Future;
use std::sync::{Arc, PoisonError, RwLock};

use deadpool_postgres::PoolError;
use roomkit::StoreError;
use roomkit::model::Metadata;
use roomkit::telemetry::{
    Attr, EndSpanOptions, SpanKind, StartSpanOptions, TelemetryProvider, get_current_span,
};
use serde_json::Value;

use crate::error::NoConnection;
use crate::sqlstates::class_of_state;

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
/// `span_error`, `str(exc) or name`).
fn error_message(error: &StoreError, sensitive: bool) -> String {
    let class = class_of(error);
    if !sensitive {
        return class.to_owned();
    }
    let text = if let StoreError::Unavailable(cause) = error {
        with_causes(&**cause)
    } else {
        error.to_string()
    };
    // Python's `TimeoutError` of a pool's wait has no text.
    if text.is_empty() || class == "TimeoutError" {
        class.to_owned()
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

/// The class Python's store raises for `error`.
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
        StoreError::Unavailable(cause) => failure_class(&**cause),
        _ => "RuntimeError",
    }
}

/// The class Python's driver or store raises for `cause`: asyncpg's, by the state
/// the server reports, which holds no value of a row.
fn failure_class(cause: &(dyn Error + 'static)) -> &'static str {
    if let Some(no_connection) = cause.downcast_ref::<NoConnection>() {
        return match no_connection {
            NoConnection::NotInitialized => "RuntimeError",
            NoConnection::TimedOut => "TimeoutError",
        };
    }
    if let Some(pool) = cause.downcast_ref::<PoolError>() {
        return if let PoolError::Backend(driver) = pool {
            driver_class(driver)
        } else {
            "InterfaceError"
        };
    }
    cause
        .downcast_ref::<tokio_postgres::Error>()
        .map_or("RuntimeError", driver_class)
}

/// The class asyncpg raises for what the client met: its state's, else a connection's.
fn driver_class(driver: &tokio_postgres::Error) -> &'static str {
    if let Some(state) = driver.code() {
        return class_of_state(state.code());
    }
    if driver.is_closed() {
        return "ConnectionDoesNotExistError";
    }
    let refused = driver
        .source()
        .and_then(|cause| cause.downcast_ref::<std::io::Error>())
        .is_some_and(|cause| cause.kind() == std::io::ErrorKind::ConnectionRefused);
    if refused {
        "ConnectionRefusedError"
    } else {
        "InterfaceError"
    }
}

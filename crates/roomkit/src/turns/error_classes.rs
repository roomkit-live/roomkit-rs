//! Application error classes carried by a turn (RFC §10.6.8).
use std::error::Error;
use std::fmt;
use std::sync::Arc;

use super::wire::ErrorRecord;
use crate::RoomKitError;

type NativeError = dyn Error + Send + Sync + 'static;
type Decoder = dyn Fn(&ErrorRecord) -> Option<Box<NativeError>> + Send + Sync;
type Encoder = dyn Fn(&(dyn Error + 'static)) -> Option<ErrorRecord> + Send + Sync;

struct Entry {
    kind: String,
    decode: Box<Decoder>,
    encode: Box<Encoder>,
}

/// The application error types a kit or standalone executor transfers by name.
/// An empty registry uses the framework's built-in error classes.
#[derive(Clone, Default)]
pub struct ErrorClasses(Arc<Vec<Arc<Entry>>>);

impl fmt::Debug for ErrorClasses {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list()
            .entries(self.0.iter().map(|entry| &entry.kind))
            .finish()
    }
}

impl ErrorClasses {
    /// Register a native error and its wire class. A decoder can decline a record.
    /// The encoder supplies its JSON arguments, attributes and base classes.
    /// The registry writes the registered name and the error's display message.
    pub fn register<E: Error + Send + Sync + 'static>(
        &mut self,
        kind: impl Into<String>,
        decode: impl Fn(&ErrorRecord) -> Option<E> + Send + Sync + 'static,
        encode: impl Fn(&E) -> ErrorRecord + Send + Sync + 'static,
    ) {
        let kind = kind.into();
        let entries = Arc::make_mut(&mut self.0);
        entries.retain(|entry| entry.kind != kind);
        entries.push(Arc::new(Entry {
            kind,
            decode: Box::new(move |record| {
                decode(record).map(|e| -> Box<NativeError> { Box::new(e) })
            }),
            encode: Box::new(move |error| error.downcast_ref::<E>().map(&encode)),
        }));
    }

    /// Rebuild an application error by its class or nearest registered base.
    /// Foreign records stay available in full, including when they cross again.
    pub fn rebuild(&self, record: &ErrorRecord) -> RoomKitError {
        let mut error = match self.decode(record) {
            Some(native) => remote_error(record, Some(native)),
            None => super::error::rebuild_kit_error(record),
        };
        if let RoomKitError::RemoteTurn {
            detail: Some(detail),
            ..
        } = &mut error
        {
            detail.causes = record
                .causes
                .iter()
                .map(|cause| RemoteTurnDetails {
                    record: cause.clone(),
                    native: self.decode(cause),
                    causes: Vec::new(),
                })
                .collect();
        }
        error
    }

    fn decode(&self, record: &ErrorRecord) -> Option<Box<NativeError>> {
        std::iter::once(&record.kind)
            .chain(&record.bases)
            .find_map(|kind| {
                let entry = self.0.iter().find(|entry| &entry.kind == kind)?;
                (entry.decode)(record)
            })
    }

    /// Encode an error and its ordered source chain for another participant.
    /// A failure received from a delivery stream retains its original wire record.
    pub fn record(&self, error: &(dyn Error + Send + Sync + 'static)) -> ErrorRecord {
        self.encode_with(error, || super::error::turn_failure(error))
    }

    pub(super) fn encode_with(
        &self,
        error: &(dyn Error + 'static),
        fallback: impl FnOnce() -> ErrorRecord,
    ) -> ErrorRecord {
        if let Some(original) = crossed(error) {
            return original.clone();
        }
        let mut recorded = fallback();
        let native_causes = std::mem::take(&mut recorded.causes);
        let encoded = self.encode_one(error);
        let custom = encoded.is_some();
        let mut record = encoded.unwrap_or(recorded);
        let mut cause = error.source();
        for index in 0..super::error::CAUSES_KEPT {
            let Some(next) = cause else {
                record.causes.extend(native_causes.into_iter().skip(index));
                break;
            };
            if let Some(original) = crossed(next) {
                let mut head = original.clone();
                let tail = std::mem::take(&mut head.causes);
                record.causes.push(head);
                record
                    .causes
                    .extend(tail.into_iter().take(super::error::CAUSES_KEPT - index - 1));
                break;
            }
            if let Some(row) = self
                .encode_one(next)
                .or_else(|| native_causes.get(index).cloned())
                .or_else(|| custom.then(|| super::error::one("Error", next.to_string())))
            {
                record.causes.push(row);
            }
            cause = next.source();
        }
        record
    }

    fn encode_one(&self, error: &(dyn Error + 'static)) -> Option<ErrorRecord> {
        self.0.iter().rev().find_map(|entry| {
            let mut row = (entry.encode)(error)?;
            row.kind.clone_from(&entry.kind);
            row.message = error.to_string();
            row.causes.clear();
            Some(row)
        })
    }
}

fn crossed<'a>(error: &'a (dyn Error + 'static)) -> Option<&'a ErrorRecord> {
    if let Some(stream) = error.downcast_ref::<super::error::RemoteError>() {
        return Some(&stream.0);
    }
    match error.downcast_ref::<RoomKitError>() {
        Some(RoomKitError::RemoteTurn {
            detail: Some(detail),
            ..
        }) => Some(detail.record()),
        _ => None,
    }
}

/// The complete remote record and the native error reconstructed from it, when registered.
#[derive(Debug)]
pub struct RemoteTurnDetails {
    record: ErrorRecord,
    native: Option<Box<NativeError>>,
    causes: Vec<RemoteTurnDetails>,
}

impl RemoteTurnDetails {
    /// The original record, with its arguments, attributes, bases, traceback and causes.
    pub fn record(&self) -> &ErrorRecord {
        &self.record
    }

    /// The ordered wire causes and their registered native errors, nearest first.
    pub fn causes(&self) -> &[Self] {
        &self.causes
    }

    /// The native error produced by the application's registered constructor.
    pub fn native_error(&self) -> Option<&(dyn Error + Send + Sync + 'static)> {
        self.native.as_deref()
    }
}

impl PartialEq for RemoteTurnDetails {
    fn eq(&self, other: &Self) -> bool {
        self.record == other.record
    }
}
impl Eq for RemoteTurnDetails {}

impl fmt::Display for RemoteTurnDetails {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.record.message)
    }
}

impl Error for RemoteTurnDetails {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.native
            .as_deref()
            .map(|e| -> &(dyn Error + 'static) { e })
    }
}

pub(super) fn remote_error(record: &ErrorRecord, native: Option<Box<NativeError>>) -> RoomKitError {
    RoomKitError::RemoteTurn {
        message: record.message.clone(),
        kind: record.kind.clone(),
        bases: record.bases.clone(),
        detail: Some(Box::new(RemoteTurnDetails {
            record: record.clone(),
            native,
            causes: Vec::new(),
        })),
    }
}

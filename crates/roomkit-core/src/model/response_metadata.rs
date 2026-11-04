//! A turn's response metadata (RFC §6.7): one live record, shared by every
//! writer of the turn, Python's `roomkit.ResponseMetadata`.
//!
//! An AI turn produces one or more messages, and the host may want the same
//! turn-level facts on each: which documents were cited, what the turn read.
//! Those facts come as the turn runs, from a memory, a hook, a tool handler,
//! so the record is one object from the turn's first moment to its last event.
//! Cloning a record shares it, as Python's pydantic field keeps the instance:
//! the clone a tool handler writes is the one the stored message is built from.

use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use serde::de::Deserializer;
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::Metadata;

/// Turn-level metadata merged into every message event the turn produces:
/// each message carries the record as it stands when the message is created
/// (RFC §6.7). Python's `ResponseMetadata`; a clone is the same record, as
/// handing the Python instance on keeps it.
///
/// ```
/// # use roomkit_core::model::ResponseMetadata;
/// let record = ResponseMetadata::new();
/// let shared = record.clone();
/// shared.insert("cited", "doc-1".into());
/// assert_eq!(record.get("cited"), Some("doc-1".into()));
/// assert_eq!(record.writes(), 1);
/// ```
#[derive(Clone, Default)]
pub struct ResponseMetadata {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Default)]
struct Inner {
    data: Metadata,
    writes: u64,
}

impl ResponseMetadata {
    /// A new record, empty: Python's `ResponseMetadata()`. One holding keys
    /// is built from them, `ResponseMetadata::from(keys)`.
    pub fn new() -> Self {
        Self::default()
    }

    /// The record itself, or a bare mapping wrapped: Python's `coerce`. A
    /// bare mapping is a snapshot, which is what passing one means.
    pub fn coerce(value: impl Into<Self>) -> Self {
        value.into()
    }

    /// How many times one of its keys was written or removed. A change made
    /// inside a value it holds ([`with_value`](Self::with_value)) is no
    /// write of a key: a reader comparing counts sees writes, not changes.
    pub fn writes(&self) -> u64 {
        self.lock().writes
    }

    /// The value under `key`: Python's `record.get(key)`.
    pub fn get(&self, key: &str) -> Option<Value> {
        self.lock().data.get(key).cloned()
    }

    /// Whether it holds `key`: Python's `key in record`.
    pub fn contains_key(&self, key: &str) -> bool {
        self.lock().data.contains_key(key)
    }

    /// How many keys it holds: Python's `len(record)`.
    pub fn len(&self) -> usize {
        self.lock().data.len()
    }

    /// Whether it holds no key.
    pub fn is_empty(&self) -> bool {
        self.lock().data.is_empty()
    }

    /// Writes `value` under `key`, returning the value it replaced: Python's
    /// `record[key] = value`.
    pub fn insert(&self, key: impl Into<String>, value: Value) -> Option<Value> {
        let mut inner = self.lock();
        inner.writes += 1;
        inner.data.insert(key.into(), value)
    }

    /// Removes `key`, returning its value; removing no key is no write:
    /// Python's `record.pop(key, None)`.
    pub fn pop(&self, key: &str) -> Option<Value> {
        let mut inner = self.lock();
        let removed = inner.data.shift_remove(key);
        if removed.is_some() {
            inner.writes += 1;
        }
        removed
    }

    /// The value under `key`, `default` written there first when it holds
    /// none: Python's `record.setdefault(key, default)`.
    pub fn setdefault(&self, key: impl Into<String>, default: Value) -> Value {
        let mut inner = self.lock();
        let key = key.into();
        if let Some(value) = inner.data.get(&key) {
            return value.clone();
        }
        inner.writes += 1;
        inner.data.insert(key, default.clone());
        default
    }

    /// Writes each of `keys`: Python's `record.update(keys)`.
    pub fn update(&self, keys: Metadata) {
        let mut inner = self.lock();
        inner.writes += u64::try_from(keys.len()).unwrap_or(u64::MAX);
        inner.data.extend(keys);
    }

    /// Removes every key: Python's `record.clear()`, a write per key.
    pub fn clear(&self) {
        let mut inner = self.lock();
        inner.writes += u64::try_from(inner.data.len()).unwrap_or(u64::MAX);
        inner.data.clear();
    }

    /// Changes the value under `key`, when it holds one, in place: Python's
    /// `record[key].append(...)`, no write of a key.
    pub fn with_value(&self, key: &str, change: impl FnOnce(&mut Value)) {
        if let Some(value) = self.lock().data.get_mut(key) {
            change(value);
        }
    }

    /// A copy of its keys as they stand: Python's `dict(record)`.
    pub fn to_metadata(&self) -> Metadata {
        self.lock().data.clone()
    }

    /// Replaces its keys with `keys`: how the room's owner reads a record
    /// the executor sent (RFC §10.6.4).
    #[doc(hidden)]
    pub fn replace(&self, keys: Metadata) {
        let mut inner = self.lock();
        inner.writes += 1;
        inner.data = keys;
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl From<Metadata> for ResponseMetadata {
    fn from(data: Metadata) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner { data, writes: 0 })),
        }
    }
}

impl PartialEq for ResponseMetadata {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner) || self.to_metadata() == other.to_metadata()
    }
}

impl PartialEq<Metadata> for ResponseMetadata {
    fn eq(&self, other: &Metadata) -> bool {
        self.lock().data == *other
    }
}

impl fmt::Debug for ResponseMetadata {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "ResponseMetadata({:?})", self.lock().data)
    }
}

impl Serialize for ResponseMetadata {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.lock().data.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ResponseMetadata {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Metadata::deserialize(deserializer).map(Self::from)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn keys(value: Value) -> Metadata {
        match value {
            Value::Object(keys) => keys,
            other => panic!("not an object: {other}"),
        }
    }

    // Python: tests/test_response_metadata.py::TestLiveRecord::test_type_keeps_identity_where_a_dict_was_copied
    #[test]
    fn a_clone_is_the_same_record_where_a_bare_mapping_is_a_snapshot() {
        let record = ResponseMetadata::new();
        let shared = ResponseMetadata::coerce(record.clone());
        shared.insert("sources", json!(["doc-1"]));
        assert_eq!(record.get("sources"), Some(json!(["doc-1"])));

        let mut bare = keys(json!({"rag_sources": []}));
        let wrapped = ResponseMetadata::coerce(bare.clone());
        bare.insert("later".to_owned(), json!(1));
        assert_eq!(wrapped, keys(json!({"rag_sources": []})));
        assert!(ResponseMetadata::default().is_empty());
    }

    // Python: tests/test_response_metadata.py::TestLiveRecord::test_a_record_counts_the_writes_to_its_keys
    #[test]
    fn a_record_counts_the_writes_to_its_keys() {
        let record = ResponseMetadata::from(keys(json!({"rag_sources": []})));
        assert_eq!(record.writes(), 0);
        record.insert("cited", json!(1));
        record.update(keys(json!({"cited": 2, "credential": "c"})));
        record.setdefault("cited", json!(3)); // present: nothing written
        record.setdefault("read", json!([]));
        record.pop("credential");
        // Reads write nothing.
        assert_eq!(record.get("cited"), Some(json!(2)));
        assert!(!record.is_empty());
        assert_eq!(record.writes(), 5);
        record.with_value("read", |read| {
            read.as_array_mut().expect("a list").push(json!("doc-1"));
        });
        assert_eq!(record.writes(), 5);
        assert_eq!(record.get("read"), Some(json!(["doc-1"])));
        record.clear();
        assert_eq!(record.writes(), 8);
    }

    #[test]
    fn it_crosses_as_the_mapping_it_wraps() {
        let record = ResponseMetadata::from(keys(json!({"a": 1})));
        assert_eq!(
            serde_json::to_value(&record).expect("a map"),
            json!({"a": 1})
        );
        let read: ResponseMetadata = serde_json::from_value(json!({"b": 2})).expect("a record");
        assert_eq!(read.get("b"), Some(json!(2)));
    }
}

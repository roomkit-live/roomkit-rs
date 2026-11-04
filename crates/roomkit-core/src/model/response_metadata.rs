//! A turn's response metadata (RFC §6.7): one live record, shared by every
//! writer of the turn, Python's `roomkit.ResponseMetadata`.
//!
//! An AI turn produces one or more messages, and the host may want the same
//! turn-level facts on each: which documents were cited, what the turn read.
//! Those facts come as the turn runs, from a memory, a hook, a tool handler,
//! so the record is one object from the turn's first moment to its last event.
//! Cloning a record shares it, as Python's pydantic field keeps the instance:
//! the clone a tool handler writes is the one the stored message is built from.

#![expect(
    clippy::disallowed_types,
    reason = "a record's own cell, held for one map operation, never across a call or an effect: no room lock (HITL acfd6f64)"
)]

use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread::{self, ThreadId};

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
    /// The changes running on a value it holds ([`ResponseMetadata::with_value`]).
    changes: Vec<Change>,
    /// The id of the next of them.
    next_change: u64,
}

/// A [`ResponseMetadata::with_value`] running on the value under `key`.
struct Change {
    id: u64,
    key: String,
    thread: ThreadId,
    /// Whether another thread wrote `key` since it read the value: it runs
    /// again, on what that thread left.
    overwritten: bool,
}

impl Inner {
    /// The current thread wrote the keys `wrote` names: another thread's
    /// change of one of them is overwritten.
    fn wrote(&mut self, wrote: impl Fn(&str) -> bool) {
        let me = thread::current().id();
        for change in &mut self.changes {
            if change.thread != me && wrote(&change.key) {
                change.overwritten = true;
            }
        }
    }

    /// Change `id` is over: whether it was overwritten.
    fn end_change(&mut self, id: u64) -> bool {
        let Some(position) = self.changes.iter().position(|change| change.id == id) else {
            return false;
        };
        self.changes.remove(position).overwritten
    }
}

/// A change running, its entry taken back however it ends, a panic included.
struct Running<'record> {
    record: &'record ResponseMetadata,
    id: u64,
}

impl Drop for Running<'_> {
    fn drop(&mut self) {
        self.record.lock().end_change(self.id);
    }
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
        let key = key.into();
        inner.writes += 1;
        inner.wrote(|written| written == key);
        inner.data.insert(key, value)
    }

    /// Removes `key`, returning its value; removing no key is no write:
    /// Python's `record.pop(key, None)`.
    pub fn pop(&self, key: &str) -> Option<Value> {
        let mut inner = self.lock();
        let removed = inner.data.shift_remove(key);
        if removed.is_some() {
            inner.writes += 1;
            inner.wrote(|written| written == key);
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
        inner.wrote(|written| written == key);
        inner.data.insert(key, default.clone());
        default
    }

    /// Writes each of `keys`: Python's `record.update(keys)`.
    pub fn update(&self, keys: Metadata) {
        let mut inner = self.lock();
        inner.writes += u64::try_from(keys.len()).unwrap_or(u64::MAX);
        inner.wrote(|written| keys.contains_key(written));
        inner.data.extend(keys);
    }

    /// Removes every key: Python's `record.clear()`, a write per key.
    pub fn clear(&self) {
        let mut inner = self.lock();
        inner.writes += u64::try_from(inner.data.len()).unwrap_or(u64::MAX);
        inner.wrote(|_| true);
        inner.data.clear();
    }

    /// Changes the value under `key`, when it holds one: Python's
    /// `record[key].append(...)`, no write of a key. `change` runs with the
    /// record free, so it may read and write it. Another thread's write of
    /// `key` meanwhile runs it again on what that thread left, no update lost,
    /// as often as other threads write the key. A write of its own thread
    /// under `key`, a `with_value` of its own among them, runs nothing again
    /// and gives way to what `change` leaves in the value.
    pub fn with_value(&self, key: &str, mut change: impl FnMut(&mut Value)) {
        loop {
            let Some((mut value, running)) = self.start_change(key) else {
                return;
            };
            change(&mut value);
            if self.commit_change(key, running, value) {
                return;
            }
        }
    }

    /// The value a change of `key` runs on, the change noted for the writes
    /// it may meet; `None` when it holds no such key.
    fn start_change(&self, key: &str) -> Option<(Value, Running<'_>)> {
        let mut inner = self.lock();
        let value = inner.data.get(key)?.clone();
        let id = inner.next_change;
        inner.next_change += 1;
        inner.changes.push(Change {
            id,
            key: key.to_owned(),
            thread: thread::current().id(),
            overwritten: false,
        });
        Some((value, Running { record: self, id }))
    }

    /// Writes what the change `running` left in the value of `key`; whether
    /// it is over, `false` for one to run again. A key gone is no value to
    /// change.
    fn commit_change(&self, key: &str, running: Running<'_>, value: Value) -> bool {
        let over = {
            let mut inner = self.lock();
            let overwritten = inner.end_change(running.id);
            match inner.data.get_mut(key) {
                Some(_) if overwritten => false,
                Some(held) => {
                    *held = value;
                    inner.wrote(|written| written == key);
                    true
                }
                None => true,
            }
        };
        drop(running);
        over
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
        inner.wrote(|_| true);
        inner.data = keys;
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl From<Metadata> for ResponseMetadata {
    fn from(data: Metadata) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                data,
                ..Inner::default()
            })),
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
        let Value::Object(keys) = value else {
            panic!("not an object");
        };
        keys
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

    // Python: record["sources"].append(record["x"]), ordinary code.
    #[test]
    fn a_change_inside_a_value_may_read_the_record() {
        let record = ResponseMetadata::from(keys(json!({"sources": [], "x": 1})));
        let again = record.clone();
        record.with_value("sources", |sources| {
            let read = again.get("x").expect("x");
            sources.as_array_mut().expect("a list").push(read);
        });
        assert_eq!(record.get("sources"), Some(json!([1])));
    }

    // Python: record["sources"] = [...], a plain assignment, which ends.
    #[test]
    fn a_change_that_writes_its_own_key_ends() {
        let record = ResponseMetadata::from(keys(json!({"sources": 0})));
        let again = record.clone();
        let mut runs = 0;
        record.with_value("sources", |sources| {
            runs += 1;
            again.insert("sources", json!(runs));
            *sources = json!("mine");
        });
        assert_eq!(record.get("sources"), Some(json!("mine")));
        assert_eq!(runs, 1);
    }

    // Python: a change that calls `with_value` on its own key, as a helper of
    // the handler's may, is two appends to one list, and ends.
    #[test]
    fn a_change_that_changes_its_own_key_ends() {
        let record = ResponseMetadata::from(keys(json!({"sources": []})));
        let again = record.clone();
        let mut runs = 0;
        record.with_value("sources", |sources| {
            runs += 1;
            assert!(runs < 100, "the change runs again and again");
            again.with_value("sources", |sources| {
                sources.as_array_mut().expect("a list").push(json!("inner"));
            });
            sources.as_array_mut().expect("a list").push(json!("outer"));
        });
        assert_eq!(runs, 1);
        assert_eq!(record.get("sources"), Some(json!(["outer"])));
    }

    // Python: tests/test_response_metadata.py::_citing_handler,
    // record.setdefault("cited", []).append(...), from threads that each write
    // a key of their own too: one thread in Python, so nothing is lost.
    #[test]
    fn the_citing_idiom_loses_nothing_between_threads_that_write_other_keys() {
        const WRITERS: usize = 8;
        const WRITES: usize = 200;
        let record = ResponseMetadata::new();
        thread::scope(|scope| {
            for writer in 0..WRITERS {
                let record = record.clone();
                scope.spawn(move || {
                    for write in 0..WRITES {
                        record.setdefault("cited", json!([]));
                        record.with_value("cited", |cited| {
                            let list = cited.as_array_mut().expect("a list");
                            list.push(json!([writer, write]));
                        });
                        record.insert(format!("writer-{writer}"), json!(write));
                    }
                });
            }
        });
        let cited = record.get("cited").expect("the list");
        assert_eq!(cited.as_array().map(Vec::len), Some(WRITERS * WRITES));
        for writer in 0..WRITERS {
            let last = record.get(&format!("writer-{writer}"));
            assert_eq!(last, Some(json!(WRITES - 1)));
        }
    }

    // A change that writes a key of its own makes no other thread's change of
    // the value it holds its own to overwrite.
    #[test]
    fn a_change_that_writes_another_key_loses_no_other_thread_s_change() {
        const WRITERS: usize = 8;
        const WRITES: usize = 200;
        let record = ResponseMetadata::from(keys(json!({"cited": []})));
        thread::scope(|scope| {
            for writer in 0..WRITERS {
                let record = record.clone();
                scope.spawn(move || {
                    let again = record.clone();
                    for write in 0..WRITES {
                        record.with_value("cited", |cited| {
                            again.insert(format!("writer-{writer}"), json!(write));
                            let list = cited.as_array_mut().expect("a list");
                            list.push(json!([writer, write]));
                        });
                    }
                });
            }
        });
        let cited = record.get("cited").expect("the list");
        assert_eq!(cited.as_array().map(Vec::len), Some(WRITERS * WRITES));
    }

    // Python: one thread, so record["sources"].append(x) never loses one.
    #[test]
    fn no_update_inside_a_value_is_lost_between_threads() {
        let record = ResponseMetadata::from(keys(json!({"sources": []})));
        thread::scope(|scope| {
            for writer in 0..8 {
                let record = record.clone();
                scope.spawn(move || {
                    for write in 0..200 {
                        record.with_value("sources", |sources| {
                            let list = sources.as_array_mut().expect("a list");
                            list.push(json!([writer, write]));
                        });
                    }
                });
            }
        });
        let sources = record.get("sources").expect("the list");
        assert_eq!(sources.as_array().map(Vec::len), Some(1600));
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

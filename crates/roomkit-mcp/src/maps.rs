//! Shared host dictionaries read when an MCP connection opens (RFC §21.2).

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::sync::{Arc, PoisonError, RwLock};

/// Python's retained MCP headers dictionary, with case-sensitive keys and
/// insertion order. Clones share updates; a connection reads an owned snapshot.
/// An empty dictionary passed to construction is replaced, as in Python.
#[derive(Clone, Default)]
pub struct McpHeaders(Arc<RwLock<Vec<(String, String)>>>);

impl McpHeaders {
    /// Keeps entries in insertion order; an existing exact key keeps its
    /// position and takes the last value. Differently cased keys stay separate.
    pub fn new(values: impl IntoIterator<Item = (String, String)>) -> Self {
        Self(Arc::new(RwLock::new(ordered(values))))
    }

    /// An owned snapshot, preserving keys, values and insertion order.
    pub fn get(&self) -> Vec<(String, String)> {
        self.0
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Atomically replaces all entries, retaining the order of the input.
    pub fn replace(&self, values: impl IntoIterator<Item = (String, String)>) {
        *self.0.write().unwrap_or_else(PoisonError::into_inner) = ordered(values);
    }

    /// Inserts or replaces one exact key atomically, returning its prior value.
    /// Replacement keeps its position; a new key is appended.
    pub fn insert(&self, name: String, value: String) -> Option<String> {
        put(
            &mut self.0.write().unwrap_or_else(PoisonError::into_inner),
            name,
            value,
        )
    }

    /// Removes one exact key atomically, returning its prior value.
    /// Reinserting that key later places it at the end.
    pub fn remove(&self, name: &str) -> Option<String> {
        let mut entries = self.0.write().unwrap_or_else(PoisonError::into_inner);
        let position = entries.iter().position(|(key, _)| key == name)?;
        Some(entries.remove(position).1)
    }
}

fn ordered(values: impl IntoIterator<Item = (String, String)>) -> Vec<(String, String)> {
    let mut entries = Vec::new();
    for (name, value) in values {
        put(&mut entries, name, value);
    }
    entries
}

fn put(entries: &mut Vec<(String, String)>, name: String, value: String) -> Option<String> {
    if let Some((_, previous)) = entries.iter_mut().find(|(key, _)| key == &name) {
        return Some(std::mem::replace(previous, value));
    }
    entries.push((name, value));
    None
}

impl From<BTreeMap<String, String>> for McpHeaders {
    fn from(values: BTreeMap<String, String>) -> Self {
        Self::new(values)
    }
}

impl From<Vec<(String, String)>> for McpHeaders {
    fn from(values: Vec<(String, String)>) -> Self {
        Self::new(values)
    }
}

impl fmt::Debug for McpHeaders {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let entries = self.0.read().unwrap_or_else(PoisonError::into_inner);
        formatter
            .debug_struct("McpHeaders")
            .field(
                "keys",
                &entries.iter().map(|(key, _)| key).collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

/// Python's retained MCP subprocess environment. Clones share updates and each
/// connection takes a fresh owned snapshot; existing processes keep their copy.
#[derive(Clone, Default)]
pub struct McpEnvironment(Arc<RwLock<BTreeMap<OsString, OsString>>>);

impl McpEnvironment {
    /// Shares the given environment overrides (RFC §21.2).
    pub fn new(values: BTreeMap<OsString, OsString>) -> Self {
        Self(Arc::new(RwLock::new(values)))
    }

    /// An owned snapshot of the environment overrides.
    pub fn get(&self) -> BTreeMap<OsString, OsString> {
        self.0
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Atomically replaces the environment overrides for later connections.
    pub fn replace(&self, values: BTreeMap<OsString, OsString>) {
        *self.0.write().unwrap_or_else(PoisonError::into_inner) = values;
    }

    /// Inserts or replaces one variable atomically, returning its prior value.
    pub fn insert(&self, name: OsString, value: OsString) -> Option<OsString> {
        self.0
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(name, value)
    }

    /// Removes one variable atomically, returning its prior value.
    pub fn remove(&self, name: &OsStr) -> Option<OsString> {
        self.0
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(name)
    }
}

impl From<BTreeMap<OsString, OsString>> for McpEnvironment {
    fn from(values: BTreeMap<OsString, OsString>) -> Self {
        Self::new(values)
    }
}

impl fmt::Debug for McpEnvironment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let entries = self.0.read().unwrap_or_else(PoisonError::into_inner);
        formatter
            .debug_struct("McpEnvironment")
            .field("keys", &entries.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

//! Shared result containers for the immutable [`ToolCallVerdict`].
//!
//! Nested handles hold strong references. An application that creates a
//! reference cycle must break it before releasing its last external handle.
//! While editing a list with [`VerdictList::update`], the callback must not
//! acquire that list again.

use std::any::Any;
use std::sync::{Arc, PoisonError, RwLock};

use serde_json::Value;

/// One value in a result list or structured dictionary.
///
/// Use `Object` or `List` when another holder must keep the same mutable
/// container. `Json` stores an ordinary owned value. The host can retain
/// a native content object (for example an adapter's text part) in `Native`.
#[derive(Clone, Debug)]
pub enum VerdictValue {
    /// A JSON value owned by this entry.
    Json(Value),
    /// The same live list wherever the handle travels.
    List(VerdictList),
    /// The same live dictionary wherever the handle travels.
    Object(VerdictObject),
    /// A native application value, preserving its type and identity.
    Native(Arc<dyn Any + Send + Sync>),
}

impl From<Value> for VerdictValue {
    fn from(value: Value) -> Self {
        Self::Json(value)
    }
}

/// A mutable result list. Cloning the handle preserves the list's identity.
#[derive(Clone, Default)]
pub struct VerdictList(Arc<RwLock<Vec<VerdictValue>>>);

impl VerdictList {
    /// Retain these entries in the supplied order.
    pub fn new(entries: Vec<VerdictValue>) -> Self {
        Self(Arc::new(RwLock::new(entries)))
    }

    /// An owned list of the current entries. Nested handles remain shared.
    pub fn snapshot(&self) -> Vec<VerdictValue> {
        self.0
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Edit the shared list synchronously. The callback must not reenter it.
    pub fn update<R>(&self, edit: impl FnOnce(&mut Vec<VerdictValue>) -> R) -> R {
        edit(&mut self.0.write().unwrap_or_else(PoisonError::into_inner))
    }
}

/// An insertion-ordered structured dictionary. Clones share subsequent edits.
#[derive(Clone, Default)]
pub struct VerdictObject(Arc<RwLock<Vec<(String, VerdictValue)>>>);

impl VerdictObject {
    /// Read an entry. A nested handle keeps pointing at the original object.
    pub fn get(&self, name: &str) -> Option<VerdictValue> {
        let entries = self.0.read().unwrap_or_else(PoisonError::into_inner);
        entries
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.clone())
    }

    /// Add or replace an entry; an existing key keeps its position.
    pub fn insert(&self, name: impl Into<String>, value: VerdictValue) -> Option<VerdictValue> {
        let key = name.into();
        let mut entries = self.0.write().unwrap_or_else(PoisonError::into_inner);
        if let Some((_, previous)) = entries.iter_mut().find(|(name, _)| *name == key) {
            return Some(std::mem::replace(previous, value));
        }
        entries.push((key, value));
        None
    }

    /// Remove an entry, preserving the other keys' order.
    pub fn remove(&self, key: &str) -> Option<VerdictValue> {
        let mut entries = self.0.write().unwrap_or_else(PoisonError::into_inner);
        let at = entries.iter().position(|(name, _)| name == key)?;
        Some(entries.remove(at).1)
    }

    /// An owned list of entries in insertion order, retaining the nested handles.
    pub fn snapshot(&self) -> Vec<(String, VerdictValue)> {
        self.0
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

// A host can hold the same object at multiple positions, including within
// itself. Formatting a handle must neither traverse a cycle nor wait on its lock.
impl std::fmt::Debug for VerdictList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerdictList").finish_non_exhaustive()
    }
}

impl std::fmt::Debug for VerdictObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerdictObject").finish_non_exhaustive()
    }
}

/// A replacement tool result as text or a shared list of content parts.
#[derive(Clone, Debug)]
pub enum VerdictResult {
    /// Replacement text.
    Text(String),
    /// The caller's live result list.
    Parts(VerdictList),
}

/// What a synchronous tool-call hook chain decided about a served call.
/// Python's frozen `ToolCallVerdict`. The optional list and dictionary
/// retain their shared contents after construction.
#[derive(Clone, Debug, Default)]
pub struct ToolCallVerdict {
    result: Option<VerdictResult>,
    blocked: bool,
    replaces_structured: bool,
    structured_content: Option<VerdictObject>,
    error_detail: Option<String>,
}

impl ToolCallVerdict {
    /// Construct the immutable verdict from its five fields.
    pub fn new(
        result: Option<VerdictResult>,
        blocked: bool,
        replaces_structured: bool,
        structured_content: Option<VerdictObject>,
        error_detail: Option<String>,
    ) -> Self {
        Self {
            result,
            blocked,
            replaces_structured,
            structured_content,
            error_detail,
        }
    }

    /// The replacement result, when the chain supplied one.
    pub fn result(&self) -> Option<&VerdictResult> {
        self.result.as_ref()
    }

    /// Whether the chain withheld the served result.
    pub fn blocked(&self) -> bool {
        self.blocked
    }

    /// Whether to replace or clear the structured copy.
    pub fn replaces_structured(&self) -> bool {
        self.replaces_structured
    }

    /// The replacement structured copy, if supplied.
    pub fn structured_content(&self) -> Option<&VerdictObject> {
        self.structured_content.as_ref()
    }

    /// Observer-only detail for a failure to serve the call.
    pub fn error_detail(&self) -> Option<&str> {
        self.error_detail.as_deref()
    }
}

//! A call's shared structured copy (RFC §9.3, §21.4).

use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use roomkit::model::{Metadata, ResponseMetadata};

/// The live optional map in [`super::ToolCallContext::structured_content`].
///
/// Python assigns a dictionary to that field. Rust calls [`replace`](Self::replace)
/// so every clone of the call sees the assignment. [`get`](Self::get) returns
/// a live map handle: its writes reach this field until the map is replaced.
/// A handle retained before replacement still names the old map.
///
/// The map belongs to this call, independently of the turn's response record.
/// `Default` is absent; converting an empty [`Metadata`] makes a present empty
/// map. Cloning shares the field; equality compares its values.
///
/// ```
/// use roomkit::model::ResponseMetadata;
/// use roomkit_ai::{ToolHandler, ToolResult, current_tool_call};
///
/// let handler = ToolHandler::new(|_name, _arguments| async {
///     if let Some(call) = current_tool_call() {
///         let copy = ResponseMetadata::new();
///         copy.insert("page", 3.into());
///         call.structured_content.replace(Some(copy));
///     }
///     Ok(ToolResult::Text("Page found.".into()))
/// });
/// # let _ = handler;
/// ```
#[derive(Clone, Default)]
pub struct ToolStructuredContent {
    inner: Arc<Mutex<Option<ResponseMetadata>>>,
}

impl ToolStructuredContent {
    /// The current live map, or `None` when the field is absent.
    ///
    /// Use its existing map operations to modify it; `to_metadata` instead
    /// takes an independent snapshot. No cell lock is held by this handle.
    pub fn get(&self) -> Option<ResponseMetadata> {
        self.lock().clone()
    }

    /// Assign `value` to the field and return its previous live map.
    ///
    /// `None` clears the field. Retained handles to the previous map remain
    /// usable and do not change its replacement.
    pub fn replace(&self, value: Option<ResponseMetadata>) -> Option<ResponseMetadata> {
        std::mem::replace(&mut *self.lock(), value)
    }

    fn lock(&self) -> MutexGuard<'_, Option<ResponseMetadata>> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl From<Metadata> for ToolStructuredContent {
    fn from(value: Metadata) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Some(ResponseMetadata::from(value)))),
        }
    }
}

impl PartialEq for ToolStructuredContent {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner) || self.get() == other.get()
    }
}

impl Eq for ToolStructuredContent {}

impl fmt::Debug for ToolStructuredContent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ToolStructuredContent")
            .field(&self.get())
            .finish()
    }
}

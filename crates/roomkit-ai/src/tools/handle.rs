//! Shared host definitions read when a turn is prepared (RFC §6.4, §21.1).

use std::sync::{Arc, PoisonError, RwLock};

use super::AiTool;

/// A host tool returned by [`crate::AiChannel::extra_tools`] or
/// [`crate::Agent::extra_tools`]. Clones share the definition; replacing it
/// changes what subsequent turns read, without changing its handler.
///
/// Python exposes mutable `AITool` objects. Rust reads an owned snapshot
/// with [`get`](Self::get) and writes it with [`replace`](Self::replace).
/// A provider's already prepared context keeps its own snapshot.
#[derive(Clone, Debug)]
pub struct AiToolHandle(Arc<RwLock<AiTool>>);

impl AiToolHandle {
    pub(crate) fn new(definition: AiTool) -> Self {
        Self(Arc::new(RwLock::new(definition)))
    }

    /// An owned snapshot of this definition, including its parameters and tags.
    pub fn get(&self) -> AiTool {
        self.0
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Replaces the definition read by subsequent turns (RFC §6.4, §21.1).
    /// This neither installs a new tool nor renames the handler serving it.
    pub fn replace(&self, definition: AiTool) {
        *self.0.write().unwrap_or_else(PoisonError::into_inner) = definition;
    }
}

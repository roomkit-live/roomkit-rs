//! Data a local owner callback reads, independent of the AI crate's scope.

use std::any::Any;
use std::collections::HashSet;
use std::sync::{Arc, OnceLock};

use roomkit_core::model::{ChannelId, Metadata, Room, RoomId};

/// Internal tool identity, copied when a call leaves its executor.
#[doc(hidden)]
#[derive(Clone, Debug, Default)]
pub struct OwnerToolIdentity {
    pub room_id: Option<RoomId>,
    pub channel_id: Option<ChannelId>,
    pub tool_call_id: String,
}

/// Readable tool context crossing to the owner, without producer lifetimes.
#[doc(hidden)]
#[derive(Clone, Debug, Default)]
pub struct OwnerToolContext {
    pub room_id: Option<RoomId>,
    pub room: Option<Arc<Room>>,
    pub actor_id: Option<String>,
    pub requester: Option<String>,
    pub allowed_names: Option<HashSet<String>>,
    pub chain_depth: u32,
    pub call: Option<OwnerToolIdentity>,
}

/// Internal scope carrier shared by core and intelligence channels.
/// Implementations return their existing allocation from `into_any`.
#[doc(hidden)]
pub trait ToolCallScope: Any + Send + Sync {
    fn into_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;
    fn owner_context(&self) -> OwnerToolContext;

    /// Snapshot the structured copy a handler wrote on this call.
    fn structured_content(&self) -> Option<Metadata> {
        None
    }

    /// Restore the structured copy returned by the owner onto this call.
    fn set_structured_content(&self, _value: Metadata) {}
}

/// One owner callback's fresh scope. A channel's adapter is constructed
/// once per call so repeated public reads share that call's mutable fields.
#[doc(hidden)]
pub struct OwnerToolScope {
    context: OwnerToolContext,
    adapter: OnceLock<Arc<dyn ToolCallScope>>,
}

impl std::fmt::Debug for OwnerToolScope {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OwnerToolScope")
            .field("context", &self.context)
            .finish_non_exhaustive()
    }
}

impl OwnerToolScope {
    pub fn new(context: OwnerToolContext) -> Self {
        Self {
            context,
            adapter: OnceLock::new(),
        }
    }

    pub fn adapter(
        &self,
        build: impl FnOnce(&OwnerToolContext) -> Arc<dyn ToolCallScope>,
    ) -> Arc<dyn ToolCallScope> {
        Arc::clone(self.adapter.get_or_init(|| build(&self.context)))
    }
}

impl ToolCallScope for OwnerToolScope {
    fn into_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }

    fn owner_context(&self) -> OwnerToolContext {
        self.context.clone()
    }

    fn structured_content(&self) -> Option<Metadata> {
        self.adapter
            .get()
            .and_then(|scope| scope.structured_content())
    }

    fn set_structured_content(&self, value: Metadata) {
        if let Some(scope) = self.adapter.get() {
            scope.set_structured_content(value);
        }
    }
}

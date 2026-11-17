//! Application callbacks declared on the owner of a turn (RFC §10.6.5).

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, PoisonError, RwLock};

use roomkit_core::model::Metadata;
use serde_json::Value;

use crate::channel::BoxFuture;
use crate::error::repr;
use crate::{ChannelError, ConversationStore, RoomKit, RoomKitError};

type Serving = BoxFuture<'static, Result<Value, ChannelError>>;

/// A callback the owner serves by name: Python's
/// `roomkit.turns.DeclaredVerb` (RFC §10.6.5).
///
/// Arguments and results cross as JSON data. The owner installs the caller's
/// room and tool context and an independent copy of its response record.
/// A clone retains the same callback and any state its closure owns.
#[derive(Clone)]
pub struct DeclaredVerb(Arc<dyn Fn(Metadata) -> Serving + Send + Sync>);

impl DeclaredVerb {
    /// Wrap an asynchronous application callback, without invoking it.
    pub fn new<F, Fut>(verb: F) -> Self
    where
        F: Fn(Metadata) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value, ChannelError>> + Send + 'static,
    {
        Self(Arc::new(move |arguments| Box::pin(verb(arguments))))
    }

    /// Invoke the callback directly: Python's `await verb(arguments)`.
    /// This preserves its result or error without installing a turn scope.
    pub fn call(
        &self,
        arguments: Metadata,
    ) -> impl Future<Output = Result<Value, ChannelError>> + Send + 'static {
        (self.0)(arguments)
    }
}

impl std::fmt::Debug for DeclaredVerb {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("DeclaredVerb")
    }
}

/// Keyword arguments of [`RoomKit::declare_host_verb`].
#[derive(Clone, Copy, Debug, Default)]
pub struct DeclareHostVerbOptions {
    /// Allow work started by the latest turn to call this verb after that
    /// turn ends, for its room/channel with no actor or response record.
    pub between_turns: bool,
}

/// Python reserves its complete built-in set, including readers that this
/// implementation has not yet ported. Reserving a name implements no reader.
pub(crate) const BUILTINS: [&str; 9] = [
    "send_event",
    "delegate",
    "wait_task",
    "post_status",
    "update_room_metadata",
    "add_observation",
    "room_tasks",
    "room_vision",
    "room_context",
];

#[derive(Clone)]
pub(crate) struct Declaration {
    pub(crate) verb: DeclaredVerb,
    pub(crate) between_turns: bool,
}

#[derive(Default)]
pub(crate) struct DeclaredVerbs(RwLock<HashMap<String, Declaration>>);

impl DeclaredVerbs {
    fn declare(
        &self,
        name: &str,
        verb: DeclaredVerb,
        options: DeclareHostVerbOptions,
    ) -> Result<(), RoomKitError> {
        if name.is_empty() {
            return Err(RoomKitError::Value("A host verb needs a name".to_owned()));
        }
        let mut declared = self.0.write().unwrap_or_else(PoisonError::into_inner);
        if BUILTINS.contains(&name) || declared.contains_key(name) {
            return Err(RoomKitError::Value(format!(
                "A turn's host already has a verb {}",
                repr(name)
            )));
        }
        declared.insert(
            name.to_owned(),
            Declaration {
                verb,
                between_turns: options.between_turns,
            },
        );
        Ok(())
    }

    pub(crate) fn get(&self, name: &str) -> Option<Declaration> {
        self.0
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(name)
            .cloned()
    }
}

impl<S: ConversationStore + 'static> RoomKit<S> {
    /// Declare a callback a turn reaches with [`crate::TurnHost::call`]
    /// (RFC §10.6.5). It runs on the owner under the call's room, actor,
    /// requester, depth, tool identity and independent response record.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::KitClosed`] once closing starts. While open,
    /// [`RoomKitError::Value`] for an empty, built-in or already declared name.
    pub fn declare_host_verb(
        &self,
        name: &str,
        verb: DeclaredVerb,
        options: DeclareHostVerbOptions,
    ) -> Result<(), RoomKitError> {
        self.inner.open()?;
        self.inner.declared_verbs.declare(name, verb, options)
    }
}

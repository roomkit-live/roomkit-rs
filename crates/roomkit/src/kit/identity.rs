//! Identity resolution's side of the kit (RFC §10.1 step 5, §11): the
//! identity hooks, registered apart from the hooks of §9.1, the resolver
//! and its settings, and the run of the hooks step 5 asks for.

use std::collections::BTreeSet;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, PoisonError};
use std::time::Duration;

use roomkit_core::model::{
    ChannelId, ChannelType, EventDirection, IdentityHookResult, IdentityResult, RoomContext,
    RoomEvent,
};
use roomkit_core::rules::hook_registry::{HookFilter, HookSubject, HookTrigger};

use super::{Inner, RoomKit};
use crate::hooks::HookError;
use crate::identity::IdentityResolver;
use crate::store::ConversationStore;
use crate::unwind;

/// What an identity hook's handler returns: its answer, `None` for
/// Python's, or the error it raised.
type IdentityHookFuture =
    Pin<Box<dyn Future<Output = Result<Option<IdentityHookResult>, HookError>> + Send>>;

/// An identity hook's handler, as the kit holds it.
type IdentityHandler =
    Arc<dyn Fn(RoomEvent, RoomContext, IdentityResult) -> IdentityHookFuture + Send + Sync>;

/// An identity hook (RFC §11.2), as the kit holds it.
pub(crate) struct IdentityHook {
    trigger: HookTrigger,
    filter: HookFilter,
    handler: IdentityHandler,
}

impl fmt::Debug for IdentityHook {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IdentityHook")
            .field("trigger", &self.trigger)
            .field("filter", &self.filter)
            .finish_non_exhaustive()
    }
}

/// What [`RoomKit::identity_hook`] takes beside its trigger and its
/// handler, Python's `identity_hook(trigger, channel_types=None,
/// channel_ids=None, directions=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IdentityHookOptions {
    /// Only events from these channel types; `None`, every type.
    pub channel_types: Option<BTreeSet<ChannelType>>,
    /// Only events from these channels; `None`, every channel.
    pub channel_ids: Option<BTreeSet<ChannelId>>,
    /// Only events of these directions; `None`, every direction.
    pub directions: Option<BTreeSet<EventDirection>>,
}

impl<S: ConversationStore> RoomKit<S> {
    /// Registers an identity hook on `trigger`, `ON_IDENTITY_AMBIGUOUS` or
    /// `ON_IDENTITY_UNKNOWN` (RFC §11.2), Python's `@kit.identity_hook`:
    /// Rust has no decorator, so the function decorated is the call's
    /// argument.
    ///
    /// Its handler receives the event the message parsed to, the room's
    /// context and what the resolver said of the sender, and answers
    /// `Ok(Some(IdentityHookResult))` to decide, or `Ok(None)` to leave it to
    /// the next hook. The identity hooks of a trigger whose filters admit
    /// the event run in their registration order, and the first answer
    /// decides; one that fails, an `Err` or a panic, is logged and the next
    /// one runs. A hook registered on these triggers through
    /// [`hook`](Self::hook) observes once they are over.
    ///
    /// ```
    /// use roomkit::model::identity::RejectOptions;
    /// use roomkit::model::{IdentityHookResult, IdentityResult, RoomContext, RoomEvent};
    /// use roomkit::{HookTrigger, IdentityHookOptions, RoomKit};
    ///
    /// let kit = RoomKit::new(Default::default());
    /// kit.identity_hook(
    ///     HookTrigger::OnIdentityUnknown,
    ///     |_event: RoomEvent, _context: RoomContext, _result: IdentityResult| async {
    ///         Ok(Some(IdentityHookResult::reject(RejectOptions::default())))
    ///     },
    ///     IdentityHookOptions::default(),
    /// );
    /// ```
    pub fn identity_hook<F, Fut>(
        &self,
        trigger: HookTrigger,
        handler: F,
        options: IdentityHookOptions,
    ) where
        F: Fn(RoomEvent, RoomContext, IdentityResult) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Option<IdentityHookResult>, HookError>> + Send + 'static,
    {
        let mut filter = HookFilter::default();
        filter.channel_types = options.channel_types;
        filter.channel_ids = options.channel_ids;
        filter.directions = options.directions;
        let handler: IdentityHandler =
            Arc::new(move |event, context, result| Box::pin(handler(event, context, result)));
        let hook = IdentityHook {
            trigger,
            filter,
            handler,
        };
        self.inner
            .identity_hooks
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .push(hook);
    }

    /// Whether identity resolution runs for a channel of `channel_type`
    /// (RFC §11.4): the kit has a resolver, and no restriction or one that
    /// names the type. The Python implementation's
    /// `identity_enabled_for(channel_type)`.
    pub fn identity_enabled_for(&self, channel_type: &ChannelType) -> bool {
        self.config().identity_enabled_for(channel_type)
    }

    /// The kit's identity resolver, `None` when it has none: Python's
    /// `identity_resolver` property.
    pub fn identity_resolver(&self) -> Option<Arc<dyn IdentityResolver>> {
        self.inner.identity_resolver.clone()
    }

    /// How long the resolver may take (RFC §11.5): Python's
    /// `identity_timeout` property.
    pub fn identity_timeout(&self) -> Duration {
        self.config()
            .identity
            .as_ref()
            .map_or(super::options::IDENTITY_TIMEOUT, |identity| {
                identity.timeout
            })
    }
}

/// Runs the identity hooks of `trigger` whose filters admit `event`, in
/// their registration order, until one answers (RFC §11.2): one that
/// fails, by an error or a panic, is logged and the next one runs. `None`
/// when none answered.
pub(crate) async fn run_identity_hooks<S>(
    inner: &Inner<S>,
    trigger: HookTrigger,
    event: &RoomEvent,
    context: &RoomContext,
    result: &IdentityResult,
) -> Option<IdentityHookResult> {
    let handlers: Vec<IdentityHandler> = inner
        .identity_hooks
        .read()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .filter(|hook| hook.trigger == trigger && hook.filter.admits(HookSubject::from(event)))
        .map(|hook| Arc::clone(&hook.handler))
        .collect();
    for handler in handlers {
        let called = handler(event.clone(), context.clone(), result.clone());
        match unwind::unwinding(called).await {
            Ok(Ok(Some(answer))) => return Some(answer),
            Ok(Ok(None)) => {}
            Ok(Err(error)) => tracing::error!(
                target: "roomkit::identity",
                room_id = %event.room_id,
                ?trigger,
                %error,
                "an identity hook failed"
            ),
            Err(panic) => tracing::error!(
                target: "roomkit::identity",
                room_id = %event.room_id,
                ?trigger,
                panic = %unwind::message(&*panic),
                "an identity hook panicked"
            ),
        }
    }
    None
}

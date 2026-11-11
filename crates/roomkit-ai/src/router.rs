//! Choosing the provider of each AI turn (RFC §6.4): Python's
//! `channels/_ai_router.py`.
//!
//! [`AiChannel::routed`](crate::AiChannel::routed) asks an [`AiRouter`]
//! which provider serves a turn, instead of serving every turn on one
//! provider. The router is asked at the turn's start, before the turn's
//! context is built, so the context is built for the provider it picks
//! (whether that model sees images, its context window, whether it holds
//! tools unseen, the rate a cost budget prices the turn at). It is asked
//! again once that provider's retries are spent with nothing delivered,
//! and may name another provider for the generation. Every later round of
//! the turn stays on the provider that served it: a tool round reads the
//! reasoning and the cache of the round before.
//!
//! A router is plain code: it may read the machine's load, the hour, the
//! room, a classifier's answer, or anything else the application knows.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use roomkit::model::{RoomEvent, RoomId};

use crate::provider::{AiContext, AiProvider, AiResponse, DynAiProvider, ProviderError};

/// Why a router is asked: Python's `RouteReason`. More reasons may come,
/// so a `match` outside this crate ends with a wildcard arm.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum RouteReason {
    /// At a turn's start.
    Turn,
    /// Once the provider a generation ran on spent its retries without
    /// delivering anything.
    Retry,
}

impl RouteReason {
    /// The reason as Python's `Literal` spells it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Turn => "turn",
            Self::Retry => "retry",
        }
    }
}

/// What a router decides on: Python's `RouteRequest(reason, text, event=None,
/// room_id=None, previous=None, failed=None)`.
#[derive(Clone)]
pub struct RouteRequest {
    /// Why the router is asked.
    pub reason: RouteReason,
    /// What the turn answers, as text: the text of the event that opened it,
    /// each media read as its label.
    pub text: String,
    /// The event the turn answers; `None` for a turn that answers none.
    pub event: Option<RoomEvent>,
    /// The room the turn runs in.
    pub room_id: Option<RoomId>,
    /// For [`RouteReason::Retry`], the provider whose retries are spent: the
    /// one the turn was on. `None` at a turn's start.
    pub previous: Option<Arc<dyn DynAiProvider>>,
    /// For [`RouteReason::Retry`], the last error that provider raised.
    pub failed: Option<ProviderError>,
}

impl RouteRequest {
    /// A request for `reason` on `text`, naming no event, no room, no
    /// provider and no error, as Python's defaults.
    pub fn new(reason: RouteReason, text: impl Into<String>) -> Self {
        Self {
            reason,
            text: text.into(),
            event: None,
            room_id: None,
            previous: None,
            failed: None,
        }
    }

    /// What the router is asked at the start of a turn that answers `event`
    /// in `room_id`, the event's room when none: Python's `turn_request`.
    pub(crate) fn turn(event: &RoomEvent, room_id: Option<&RoomId>) -> Self {
        let text = crate::transcript_content::content_text(&event.content).unwrap_or_default();
        Self {
            event: Some(event.clone()),
            room_id: Some(room_id.unwrap_or(&event.room_id).clone()),
            ..Self::new(RouteReason::Turn, text)
        }
    }

    /// The same request, asked again once `previous` spent its retries on
    /// `failed` (Python's `replace(asked, reason="retry", ...)`).
    pub(crate) fn retry(&self, previous: Arc<dyn DynAiProvider>, failed: ProviderError) -> Self {
        Self {
            reason: RouteReason::Retry,
            previous: Some(previous),
            failed: Some(failed),
            ..self.clone()
        }
    }
}

impl fmt::Debug for RouteRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RouteRequest")
            .field("reason", &self.reason)
            .field("text", &self.text)
            .field("event", &self.event.as_ref().map(|event| &event.id))
            .field("room_id", &self.room_id)
            .field("previous", &self.previous.as_ref().map(|p| p.dyn_name()))
            .field("failed", &self.failed)
            .finish()
    }
}

/// What [`AiRouter::route`] returns: the provider it names, as an object.
pub type AiRouterFuture<'a> =
    Pin<Box<dyn Future<Output = Option<Arc<dyn DynAiProvider>>> + Send + 'a>>;

/// Picks the provider of each turn of an [`AiChannel`](crate::AiChannel):
/// Python's `AIRouter`. The channel holds it as an object, so `route`
/// returns a boxed future.
///
/// The channel closes its router when it closes; a router closes the
/// providers it holds.
pub trait AiRouter: Send + Sync + 'static {
    /// The provider for `request`.
    ///
    /// For a turn's start ([`RouteReason::Turn`]) a router must name one.
    /// For a [`RouteReason::Retry`], `None` names no other: the last error
    /// is the turn's. A provider named for a retry gets one attempt; when it
    /// fails before it delivers anything, the turn's error is the one
    /// `previous` raised, its own the [`source`](std::error::Error::source).
    fn route(&self, request: RouteRequest) -> AiRouterFuture<'_>;

    /// The router's label, which the channel's
    /// [`provider_name`](roomkit::Channel::provider_name) reads: Python's
    /// `name`, the router's type name by default.
    fn name(&self) -> &str {
        crate::provider::short_type_name::<Self>()
    }

    /// Releases what the router holds, the providers it routes to: Python's
    /// `close()`. Nothing by default.
    fn close(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async {})
    }
}

/// Every turn on `primary`; once a generation's provider spent its retries,
/// the other provider: Python's `FallbackRouter(primary, fallback)`.
///
/// A turn the fallback served stays on it, and a generation it then fails is
/// handed back to the primary. Its close closes both providers.
pub struct FallbackRouter {
    primary: Arc<dyn DynAiProvider>,
    fallback: Arc<dyn DynAiProvider>,
}

impl FallbackRouter {
    /// Every turn on `primary`, a spent generation handed to the other one.
    pub fn new(primary: Arc<dyn DynAiProvider>, fallback: Arc<dyn DynAiProvider>) -> Self {
        Self { primary, fallback }
    }

    /// The provider every turn starts on: Python's `primary`.
    pub fn primary(&self) -> &Arc<dyn DynAiProvider> {
        &self.primary
    }

    /// The provider a generation the primary spent its retries on is handed
    /// to: Python's `fallback`.
    pub fn fallback(&self) -> &Arc<dyn DynAiProvider> {
        &self.fallback
    }
}

impl fmt::Debug for FallbackRouter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FallbackRouter")
            .field("primary", &self.primary.dyn_name())
            .field("fallback", &self.fallback.dyn_name())
            .finish()
    }
}

impl AiRouter for FallbackRouter {
    fn route(&self, request: RouteRequest) -> AiRouterFuture<'_> {
        let picked = match request.reason {
            RouteReason::Turn => &self.primary,
            RouteReason::Retry => match &request.previous {
                Some(previous) if same_provider(previous, &self.primary) => &self.fallback,
                _ => &self.primary,
            },
        };
        Box::pin(std::future::ready(Some(Arc::clone(picked))))
    }

    fn close(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async {
            self.primary.dyn_close().await;
            self.fallback.dyn_close().await;
        })
    }
}

/// Whether `a` and `b` are the same provider, as Python's `is`: the same
/// object, whatever vtable each pointer carries.
pub(crate) fn same_provider(a: &Arc<dyn DynAiProvider>, b: &Arc<dyn DynAiProvider>) -> bool {
    std::ptr::addr_eq(Arc::as_ptr(a), Arc::as_ptr(b))
}

/// The provider type of an AI channel a router serves (RFC §6.4): it has
/// none of its own, each turn running on the provider its router picks.
/// Python's `AIChannel(router=...)`, whose `provider` is `None`. No value of
/// it exists.
#[derive(Clone, Copy, Debug)]
pub enum Routed {}

impl AiProvider for Routed {
    fn model_name(&self) -> &str {
        match *self {}
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "no value of it exists, so it never generates"
    )]
    async fn generate(&self, _context: &AiContext) -> Result<AiResponse, ProviderError> {
        match *self {}
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::{MockAiProvider, MockAiProviderOptions};

    fn model() -> Arc<dyn DynAiProvider> {
        Arc::new(MockAiProvider::new(MockAiProviderOptions::default()))
    }

    // Python: channels/_ai_router.py FallbackRouter.route
    #[tokio::test]
    async fn a_fallback_router_starts_on_the_primary_and_hands_a_retry_to_the_other() {
        let (primary, fallback) = (model(), model());
        let router = FallbackRouter::new(Arc::clone(&primary), Arc::clone(&fallback));
        let failed = ProviderError::from("overloaded");
        let picked = |request| async { router.route(request).await.unwrap() };
        let turn = RouteRequest::new(RouteReason::Turn, "hi");

        let at_start = picked(turn.clone()).await;
        let after_primary = picked(turn.retry(Arc::clone(&primary), failed.clone())).await;
        let after_fallback = picked(turn.retry(Arc::clone(&fallback), failed)).await;

        assert_eq!(
            (
                same_provider(&at_start, &primary),
                same_provider(&after_primary, &fallback),
                same_provider(&after_fallback, &primary),
            ),
            (true, true, true)
        );
        assert_eq!(router.name(), "FallbackRouter");
    }
}

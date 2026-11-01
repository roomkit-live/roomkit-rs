//! Identity resolution (RFC §11): who sent an inbound message.
//!
//! The kit asks its [`IdentityResolver`]
//! ([`RoomKitOptions::identity_resolver`](crate::RoomKitOptions::identity_resolver))
//! at step 5 of the inbound pipeline (RFC §10.1), once the channel parsed
//! the message, for the channel types it resolves
//! ([`RoomKitOptions::identity_channel_types`](crate::RoomKitOptions::identity_channel_types)),
//! within its timeout. What it answers, an [`IdentityResult`], decides who
//! the event is from, and the identity hooks
//! ([`RoomKit::identity_hook`](crate::RoomKit::identity_hook)) decide what
//! an ambiguous or an unknown sender comes to. [`MockIdentityResolver`]
//! answers from a mapping, for tests.

mod mock;

use std::error::Error;
use std::future::Future;
use std::pin::Pin;

use roomkit_core::model::{IdentityResult, InboundMessage, RoomContext};

pub use self::mock::{MockIdentityResolver, MockIdentityResolverOptions};

/// What a resolver failed with, Python's `Exception`: the message fails
/// with it, before anything is written.
pub type IdentityError = Box<dyn Error + Send + Sync>;

/// What a resolver's [`resolve`](IdentityResolver::resolve) returns: a
/// boxed future, so that the kit holds its resolver as a trait object.
pub type IdentityFuture<'a> =
    Pin<Box<dyn Future<Output = Result<IdentityResult, IdentityError>> + Send + 'a>>;

/// Resolves who sent an inbound message (RFC §11.1), the Python
/// implementation's `IdentityResolver`.
///
/// ```
/// use roomkit::identity::{IdentityFuture, IdentityResolver};
/// use roomkit::model::{
///     IdentificationStatus, Identity, IdentityId, IdentityResult, InboundMessage, RoomContext,
/// };
///
/// struct OneCustomer;
///
/// impl IdentityResolver for OneCustomer {
///     fn resolve<'a>(
///         &'a self,
///         message: &'a InboundMessage,
///         _context: &'a RoomContext,
///     ) -> IdentityFuture<'a> {
///         Box::pin(async move {
///             if message.sender_id == "+15551234567" {
///                 let alice = Identity::new(IdentityId::new("alice")?);
///                 return Ok(IdentityResult {
///                     identity: Some(alice),
///                     ..IdentityResult::new(IdentificationStatus::Identified)
///                 });
///             }
///             Ok(IdentityResult::new(IdentificationStatus::Unknown))
///         })
///     }
/// }
/// ```
pub trait IdentityResolver: Send + Sync {
    /// Who sent `message`, its sender in its channel's form, in the room
    /// `context` holds (its bindings, its participants): identified,
    /// ambiguous, pending, unknown, a challenge sent, or rejected.
    fn resolve<'a>(
        &'a self,
        message: &'a InboundMessage,
        context: &'a RoomContext,
    ) -> IdentityFuture<'a>;
}

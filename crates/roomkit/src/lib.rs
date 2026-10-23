//! RoomKit for Rust.
//!
//! Rooms hold conversations between humans, AI agents and programs, each
//! reaching the room through a channel. This crate runs the decisions of
//! [`roomkit_core`] on a tokio runtime: [`RoomKit`] executes their effects
//! against the registered channels, hooks and store.
//!
//! - [`Channel`]: implement it for a transport or an intelligence channel.
//! - [`RoomKit`]: register channels and hooks, run rooms through their
//!   lifecycle and timers, process messages into the room they name or the
//!   room their router finds, inject events and commit records directly,
//!   wait for the work the kit runs in the background or end it. A
//!   hook, a turn or a delivery that calls into its own room never waits for
//!   itself: the room lock is reentrant within the call's execution context,
//!   a call made under the lock or in the room's admission order is adopted
//!   by the call whose work made it, and a turn's or a delivery's call
//!   returns at its decision.
//! - [`OrganizationKit`]: the kit acting for one organization, every call
//!   reaching its rooms only (RFC §17.2); the kit itself acts for none.
//! - [`InboundRoomRouter`]: a routing strategy; [`DefaultRouter`] is the
//!   RFC's.
//! - [`store`]: where rooms live, in memory by default.
//! - [`testing`]: a recording transport and an echo agent, for tests and
//!   examples.
//!
//! The core's model is re-exported whole as [`model`]. Of its operations
//! and rules, only what a caller names is: the results and errors of the
//! kit's calls, the framework events, what a channel declares and how it
//! writes its addresses, the routing query a router answers and the default
//! strategy's decision ([`default_route`]), the environment, and the rules
//! that read an event; the protocol the driver speaks with the core stays
//! the core's.

mod channel;
mod context;
mod driver;
mod environment;
mod error;
mod hooks;
mod kit;
mod locks;
mod queues;
mod router;
pub mod store;
mod tasks;
pub mod testing;

pub use roomkit_core::model;
pub use roomkit_core::ops::{
    CHANNEL_NOT_REGISTERED, ChainDepthData, ChannelDeclaration, ConfigError, Environment,
    FrameworkEvent, InboundError, InboundResult, LifecycleChange, LifecycleError, LifecyclePayload,
    MutationError, ProtocolError, RefusedOperation, SequentialEnvironment, TransitionReason,
    default_inbound_event,
};
pub use roomkit_core::rules::addresses::{
    AddressForm, Addressing, CountryCode, InvalidCountryCode,
};
pub use roomkit_core::rules::edits::{is_deleted, is_edited};
pub use roomkit_core::rules::hook_registry::{
    HookExecution, HookFilter, HookId, HookSpec, HookTrigger, RegistrationError,
};
pub use roomkit_core::rules::lifecycle::Transition;
pub use roomkit_core::rules::routing::{Candidate, RouteQuery, Sender, route as default_route};
pub use roomkit_core::rules::transcoding::text_of;

pub use self::channel::{Channel, ChannelError, EventDraft};
pub use self::context::keep_context;
pub use self::environment::SystemEnvironment;
pub use self::error::KitError;
pub use self::hooks::{HookError, HookHandler, HookReturn};
pub use self::kit::{OrganizationKit, RoomKit, TimerSweeper};
pub use self::router::{DefaultRouter, InboundRoomRouter, RouterError};
pub use self::store::{
    ConversationStore, EventFilter, InMemoryStore, RoomFilter, StoreError, StoreFailure,
};

/// The README's code, compiled and run as doctests, so that the first room
/// it shows is one the crate runs.
#[cfg(doctest)]
#[doc = include_str!("../../../README.md")]
struct ReadmeDoctests;

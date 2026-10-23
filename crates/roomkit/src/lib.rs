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
//!   room their router finds, inject events and commit records directly. A
//!   hook, a turn or a delivery that calls into its own room never waits for
//!   itself: the room lock is reentrant within the call's execution context,
//!   and a call into the room is adopted by the work that holds it.
//! - [`InboundRoomRouter`]: a routing strategy; [`DefaultRouter`] is the
//!   RFC's.
//! - [`store`]: where rooms live, in memory by default.
//! - [`testing`]: a recording transport and an echo agent, for tests and
//!   examples.
//!
//! The core's model, operations and rules are re-exported as [`model`],
//! [`ops`] and [`rules`].

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
pub mod testing;

pub use roomkit_core::rules::hook_registry::{
    HookExecution, HookFilter, HookId, HookSpec, HookTrigger,
};
pub use roomkit_core::rules::transcoding::text_of;
pub use roomkit_core::{model, ops, rules};

pub use self::channel::{Channel, ChannelError, EventDraft};
pub use self::context::keep_context;
pub use self::environment::SystemEnvironment;
pub use self::error::KitError;
pub use self::hooks::{HookError, HookHandler, HookReturn};
pub use self::kit::{RoomKit, TimerSweeper};
pub use self::router::{DefaultRouter, InboundRoomRouter, RouterError};
pub use self::store::{ConversationStore, EventFilter, InMemoryStore, RoomFilter, StoreError};

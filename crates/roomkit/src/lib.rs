//! RoomKit for Rust.
//!
//! Rooms hold conversations between humans, AI agents and programs, each
//! reaching the room through a channel. This crate runs the decisions of
//! [`roomkit_core`] on a tokio runtime: [`RoomKit`] executes their effects
//! against the registered channels, hooks and store.
//!
//! - [`Channel`]: implement it for a transport or an intelligence channel.
//! - [`RoomKit`]: register channels and hooks, create rooms, process
//!   messages.
//! - [`store`]: where rooms live, in memory by default.
//! - [`testing`]: a recording transport and an echo agent, for tests and
//!   examples.
//!
//! The core's model, operations and rules are re-exported as [`model`],
//! [`ops`] and [`rules`].

mod channel;
mod driver;
mod environment;
mod error;
mod hooks;
mod kit;
mod locks;
pub mod store;
pub mod testing;

pub use roomkit_core::rules::hook_registry::{
    HookExecution, HookFilter, HookId, HookSpec, HookTrigger,
};
pub use roomkit_core::rules::transcoding::text_of;
pub use roomkit_core::{model, ops, rules};

pub use self::channel::{Channel, ChannelError, EventDraft};
pub use self::environment::SystemEnvironment;
pub use self::error::KitError;
pub use self::hooks::{HookError, HookHandler, HookReturn};
pub use self::kit::RoomKit;
pub use self::store::{ConversationStore, InMemoryStore, StoreError};

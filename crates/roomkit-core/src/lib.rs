//! The sans-IO core of RoomKit.
//!
//! This crate decides what happens to a conversation's events and performs
//! none of it: it never reads a store, calls a channel or runs a hook itself.
//! A driver executes what the core asks for and hands back the outcome. See
//! `docs/architecture.md` in the repository for the design.
//!
//! - [`model`]: the data types of the RoomKit RFC, §5, with the rules that
//!   belong to a type (a visibility's grammar and intersection, a binding
//!   update carrying over what it does not name).
//! - [`rules`]: the RFC's rules as functions: permissions and delivery
//!   planning, addresses, organizations and routing, attachments, the
//!   lifecycle and its timers, transcoding, the chain depth and the reentry
//!   budget, hook registrations and chains, edits and deletes,
//!   instructions, thread roots.
//! - [`ops`]: the operations, run by a driver through the effect protocol:
//!   the inbound pipeline and direct injection, the room lifecycle, the
//!   host's direct changes of stored events and records committed outside
//!   the pipeline.

pub mod model;
pub mod ops;
pub mod rules;

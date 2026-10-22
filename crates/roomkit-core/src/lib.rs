//! The sans-IO core of RoomKit.
//!
//! This crate decides what happens to a conversation's events and performs
//! none of it: it never reads a store, calls a channel or runs a hook itself.
//! A driver executes what the core asks for and hands back the outcome. See
//! `docs/architecture.md` in the repository for the design.
//!
//! - [`model`]: the data types of the RoomKit RFC, §5.
//! - [`rules`]: the RFC's rules as pure functions: permissions, transcoding,
//!   chain depth, hooks.

pub mod model;
pub mod rules;

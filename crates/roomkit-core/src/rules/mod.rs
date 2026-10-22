//! The RFC's rules, as pure functions over the model.
//!
//! Nothing here holds state between calls or performs I/O: each function
//! answers one question the pipeline asks, and the operations compose them.
//!
//! | Module | RFC |
//! |---|---|
//! | [`permissions`] | §7.5, §10.2 step 2, §19.3: who reads, who writes, whom a broadcast reaches |
//! | [`transcoding`] | §6.6: what a channel receives for a content it cannot carry |
//! | [`chain_depth`] | §8.3: the chain-depth limit, its records, the reentry budget |
//! | [`hook_registry`] | §9.1, §9.2: hook registrations and which hooks an event fires |
//! | [`hook_chain`] | §9.3, §9.4: how a chain of SYNC hook outcomes decides |
//!
//! Visibility itself (§7.3) is a model type: see
//! [`Visibility::admits`](crate::model::Visibility::admits) and
//! [`Visibility::intersect`](crate::model::Visibility::intersect).

pub mod chain_depth;
pub mod hook_chain;
pub mod hook_registry;
pub mod permissions;
pub mod transcoding;

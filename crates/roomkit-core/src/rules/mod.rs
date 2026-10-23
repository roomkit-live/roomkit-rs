//! The RFC's rules, as pure functions over the model.
//!
//! Nothing here holds state between calls or performs I/O: each function
//! answers one question the pipeline asks, and the operations compose them.
//!
//! | Module | RFC |
//! |---|---|
//! | [`permissions`] | §7.5, §10.2 step 2, §19.3: who reads, who writes, whom a broadcast reaches |
//! | [`attachment`] | §7.5 rule 7, §10.6.5: a detach's revocation, when the framework attaches a channel itself, and the room metadata the implementation reserves |
//! | [`addresses`] | §5.7, §10.1 step 1, §10.2 step 3d: an address in its channel's form, and a binding's recipient |
//! | [`organizations`] | §17.2: which rooms a caller reaches, by the organization it acts for |
//! | [`transcoding`] | §6.6: what a channel receives for a content it cannot carry |
//! | [`chain_depth`] | §8.3: the chain-depth limit, its records, the reentry budget |
//! | [`hook_registry`] | §9.1, §9.2: hook registrations and which hooks an event fires |
//! | [`hook_chain`] | §9.3, §9.4: how a chain of SYNC hook outcomes decides |
//! | [`lifecycle`] | §5.1: which status each room transition leaves from, and what it writes |
//! | [`timers`] | §5.1: which timer transition falls due, and when |
//!
//! Visibility itself (§7.3) is a model type: see
//! [`Visibility::admits`](crate::model::Visibility::admits) and
//! [`Visibility::intersect`](crate::model::Visibility::intersect). So is
//! rule 6 of §7.5, which every write to an existing binding goes through:
//! [`BindingUpdate::apply`](crate::model::BindingUpdate::apply).

pub mod addresses;
pub mod attachment;
pub mod chain_depth;
pub mod hook_chain;
pub mod hook_registry;
pub mod lifecycle;
pub mod organizations;
pub mod permissions;
pub mod timers;
pub mod transcoding;

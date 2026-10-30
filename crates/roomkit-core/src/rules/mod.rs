//! The RFC's rules, as functions over the model.
//!
//! Nothing here performs I/O or reads a clock: each function answers one
//! question the pipeline asks, and the operations compose them. Two rules
//! keep what they count between calls, each as a value its owner holds: a
//! kit's hook registrations ([`hook_registry::HookRegistry`]) and a
//! cascade's reentry budget ([`chain_depth::ReentryBudget`], shared by the
//! calls that draw on it).
//!
//! | Module | RFC |
//! |---|---|
//! | `permissions` | §7.5, §10.2 step 2, §19.3: who reads, who writes, whom a broadcast reaches; [`visible_events`], what a channel may know of a room's history, and [`visibility_allows`] |
//! | [`attachment`] | §7.5 rule 7, §10.6.5: a detach's revocation, when the framework attaches a channel itself, and the room metadata the implementation reserves |
//! | [`addresses`] | §5.7, §10.1 step 1, §10.2 step 3d: an address in its channel's form, and a binding's recipient |
//! | [`organizations`] | §17.2: which rooms a caller reaches, by the organization it acts for |
//! | [`routing`] | §10.4, §10.1 step 2: the default routing strategy, and what step 2 records of a routed sender |
//! | [`transcoding`] | §6.6: what a channel receives for a content it cannot carry |
//! | [`chain_depth`] | §8.3: the chain-depth limit, its records, the reentry budget |
//! | [`hook_registry`] | §9.1, §9.2: hook registrations and which hooks an event fires |
//! | [`hook_chain`] | §9.3, §9.4: how a chain of SYNC hook outcomes decides |
//! | [`lifecycle`] | §5.1: which status each room transition leaves from, and what it writes |
//! | [`timers`] | §5.1: which timer transition falls due, and when |
//! | [`edits`] | §10.1 step 8a, §10.3: whether an edit or a delete may change its target, and what the commit writes on it |
//! | [`idempotency`] | §13.4: a blank idempotency key is no key |
//! | `host` | §10.5: who a post of the host's own voice may speak as |
//! | [`instructions`] | §10.1.1: what an instruction must be before anything is read, what a modify keeps of it, what its turn reads, and its responses' fingerprint |
//! | [`tool_intent`] | §6.4: a tool call's intent, the reserved property it rides in, how its value is normalized and how a replay shows it |
//! | [`transcript`] | §6.4, §10.1.1 rule 6: the conversation an intelligence channel's turn reads, each message in its author's role, labelled by its speaker when several speak, an instruction marked as the application's, and no copy of the runtime's marks; a block text from outside cannot close |
//!
//! The crate's own `threads` (§5.2, §10.1 step 8) gives the thread root an
//! event's parent is rooted to, and its own `records` (§5.1 Lifecycle
//! records) what a lifecycle record says of its change; the pipeline alone
//! applies them.
//!
//! Visibility itself (§7.3) is a model type: see
//! [`Visibility::admits`](crate::model::Visibility::admits) and
//! `Visibility::intersect`. So is
//! rule 6 of §7.5, which every write to an existing binding goes through:
//! [`BindingUpdate::apply`](crate::model::BindingUpdate::apply).

pub mod addresses;
pub mod attachment;
pub(crate) mod authors;
pub mod chain_depth;
pub mod edits;
pub mod hook_chain;
pub mod hook_registry;
pub(crate) mod host;
pub mod idempotency;
pub mod instructions;
pub mod lifecycle;
mod lookalike;
pub mod organizations;
pub(crate) mod permissions;
pub(crate) mod records;
pub mod routing;
pub(crate) mod threads;
pub mod timers;
pub mod tool_intent;
pub mod transcoding;
pub mod transcript;
mod unicode;
mod unicode_data;

pub use self::permissions::{visibility_allows, visible_events};

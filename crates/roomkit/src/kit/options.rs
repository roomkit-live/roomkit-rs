//! What a kit is built with: the Python implementation's `RoomKit`
//! constructor keywords, for every feature this crate has (step 5,
//! decision 2).

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use roomkit_core::model::AgentResponsePolicy;

use crate::router::InboundRoomRouter;
use crate::store::InMemoryStore;

/// Python's `max_chain_depth=5` (RFC §8.3).
const MAX_CHAIN_DEPTH: u32 = 5;

/// Python's `process_timeout=30.0` (RFC §13.6).
const PROCESS_TIMEOUT: Duration = Duration::from_secs(30);

/// What [`RoomKit::new`](crate::RoomKit::new) builds a kit with, Python's
/// `RoomKit(store=None, inbound_router=None, max_chain_depth=5,
/// agent_response_policy=AGENT_CHAIN, process_timeout=30.0)`.
///
/// The in-memory store, with one keyword changed:
///
/// ```
/// use roomkit::{RoomKit, RoomKitOptions};
///
/// let kit = RoomKit::new(RoomKitOptions { max_chain_depth: 3, ..Default::default() });
/// # let _ = kit;
/// ```
///
/// A struct update cannot change the store's type, so a store of another
/// type comes through [`with_store`](Self::with_store):
/// `RoomKit::new(RoomKitOptions { process_timeout, ..RoomKitOptions::with_store(store) })`.
pub struct RoomKitOptions<S = InMemoryStore> {
    /// Python `store=None`: where rooms and their timelines live (RFC §14),
    /// an in-memory store by default. The kit shares it, as Python's object
    /// is shared: a router built over it reads what the kit writes
    /// ([`DefaultInboundRoomRouter::new`](crate::DefaultInboundRoomRouter::new)).
    pub store: Arc<S>,
    /// Python `inbound_router=None`: the strategy that finds the room of a
    /// message that names none (RFC §10.4); `None` is the default strategy
    /// over the kit's store and its registered channels.
    pub inbound_router: Option<Arc<dyn InboundRoomRouter>>,
    /// Python `max_chain_depth=5`: the chain-depth limit (RFC §8.3).
    pub max_chain_depth: u32,
    /// Python `agent_response_policy=AgentResponsePolicy.AGENT_CHAIN`: what
    /// an agent's own output solicits in a room created without a policy
    /// of its own (RFC §19.3.1).
    pub agent_response_policy: AgentResponsePolicy,
    /// Python `process_timeout=30.0`: how long an inbound call may take from
    /// its start to its decision (RFC §13.6). Past it, the call writes
    /// nothing of its event, lets go of what it holds and returns `blocked`
    /// with the reason `process_timeout`, after the `process_timeout`
    /// framework event. The commit and what follows it, the event's delivery
    /// and its responses, are never bounded. The bound counts from the
    /// call's start; the waits for the room's admission ticket and its lock
    /// count against it, the wait for the event's turn in the room's
    /// delivery lane, after the commit, does not (gap G-068).
    pub process_timeout: Duration,
}

/// Python's defaults, over a new in-memory store. Defined for the in-memory
/// store alone, so that `..Default::default()` names the kit's store type.
impl Default for RoomKitOptions {
    fn default() -> Self {
        Self::with_store(InMemoryStore::new())
    }
}

impl<S> RoomKitOptions<S> {
    /// Python's defaults over `store`, for a store of another type than the
    /// in-memory one.
    pub fn with_store(store: S) -> Self {
        Self {
            store: Arc::new(store),
            inbound_router: None,
            max_chain_depth: MAX_CHAIN_DEPTH,
            agent_response_policy: AgentResponsePolicy::AgentChain,
            process_timeout: PROCESS_TIMEOUT,
        }
    }
}

impl<S> fmt::Debug for RoomKitOptions<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RoomKitOptions")
            .field("inbound_router", &self.inbound_router.is_some())
            .field("max_chain_depth", &self.max_chain_depth)
            .field("agent_response_policy", &self.agent_response_policy)
            .field("process_timeout", &self.process_timeout)
            .finish_non_exhaustive()
    }
}

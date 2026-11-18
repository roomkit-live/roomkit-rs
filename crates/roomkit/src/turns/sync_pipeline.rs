//! The result exposed to an application that runs a synchronous hook chain.
use crate::model::{InjectedEvent, Metadata, Observation, Task};

/// The outcome, final payload, side effects and diagnostics of one synchronous hook chain.
#[derive(Clone, Debug, PartialEq)]
pub struct SyncPipelineResult<P> {
    /// Whether the chain allowed the operation.
    pub allowed: bool,
    /// The payload as the last hook left it.
    pub event: Option<P>,
    /// The reason the operation was blocked.
    pub reason: Option<String>,
    /// The name of the hook that blocked the operation, when identified.
    pub blocked_by: Option<String>,
    /// Events accepted for injection by this trigger (RFC §9.4).
    pub injected_events: Vec<InjectedEvent>,
    /// Tasks produced by hooks, including a hook that blocked.
    pub tasks: Vec<Task>,
    /// Observations produced by hooks, including a hook that blocked.
    pub observations: Vec<Observation>,
    /// Hook failures in execution order, each with its `hook` and `error`.
    pub hook_errors: Vec<Metadata>,
    /// Metadata returned by hooks, with later values taking precedence.
    pub metadata: Metadata,
    /// Whether a failure closed the chain.
    pub failed_closed: bool,
}

impl<P> Default for SyncPipelineResult<P> {
    fn default() -> Self {
        Self {
            allowed: true,
            event: None,
            reason: None,
            blocked_by: None,
            injected_events: Vec::new(),
            tasks: Vec::new(),
            observations: Vec::new(),
            hook_errors: Vec::new(),
            metadata: Metadata::new(),
            failed_closed: false,
        }
    }
}

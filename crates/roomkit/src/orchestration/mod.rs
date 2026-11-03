//! Multi-agent coordination: Python's `roomkit.orchestration`. A room's
//! conversation state and the router reading it (RFC §19.2, §19.4), the
//! strategies installed on a room (§19.7), and the status bus (§19.8), the
//! shared log every agent posts its status to and any code can follow: the
//! entry, its level, and the in-memory backend a bus uses by default;
//! [`backend`] holds what another backend implements. The tool and session
//! auditors (§15.8) record an agent's tool calls and a session's timeline.

pub mod backend;
mod memory;
mod router;
mod session_audit;
mod session_transcript;
mod state;
mod status;
mod strategy;
mod text;
mod tool_audit;

pub use self::memory::{InMemoryStatusBackend, InMemoryStatusBackendOptions};
pub use self::router::{
    ConversationRouter, ConversationRouterOptions, RoutingConditions, RoutingRule,
};
pub use self::session_audit::{JsonlSessionAuditor, SessionAuditEntry, SessionAuditor};
pub use self::state::{
    ConversationPhase, ConversationState, PhaseTransition, TransitionOptions,
    get_conversation_state, save_conversation_state, set_conversation_state,
};
pub use self::status::{
    HasCompletedOptions, PostAsyncOptions, PostOptions, RecentOptions, StatusBus, StatusBusOptions,
    StatusEntry, StatusLevel,
};
pub use self::strategy::{Orchestration, StrategyAgent, StrategyFuture};
pub use self::text::RecentTextOptions;
pub use self::tool_audit::{
    ConsoleToolAuditor, ConsoleToolAuditorOptions, JsonlToolAuditor, ToolAuditEntry, ToolAuditor,
};

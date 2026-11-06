//! Multi-agent coordination: Python's `roomkit.orchestration`. A room's
//! conversation state and the router reading it (RFC §19.2, §19.4), the
//! strategies installed on a room (§19.7), and the status bus (§19.8), the
//! shared log every agent posts its status to and any code can follow: the
//! entry, its level, and the in-memory backend a bus uses by default;
//! [`backend`] holds what another backend implements. The tool and session
//! auditors (§15.8) record an agent's tool calls and a session's timeline.
//! The handoff (§19.6) hands a room from one agent to another, along the stages
//! of a [`ConversationPipeline`] (§19.5). [`strategies`] holds the strategies the kit builds
//! in: the pipeline (§19.7.1), the loop (§19.7.4), the supervisor (§19.7.3) and
//! the swarm (§19.7).

mod agents;
pub mod backend;
mod background;
mod conversation_pipeline;
mod gather;
pub(crate) mod handoff;
mod handoff_end;
mod handoff_greeting;
mod handoff_tool;
pub(crate) mod lifecycle;
mod memory;
mod router;
mod session_audit;
mod session_transcript;
mod state;
mod status;
pub mod strategies;
mod strategy;
mod text;
mod tool_audit;
mod worker_run;

/// What only a pipeline's install takes: [`InstallOptions`](pipeline::InstallOptions), named as
/// the router's are.
pub mod pipeline {
    pub use super::conversation_pipeline::InstallOptions;
}

pub use self::conversation_pipeline::{
    ConversationPipeline, ConversationPipelineOptions, PipelineStage,
};
pub use self::handoff::{
    HandoffHandler, HandoffHandlerOptions, HandoffRequest, HandoffResult, OnHandoffComplete,
};
pub use self::handoff_greeting::SendGreetingOptions;
#[doc(hidden)]
pub use self::handoff_tool::handoff_tool_definition;
pub use self::memory::{InMemoryStatusBackend, InMemoryStatusBackendOptions};
pub(crate) use self::router::router_hook_type;
#[doc(inline)]
#[doc(inline)]
pub use self::router::{
    ConversationRouter, ConversationRouterOptions, InstallOptions, RoutingConditions, RoutingRule,
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
#[doc(inline)]
pub use self::strategies::discussion::dispatch::{
    DispatchError, DispatchFuture, MockDispatchPolicyOptions,
};
pub use self::strategies::discussion::strategy::{DiscussionOptions, DoneFn};
#[doc(inline)]
pub use self::strategies::discussion::{
    Discussion, DispatchCandidate, DispatchDecision, DispatchDecisionEvent, DispatchPolicy,
    DispatchTurn, MockDispatchPolicy, SpeakQueue, SpeakQueueChange, SpeakQueueEvent,
};
pub use self::strategies::pipeline::Pipeline;
pub use self::strategies::pipeline::PipelineOptions;
pub use self::strategies::review_loop::{Loop, LoopOptions};
#[doc(inline)]
pub use self::strategies::supervisor::Supervisor;
pub use self::strategies::supervisor::strategy::SupervisorOptions;
#[doc(inline)]
pub use self::strategies::swarm::Swarm;
pub use self::strategy::{Orchestration, StrategyAgent, StrategyFuture};
pub use self::text::RecentTextOptions;
pub use self::tool_audit::{
    ConsoleToolAuditor, ConsoleToolAuditorOptions, JsonlToolAuditor, ToolAuditEntry, ToolAuditor,
};

//! RoomKit for Rust.
//!
//! Rooms hold conversations between humans, AI agents and programs, each
//! reaching the room through a channel. This crate runs the decisions of
//! [`roomkit_core`] on a tokio runtime: [`RoomKit`] executes their effects
//! against the registered channels, hooks and store.
//!
//! - [`Channel`]: implement it for a transport or an intelligence channel.
//! - [`RoomKit`], built with [`RoomKitOptions`], the Python
//!   implementation's constructor keywords: register channels and hooks,
//!   run rooms through their lifecycle and timers, process messages into
//!   the room they name or the room their router finds, deferring their
//!   delivery if asked ([`DeliveryHandle`]), inject events and commit
//!   records directly, and close, which refuses every new call, cuts what
//!   has not started and every turn, and gives the running work one grace
//!   before it cuts it. A
//!   hook, a turn or a delivery that calls into its own room never waits for
//!   itself: the room lock is reentrant within the call's execution context,
//!   a call made under the lock or in the room's admission order is adopted
//!   by the call whose work made it, and a turn's or a delivery's call
//!   returns at its decision.
//! - [`WeakRoomKit`]: the kit held without keeping it alive, for a hook or
//!   a channel that calls into the kit holding it.
//! - [`InboundRoomRouter`]: a routing strategy; [`DefaultInboundRoomRouter`]
//!   is the RFC's.
//! - [`ConversationStore`]: where rooms and their timelines live (RFC §14),
//!   [`InMemoryStore`] by default.
//! - [`testing`]: a recording transport and an echo agent, and a kit on a
//!   clock and ids a test sets, for tests and examples.
//! - [`tools`]: Python's `roomkit.tools` helpers, the fence a tool puts
//!   around what came from outside it (RFC §6.4).
//! - [`current_turn_host`] and [`TurnHost`]: the intelligence turn the current
//!   task runs within, which writes to its room and runs the kit's tool
//!   hooks, registered with [`RoomKit::hook`] on `BEFORE_TOOL_USE` and
//!   `ON_TOOL_CALL` (RFC §9.3), on each call its tool loop makes.
//! - The turn backend (RFC §10.6): every intelligence turn runs through the
//!   kit's in-memory backend, between the kit, the room's owner, and the
//!   kit's own turn executor, as serialized messages; its bounds are
//!   [`TurnTimeouts`], and a failure it carries a
//!   [`RoomKitError::TurnLink`] or a [`RoomKitError::RemoteTurn`].
//!
//! The core's model is re-exported as [`model`], but for the requests the
//! kit's calls build from their options. Of its operations
//! and rules, only what a caller names is: the results and errors of the
//! kit's calls, the framework events, what a channel declares and how it
//! writes its addresses, what a store's commit writes, and the rules that
//! read an event; the protocol the driver speaks with the core stays the
//! core's.

mod channel;
#[doc(hidden)]
pub mod channel_publication;
mod classed;
mod content_logging;
mod context;
mod cursors;
mod deadline;
mod declared_channel;
pub mod delivery;
mod delivery_keys;
mod discussion_turn;
mod driver;
mod environment;
mod error;
mod events;
#[doc(hidden)]
pub mod external_tools;
mod gaps;
mod history;
mod hooks;
mod host;
mod host_locks;
pub mod human_input;
pub mod identity;
mod inbound;
mod kit;
mod lock_manager;
#[doc(hidden)]
pub use host_locks::track_host_room_lock;
mod locks;
mod media;
pub mod orchestration;
pub mod providers;
mod queues;
pub mod realtime;
mod registered;
mod room_turns;
mod router;
mod sends;
mod start_order;
mod steering;
pub mod store;
mod stream;
pub mod tasks;
pub mod telemetry;
pub mod testing;
mod text_errors;
mod tool_call_scope;
mod tool_calls;
mod tool_verdict;
mod transport;
mod turn;
mod turn_record;
mod turn_runners;
pub mod turns;
mod unwind;

/// The data model of RFC §5, field for field: the core's, but for the
/// requests the kit's calls build from their options (a binding's update,
/// a post), which are no model.
pub mod model {
    pub use crate::tool_verdict::ToolCallVerdict;

    /// The result and shared data passed to a [`ToolCallVerdict`].
    ///
    /// Nested handles are strong references. Break any cycles before
    /// releasing the last external handle. A synchronous list update
    /// must not reacquire that same list's lock.
    pub mod tool_verdict {
        pub use crate::tool_verdict::{VerdictList, VerdictObject, VerdictResult, VerdictValue};
    }
    pub use roomkit_core::model::{
        Access, AgentResponsePolicy, AudioContent, ChannelBinding, ChannelCapabilities,
        ChannelCategory, ChannelDirection, ChannelId, ChannelMediaType, ChannelOutput, ChannelSet,
        ChannelType, CompositeContent, Confidence, DeleteContent, DeleteType, DeliveryError,
        DeliveryResult, DeliveryResultStatus, DeliveryStatus, EditContent, EditSource, ErrorRecord,
        EventContent, EventDirection, EventId, EventSource, EventStatus, EventType, HookAction,
        HookResult, IdentificationStatus, Identity, IdentityHookResult, IdentityId, IdentityResult,
        InboundMessage, InjectedEvent, Latitude, LocationContent, Longitude, MediaContent,
        Metadata, ModelError, Observation, ObservationId, Participant, ParticipantId,
        ParticipantRole, ParticipantStatus, PersistencePolicy, ProviderResult, RateLimit,
        ResponseMetadata, RetryPolicy, RichContent, RichFormat, Room, RoomContext, RoomEvent,
        RoomId, RoomStatus, RoomTimers, STANDALONE, SystemContent, TURNS_KEY, Task, TaskId,
        TaskStatus, TemplateContent, TextContent, ThreadSummary, Timestamp, ToolCallContent,
        ToolCallOutcome, ToolCallStatus, Verification, VerificationPolicy, VerificationRequest,
        VerificationRequestStatus, VerificationStatus, VideoContent, Visibility,
    };

    pub use crate::delivery::outcome::{DeliveryOutcome, DeliveryOutcomeStatus};
    pub use crate::steering::{Cancel, InjectMessage, SteeringDirective, UpdateSystemPrompt};
    pub use crate::stream::{LoopEndMarker, LoopEndReason, UsageMarker};
    #[doc(hidden)]
    pub use crate::stream::{add_usage, reports_counters};
    /// What a hook returns: [`HookResult`]'s constructors and their options,
    /// Python's keywords of `HookResult.allow`, `block` and `modify`.
    pub mod hook {
        pub use roomkit_core::model::hook::{AllowOptions, BlockOptions, ModifyOptions};
    }
    /// What an identity hook answers: [`IdentityHookResult`]'s constructors'
    /// options, Python's keywords of `IdentityHookResult.pending`,
    /// `challenge` and `reject`.
    pub mod identity {
        pub use roomkit_core::model::identity::{ChallengeOptions, PendingOptions, RejectOptions};
    }
    // Outside the public API: the Python implementation keeps them private,
    // and the workspace's crates read them across crates. `ToolCall` leaves
    // with lot 3 (`ToolCallEvent`) and lot 5 (`AiToolCall`).
    #[doc(hidden)]
    pub use roomkit_core::model::{HOST_CHANNEL_ID, MAX_COMPOSITE_DEPTH, ToolCall};
}
pub use roomkit_core::model::SUPERSEDED;
pub use roomkit_core::rules::addresses::{
    Addressing, ChannelAddressing, ChannelAddressingOptions, CountryCode, InvalidCountryCode,
    NormalizeOptions, addressing_of,
};
pub use roomkit_core::rules::hook_registry::{HookExecution, HookTrigger, TURN_TRIGGERS};
pub use roomkit_core::rules::{received_events, visibility_allows, visible_events};
// Outside the public API: the Python implementation keeps it private, and the
// agent's crate reads it across crates (RFC §6.4).
#[doc(hidden)]
pub use roomkit_core::rules::tool_intent;
// Outside the public API (step 5, decision 16): the examples' scripted agent
// reads its turn's history with it until lot 5 makes it an AI channel over a
// mock provider.
#[doc(hidden)]
pub use roomkit_core::rules::instructions::turn_context;

pub use self::channel::{
    AgentIdentity, Channel, ChannelDeclaration, ChannelError, default_inbound_event,
};
// Outside the public API (RMK-1208): the agent's crate names its
// provider errors' Python class with it, which the kit cannot see.
#[doc(hidden)]
pub use self::classed::ClassedError;
pub use self::content_logging::{content_logging_enabled, set_content_logging};
pub use self::declared_channel::DeclaredChannel;
pub use self::text_errors::{
    ChannelRefusalError, TaskCutShortError, TaskTurnFailedError, ToolTimeoutError,
    TurnCutShortError,
};
// Outside the public API: what an AI channel reads of a discussion's turn.
pub use self::context::keep_context;
pub use self::delivery::backend::{
    DeliveryBackend, DeliveryBackendError, DequeueOptions, GetDeadLetterItemsOptions, NackOptions,
};
pub use self::delivery::item::{DeliveryItem, DeliveryItemStatus};
pub use self::delivery::memory::{InMemoryDeliveryBackend, InMemoryDeliveryBackendOptions};
pub use self::delivery::queued::{Queued, QueuedOptions};
pub use self::delivery::strategy::{
    DeliveryContext, DeliveryStrategy, Immediate, WaitForIdle, WaitForIdleOptions,
};
#[doc(hidden)]
pub use self::discussion_turn::{discussion_notes, discussion_turn, issue_mark, retire_mark};
pub use self::error::RoomKitError;
pub use self::events::FrameworkEvent;
pub use self::hooks::{
    HookError, HookFuture, HookHandler, HookOptions, HookPayload, HookRegistration, HookReturn,
    IntoHookReturn, SessionStartedEvent,
};
pub use self::lock_manager::{
    InMemoryLockManager, InMemoryLockManagerOptions, RoomLock, RoomLockManager,
};
#[doc(inline)]
pub use self::realtime::RealtimeFailure;
// For the AI channel's loop alone: the payloads of its triggers, which
// roomkit-ai defines, and what their hooks came to.
#[doc(hidden)]
pub use self::context::within_tool_turn_depth;
#[doc(hidden)]
pub use self::hooks::TurnPayload;
pub use self::host::{HostChannel, HostChannelOptions};
pub use self::inbound::{CancelOptions, CascadeReport, DeliveryHandle, InboundResult};
pub use self::kit::{
    AddMemberOptions, EnsureParticipantOptions, IsMemberOptions, RemoveMemberOptions,
    RenameMemberOptions, ResolveParticipantOptions,
};
pub use self::kit::{
    AddRoomHookOptions, AgentPresenceOptions, ArchiveRoomOptions, AttachChannelOptions,
    CheckAllTimersOptions, CheckRoomTimersOptions, CloseRoomOptions, CommitEventOptions,
    CreateRoomOptions, DeleteEventOptions, DetachChannelOptions, GetBindingOptions, GetRoomOptions,
    GetTimelineOptions, HasHooksOptions, HookEngine, IdentityHookOptions, ListBindingsOptions,
    ListMembersOptions, MuteOptions, MuteOutputOptions, PauseRoomOptions, PostOptions,
    ProcessInboundOptions, RemoveRoomHookOptions, ReopenRoomOptions, ResumeRoomOptions, RoomKit,
    RoomKitOptions, SendEventOptions, SendGreetingOptions, SetAccessOptions,
    SetAgentResponsePolicyOptions, SetRoomTimersOptions, SetVisibilityOptions, UnmuteOptions,
    UnmuteOutputOptions, UpdateBindingMetadataOptions, UpdateEventOptions, WeakRoomKit,
};
pub use self::kit::{
    CancelDeliveryOptions, CancelTaskOptions, SetupBackgroundOptions, StartTaskOptions,
    SteerOptions,
};
pub use self::kit::{
    ChannelCallOptions, ChannelRegistration, FrameworkAware, FrameworkAwareChannel, TurnExecutor,
    TurnExecutorOptions, framework_aware,
};
pub use self::kit::{
    CompleteVerificationOptions, RecordFailedAttemptOptions, RequestVerificationOptions,
    RevokeVerificationOptions, VerificationOfOptions,
};
pub use self::kit::{
    DelegateOptions, DeliverOptions, ProcessDeliveryStatusOptions, ProcessWebhookOptions,
};
pub use self::kit::{
    DisableRoomHookOptions, EnableRoomHookOptions, InputRequestOptions, RejectInputOptions,
};
pub use self::kit::{
    ListObservationsOptions, ListTasksOptions, SubmitFeedbackOptions, UpdateRoomMetadataOptions,
};
pub use self::kit::{ListReadMarkersOptions, MarkAllReadOptions, MarkReadOptions};
pub use self::kit::{
    PublishPresenceOptions, PublishReactionOptions, PublishReadReceiptOptions,
    PublishToolCallOptions, PublishTypingOptions, SubscribeRoomOptions,
};
pub use self::kit::{RegenerateResponseOptions, RegenerateTargetOptions};
pub use self::media::{AttachedMedia, MediaResolver};
pub use self::registered::RegisteredChannel;
pub use self::router::{
    ChannelOf, DefaultInboundRoomRouter, DefaultInboundRoomRouterOptions, InboundRoomRouter,
    RouteOptions, RouterError, RouterFuture,
};
pub use self::store::error::{StoreError, StoreFailure};
pub use self::store::query::EventFilter;
pub use self::store::rooms::RoomFilter;
#[doc(hidden)]
pub use self::store::written;
#[doc(inline)]
pub use self::store::{ConversationStore, InMemoryStore};
pub use self::stream::{DeliveryStream, ResponseItem, ResponseStream, StreamItem};
#[doc(hidden)]
pub use self::tool_call_scope::{
    OwnerToolContext, OwnerToolIdentity, OwnerToolScope, ToolCallScope, current_tool_call_scope,
    current_tool_call_scopes, within_tool_call_scope,
};
pub use self::tool_calls::ToolCallEvent;
#[doc(hidden)]
pub use self::tool_calls::ToolCallReport;
pub use self::tool_verdict::ToolCallVerdict;
pub use self::transport::{TransportChannel, TransportChannelOptions};
#[doc(hidden)]
pub use self::turn::TurnChain;
pub use self::turn::{PostStatusOptions, ToolGate, TurnHost, WaitTaskOptions, current_turn_host};
pub use self::turn_record::{Scoped, TurnRecord, current_turn_record};
#[doc(inline)]
pub use self::turns::current_turn_id;
// For the AI channel's turn alone: the answer of a tool the owner serves.
pub use self::kit::{
    ClaimRoomStrategyOptions, InstallStrategyOptions, ListenOnlyOptions,
    RoomStrategyIsCurrentOptions, RoomStrategyOptions, SpeakQueueOptions, TalkAgainOptions,
    UninstallStrategyOptions,
};
#[doc(inline)]
pub use self::orchestration::Discussion;
#[doc(inline)]
pub use self::orchestration::Loop;
#[doc(inline)]
pub use self::orchestration::Orchestration;
#[doc(inline)]
pub use self::orchestration::Pipeline;
#[doc(inline)]
pub use self::orchestration::Supervisor;
#[doc(inline)]
pub use self::orchestration::Swarm;
#[doc(inline)]
pub use self::orchestration::{
    ConversationPhase, ConversationPipeline, ConversationRouter, ConversationState, PipelineStage,
    RoutingConditions, RoutingRule, get_conversation_state, save_conversation_state,
    set_conversation_state,
};
#[doc(inline)]
pub use self::orchestration::{
    DispatchCandidate, DispatchDecision, DispatchDecisionEvent, DispatchPolicy, DispatchTurn,
    MockDispatchPolicy, SpeakQueue, SpeakQueueChange, SpeakQueueEvent,
};
#[doc(inline)]
pub use self::orchestration::{HandoffHandler, HandoffRequest, HandoffResult};
#[doc(inline)]
pub use self::orchestration::{ResultTool, SUBMIT_RESULT};
#[doc(hidden)]
pub use self::turn::OwnerToolAnswer;
#[doc(inline)]
pub use self::turns::{InMemoryTurnBackend, TurnTimeouts};

/// Python's `roomkit.tools` helpers this crate has.
pub mod tools {
    /// The default block tags read by [`named_blocks`] (RFC §6.4).
    #[doc(inline)]
    pub use roomkit_core::rules::transcript::FENCED_TAGS;

    /// `text` fenced as untrusted under `tag` (RFC §6.4), Python's
    /// `roomkit.tools.fence(tag, text)`: what a tool hands the model that
    /// came from outside it.
    pub use roomkit_core::rules::transcript::fence;

    /// Replace each selected block with its tag in brackets, including a block
    /// cut off before its end: Python's `roomkit.tools.named_blocks` (RFC §6.4).
    ///
    /// `None` uses [`FENCED_TAGS`]. An empty slice leaves the text unchanged.
    /// Tags are applied in the supplied order; each replacement keeps the
    /// spelling of its selected tag.
    ///
    /// ```
    /// use roomkit::tools::named_blocks;
    ///
    /// assert_eq!(named_blocks("a <tool_result>x</tool_result> b", None), "a [tool_result] b");
    /// assert_eq!(named_blocks("<custom>x</custom>", Some(&["custom"])), "[custom]");
    /// assert_eq!(named_blocks("<custom>x</custom>", Some(&[])), "<custom>x</custom>");
    /// ```
    pub fn named_blocks(text: &str, tags: Option<&[&str]>) -> String {
        roomkit_core::rules::transcript::named_blocks(text, tags.unwrap_or(FENCED_TAGS))
    }
}

/// The README's code, compiled and run as doctests, so that the first room
/// it shows is one the crate runs.
#[cfg(doctest)]
#[doc = include_str!("../../../README.md")]
struct ReadmeDoctests;

#[cfg(test)]
#[path = "../tests/support/warnings.rs"]
mod test_warnings;

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
mod classed;
mod context;
mod cursors;
pub mod delivery;
mod delivery_keys;
mod driver;
mod environment;
mod error;
mod events;
mod gaps;
mod history;
mod hooks;
mod host;
pub mod human_input;
pub mod identity;
mod inbound;
mod kit;
mod lock_manager;
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
mod steering;
pub mod store;
mod stream;
pub mod tasks;
pub mod telemetry;
pub mod testing;
mod tool_call_scope;
mod tool_calls;
mod transport;
mod turn;
mod turn_record;
mod turn_runners;
mod turns;
mod unwind;

/// The data model of RFC §5, field for field: the core's, but for the
/// requests the kit's calls build from their options (a binding's update,
/// a post), which are no model.
pub mod model {
    pub use roomkit_core::model::{
        Access, AgentResponsePolicy, AudioContent, ChannelBinding, ChannelCapabilities,
        ChannelCategory, ChannelDirection, ChannelId, ChannelMediaType, ChannelOutput, ChannelSet,
        ChannelType, CompositeContent, Confidence, DeleteContent, DeleteType, DeliveryError,
        DeliveryResult, DeliveryResultStatus, DeliveryStatus, EditContent, EditSource, ErrorRecord,
        EventContent, EventDirection, EventId, EventSource, EventStatus, EventType, HookAction,
        HookResult, IdentificationStatus, Identity, IdentityHookResult, IdentityId, IdentityResult,
        InboundMessage, InjectedEvent, Latitude, LocationContent, Longitude, MediaContent,
        Metadata, ModelError, Observation, ObservationId, Participant, ParticipantId,
        ParticipantRole, ParticipantStatus, ProviderResult, RateLimit, ResponseMetadata,
        RetryPolicy, RichContent, RichFormat, Room, RoomContext, RoomEvent, RoomId, RoomStatus,
        RoomTimers, SystemContent, Task, TaskId, TaskStatus, TemplateContent, TextContent,
        ThreadSummary, Timestamp, ToolCallContent, ToolCallOutcome, ToolCallStatus, Verification,
        VerificationPolicy, VerificationRequest, VerificationRequestStatus, VerificationStatus,
        VideoContent, Visibility,
    };

    pub use crate::delivery::outcome::{DeliveryOutcome, DeliveryOutcomeStatus};
    pub use crate::steering::{Cancel, InjectMessage, SteeringDirective, UpdateSystemPrompt};
    pub use crate::stream::{LoopEndMarker, LoopEndReason};
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
pub use roomkit_core::rules::addresses::{
    Addressing, ChannelAddressing, ChannelAddressingOptions, CountryCode, InvalidCountryCode,
    NormalizeOptions, addressing_of,
};
pub use roomkit_core::rules::hook_registry::{HookExecution, HookTrigger};
pub use roomkit_core::rules::{visibility_allows, visible_events};
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
pub use self::error::RoomKitError;
pub use self::events::FrameworkEvent;
pub use self::hooks::{
    HookError, HookFuture, HookHandler, HookOptions, HookPayload, HookRegistration, HookReturn,
    IntoHookReturn, SessionStartedEvent,
};
pub use self::lock_manager::{
    InMemoryLockManager, InMemoryLockManagerOptions, RoomLock, RoomLockManager,
};
// For the AI channel's loop alone: the payloads of its triggers, which
// roomkit-ai defines, and what their hooks came to.
#[doc(hidden)]
pub use self::hooks::TurnPayload;
pub use self::host::{HostChannel, HostChannelOptions};
pub use self::inbound::{CancelOptions, DeliveryHandle, InboundResult};
pub use self::kit::RejectInputOptions;
pub use self::kit::{
    AddMemberOptions, EnsureParticipantOptions, IsMemberOptions, RemoveMemberOptions,
    RenameMemberOptions, ResolveParticipantOptions,
};
pub use self::kit::{
    AddRoomHookOptions, ArchiveRoomOptions, AttachChannelOptions, CheckAllTimersOptions,
    CheckRoomTimersOptions, CloseRoomOptions, CommitEventOptions, CreateRoomOptions,
    DeleteEventOptions, DetachChannelOptions, GetBindingOptions, GetRoomOptions,
    GetTimelineOptions, HasHooksOptions, HookEngine, IdentityHookOptions, ListBindingsOptions,
    ListMembersOptions, MuteOptions, MuteOutputOptions, PauseRoomOptions, PostOptions,
    ProcessInboundOptions, RemoveRoomHookOptions, ReopenRoomOptions, ResumeRoomOptions, RoomKit,
    RoomKitOptions, SendEventOptions, SetAccessOptions, SetVisibilityOptions, UnmuteOptions,
    UnmuteOutputOptions, UpdateBindingMetadataOptions, UpdateEventOptions, WeakRoomKit,
};
pub use self::kit::{
    CancelDeliveryOptions, CancelTaskOptions, SetupBackgroundOptions, StartTaskOptions,
    SteerOptions,
};
pub use self::kit::{
    CompleteVerificationOptions, RecordFailedAttemptOptions, RequestVerificationOptions,
    RevokeVerificationOptions, VerificationOfOptions,
};
pub use self::kit::{
    DelegateOptions, DeliverOptions, ProcessDeliveryStatusOptions, ProcessWebhookOptions,
};
pub use self::kit::{
    ListObservationsOptions, ListTasksOptions, SubmitFeedbackOptions, UpdateRoomMetadataOptions,
};
pub use self::kit::{ListReadMarkersOptions, MarkAllReadOptions, MarkReadOptions};
pub use self::kit::{
    PublishPresenceOptions, PublishReactionOptions, PublishReadReceiptOptions,
    PublishToolCallOptions, PublishTypingOptions, SubscribeRoomOptions,
};
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
pub use self::tool_call_scope::{current_tool_call_scope, within_tool_call_scope};
pub use self::tool_calls::ToolCallEvent;
#[doc(hidden)]
pub use self::tool_calls::ToolCallReport;
pub use self::transport::{TransportChannel, TransportChannelOptions};
#[doc(hidden)]
pub use self::turn::TurnChain;
pub use self::turn::{PostStatusOptions, ToolGate, TurnHost, WaitTaskOptions, current_turn_host};
pub use self::turn_record::{Scoped, TurnRecord, current_turn_record};
// For the AI channel's turn alone: the answer of a tool the owner serves.
pub use self::kit::{
    ClaimRoomStrategyOptions, InstallStrategyOptions, RoomStrategyOptions, UninstallStrategyOptions,
};
#[doc(inline)]
pub use self::orchestration::Loop;
#[doc(inline)]
pub use self::orchestration::Orchestration;
#[doc(inline)]
pub use self::orchestration::Supervisor;
#[doc(inline)]
pub use self::orchestration::Swarm;
#[doc(inline)]
pub use self::orchestration::{
    ConversationPhase, ConversationRouter, ConversationState, RoutingConditions, RoutingRule,
    get_conversation_state, save_conversation_state, set_conversation_state,
};
#[doc(inline)]
pub use self::orchestration::{HandoffHandler, HandoffRequest, HandoffResult};
pub use self::tasks::result_tool::ResultTool;
#[doc(hidden)]
pub use self::turn::OwnerToolAnswer;
pub use self::turns::TurnTimeouts;

/// Python's `roomkit.tools` helpers this crate has.
pub mod tools {
    /// `text` fenced as untrusted under `tag` (RFC §6.4), Python's
    /// `roomkit.tools.fence(tag, text)`: what a tool hands the model that
    /// came from outside it.
    pub use roomkit_core::rules::transcript::fence;
}

/// The README's code, compiled and run as doctests, so that the first room
/// it shows is one the crate runs.
#[cfg(doctest)]
#[doc = include_str!("../../../README.md")]
struct ReadmeDoctests;

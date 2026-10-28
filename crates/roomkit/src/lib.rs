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
//!   records directly, and close, which drains the work the kit runs in
//!   the background before it ends it. A
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
//! - [`transcript`]: the conversation an agent's turn hands its model, each
//!   message in its author's role and labelled by its speaker as RFC §6.4
//!   requires of an AI channel, and the tool rounds within a turn.
//! - [`current_turn`] and [`TurnHost`]: the intelligence turn the current
//!   task runs within, which writes to its room and runs the kit's tool
//!   hooks ([`RoomKit::before_tool_use`], [`RoomKit::on_tool_call`],
//!   [`RoomKit::observe_tool_calls`], RFC §9.3) on each call its tool loop
//!   makes.
//! - The turn backend (RFC §10.6): every intelligence turn runs through the
//!   kit's in-memory backend, between the kit, the room's owner, and the
//!   kit's own turn executor, as serialized messages; its bounds are
//!   [`TurnTimeouts`], a failure it carries a [`TurnError`], and
//!   [`testing::TurnTap`] shows what crosses.
//!
//! The core's model is re-exported as [`model`], but for the requests the
//! kit's calls build from their options. Of its operations
//! and rules, only what a caller names is: the results and errors of the
//! kit's calls, the framework events, what a channel declares and how it
//! writes its addresses, what a store's commit writes, and the rules that
//! read an event; the protocol the driver speaks with the core stays the
//! core's.

mod channel;
mod context;
mod cursors;
mod driver;
mod environment;
mod error;
mod hooks;
mod host;
mod inbound;
mod kit;
mod locks;
mod queues;
mod registered;
mod router;
mod store;
mod stream;
mod tasks;
pub mod testing;
mod tools;
mod turn;
mod turns;
mod unwind;

/// The data model of RFC §5, field for field: the core's, but for the
/// requests the kit's calls build from their options (a binding's update,
/// a post), which are no model.
pub mod model {
    pub use roomkit_core::model::{
        Access, AgentResponsePolicy, AudioContent, ChannelBinding, ChannelCapabilities,
        ChannelCategory, ChannelDirection, ChannelId, ChannelMediaType, ChannelOutput, ChannelSet,
        ChannelType, CompositeContent, Confidence, Consumption, ConsumptionState, DeleteContent,
        DeleteType, DeliveryError, DeliveryResult, DeliveryStatus, DirectEvent, EditContent,
        EditSource, ErrorRecord, EventContent, EventDirection, EventId, EventSource, EventStatus,
        EventType, EventUpdates, HOST_CHANNEL_ID, HookDecision, HookResult, IdentificationStatus,
        Identity, IdentityId, InboundMessage, InjectedEvent, Latitude, LocationContent, Longitude,
        MAX_COMPOSITE_DEPTH, MediaContent, Metadata, ModelError, Observation, ObservationId,
        Participant, ParticipantConsumption, ParticipantId, ParticipantRole, ParticipantStatus,
        ProviderResult, RateLimit, RetryPolicy, RichContent, RichFormat, Room, RoomContext,
        RoomEvent, RoomId, RoomStatus, RoomTimers, RoomUpdates, SystemContent, Task, TaskId,
        TaskStatus, TemplateContent, TextContent, Timestamp, ToolCall, ToolCallContent,
        ToolCallOutcome, ToolCallStatus, VerificationStatus, VideoContent, Visibility,
    };
}
pub use roomkit_core::ops::{
    CHANNEL_NOT_REGISTERED, ChainDepthData, ChannelDeclaration, ConfigError, FrameworkEvent,
    InboundError, LifecycleChange, LifecycleError, LifecyclePayload, MutationError, ProtocolError,
    RefusedOperation, TransitionReason, default_inbound_event,
};
pub use roomkit_core::rules::addresses::{
    AddressForm, Addressing, CountryCode, InvalidCountryCode,
};
pub use roomkit_core::rules::edits::{is_deleted, is_edited};
pub use roomkit_core::rules::hook_registry::{
    HookExecution, HookFilter, HookId, HookSpec, HookTrigger, RegistrationError,
};
pub use roomkit_core::rules::instructions::turn_context;
pub use roomkit_core::rules::lifecycle::Transition;
pub use roomkit_core::rules::transcoding::text_of;
pub use roomkit_core::rules::transcript;
pub use roomkit_core::rules::{visibility_allows, visible_events};

pub use self::channel::{Channel, ChannelError, EventDraft};
pub use self::context::keep_context;
pub use self::error::KitError;
pub use self::hooks::{HookError, HookFuture, HookHandler, HookReturn};
pub use self::host::HostChannel;
pub use self::inbound::{DeliveryHandle, InboundResult};
pub use self::kit::{
    ArchiveRoomOptions, AttachChannelOptions, CheckAllTimersOptions, CheckRoomTimersOptions,
    CloseRoomOptions, CommitEventOptions, CreateRoomOptions, DeleteEventOptions,
    DetachChannelOptions, GetBindingOptions, GetRoomOptions, GetTimelineOptions,
    ListBindingsOptions, ListMembersOptions, MuteOptions, PauseRoomOptions, PostOptions,
    ProcessInboundOptions, ReopenRoomOptions, ResumeRoomOptions, RoomKit, RoomKitOptions,
    SendEventOptions, SetAccessOptions, SetVisibilityOptions, UnmuteOptions,
    UpdateBindingMetadataOptions, UpdateEventOptions, WeakRoomKit,
};
pub use self::registered::RegisteredChannel;
pub use self::router::{
    ChannelOf, DefaultInboundRoomRouter, DefaultInboundRoomRouterOptions, InboundRoomRouter,
    RouteOptions, RouterError, RouterFuture,
};
pub use self::store::{
    ConversationStore, EventFilter, EventQuery, InMemoryStore, RoomCounters, RoomFilter,
    StoreError, StoreFailure, TargetUpdate,
};
pub use self::stream::{DeliveryStream, ResponseItem, ResponseStream, StreamItem};
pub use self::tools::{ToolCallEvent, ToolHookSpec, ToolUse, ToolVerdict};
pub use self::turn::{ToolGate, TurnHost, current_turn};
pub use self::turns::{TurnError, TurnTimeouts};

/// The README's code, compiled and run as doctests, so that the first room
/// it shows is one the crate runs.
#[cfg(doctest)]
#[doc = include_str!("../../../README.md")]
struct ReadmeDoctests;

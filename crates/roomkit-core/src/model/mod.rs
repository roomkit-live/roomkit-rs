//! The data model of RFC §5, field for field.
//!
//! Records have public fields and mirror the RFC's names. Invariants the RFC
//! states live in small value types ([`ChannelId`], [`Visibility`],
//! [`CompositeContent`], [`Latitude`], [`Confidence`], ...) that refuse an
//! invalid value when built or deserialized, so a record that exists is
//! valid. The RFC is a draft and its records grow: before 1.0, a field or a
//! variant added to follow it is a minor-version change.
//!
//! Every type serializes to the JSON form documented in
//! `docs/architecture.md` (gap G-001).

mod binding;
mod channel;
mod content;
mod context;
mod delivery;
mod direct;
mod error;
mod event;
mod hook;
mod identity;
mod ids;
mod inbound;
mod options;
mod output;
mod participant;
mod post;
mod room;
mod side_effects;
mod time;
mod tool_call;
mod updates;
mod visibility;

pub use binding::{Access, ChannelBinding, RetryPolicy};
pub use channel::{
    ChannelCapabilities, ChannelCategory, ChannelDirection, ChannelMediaType, ChannelType,
    RateLimit,
};
pub use content::{
    AudioContent, CompositeContent, DeleteContent, DeleteType, EditContent, EditSource,
    EventContent, Latitude, LocationContent, Longitude, MAX_COMPOSITE_DEPTH, MediaContent,
    RichContent, RichFormat, SystemContent, TemplateContent, TextContent, ToolCallContent,
    ToolCallOutcome, ToolCallStatus, VideoContent,
};
pub use context::RoomContext;
pub use delivery::{DeliveryError, DeliveryResult, DeliveryStatus, ProviderResult};
pub use direct::DirectEvent;
pub use error::ModelError;
pub use event::{
    Consumption, ConsumptionState, EventDirection, EventSource, EventStatus, EventType,
    ParticipantConsumption, RoomEvent,
};
pub use hook::{HookDecision, HookResult, InjectedEvent};
pub use identity::Identity;
pub use ids::{
    ChannelId, EventId, HOST_CHANNEL_ID, IdentityId, ObservationId, ParticipantId, RoomId, TaskId,
};
pub use inbound::InboundMessage;
pub use options::{BindingOptions, RoomOptions};
pub use output::{ChannelOutput, ErrorRecord};
pub use participant::{
    IdentificationStatus, Participant, ParticipantRole, ParticipantStatus, VerificationStatus,
};
pub use post::Post;
pub use room::{AgentResponsePolicy, Room, RoomStatus, RoomTimers};
pub use side_effects::{Confidence, Observation, Task, TaskStatus};
pub use time::Timestamp;
pub use tool_call::ToolCall;
pub use updates::{BindingUpdate, EventUpdates, RoomUpdates};
pub use visibility::{ChannelSet, Visibility};

/// Integrator-defined data: the RFC's `map<string, any>`.
pub type Metadata = serde_json::Map<String, serde_json::Value>;

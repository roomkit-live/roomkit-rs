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
mod error;
mod event;
mod hook;
mod identity;
mod ids;
mod inbound;
mod output;
mod participant;
mod room;
mod side_effects;
mod time;
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
pub use error::ModelError;
pub use event::{
    Consumption, ConsumptionState, EventDirection, EventSource, EventStatus, EventType,
    ParticipantConsumption, RoomEvent,
};
pub use hook::{HookDecision, HookResult, InjectedEvent};
pub use identity::Identity;
pub use ids::{ChannelId, EventId, IdentityId, ObservationId, ParticipantId, RoomId, TaskId};
pub use inbound::InboundMessage;
pub use output::{ChannelOutput, ErrorRecord};
pub use participant::{
    IdentificationStatus, Participant, ParticipantRole, ParticipantStatus, VerificationStatus,
};
pub use room::{AgentResponsePolicy, Room, RoomStatus, RoomTimers};
pub use side_effects::{Confidence, Observation, Task, TaskStatus};
pub use time::Timestamp;
pub use updates::{BindingUpdate, RoomUpdates};
pub use visibility::{ChannelSet, Visibility};

/// Integrator-defined data: the RFC's `map<string, any>`.
pub type Metadata = serde_json::Map<String, serde_json::Value>;

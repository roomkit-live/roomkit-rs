//! Events: everything that happens in a room (RFC §4.3, §5.2, §8.5).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::{
    ChannelId, ChannelType, EventContent, EventId, Metadata, ParticipantId, RoomId, Timestamp,
    VerificationStatus, Visibility,
};

/// What kind of event it is (RFC §5.2).
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum EventType {
    /// A text, media or rich message.
    #[default]
    Message,
    /// A framework-generated notification.
    System,
    /// The application directs an agent; delivered to it, never stored.
    Instruction,
    /// A user is typing.
    Typing,
    /// A user has read up to an index.
    ReadReceipt,
    /// A provider confirms delivery.
    DeliveryReceipt,
    /// A user is online, offline or away.
    Presence,
    /// An emoji reaction to another event.
    Reaction,
    /// An edit of a previous message.
    Edit,
    /// A deletion of a previous message.
    Delete,
    /// A participant entered the room.
    ParticipantJoined,
    /// A participant left the room.
    ParticipantLeft,
    /// A pending participant was identified.
    ParticipantIdentified,
    /// A participant's presentation changed.
    ParticipantUpdated,
    /// A channel was attached to the room.
    ChannelAttached,
    /// A channel was detached from the room.
    ChannelDetached,
    /// A channel was muted.
    ChannelMuted,
    /// A channel was unmuted.
    ChannelUnmuted,
    /// A channel binding was modified.
    ChannelUpdated,
    /// A DTMF tone was detected.
    Dtmf,
    /// An audio recording started.
    RecordingStarted,
    /// An audio recording stopped.
    RecordingStopped,
    /// A task was created.
    TaskCreated,
    /// A task was delegated to a child room; never stored.
    TaskDelegated,
    /// A delegated task ended; never stored.
    TaskCompleted,
    /// An observation was recorded.
    Observation,
    /// A tool call started.
    ToolCallStart,
    /// A tool call ended.
    ToolCallEnd,
}

/// What became of an event (RFC §5.2).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventStatus {
    /// Created, not yet processed.
    #[default]
    Pending,
    /// Stored and broadcast; an allowed instruction, never stored, reads
    /// so too (RFC §10.1.1).
    Delivered,
    /// Read by its recipient.
    Read,
    /// Delivery failed after every retry.
    Failed,
    /// Refused: by a SYNC hook, by a source that cannot write, by the
    /// chain-depth limit or by the reentry budget (RFC §8.3).
    Blocked,
}

/// The direction an event travelled (RFC §5.2): one way, never both.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventDirection {
    /// From outside into the room.
    Inbound,
    /// From the room towards outside.
    Outbound,
}

/// Where an event came from (RFC §5.2).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventSource {
    /// The channel that produced the event.
    pub channel_id: ChannelId,
    /// That channel's type.
    pub channel_type: ChannelType,
    /// The direction the event travelled.
    pub direction: EventDirection,
    /// The human behind the event, when there is one.
    #[serde(default)]
    pub participant_id: Option<ParticipantId>,
    /// The sender's verification when the event was received (RFC §11.7).
    #[serde(default)]
    pub verification: Option<VerificationStatus>,
    /// An external system's reference.
    #[serde(default)]
    pub external_id: Option<String>,
    /// The provider or backend that carried the event.
    #[serde(default)]
    pub provider: Option<String>,
    /// The provider's original payload, preserved unmodified.
    #[serde(default)]
    pub raw_payload: Metadata,
    /// The provider's message id.
    #[serde(default)]
    pub provider_message_id: Option<String>,
}

impl EventSource {
    /// A source with no participant, provider data or verification.
    pub fn new(
        channel_id: ChannelId,
        channel_type: ChannelType,
        direction: EventDirection,
    ) -> Self {
        Self {
            channel_id,
            channel_type,
            direction,
            participant_id: None,
            verification: None,
            external_id: None,
            provider: None,
            raw_payload: Metadata::new(),
            provider_message_id: None,
        }
    }
}

/// How much of a response the people it reached took in (RFC §8.5).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsumptionState {
    /// Taken in whole.
    Consumed,
    /// Taken in part.
    Partial,
    /// Delivered, but nothing of it reached the person.
    Unconsumed,
}

/// One delivering channel's record of a response's consumption (RFC §8.5).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Consumption {
    /// The least consumed state of the channel's participants, or the
    /// channel's own when it cannot tell them apart.
    pub state: ConsumptionState,
    /// When the state was last recorded.
    pub at: Timestamp,
    /// How far, by channel: `played_ms`, `played_percentage`.
    #[serde(default)]
    pub detail: Metadata,
    /// Each participant's record; empty when the channel cannot tell them
    /// apart.
    #[serde(default)]
    pub participants: BTreeMap<ParticipantId, ParticipantConsumption>,
}

/// One participant's record of a response's consumption (RFC §8.5).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ParticipantConsumption {
    /// How much the participant took in.
    pub state: ConsumptionState,
    /// When the state was last recorded.
    pub at: Timestamp,
    /// How far.
    #[serde(default)]
    pub detail: Metadata,
}

/// A record of something that happened in a room (RFC §5.2).
///
/// Once committed, an event keeps its index and its status (RFC §4.3).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RoomEvent {
    /// The event's id.
    pub id: EventId,
    /// The room it belongs to.
    pub room_id: RoomId,
    /// What kind of event it is.
    #[serde(rename = "type")]
    pub event_type: EventType,
    /// Where it came from.
    pub source: EventSource,
    /// What it says.
    pub content: EventContent,
    /// What became of it.
    #[serde(default)]
    pub status: EventStatus,
    /// What refused it, when it is blocked.
    #[serde(default)]
    pub blocked_by: Option<String>,
    /// Who may see it; an empty value read from a store or the wire reads
    /// `none` (RFC §7.3).
    #[serde(default, deserialize_with = "super::visibility::on_event")]
    pub visibility: Visibility,
    /// The intelligence channels asked to act; `None` leaves it to the room
    /// (RFC §19.3).
    #[serde(default)]
    pub addressed_to: Option<Vec<ChannelId>>,
    /// Where the answers to it may go, at any depth of its cascade (RFC
    /// §5.2, §5.12, §8.5); `None` restricts nothing. A response carries the
    /// scope it was answered within, intersected with any its channel gave
    /// it, and a hook's modify may narrow it, never widen or clear it (RFC
    /// §9.4, gap G-073). An empty value read from a store or the wire reads
    /// `none` (RFC §7.3).
    #[serde(default, deserialize_with = "super::visibility::scope_on_event")]
    pub response_visibility: Option<Visibility>,
    /// Its position in the room's timeline, assigned at commit; an
    /// instruction's is provisional, the index the room's next event takes
    /// (RFC §10.1.1 rule 5).
    #[serde(default)]
    pub index: u64,
    /// How many response hops separate it from the external event that
    /// started its chain (RFC §8.3).
    #[serde(default)]
    pub chain_depth: u32,
    /// The root of its in-app thread, as RFC §10.1 step 8 stores it.
    #[serde(default)]
    pub parent_event_id: Option<EventId>,
    /// The event this response answers (RFC §8.5).
    #[serde(default)]
    pub responds_to: Option<EventId>,
    /// Whether each delivering channel's people took it in (RFC §8.5).
    #[serde(default)]
    pub consumption: BTreeMap<ChannelId, Consumption>,
    /// The integrator's external reference.
    #[serde(default)]
    pub correlation_id: Option<String>,
    /// The key that prevents a duplicate (RFC §13.4).
    #[serde(default)]
    pub idempotency_key: Option<String>,
    /// When it was created.
    pub created_at: Timestamp,
    /// Integrator-defined data.
    #[serde(default)]
    pub metadata: Metadata,
    /// Provider-specific structured data; unknown data is kept as it came
    /// (RFC §5.4).
    #[serde(default)]
    pub channel_data: Metadata,
    /// Per-channel delivery outcomes.
    #[serde(default)]
    pub delivery_results: Metadata,
}

impl RoomEvent {
    /// A pending event with the RFC defaults: visible to all, unaddressed,
    /// chain depth 0, index 0 until it is committed.
    pub fn new(
        id: EventId,
        room_id: RoomId,
        event_type: EventType,
        source: EventSource,
        content: EventContent,
        created_at: Timestamp,
    ) -> Self {
        Self {
            id,
            room_id,
            event_type,
            source,
            content,
            status: EventStatus::Pending,
            blocked_by: None,
            visibility: Visibility::All,
            addressed_to: None,
            response_visibility: None,
            index: 0,
            chain_depth: 0,
            parent_event_id: None,
            responds_to: None,
            consumption: BTreeMap::new(),
            correlation_id: None,
            idempotency_key: None,
            created_at,
            metadata: Metadata::new(),
            channel_data: Metadata::new(),
            delivery_results: Metadata::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    fn event() -> RoomEvent {
        RoomEvent::new(
            EventId::new("evt_1").unwrap(),
            RoomId::new("room_1").unwrap(),
            EventType::Message,
            EventSource::new(
                ChannelId::new("sms").unwrap(),
                ChannelType::SMS,
                EventDirection::Inbound,
            ),
            EventContent::text("Bonjour"),
            Timestamp::UNIX_EPOCH,
        )
    }

    #[test]
    fn an_event_serializes_its_type_under_the_rfc_field_name() {
        let json = serde_json::to_value(event()).unwrap();
        assert_eq!(json["type"], json!("message"));
    }

    #[test]
    fn a_minimal_event_deserializes_with_the_rfc_defaults() {
        let parsed: RoomEvent = serde_json::from_value(json!({
            "id": "evt_1",
            "room_id": "room_1",
            "type": "message",
            "source": { "channel_id": "sms", "channel_type": "sms", "direction": "inbound" },
            "content": { "type": "text", "body": "Bonjour" },
            "created_at": "1970-01-01T00:00:00Z",
        }))
        .unwrap();
        assert_eq!(parsed, event());
    }

    #[test]
    fn a_bidirectional_source_direction_is_refused() {
        let parsed = serde_json::from_value::<EventSource>(json!({
            "channel_id": "sms",
            "channel_type": "sms",
            "direction": "bidirectional",
        }));
        assert!(parsed.is_err());
    }

    #[test]
    fn event_types_serialize_in_snake_case() {
        assert_eq!(
            serde_json::to_value(EventType::ToolCallEnd).unwrap(),
            json!("tool_call_end")
        );
    }
}

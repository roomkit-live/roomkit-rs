//! An event the integrator injects into a room directly (RFC §10.5).

use super::{
    ChannelId, EventContent, EventId, EventType, Metadata, ParticipantId, Timestamp, Visibility,
};

/// An event injected into a room without a channel's message, from a REST
/// API or a tool call (RFC §10.5 `send_event(room_id, channel_id, content,
/// event_type, ...)`).
///
/// It takes the inbound pipeline from the room lock on, `handle_inbound()`
/// aside: its index, its hooks, its source's write permission, an edit's or
/// a delete's target, its commit, its delivery lane and its reentry passes.
/// The room is passed beside it. Its source is the registered channel
/// `channel_id`: `INBOUND` for a transport, `OUTBOUND` for an intelligence
/// channel's event, as its own words would travel (gap G-069).
#[derive(Clone, Debug, PartialEq)]
pub struct DirectEvent {
    /// The registered channel the event is from.
    pub channel_id: ChannelId,
    /// What it says.
    pub content: EventContent,
    /// Its type: `message`, `system`, `instruction` (RFC §10.1.1), `edit` or
    /// `delete` (RFC §10.3), ...
    pub event_type: EventType,
    /// The participant behind it, when there is one.
    pub participant_id: Option<ParticipantId>,
    /// Who may see it (RFC §7.3). `internal` is the framework's alone (gap
    /// G-041).
    pub visibility: Visibility,
    /// The provider or backend behind it, written on its source (RFC §5.2);
    /// `None`, the channel's own.
    pub provider: Option<String>,
    /// When it was created; `None`, the instant it is injected at.
    pub created_at: Option<Timestamp>,
    /// Where its answers may go, as a message's `response_visibility` (RFC
    /// §5.12, gap G-073): every response its cascade derives from it is
    /// kept within it; `None` restricts nothing. Never `internal` (gap
    /// G-041).
    pub response_visibility: Option<Visibility>,
    /// The intelligence channels asked to act; `None` for any eligible one
    /// (RFC §19.3).
    pub addressed_to: Option<Vec<ChannelId>>,
    /// The event it replies to, in its thread; the pipeline stores its
    /// thread's root (RFC §10.1 step 8).
    pub parent_event_id: Option<EventId>,
    /// The key that prevents a duplicate, per room and channel (RFC §13.4,
    /// gap G-023).
    pub idempotency_key: Option<String>,
    /// Extra data.
    pub metadata: Metadata,
    /// The chain it continues (RFC §8.3).
    pub chain_depth: u32,
    /// For an instruction only: its turn reads nothing of the room (RFC
    /// §10.1.1 rule 7).
    pub standalone: bool,
}

impl DirectEvent {
    /// A `message` event from `channel_id`, visible to all.
    pub fn new(channel_id: ChannelId, content: EventContent) -> Self {
        Self {
            channel_id,
            content,
            event_type: EventType::Message,
            participant_id: None,
            visibility: Visibility::All,
            provider: None,
            created_at: None,
            response_visibility: None,
            addressed_to: None,
            parent_event_id: None,
            idempotency_key: None,
            metadata: Metadata::new(),
            chain_depth: 0,
            standalone: false,
        }
    }
}

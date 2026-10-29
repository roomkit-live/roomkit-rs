//! What the host says in a room in its own name (RFC §10.5 `post()`).

use super::{ChannelId, DirectEvent, EventContent, EventType, Metadata, ParticipantId};

/// A post: the host application writing to a room's people in its own name
/// (a notice, a workflow's question, a reminder), from a host channel (RFC
/// §10.5 `post(room_id, content, participant_id, addressed_to, metadata,
/// channel_id, organization_id, idempotency_key, chain_depth)`). The room
/// and the organization are passed beside it.
///
/// It is injected as a direct event of the host channel: a party's message,
/// `INBOUND`, broadcast to the room's transports, soliciting the agents it
/// addresses only.
#[derive(Clone, Debug, PartialEq)]
pub struct Post {
    /// What it says: any content, transcoded per transport (RFC §6.6).
    pub content: EventContent,
    /// Who speaks: a participant of the room the host channel reaches, by
    /// their id or their identity's (RFC §10.5, §5.5); `None` for the host
    /// itself.
    pub participant_id: Option<ParticipantId>,
    /// The intelligence channels asked to answer; none by default: the
    /// host's words solicit no agent unless they name one (RFC §19.3).
    pub addressed_to: Vec<ChannelId>,
    /// Where it comes from: a workflow, its step, a job.
    pub metadata: Metadata,
    /// The registered host channel it speaks from; `None` for the kit's
    /// own, registered on first use under the id `host`.
    pub channel_id: Option<ChannelId>,
    /// The key that makes a retry a replay, per room and host channel (RFC
    /// §13.4); a blank key is no key.
    pub idempotency_key: Option<String>,
    /// The chain it continues (RFC §8.3), 0 opening one.
    pub chain_depth: u32,
}

impl Post {
    /// A post of `content`, from the kit's own host channel, addressing no
    /// agent, as the host itself.
    pub fn new(content: impl Into<EventContent>) -> Self {
        Self {
            content: content.into(),
            participant_id: None,
            addressed_to: Vec::new(),
            metadata: Metadata::new(),
            channel_id: None,
            idempotency_key: None,
            chain_depth: 0,
        }
    }

    /// The direct event the post injects from `channel_id`, its host
    /// channel (RFC §10.5): a message, addressed to the agents the post
    /// names, none by default.
    pub(crate) fn into_event(self, channel_id: ChannelId) -> DirectEvent {
        DirectEvent {
            event_type: EventType::Message,
            participant_id: self.participant_id,
            addressed_to: Some(self.addressed_to),
            idempotency_key: self.idempotency_key,
            metadata: self.metadata,
            chain_depth: self.chain_depth,
            ..DirectEvent::new(channel_id, self.content)
        }
    }
}

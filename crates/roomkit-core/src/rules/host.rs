//! The host's own voice (RFC §10.5): who a post may speak as.

use crate::model::{ChannelCategory, ChannelId, ChannelType, Participant, ParticipantId};

/// Whom a post's `participant_id` names, as a turn's sender is read (RFC
/// §5.5): an id names its participant only on the channels the room
/// reached them through.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Speaker {
    /// A participant of the room the host channel reaches, by their id or
    /// their identity's: the post speaks as them.
    Reached,
    /// A participant of the room reached through other channels only.
    ReachedElsewhere,
    /// Nobody of the room.
    Nobody,
}

/// Whom `participant_id` names among `participants`, for a post from the
/// host channel `channel_id` (RFC §10.5): a participant whose id, or whose
/// identity's, it is, reached through that channel (`connected_via`). An
/// id that is a participant's own, reached through other channels only,
/// names them elsewhere; an identity's id names nobody unless the host
/// channel reaches its participant, as Python's post reads it.
pub fn speaker(
    participants: &[Participant],
    participant_id: &ParticipantId,
    channel_id: &ChannelId,
) -> Speaker {
    let by_identity = |participant: &Participant| {
        participant
            .identity_id
            .as_ref()
            .is_some_and(|identity| identity.as_str() == participant_id.as_str())
    };
    let reached = participants.iter().any(|participant| {
        (&participant.id == participant_id || by_identity(participant))
            && participant.connected_via.contains(channel_id)
    });
    if reached {
        return Speaker::Reached;
    }
    if participants
        .iter()
        .any(|participant| &participant.id == participant_id)
    {
        Speaker::ReachedElsewhere
    } else {
        Speaker::Nobody
    }
}

/// Whether a channel of `channel_type` would be bound as `category` against
/// RFC §6.2 and §10.5: a host channel is bound as a transport only. The
/// attach of the lifecycle and a post's first attach both refuse it.
pub fn host_as_intelligence(channel_type: ChannelType, category: ChannelCategory) -> bool {
    channel_type == ChannelType::Host && category == ChannelCategory::Intelligence
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::model::{IdentityId, RoomId, Timestamp};

    fn id(id: &str) -> ParticipantId {
        ParticipantId::new(id).unwrap()
    }

    fn channel(id: &str) -> ChannelId {
        ChannelId::new(id).unwrap()
    }

    fn on(participant: &str, channel_id: &str) -> Participant {
        Participant::new(
            id(participant),
            RoomId::new("r").unwrap(),
            channel(channel_id),
            Timestamp::UNIX_EPOCH,
        )
    }

    /// RFC §10.5, §5.5: a member added on the host channel, by their id or
    /// their identity's; an SMS member added on it too; one reached through
    /// SMS only, by their id, elsewhere, by their identity's, nobody; nobody.
    #[test]
    fn a_speaker_is_a_participant_the_host_channel_reaches() {
        let mut desk = on("desk", "host");
        desk.identity_id = Some(IdentityId::new("id-desk").unwrap());
        let mut both = on("+15550100001", "sms");
        both.connected_via.push(channel("host"));
        let mut sms_only = on("+15550100002", "sms");
        sms_only.identity_id = Some(IdentityId::new("id-marie").unwrap());
        let room = [desk, both, sms_only];
        let read = [
            "desk",
            "id-desk",
            "+15550100001",
            "+15550100002",
            "id-marie",
            "ghost",
        ]
        .map(|named| speaker(&room, &id(named), &channel("host")));
        assert_eq!(
            read,
            [
                Speaker::Reached,
                Speaker::Reached,
                Speaker::Reached,
                Speaker::ReachedElsewhere,
                Speaker::Nobody,
                Speaker::Nobody
            ]
        );
    }
}

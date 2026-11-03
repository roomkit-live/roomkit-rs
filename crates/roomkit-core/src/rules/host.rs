//! The host's own voice (RFC §10.5): who a post may speak as.

use crate::model::{ChannelCategory, ChannelId, ChannelType, Participant, ParticipantId};
use crate::rules::authors;

/// Whom a post's `participant_id` names, as a turn's sender is read (RFC
/// §5.5): an id names its participant only on the channels the room
/// reached them through.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Speaker {
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
/// identity's, it is, reached through that channel (`connected_via`), or
/// the sender's own record the id names there, as Python's post reads it
/// through `People.named_on`. An id that is a participant's own, reached
/// through other channels only, names them elsewhere; an identity's id
/// names nobody unless the host channel reaches its participant.
pub(crate) fn speaker(
    participants: &[Participant],
    participant_id: &ParticipantId,
    channel_id: &ChannelId,
) -> Speaker {
    if authors::named_on(participants, participant_id.as_str(), channel_id.as_str()).is_some() {
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
pub(crate) fn host_as_intelligence(channel_type: &ChannelType, category: ChannelCategory) -> bool {
    channel_type == &ChannelType::HOST && category == ChannelCategory::Intelligence
}

/// Whether a channel declared of `channel_type` claims the type reserved
/// for the framework's own records, `system`, which no channel may claim:
/// the application's words are never a `SYSTEM` source (RFC §5.3). Its
/// registration is refused. A channel of type `host` is registered whatever
/// its category and direction, as the Python implementation registers it
/// (step 5, point 4): attached as an intelligence channel, it is refused
/// ([`host_as_intelligence`]).
pub(crate) fn claims_reserved_type(channel_type: &ChannelType) -> bool {
    channel_type == &ChannelType::SYSTEM
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
        Participant::at(
            id(participant),
            RoomId::new("r").unwrap(),
            channel(channel_id),
            Timestamp::UNIX_EPOCH,
        )
    }

    /// RFC §10.5, §5.5: a member added on the host channel, by their id or
    /// their identity's; an SMS member added on it too; one reached through
    /// SMS only, by their id, elsewhere, by their identity's, nobody; the
    /// own record an id names on the host channel; nobody.
    #[test]
    fn a_speaker_is_a_participant_the_host_channel_reaches() {
        let mut desk = on("desk", "host");
        desk.identity_id = Some(IdentityId::new("id-desk").unwrap());
        let mut both = on("+15550100001", "sms");
        both.connected_via.push(channel("host"));
        let mut sms_only = on("+15550100002", "sms");
        sms_only.identity_id = Some(IdentityId::new("id-marie").unwrap());
        // The own record the id "kiosk" names on the host channel (RFC
        // §11.2), as Python's People.named_on reads it (RMK-1163).
        let own = on(&authors::own_record_id("host", "kiosk"), "host");
        let room = [desk, both, sms_only, own];
        let read = [
            "desk",
            "id-desk",
            "+15550100001",
            "+15550100002",
            "id-marie",
            "kiosk",
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
                Speaker::Reached,
                Speaker::Nobody
            ]
        );
    }
}

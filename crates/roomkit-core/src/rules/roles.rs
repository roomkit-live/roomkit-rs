//! Internal bridge for selecting the waking participant's role (RFC §21.1).

use super::authors::named_record;
use crate::model::{Participant, ParticipantRole, RoomEvent};

/// The role of the record the event names on its source channel, without
/// replacing it by the canonical record of its identity (RFC §5.5, §21.1).
/// A missing or unreachable sender has no role. The returned spelling is
/// the participant model's serialized role, used as a policy override key.
pub fn participant_role(event: &RoomEvent, participants: &[Participant]) -> Option<&'static str> {
    named_record(event, participants).map(|record| match record.role {
        ParticipantRole::Owner => "owner",
        ParticipantRole::Agent => "agent",
        ParticipantRole::Member => "member",
        ParticipantRole::Observer => "observer",
        ParticipantRole::Bot => "bot",
    })
}

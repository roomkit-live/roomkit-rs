//! The people taking part in a room (RFC §4.4, §5.5).

use serde::{Deserialize, Serialize};

use super::{ChannelId, IdentityId, Metadata, ParticipantId, RoomId, Timestamp};

/// A participant's role in a room (RFC §5.5).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ParticipantRole {
    /// The room's creator or primary responsible party.
    Owner,
    /// A human agent: an advisor, a support representative.
    Agent,
    /// A regular participant.
    #[default]
    Member,
    /// A read-only participant: a supervisor, an auditor.
    Observer,
    /// An automated system participant.
    Bot,
}

/// Whether a participant is still taking part (RFC §5.5).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ParticipantStatus {
    /// Currently participating.
    #[default]
    Active,
    /// No recent activity.
    Inactive,
    /// Explicitly left the room.
    Left,
    /// Removed and blocked.
    Banned,
}

/// Where a participant's identity resolution stands (RFC §5.5).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum IdentificationStatus {
    /// Identity confirmed: the identity id is set.
    Identified,
    /// Awaiting resolution, possibly with candidates.
    #[default]
    Pending,
    /// Several candidates found; needs disambiguation.
    Ambiguous,
    /// No matching identity.
    Unknown,
    /// An identification challenge was sent; not a verification.
    ChallengeSent,
    /// The identity challenge failed or was rejected.
    Rejected,
}

/// Whether an identified sender has proven who they are (RFC §5.5, §11.7).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum VerificationStatus {
    /// Identified, not proven.
    Unverified,
    /// A verification request is open.
    Pending,
    /// Proven, until it expires.
    Verified,
    /// Was verified; its time ran out.
    Expired,
    /// Ended by the integrator before its time.
    Revoked,
}

/// A human, or a system identity, in a room (RFC §5.5).
///
/// AI and integration channels are not participants: they are channels.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Participant {
    /// The participant's id, unique within its room.
    pub id: ParticipantId,
    /// The room.
    pub room_id: RoomId,
    /// The primary channel the participant joined through.
    pub channel_id: ChannelId,
    /// The human-readable name.
    #[serde(default)]
    pub display_name: Option<String>,
    /// The role in the room.
    #[serde(default)]
    pub role: ParticipantRole,
    /// Whether the participant is still taking part.
    #[serde(default)]
    pub status: ParticipantStatus,
    /// Where identity resolution stands.
    #[serde(default)]
    pub identification: IdentificationStatus,
    /// The resolved identity.
    #[serde(default)]
    pub identity_id: Option<IdentityId>,
    /// The candidate identities, while ambiguous.
    #[serde(default)]
    pub candidates: Option<Vec<IdentityId>>,
    /// Every channel the room reached the participant through, the primary
    /// one included.
    #[serde(default)]
    pub connected_via: Vec<ChannelId>,
    /// The integrator's external reference.
    #[serde(default)]
    pub external_id: Option<String>,
    /// When the identity was confirmed.
    #[serde(default)]
    pub resolved_at: Option<Timestamp>,
    /// What confirmed it: a hook name, `auto`, `manual`.
    #[serde(default)]
    pub resolved_by: Option<String>,
    /// When the participant entered the room.
    pub joined_at: Timestamp,
    /// Integrator-defined data.
    #[serde(default)]
    pub metadata: Metadata,
}

impl Participant {
    /// An active member joining through `channel_id`, identity pending.
    pub fn new(
        id: ParticipantId,
        room_id: RoomId,
        channel_id: ChannelId,
        joined_at: Timestamp,
    ) -> Self {
        Self {
            id,
            room_id,
            connected_via: vec![channel_id.clone()],
            channel_id,
            display_name: None,
            role: ParticipantRole::default(),
            status: ParticipantStatus::default(),
            identification: IdentificationStatus::default(),
            identity_id: None,
            candidates: None,
            external_id: None,
            resolved_at: None,
            resolved_by: None,
            joined_at,
            metadata: Metadata::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    #[test]
    fn a_new_participant_is_connected_via_its_primary_channel() {
        let sms = ChannelId::new("sms").unwrap();
        let participant = Participant::new(
            ParticipantId::new("p_jean").unwrap(),
            RoomId::new("room_1").unwrap(),
            sms.clone(),
            Timestamp::UNIX_EPOCH,
        );
        assert_eq!(participant.connected_via, vec![sms]);
    }

    #[test]
    fn identification_statuses_serialize_in_snake_case() {
        assert_eq!(
            serde_json::to_value(IdentificationStatus::ChallengeSent).unwrap(),
            json!("challenge_sent")
        );
    }
}

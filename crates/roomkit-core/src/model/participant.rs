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
/// AI and integration channels are not participants: they are channels. As
/// in the Python implementation, a participant read lists its primary
/// channel first in `connected_via`, once.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "ParticipantWire")]
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
    /// An active member joining through `channel_id` now, identity
    /// pending, as the Python implementation's model defaults fill
    /// `joined_at`. A caller sets the other fields by struct update.
    #[cfg(feature = "system-environment")]
    pub fn new(id: ParticipantId, room_id: RoomId, channel_id: ChannelId) -> Self {
        use crate::ops::{Environment, SystemEnvironment};
        Self::at(id, room_id, channel_id, SystemEnvironment.now())
    }

    /// [`Participant::new`] joining at `joined_at`: what the core's
    /// operations build. Outside the public API.
    #[doc(hidden)]
    pub fn at(
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

/// A participant as it is read: [`Participant`]'s fields, before its
/// primary channel is put first in `connected_via`.
#[derive(Deserialize)]
struct ParticipantWire {
    id: ParticipantId,
    room_id: RoomId,
    channel_id: ChannelId,
    #[serde(default)]
    display_name: Option<String>,
    #[serde(default)]
    role: ParticipantRole,
    #[serde(default)]
    status: ParticipantStatus,
    #[serde(default)]
    identification: IdentificationStatus,
    #[serde(default)]
    identity_id: Option<IdentityId>,
    #[serde(default)]
    candidates: Option<Vec<IdentityId>>,
    #[serde(default)]
    connected_via: Vec<ChannelId>,
    #[serde(default)]
    external_id: Option<String>,
    #[serde(default)]
    resolved_at: Option<Timestamp>,
    #[serde(default)]
    resolved_by: Option<String>,
    joined_at: Timestamp,
    #[serde(default)]
    metadata: Metadata,
}

impl From<ParticipantWire> for Participant {
    fn from(wire: ParticipantWire) -> Self {
        // Python's `_include_primary_channel`: the primary channel first,
        // each channel once, in the order of first sight.
        let mut connected_via = vec![wire.channel_id.clone()];
        for channel_id in wire.connected_via {
            if !connected_via.contains(&channel_id) {
                connected_via.push(channel_id);
            }
        }
        Self {
            id: wire.id,
            room_id: wire.room_id,
            channel_id: wire.channel_id,
            display_name: wire.display_name,
            role: wire.role,
            status: wire.status,
            identification: wire.identification,
            identity_id: wire.identity_id,
            candidates: wire.candidates,
            connected_via,
            external_id: wire.external_id,
            resolved_at: wire.resolved_at,
            resolved_by: wire.resolved_by,
            joined_at: wire.joined_at,
            metadata: wire.metadata,
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
        let participant = Participant::at(
            ParticipantId::new("p_jean").unwrap(),
            RoomId::new("room_1").unwrap(),
            sms.clone(),
            Timestamp::UNIX_EPOCH,
        );
        assert_eq!(participant.connected_via, vec![sms]);
    }

    /// Python's `_include_primary_channel`: a participant read lists its
    /// primary channel first, each channel once.
    #[test]
    fn a_participant_read_lists_its_primary_channel_first_once() {
        let read: Participant = serde_json::from_value(json!({
            "id": "p_jean",
            "room_id": "room_1",
            "channel_id": "sms",
            "connected_via": ["ws", "sms", "ws", "email"],
            "joined_at": "1970-01-01T00:00:00Z",
        }))
        .unwrap();
        assert_eq!(
            read.connected_via,
            ["sms", "ws", "email"].map(|id| ChannelId::new(id).unwrap())
        );
    }

    #[test]
    fn identification_statuses_serialize_in_snake_case() {
        assert_eq!(
            serde_json::to_value(IdentificationStatus::ChallengeSent).unwrap(),
            json!("challenge_sent")
        );
    }
}

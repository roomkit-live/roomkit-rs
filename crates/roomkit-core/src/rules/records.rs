//! The lifecycle records the framework writes in a room's timeline (RFC §5.1
//! Lifecycle records): what each one says of the change it marks.
//!
//! A channel's attach, detach, mute, unmute and binding update, and a
//! timer's transition, each write one record, the framework's: a `SystemContent`
//! whose `code` names the change and whose `data` holds what it changed. The
//! records follow the Python implementation's form field for field (its
//! `channel_ops` and timer transitions), so that the two timelines compare
//! (gap G-046). The integrator's own transitions of a room (pause, resume,
//! reopen, close, archive) write none, nor does a room's creation.

use serde_json::Value;

use crate::model::{
    Access, ChannelBinding, ChannelId, EventType, IdentityId, Metadata, ParticipantId,
    ParticipantStatus, RoomId, SystemContent, Timestamp, Visibility,
};
use crate::rules::lifecycle::Transition;
use crate::rules::python_repr;

/// A change a lifecycle record marks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Record {
    /// A channel attached, by the integrator or by the framework (RFC §10.1
    /// step 2, §10.5), with the binding's access and visibility.
    Attached {
        channel_id: ChannelId,
        access: Access,
        visibility: Visibility,
    },
    /// A channel detached (RFC §7.5 rule 7).
    Detached(ChannelId),
    /// A channel muted (RFC §7.2).
    Muted(ChannelId),
    /// A channel unmuted (RFC §7.2).
    Unmuted(ChannelId),
    /// A binding's access changed.
    AccessChanged {
        channel_id: ChannelId,
        old: Access,
        new: Access,
    },
    /// A binding's visibility changed (RFC §7.3).
    VisibilityChanged {
        channel_id: ChannelId,
        old: Visibility,
        new: Visibility,
    },
    /// A binding's metadata changed: the keys the update named, in the
    /// order it named them.
    MetadataUpdated {
        channel_id: ChannelId,
        keys: Vec<String>,
    },
    /// A timer's transition of the room, the timer's threshold in seconds
    /// and the instant the transition fell due (RFC §5.1 When).
    Timer {
        room_id: RoomId,
        transition: Transition,
        threshold: u64,
        at: Timestamp,
    },
    /// A sender identity resolution identified joined the room (RFC §10.1
    /// step 5, §11.2), the Python implementation's
    /// `participant_joined_identified`.
    JoinedIdentified(ParticipantId),
    /// A sender identity resolution left pending joined the room, the
    /// Python implementation's `participant_joined_pending`.
    JoinedPending(ParticipantId),
    /// `resolve_participant` identified a pending participant as an
    /// identity, of that display name (RFC §5.5, Appendix B.3).
    ParticipantIdentified {
        participant_id: ParticipantId,
        display_name: Option<String>,
        identity_id: IdentityId,
        resolved_by: String,
    },
    /// `add_member` joined a member, under the identity it named (RFC §5.5).
    MemberJoined {
        participant_id: ParticipantId,
        identity_id: Option<IdentityId>,
    },
    /// `remove_member` set a member's status (RFC §5.5).
    MemberLeft {
        participant_id: ParticipantId,
        status: ParticipantStatus,
    },
    /// `rename_member` changed a member's display name (RFC §5.5).
    MemberRenamed {
        participant_id: ParticipantId,
        display_name: String,
    },
}

impl Record {
    /// The record of `binding` attached.
    pub(crate) fn attached(binding: &ChannelBinding) -> Self {
        Self::Attached {
            channel_id: binding.channel_id.clone(),
            access: binding.access,
            visibility: binding.visibility.clone(),
        }
    }

    /// The records of a binding update that took `old` to `new`, one per
    /// field it changed, in the order mute, access, visibility, metadata;
    /// `keys` are the metadata keys the update named. A change of the
    /// binding's correspondent writes none.
    pub(crate) fn of_update(
        old: &ChannelBinding,
        new: &ChannelBinding,
        keys: &Metadata,
    ) -> Vec<Self> {
        let channel_id = &new.channel_id;
        let mut records = Vec::new();
        if old.muted != new.muted {
            records.push(if new.muted {
                Self::Muted(channel_id.clone())
            } else {
                Self::Unmuted(channel_id.clone())
            });
        }
        if old.access != new.access {
            records.push(Self::AccessChanged {
                channel_id: channel_id.clone(),
                old: old.access,
                new: new.access,
            });
        }
        if old.visibility != new.visibility {
            records.push(Self::VisibilityChanged {
                channel_id: channel_id.clone(),
                old: old.visibility.clone(),
                new: new.visibility.clone(),
            });
        }
        if old.metadata != new.metadata {
            records.push(Self::MetadataUpdated {
                channel_id: channel_id.clone(),
                keys: keys.keys().cloned().collect(),
            });
        }
        records
    }

    /// The event type the record takes (RFC §5.2): the lifecycle type that
    /// names the change, `SYSTEM` for a timer's transition, which none
    /// names.
    pub(crate) fn event_type(&self) -> EventType {
        match self {
            Self::Attached { .. } => EventType::ChannelAttached,
            Self::Detached(_) => EventType::ChannelDetached,
            Self::Muted(_) => EventType::ChannelMuted,
            Self::Unmuted(_) => EventType::ChannelUnmuted,
            Self::AccessChanged { .. }
            | Self::VisibilityChanged { .. }
            | Self::MetadataUpdated { .. } => EventType::ChannelUpdated,
            Self::Timer { .. } => EventType::System,
            Self::JoinedIdentified(_) | Self::JoinedPending(_) | Self::MemberJoined { .. } => {
                EventType::ParticipantJoined
            }
            Self::ParticipantIdentified { .. } => EventType::ParticipantIdentified,
            Self::MemberLeft { .. } => EventType::ParticipantLeft,
            Self::MemberRenamed { .. } => EventType::ParticipantUpdated,
        }
    }

    /// Whether the record is written in a room whose status refuses new
    /// events (RFC §5.1): only a timer's, the record of the transition
    /// itself, which a close leaves the room CLOSED by.
    pub(crate) fn records_transition(&self) -> bool {
        matches!(self, Self::Timer { .. })
    }

    /// What the record says: a body for a person, a code naming the change,
    /// and its data.
    pub(crate) fn content(&self) -> SystemContent {
        let Parts { body, code, data } = match self {
            Self::Attached {
                channel_id,
                access,
                visibility,
            } => Parts {
                body: format!(
                    "Channel {channel_id} attached ({}, {visibility})",
                    access_name(*access)
                ),
                code: "channel_attached",
                data: vec![
                    ("channel_id", channel_id.as_str().into()),
                    ("access", access_name(*access).into()),
                    ("visibility", visibility.to_string().into()),
                ],
            },
            Self::Detached(channel_id) => {
                Parts::of_channel(channel_id, "detached", "channel_detached")
            }
            Self::Muted(channel_id) => Parts::of_channel(channel_id, "muted", "channel_muted"),
            Self::Unmuted(channel_id) => {
                Parts::of_channel(channel_id, "unmuted", "channel_unmuted")
            }
            Self::AccessChanged {
                channel_id,
                old,
                new,
            } => Parts::of_change(channel_id, "access", access_name(*old), access_name(*new)),
            Self::VisibilityChanged {
                channel_id,
                old,
                new,
            } => Parts::of_change(channel_id, "visibility", &old.to_string(), &new.to_string()),
            Self::MetadataUpdated { channel_id, keys } => Parts {
                body: format!("Channel {channel_id} metadata updated"),
                code: "channel_metadata_updated",
                data: vec![
                    ("channel_id", channel_id.as_str().into()),
                    ("keys", keys.clone().into()),
                ],
            },
            Self::Timer {
                room_id,
                transition,
                threshold,
                at,
            } => Parts::of_timer(room_id, *transition, *threshold, *at),
            Self::JoinedIdentified(participant_id) => Parts::of_join(participant_id, "identified"),
            Self::JoinedPending(participant_id) => Parts::of_join(participant_id, "pending"),
            Self::ParticipantIdentified {
                participant_id,
                display_name,
                identity_id,
                resolved_by,
            } => Parts::of_identified(
                participant_id,
                display_name.as_deref(),
                identity_id,
                resolved_by,
            ),
            Self::MemberJoined {
                participant_id,
                identity_id,
            } => Parts::of_member_joined(participant_id, identity_id.as_ref()),
            Self::MemberLeft {
                participant_id,
                status,
            } => Parts::of_member_left(participant_id, *status),
            Self::MemberRenamed {
                participant_id,
                display_name,
            } => Parts::of_member_renamed(participant_id, display_name),
        };
        SystemContent {
            body,
            code: Some(code.to_owned()),
            data: data
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value))
                .collect(),
        }
    }
}

/// A record's content, before it is written.
struct Parts {
    body: String,
    code: &'static str,
    data: Vec<(&'static str, Value)>,
}

impl Parts {
    /// A change of a channel that names the channel alone, its detach,
    /// mute or unmute: `verb` in the body, `code` naming it.
    fn of_channel(channel_id: &ChannelId, verb: &str, code: &'static str) -> Self {
        Self {
            body: format!("Channel {channel_id} {verb}"),
            code,
            data: vec![("channel_id", channel_id.as_str().into())],
        }
    }

    /// A change of a binding's `field`, `access` or `visibility`, from
    /// `old` to `new`.
    fn of_change(channel_id: &ChannelId, field: &'static str, old: &str, new: &str) -> Self {
        let (code, old_key) = if field == "access" {
            ("channel_access_changed", "old_access")
        } else {
            ("channel_visibility_changed", "old_visibility")
        };
        Self {
            body: format!("Channel {channel_id} {field}: {old} -> {new}"),
            code,
            data: vec![
                ("channel_id", channel_id.as_str().into()),
                (old_key, old.into()),
                (field, new.into()),
            ],
        }
    }

    /// A sender joining the room as identity resolution left them,
    /// `identified` or `pending` (RFC §10.1 step 5).
    fn of_join(participant_id: &ParticipantId, status: &'static str) -> Self {
        let (how, code) = if status == "identified" {
            ("as identified", "participant_joined_identified")
        } else {
            ("with pending identification", "participant_joined_pending")
        };
        Self {
            body: format!("Participant {participant_id} joined {how}"),
            code,
            data: vec![
                ("participant_id", participant_id.as_str().into()),
                ("status", status.into()),
            ],
        }
    }

    /// `resolve_participant`'s identification of a pending participant
    /// (RFC §5.5).
    fn of_identified(
        participant_id: &ParticipantId,
        display_name: Option<&str>,
        identity_id: &IdentityId,
        resolved_by: &str,
    ) -> Self {
        Self {
            body: format!(
                "Participant {participant_id} identified as {}",
                display_name.unwrap_or("None")
            ),
            code: "participant_identified",
            data: vec![
                ("participant_id", participant_id.as_str().into()),
                ("identity_id", identity_id.as_str().into()),
                ("resolved_by", resolved_by.into()),
            ],
        }
    }

    /// `add_member`'s join, under the identity it named (RFC §5.5).
    fn of_member_joined(participant_id: &ParticipantId, identity_id: Option<&IdentityId>) -> Self {
        Self {
            body: format!("Participant {participant_id} joined"),
            code: "participant_joined",
            data: vec![
                ("participant_id", participant_id.as_str().into()),
                (
                    "identity_id",
                    identity_id.map_or(Value::Null, |id| id.as_str().into()),
                ),
            ],
        }
    }

    /// `remove_member`'s leave, with the status it set (RFC §5.5).
    fn of_member_left(participant_id: &ParticipantId, status: ParticipantStatus) -> Self {
        Self {
            body: format!("Participant {participant_id} left"),
            code: "participant_left",
            data: vec![
                ("participant_id", participant_id.as_str().into()),
                ("status", participant_status_name(status).into()),
            ],
        }
    }

    /// `rename_member`'s new display name (RFC §5.5), quoted as Python's
    /// `{display_name!r}`.
    fn of_member_renamed(participant_id: &ParticipantId, display_name: &str) -> Self {
        Self {
            body: format!(
                "Participant {participant_id} is now displayed as {}",
                python_repr(display_name)
            ),
            code: "participant_updated",
            data: vec![
                ("participant_id", participant_id.as_str().into()),
                ("display_name", display_name.into()),
            ],
        }
    }

    /// A timer's transition: a timer pauses or closes a room, nothing else.
    fn of_timer(room_id: &RoomId, transition: Transition, threshold: u64, at: Timestamp) -> Self {
        let (verb, code) = match transition {
            Transition::Pause => ("paused", "room_paused_by_timer"),
            Transition::Resume | Transition::Reopen | Transition::Close | Transition::Archive => {
                ("closed", "room_closed_by_timer")
            }
        };
        Self {
            body: format!("Room {room_id} {verb} by timer"),
            code,
            data: vec![
                ("threshold", threshold.into()),
                ("at", at.to_iso_offset().into()),
            ],
        }
    }
}

/// The name of `access` in the serialized form (`read_write`).
fn access_name(access: Access) -> &'static str {
    match access {
        Access::ReadWrite => "read_write",
        Access::ReadOnly => "read_only",
        Access::WriteOnly => "write_only",
        Access::None => "none",
    }
}

/// A participant status as the serialized form writes it.
fn participant_status_name(status: ParticipantStatus) -> &'static str {
    match status {
        ParticipantStatus::Active => "active",
        ParticipantStatus::Inactive => "inactive",
        ParticipantStatus::Left => "left",
        ParticipantStatus::Banned => "banned",
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    fn channel(id: &str) -> ChannelId {
        ChannelId::new(id).unwrap()
    }

    /// The content as the serialized form writes it.
    fn written(record: &Record) -> Value {
        serde_json::to_value(record.content()).unwrap()
    }

    /// The Python implementation's records, read back from its timeline
    /// (roomkit 5635662c): `attach_channel`, `set_access`, `set_visibility`,
    /// `update_binding_metadata`.
    #[test]
    fn a_record_s_content_is_the_python_implementation_s() {
        let records = [
            Record::Attached {
                channel_id: channel("sms"),
                access: Access::ReadWrite,
                visibility: Visibility::All,
            },
            Record::AccessChanged {
                channel_id: channel("ws-a"),
                old: Access::ReadWrite,
                new: Access::ReadOnly,
            },
            Record::VisibilityChanged {
                channel_id: channel("ws-a"),
                old: Visibility::All,
                new: Visibility::Transport,
            },
            Record::MetadataUpdated {
                channel_id: channel("ws-a"),
                keys: vec!["k".to_owned()],
            },
        ];
        assert_eq!(
            records.iter().map(written).collect::<Vec<_>>(),
            vec![
                json!({
                    "body": "Channel sms attached (read_write, all)",
                    "code": "channel_attached",
                    "data": { "channel_id": "sms", "access": "read_write", "visibility": "all" },
                }),
                json!({
                    "body": "Channel ws-a access: read_write -> read_only",
                    "code": "channel_access_changed",
                    "data": { "channel_id": "ws-a", "old_access": "read_write", "access": "read_only" },
                }),
                json!({
                    "body": "Channel ws-a visibility: all -> transport",
                    "code": "channel_visibility_changed",
                    "data": { "channel_id": "ws-a", "old_visibility": "all", "visibility": "transport" },
                }),
                json!({
                    "body": "Channel ws-a metadata updated",
                    "code": "channel_metadata_updated",
                    "data": { "channel_id": "ws-a", "keys": ["k"] },
                }),
            ]
        );
    }

    /// A metadata record names the keys in the order the update gave them,
    /// as Python's `list(metadata.keys())` (rfc-gaps G-046, RMK-1198).
    #[test]
    fn a_metadata_record_names_the_keys_in_the_update_s_order() {
        let old = ChannelBinding::at(
            channel("ws-a"),
            RoomId::new("r1").unwrap(),
            crate::model::ChannelType::WEBSOCKET,
            crate::model::ChannelCategory::default(),
            crate::model::ChannelDirection::default(),
            Timestamp::from_unix_millis(0).unwrap(),
        );
        let mut update = Metadata::new();
        update.insert("zone".to_owned(), json!("eu"));
        update.insert("alpha".to_owned(), json!(1));
        let mut new = old.clone();
        new.metadata.clone_from(&update);
        let keys: Vec<Value> = Record::of_update(&old, &new, &update)
            .iter()
            .map(|record| written(record)["data"]["keys"].clone())
            .collect();
        assert_eq!(keys, [json!(["zone", "alpha"])]);
    }

    /// A timer's record names the threshold and the instant it fell due, in
    /// the form Python's `isoformat` writes.
    #[test]
    fn a_timer_s_record_names_its_threshold_and_its_instant() {
        let record = Record::Timer {
            room_id: RoomId::new("room_1").unwrap(),
            transition: Transition::Pause,
            threshold: 60,
            at: Timestamp::from_unix_millis(1_792_677_840_000).unwrap(),
        };
        assert_eq!(
            written(&record),
            json!({
                "body": "Room room_1 paused by timer",
                "code": "room_paused_by_timer",
                "data": { "threshold": 60, "at": "2026-10-22T14:04:00+00:00" },
            })
        );
    }
}

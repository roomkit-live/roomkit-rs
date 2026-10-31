//! The payload of a lifecycle hook (RFC §9.2, §26.1): the system event the
//! framework writes for a change of a room or of a channel's binding.
//!
//! The framework writes it from a [`LifecyclePayload`], and a hook reads it
//! back with [`LifecyclePayload::of`]: its `code` names the change, its
//! `data` the room, the channel a channel's change is about, the type of
//! the channel a session started on, and the reason of a transition its
//! timer made. The one type writes and reads the shape, so no hook digs
//! the fields out of the content's JSON `data`.

use serde_json::Value;

use crate::model::{ChannelId, ChannelType, EventContent, Metadata, RoomEvent, RoomId};
use crate::ops::TransitionReason;

/// The `data` key naming the channel a channel's change is about.
const CHANNEL_ID: &str = "channel_id";
/// The `data` key naming the type of the channel a session started on.
const CHANNEL_TYPE: &str = "channel_type";

/// A change a lifecycle hook's payload reports, which its `code` names (RFC
/// §9.2, §26.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum LifecycleChange {
    /// `room_created`, the payload of `ON_ROOM_CREATED`.
    RoomCreated,
    /// `room_paused`, the payload of `ON_ROOM_PAUSED`.
    RoomPaused,
    /// `room_closed`, the payload of `ON_ROOM_CLOSED`.
    RoomClosed,
    /// `room_archived`: an archive fires no hook (RFC §26.1).
    RoomArchived,
    /// `channel_attached`, the payload of `ON_CHANNEL_ATTACHED`.
    ChannelAttached,
    /// `channel_detached`, the payload of `ON_CHANNEL_DETACHED`.
    ChannelDetached,
    /// `channel_muted`, the payload of `ON_CHANNEL_MUTED`.
    ChannelMuted,
    /// `channel_unmuted`, the payload of `ON_CHANNEL_UNMUTED`.
    ChannelUnmuted,
    /// `session_started`, the payload of `ON_SESSION_STARTED`: the inbound
    /// pipeline created a room for a message on the channel (RFC §10.1
    /// step 2).
    SessionStarted,
}

impl LifecycleChange {
    /// Every change, each once.
    const ALL: [Self; 9] = [
        Self::RoomCreated,
        Self::RoomPaused,
        Self::RoomClosed,
        Self::RoomArchived,
        Self::ChannelAttached,
        Self::ChannelDetached,
        Self::ChannelMuted,
        Self::ChannelUnmuted,
        Self::SessionStarted,
    ];

    /// The payload's `code` for the change.
    pub fn code(self) -> &'static str {
        match self {
            Self::RoomCreated => "room_created",
            Self::RoomPaused => "room_paused",
            Self::RoomClosed => "room_closed",
            Self::RoomArchived => "room_archived",
            Self::ChannelAttached => "channel_attached",
            Self::ChannelDetached => "channel_detached",
            Self::ChannelMuted => "channel_muted",
            Self::ChannelUnmuted => "channel_unmuted",
            Self::SessionStarted => "session_started",
        }
    }

    /// The change `code` names, if it names one.
    pub fn from_code(code: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|change| change.code() == code)
    }
}

/// What a lifecycle hook's payload says of its change (RFC §9.2, §26.1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LifecyclePayload {
    /// The change.
    pub change: LifecycleChange,
    /// The room it changed.
    pub room_id: RoomId,
    /// The channel the change is about: the one attached, detached, muted
    /// or unmuted, or the one a session started on; `None` for a change of
    /// the room alone.
    pub channel_id: Option<ChannelId>,
    /// The type of the channel a session started on; `None` for any other
    /// change.
    pub channel_type: Option<ChannelType>,
    /// What made the room transition on its own: `timer` for a pause or a
    /// close its timer made (RFC §5.1); `None` for the integrator's, and for
    /// any other change.
    pub reason: Option<TransitionReason>,
}

impl LifecyclePayload {
    /// What `event` says of its change, when it is a lifecycle hook's
    /// payload: an event from the framework's `system` source whose system
    /// content's code names a change, `_by_timer` after it for a transition
    /// the room's timer made, as Python's `_fire_lifecycle_hook` writes it.
    /// `None` for any other event, a system notice a channel or a hook
    /// writes among them.
    pub fn of(event: &RoomEvent) -> Option<Self> {
        let EventContent::System(content) = &event.content else {
            return None;
        };
        if event.source.channel_id != ChannelId::system() {
            return None;
        }
        let code = content.code.as_deref()?;
        let (change, reason) = match code.strip_suffix("_by_timer") {
            Some(change) => (
                LifecycleChange::from_code(change)?,
                Some(TransitionReason::Timer),
            ),
            None => (LifecycleChange::from_code(code)?, None),
        };
        let data = &content.data;
        Some(Self {
            change,
            room_id: event.room_id.clone(),
            channel_id: field(data, CHANNEL_ID, |id| ChannelId::new(id.as_str()?).ok()).ok()?,
            channel_type: field(data, CHANNEL_TYPE, from_value).ok()?,
            reason,
        })
    }
}

/// A `data` value that does not read as its key's: no payload of the
/// framework's holds one.
#[derive(Debug)]
struct Unreadable;

/// The value `data` holds under `key`, read by `read`; `None` when it holds
/// none.
fn field<T>(
    data: &Metadata,
    key: &str,
    read: impl Fn(&Value) -> Option<T>,
) -> Result<Option<T>, Unreadable> {
    data.get(key)
        .map(|value| read(value).ok_or(Unreadable))
        .transpose()
}

/// The enumeration value `value` writes, if it writes one.
fn from_value<T: serde::de::DeserializeOwned>(value: &Value) -> Option<T> {
    T::deserialize(value).ok()
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;
    use crate::model::{EventId, EventSource, EventType, SystemContent, Timestamp};

    fn room_id() -> RoomId {
        RoomId::new("room_1").unwrap()
    }

    /// A system event from `source` carrying `code` and `data`.
    fn event(source: &str, code: &str, data: Value) -> RoomEvent {
        let content = SystemContent {
            code: Some(code.to_owned()),
            data: if let Value::Object(members) = data {
                members
            } else {
                Metadata::new()
            },
            ..SystemContent::new("notice")
        };
        RoomEvent::at(
            EventId::new("evt_1").unwrap(),
            room_id(),
            EventType::System,
            EventSource::new(ChannelId::new(source).unwrap(), ChannelType::SYSTEM),
            EventContent::System(content),
            Timestamp::UNIX_EPOCH,
        )
    }

    /// What the framework writes, read back: a timer's close, a session's
    /// start.
    #[test]
    fn reads_what_the_framework_writes() {
        let closed = event("system", "room_closed_by_timer", json!({ "threshold": 60 }));
        let started = event(
            "system",
            "session_started",
            json!({ "room_id": "room_1", "channel_id": "sms", "channel_type": "sms" }),
        );
        assert_eq!(
            [
                LifecyclePayload::of(&closed),
                LifecyclePayload::of(&started)
            ],
            [
                Some(LifecyclePayload {
                    change: LifecycleChange::RoomClosed,
                    room_id: room_id(),
                    channel_id: None,
                    channel_type: None,
                    reason: Some(TransitionReason::Timer),
                }),
                Some(LifecyclePayload {
                    change: LifecycleChange::SessionStarted,
                    room_id: room_id(),
                    channel_id: Some(ChannelId::new("sms").unwrap()),
                    channel_type: Some(ChannelType::SMS),
                    reason: None,
                }),
            ]
        );
    }

    #[test]
    fn every_change_is_read_from_its_code() {
        let read: Vec<Option<LifecycleChange>> = LifecycleChange::ALL
            .into_iter()
            .map(|change| LifecycleChange::from_code(change.code()))
            .collect();
        assert_eq!(read, LifecycleChange::ALL.map(Some));
    }

    /// A channel, or a hook under a source of its own, writes no lifecycle
    /// payload, whatever its code says; nor does a notice that names no
    /// change.
    #[test]
    fn is_none_for_any_other_system_event() {
        let others = [
            event("sms", "room_closed", json!({ "room_id": "room_1" })),
            event("system", "welcome", json!({ "room_id": "room_1" })),
        ];
        let read: Vec<Option<LifecyclePayload>> = others.iter().map(LifecyclePayload::of).collect();
        assert_eq!(read, [None, None]);
    }
}

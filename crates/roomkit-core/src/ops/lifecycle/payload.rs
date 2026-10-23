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

use crate::model::{
    ChannelId, ChannelType, EventContent, EventType, Metadata, RoomEvent, RoomId, SystemContent,
};
use crate::ops::TransitionReason;

/// The `data` key naming the room.
const ROOM_ID: &str = "room_id";
/// The `data` key naming the channel a channel's change is about.
const CHANNEL_ID: &str = "channel_id";
/// The `data` key naming the type of the channel a session started on.
const CHANNEL_TYPE: &str = "channel_type";
/// The `data` key naming what made a room transition on its own.
const REASON: &str = "reason";

/// A change a lifecycle hook's payload reports, which its `code` names (RFC
/// §9.2, §26.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
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
    /// payload: a system event from the framework's `system` source, whose
    /// content's code names a change and whose data names the room. `None`
    /// for any other event, a system notice a channel or a hook writes
    /// among them.
    pub fn of(event: &RoomEvent) -> Option<Self> {
        let EventContent::System(content) = &event.content else {
            return None;
        };
        let from_the_framework =
            event.event_type == EventType::System && event.source.channel_id == ChannelId::system();
        if !from_the_framework {
            return None;
        }
        let change = LifecycleChange::from_code(content.code.as_deref()?)?;
        let data = &content.data;
        Some(Self {
            change,
            room_id: field(data, ROOM_ID, |id| RoomId::new(id.as_str()?).ok()).ok()??,
            channel_id: field(data, CHANNEL_ID, |id| ChannelId::new(id.as_str()?).ok()).ok()?,
            channel_type: field(data, CHANNEL_TYPE, from_value).ok()?,
            reason: field(data, REASON, from_value).ok()?,
        })
    }

    /// The payload's content, as the framework writes it: the change's code,
    /// a body reading it as words, and the data naming the room and, when
    /// the change has them, its channel, its channel's type and its reason.
    pub(crate) fn content(&self) -> SystemContent {
        let mut data = Metadata::new();
        data.insert(ROOM_ID.to_owned(), self.room_id.as_str().into());
        if let Some(channel_id) = &self.channel_id {
            data.insert(CHANNEL_ID.to_owned(), channel_id.as_str().into());
        }
        if let Some(channel_type) = self.channel_type {
            data.insert(CHANNEL_TYPE.to_owned(), to_value(channel_type));
        }
        if let Some(reason) = self.reason {
            data.insert(REASON.to_owned(), to_value(reason));
        }
        let code = self.change.code();
        SystemContent {
            body: code.replace('_', " "),
            code: Some(code.to_owned()),
            data,
        }
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

/// `value` in its JSON form; a unit variant of a serializable enumeration
/// always has one.
fn to_value(value: impl serde::Serialize) -> Value {
    serde_json::to_value(value).unwrap_or_default()
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
    use crate::model::{EventDirection, EventId, EventSource, Timestamp};

    fn room_id() -> RoomId {
        RoomId::new("room_1").unwrap()
    }

    /// A system event from `source` carrying `content`.
    fn event(source: &str, content: SystemContent) -> RoomEvent {
        RoomEvent::new(
            EventId::new("evt_1").unwrap(),
            room_id(),
            EventType::System,
            EventSource::new(
                ChannelId::new(source).unwrap(),
                ChannelType::System,
                EventDirection::Outbound,
            ),
            EventContent::System(content),
            Timestamp::UNIX_EPOCH,
        )
    }

    fn closed_by_timer() -> LifecyclePayload {
        LifecyclePayload {
            change: LifecycleChange::RoomClosed,
            room_id: room_id(),
            channel_id: None,
            channel_type: None,
            reason: Some(TransitionReason::Timer),
        }
    }

    fn session_started() -> LifecyclePayload {
        LifecyclePayload {
            change: LifecycleChange::SessionStarted,
            room_id: room_id(),
            channel_id: Some(ChannelId::new("sms").unwrap()),
            channel_type: Some(ChannelType::Sms),
            reason: None,
        }
    }

    #[test]
    fn its_content_names_the_change_and_its_fields() {
        let written = [closed_by_timer(), session_started()].map(|payload| {
            let content = payload.content();
            (content.body, content.code, Value::Object(content.data))
        });
        assert_eq!(
            written,
            [
                (
                    "room closed".to_owned(),
                    Some("room_closed".to_owned()),
                    json!({ "room_id": "room_1", "reason": "timer" })
                ),
                (
                    "session started".to_owned(),
                    Some("session_started".to_owned()),
                    json!({ "room_id": "room_1", "channel_id": "sms", "channel_type": "sms" })
                ),
            ]
        );
    }

    #[test]
    fn reads_back_what_the_framework_writes() {
        let read = [closed_by_timer(), session_started()]
            .map(|payload| LifecyclePayload::of(&event("system", payload.content())));
        assert_eq!(read, [Some(closed_by_timer()), Some(session_started())]);
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
    /// change, or no room.
    #[test]
    fn is_none_for_any_other_system_event() {
        let notice = |code: &str, data: Value| SystemContent {
            code: Some(code.to_owned()),
            data: data.as_object().cloned().unwrap_or_default(),
            ..SystemContent::new("notice")
        };
        let others = [
            event("sms", closed_by_timer().content()),
            event("system", notice("welcome", json!({ "room_id": "room_1" }))),
            event("system", notice("room_closed", json!({}))),
            event(
                "system",
                notice(
                    "room_closed",
                    json!({ "room_id": "room_1", "reason": "bored" }),
                ),
            ),
        ];
        let read: Vec<Option<LifecyclePayload>> = others.iter().map(LifecyclePayload::of).collect();
        assert_eq!(read, [None, None, None, None]);
    }
}

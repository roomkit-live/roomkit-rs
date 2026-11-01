//! Side effects: what a channel or a hook produces besides events (RFC §4.6,
//! §5.10, §5.11).
//!
//! Side effects are always collected, whatever the producer's access or mute
//! state (RFC §7.5 rule 3).

use serde::{Deserialize, Serialize};

use super::{ChannelId, Metadata, ModelError, ObservationId, RoomId, TaskId, Timestamp};

/// Where a task stands (RFC §5.10).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum TaskStatus {
    /// Not started.
    #[default]
    Pending,
    /// Being worked on.
    InProgress,
    /// Done.
    Completed,
    /// Could not be done.
    Failed,
    /// Abandoned.
    Cancelled,
}

/// An actionable work item (RFC §5.10).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Task {
    /// The task's id.
    pub id: TaskId,
    /// The room it came from.
    pub room_id: RoomId,
    /// A human-readable title.
    pub title: String,
    /// A detailed description.
    #[serde(default)]
    pub description: Option<String>,
    /// Who is responsible for it.
    #[serde(default)]
    pub assigned_to: Option<String>,
    /// Where it stands.
    #[serde(default)]
    pub status: TaskStatus,
    /// When it was created.
    pub created_at: Timestamp,
    /// Integrator-defined data.
    #[serde(default)]
    pub metadata: Metadata,
}

impl Task {
    /// A pending, unassigned task created now, as the Python
    /// implementation's model defaults fill `created_at`. A caller sets the
    /// other fields by struct update.
    #[cfg(feature = "system-environment")]
    pub fn new(id: TaskId, room_id: RoomId, title: impl Into<String>) -> Self {
        use crate::ops::{Environment, SystemEnvironment};
        Self::at(id, room_id, title, SystemEnvironment.now())
    }

    /// [`Task::new`] created at `created_at`. Outside the public API.
    #[doc(hidden)]
    pub fn at(
        id: TaskId,
        room_id: RoomId,
        title: impl Into<String>,
        created_at: Timestamp,
    ) -> Self {
        Self {
            id,
            room_id,
            title: title.into(),
            description: None,
            assigned_to: None,
            status: TaskStatus::Pending,
            created_at,
            metadata: Metadata::new(),
        }
    }
}

/// A passive insight about a room (RFC §5.11).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Observation {
    /// The observation's id.
    pub id: ObservationId,
    /// The room it is about.
    pub room_id: RoomId,
    /// The channel that produced it; `None` for none, which the serialized
    /// form writes as Python's empty `channel_id` (a feedback naming no
    /// channel, RFC §15.9).
    #[serde(with = "empty_as_none")]
    pub channel_id: Option<ChannelId>,
    /// The observation, as text.
    pub content: String,
    /// Its category, for example `sentiment` or `compliance_violation`.
    #[serde(default)]
    pub category: Option<String>,
    /// How confident its producer is.
    #[serde(default)]
    pub confidence: Confidence,
    /// When it was created.
    pub created_at: Timestamp,
    /// Integrator-defined data.
    #[serde(default)]
    pub metadata: Metadata,
}

impl Observation {
    /// An uncategorized observation with full confidence, created now, as
    /// the Python implementation's model defaults fill `created_at`. A
    /// caller sets the other fields by struct update.
    #[cfg(feature = "system-environment")]
    pub fn new(
        id: ObservationId,
        room_id: RoomId,
        channel_id: impl Into<Option<ChannelId>>,
        content: impl Into<String>,
    ) -> Self {
        use crate::ops::{Environment, SystemEnvironment};
        Self::at(id, room_id, channel_id, content, SystemEnvironment.now())
    }

    /// [`Observation::new`] created at `created_at`. Outside the public
    /// API.
    #[doc(hidden)]
    pub fn at(
        id: ObservationId,
        room_id: RoomId,
        channel_id: impl Into<Option<ChannelId>>,
        content: impl Into<String>,
        created_at: Timestamp,
    ) -> Self {
        Self {
            id,
            room_id,
            channel_id: channel_id.into(),
            content: content.into(),
            category: None,
            confidence: Confidence::FULL,
            created_at,
            metadata: Metadata::new(),
        }
    }
}

/// A confidence from 0.0 to 1.0 (RFC §5.11); 1.0 by default.
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "f64", into = "f64")]
pub struct Confidence(f64);

impl Confidence {
    /// Full confidence, 1.0.
    pub const FULL: Self = Self(1.0);

    /// Builds a confidence.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::OutOfRange`] outside 0.0..=1.0, `NaN` included.
    pub fn new(value: f64) -> Result<Self, ModelError> {
        if (0.0..=1.0).contains(&value) {
            Ok(Self(value))
        } else {
            Err(ModelError::OutOfRange {
                name: "confidence",
                value,
                min: 0.0,
                max: 1.0,
            })
        }
    }

    /// The confidence as a number.
    pub fn value(self) -> f64 {
        self.0
    }
}

impl Default for Confidence {
    fn default() -> Self {
        Self::FULL
    }
}

impl TryFrom<f64> for Confidence {
    type Error = ModelError;

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<Confidence> for f64 {
    fn from(confidence: Confidence) -> Self {
        confidence.0
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn a_confidence_above_one_is_refused() {
        assert!(Confidence::new(1.5).is_err());
    }

    #[test]
    fn a_negative_confidence_is_refused() {
        assert!(Confidence::new(-0.1).is_err());
    }

    #[test]
    fn an_observation_without_confidence_has_full_confidence() {
        let parsed: Observation = serde_json::from_value(json!({
            "id": "obs_1",
            "room_id": "room_1",
            "channel_id": "ai",
            "content": "sentiment: negative",
            "created_at": "1970-01-01T00:00:00Z",
        }))
        .unwrap();
        assert_eq!(parsed.confidence, Confidence::FULL);
    }
}

/// An observation's channel in the serialized form: Python's `str`, empty
/// for none.
mod empty_as_none {
    use serde::de::Error as _;
    use serde::{Deserialize, Deserializer, Serializer};

    use crate::model::ChannelId;

    #[expect(
        clippy::ref_option,
        reason = "serde's `with` hands the field by reference"
    )]
    pub(super) fn serialize<S: Serializer>(
        channel_id: &Option<ChannelId>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(channel_id.as_ref().map_or("", ChannelId::as_str))
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<ChannelId>, D::Error> {
        let text = String::deserialize(deserializer)?;
        if text.is_empty() {
            return Ok(None);
        }
        ChannelId::new(text).map(Some).map_err(D::Error::custom)
    }
}

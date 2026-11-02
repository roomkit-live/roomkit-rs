//! A record's JSON in its row's `data` column, and the values of its
//! indexed columns.
//!
//! The Python implementation may write members roomkit-rs does not model (a
//! channel type no implementation lists among an identity's addresses, a
//! member a later version adds). Rewriting
//! a row keeps them: the members the stored JSON holds and the record's
//! serialization lacks are put back, at the levels where the model has a
//! structure of its own. The record's maps (`metadata`, `raw_payload`, ...)
//! are written as the record holds them: a key it removed stays removed.

use roomkit::model::{ChannelType, EventContent, Identity, Timestamp};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};

use crate::error::Failure;

/// A kind of record, by the structures of its JSON form whose members the
/// model fixes, written as JSON pointers.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Kind {
    Room,
    Event,
    Binding,
    Participant,
    Identity,
    Task,
    Observation,
    Verification,
    VerificationRequest,
}

impl Kind {
    /// The structures of the record's JSON a rewrite keeps unknown members
    /// in. An event's content is kept whole instead, when the record holds
    /// it unchanged ([`written`]).
    fn structures(self) -> &'static [&'static str] {
        match self {
            Self::Room => &["", "/timers"],
            Self::Event => &["", "/source"],
            Self::Binding => &["", "/capabilities", "/rate_limit", "/retry_policy"],
            Self::Participant
            | Self::Identity
            | Self::Task
            | Self::Observation
            | Self::Verification
            | Self::VerificationRequest => &[""],
        }
    }
}

/// The record a row's `data` holds.
pub(crate) fn read<T: DeserializeOwned>(data: &str) -> Result<T, Failure> {
    Ok(serde_json::from_str(data)?)
}

/// The member of an identity's JSON holding its addresses by channel type.
const ADDRESSES: &str = "channel_addresses";

/// The identity a row's `data` holds. The Python implementation takes any
/// key in its addresses; one that is no channel type's name for roomkit-rs
/// (`Slack`) is a member the model does not read, left out here and kept
/// by [`written`], so that it never makes the identity unreadable.
pub(crate) fn read_identity(data: &str) -> Result<Identity, Failure> {
    let mut value: Value = serde_json::from_str(data)?;
    if let Some(Value::Object(addresses)) = value.get_mut(ADDRESSES) {
        addresses.retain(|channel_type, _| ChannelType::new(channel_type.as_str()).is_ok());
    }
    Ok(serde_json::from_value(value)?)
}

/// Puts back in `rewritten` the addresses `stored` keeps under keys that
/// are no channel type's name, which the model never read.
fn keep_unread_addresses(stored: &Value, rewritten: &mut Value) {
    let (Some(Value::Object(kept)), Some(Value::Object(addresses))) =
        (stored.get(ADDRESSES), rewritten.get_mut(ADDRESSES))
    else {
        return;
    };
    for (channel_type, value) in kept {
        if ChannelType::new(channel_type.as_str()).is_err() {
            addresses.insert(channel_type.clone(), value.clone());
        }
    }
}

/// The JSON `record` is written as, keeping what `stored`, the row's
/// current JSON, holds that the record does not model.
pub(crate) fn written<T: Serialize>(
    record: &T,
    kind: Kind,
    stored: Option<&str>,
) -> Result<String, Failure> {
    let mut value = serde_json::to_value(record)?;
    if let Some(stored) = stored {
        let stored: Value = serde_json::from_str(stored)?;
        match kind {
            Kind::Event => keep_unchanged_content(&stored, &mut value),
            Kind::Identity => keep_unread_addresses(&stored, &mut value),
            Kind::Room
            | Kind::Binding
            | Kind::Participant
            | Kind::Task
            | Kind::Observation
            | Kind::Verification
            | Kind::VerificationRequest => {}
        }
        for pointer in kind.structures() {
            if let (Some(Value::Object(kept)), Some(Value::Object(rewritten))) =
                (stored.pointer(pointer), value.pointer_mut(pointer))
            {
                keep_unknown(kept, rewritten);
            }
        }
    }
    Ok(serde_json::to_string(&value)?)
}

/// Puts back in `rewritten` the members of `kept` it lacks.
fn keep_unknown(kept: &Map<String, Value>, rewritten: &mut Map<String, Value>) {
    for (member, value) in kept {
        if !rewritten.contains_key(member) {
            rewritten.insert(member.clone(), value.clone());
        }
    }
}

/// Writes the stored content back whole when the record holds it as the
/// model reads it, unchanged: whatever its parts hold that the model does
/// not is kept. A content an edit replaced is written as the record holds
/// it.
fn keep_unchanged_content(stored: &Value, rewritten: &mut Value) {
    let (Some(kept), Some(content)) = (stored.get("content"), rewritten.get_mut("content")) else {
        return;
    };
    let unchanged = serde_json::from_value::<EventContent>(kept.clone())
        .ok()
        .zip(serde_json::from_value::<EventContent>(content.clone()).ok())
        .is_some_and(|(kept, written)| kept == written);
    if unchanged {
        content.clone_from(kept);
    }
}

/// The name a value of the model's enumerations, or of its open names, is
/// written as: its JSON string.
pub(crate) fn name<T: Serialize>(value: &T) -> Result<String, Failure> {
    match serde_json::to_value(value)? {
        Value::String(name) => Ok(name),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::Array(_) | Value::Object(_) => {
            Err(Failure::OutOfRange("a name written other than as a string"))
        }
    }
}

/// An instant as the seconds since the Unix epoch, the form of the Python
/// implementation's `created_ts` columns.
pub(crate) fn seconds(instant: Timestamp) -> f64 {
    match instant.duration_since(Timestamp::UNIX_EPOCH) {
        Some(after) => after.as_secs_f64(),
        None => Timestamp::UNIX_EPOCH
            .duration_since(instant)
            .map_or(0.0, |before| -before.as_secs_f64()),
    }
}

/// The instant `seconds` after the Unix epoch, a `created_ts` column read
/// back, at the model's millisecond precision.
#[expect(
    clippy::cast_possible_truncation,
    reason = "a created_ts holds an instant the model wrote, well inside i64 milliseconds"
)]
pub(crate) fn instant(seconds: f64) -> Result<Timestamp, Failure> {
    let millis = (seconds * 1000.0).round() as i64;
    Timestamp::from_unix_millis(millis).map_err(|_| Failure::OutOfRange("an instant"))
}

/// A count or an index as SQLite's integer.
pub(crate) fn integer(value: u64) -> Result<i64, Failure> {
    i64::try_from(value).map_err(|_| Failure::OutOfRange("an index"))
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    fn rewritten(record: &Value, kind: Kind, stored: &Value) -> Value {
        let written = written(record, kind, Some(&stored.to_string())).unwrap();
        serde_json::from_str(&written).unwrap()
    }

    /// A member the model does not know is kept at every structure the
    /// model fixes, never in a map the record holds whole.
    #[test]
    fn a_rewrite_keeps_unknown_members_but_not_removed_keys() {
        let stored = json!({
            "id": "b", "email": "a@b.c",
            "capabilities": { "max_length": 1, "supported_audio_formats": ["ogg"] },
            "metadata": { "gone": true },
        });
        let record = json!({ "id": "b", "capabilities": { "max_length": 2 }, "metadata": {} });
        assert_eq!(
            rewritten(&record, Kind::Binding, &stored),
            json!({
                "id": "b", "email": "a@b.c",
                "capabilities": { "max_length": 2, "supported_audio_formats": ["ogg"] },
                "metadata": {},
            })
        );
    }

    /// An event's content is kept whole while unchanged, replaced by an
    /// edit's.
    #[test]
    fn an_unchanged_content_is_kept_whole_and_a_replaced_one_is_not() {
        let stored =
            json!({ "content": { "type": "text", "body": "hi", "language": null, "tone": "dry" } });
        let same = json!({ "content": { "type": "text", "body": "hi", "language": null } });
        let edited = json!({ "content": { "type": "text", "body": "hello", "language": null } });
        assert_eq!(
            (
                rewritten(&same, Kind::Event, &stored),
                rewritten(&edited, Kind::Event, &stored)
            ),
            (stored.clone(), edited.clone())
        );
    }

    #[test]
    fn an_instant_is_written_in_seconds() {
        let instant = Timestamp::from_unix_millis(1_500).unwrap();
        assert!((seconds(instant) - 1.5).abs() < f64::EPSILON);
    }
}

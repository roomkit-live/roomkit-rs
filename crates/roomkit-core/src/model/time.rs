//! Instants in time, as the model records them.

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::ModelError;

/// An instant, serialized as an RFC 3339 string in UTC
/// (`2026-10-22T14:03:00Z`).
///
/// The core never reads the clock: an operation receives the current instant
/// from its environment. Precision is the nanosecond; the supported range is
/// the years -9999 to 9999.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(jiff::Timestamp);

impl Timestamp {
    /// The Unix epoch, 1970-01-01T00:00:00Z.
    pub const UNIX_EPOCH: Self = Self(jiff::Timestamp::UNIX_EPOCH);

    /// The instant `millis` milliseconds after the Unix epoch, before it when
    /// negative.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::InvalidTimestamp`] outside the supported range.
    pub fn from_unix_millis(millis: i64) -> Result<Self, ModelError> {
        jiff::Timestamp::from_millisecond(millis)
            .map(Self)
            .map_err(|error| ModelError::InvalidTimestamp {
                value: millis.to_string(),
                reason: error.to_string(),
            })
    }

    /// The milliseconds since the Unix epoch, dropping any finer precision.
    pub fn as_unix_millis(self) -> i64 {
        self.0.as_millisecond()
    }

    /// This instant moved `duration` later, or `None` past the supported range.
    pub fn checked_add(self, duration: Duration) -> Option<Self> {
        self.0.checked_add(duration).ok().map(Self)
    }

    /// How much later than `earlier` this instant is, or `None` when `earlier`
    /// is the later one.
    pub fn duration_since(self, earlier: Self) -> Option<Duration> {
        Duration::try_from(self.0.duration_since(earlier.0)).ok()
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, formatter)
    }
}

impl FromStr for Timestamp {
    type Err = ModelError;

    /// Parses an RFC 3339 instant; an offset other than `Z` is converted to
    /// UTC.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        jiff::Timestamp::from_str(value)
            .map(Self)
            .map_err(|error| ModelError::InvalidTimestamp {
                value: value.to_owned(),
                reason: error.to_string(),
            })
    }
}

impl Serialize for Timestamp {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Timestamp {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    #[test]
    fn a_timestamp_displays_as_rfc_3339_in_utc() {
        let instant = Timestamp::from_unix_millis(1_792_677_780_000).unwrap();
        assert_eq!(instant.to_string(), "2026-10-22T14:03:00Z");
    }

    #[test]
    fn an_offset_is_converted_to_utc() {
        let instant: Timestamp = "2026-10-22T10:03:00-04:00".parse().unwrap();
        assert_eq!(instant.to_string(), "2026-10-22T14:03:00Z");
    }

    #[test]
    fn sub_second_precision_survives_a_round_trip() {
        let written = "2026-10-22T14:03:00.123456789Z";
        let instant: Timestamp = written.parse().unwrap();
        assert_eq!(instant.to_string(), written);
    }

    #[test]
    fn a_timestamp_without_an_offset_is_refused() {
        assert!("2026-10-22T14:03:00".parse::<Timestamp>().is_err());
    }

    #[test]
    fn a_millisecond_count_outside_the_range_is_refused() {
        assert!(Timestamp::from_unix_millis(i64::MAX).is_err());
    }

    #[test]
    fn duration_since_measures_the_gap() {
        let start = Timestamp::UNIX_EPOCH;
        let later = start.checked_add(Duration::from_secs(90)).unwrap();
        assert_eq!(later.duration_since(start), Some(Duration::from_secs(90)));
    }

    #[test]
    fn duration_since_a_later_instant_is_none() {
        let start = Timestamp::UNIX_EPOCH;
        let later = start.checked_add(Duration::from_secs(1)).unwrap();
        assert_eq!(start.duration_since(later), None);
    }

    #[test]
    fn a_timestamp_serializes_as_its_rfc_3339_string() {
        let instant = Timestamp::UNIX_EPOCH;
        assert_eq!(
            serde_json::to_value(instant).unwrap(),
            json!("1970-01-01T00:00:00Z")
        );
    }

    #[test]
    fn a_malformed_timestamp_does_not_deserialize() {
        assert!(serde_json::from_value::<Timestamp>(json!("yesterday")).is_err());
    }
}

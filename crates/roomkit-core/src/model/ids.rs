//! Identifiers of the model's records.
//!
//! Each kind of record has its own identifier type, so a room id cannot be
//! passed where an event id is expected. An identifier is never empty; a
//! channel id also obeys the visibility grammar it appears in (RFC §7.3).

use std::borrow::Borrow;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use super::ModelError;

/// The keywords of the visibility grammar (RFC §7.3). No channel id may be
/// one, or a visibility naming it would read as the keyword (gap G-002).
pub(crate) const VISIBILITY_KEYWORDS: [&str; 5] =
    ["all", "none", "internal", "transport", "intelligence"];

macro_rules! identifier {
    ($(#[$doc:meta])* $name:ident, $kind:literal $(, $extra_rules:path)?) => {
        $(#[$doc])*
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            #[doc = concat!("Builds a ", $kind, ".")]
            ///
            /// # Errors
            ///
            /// Returns [`ModelError`] when the value is empty or breaks the
            /// identifier's own rules.
            pub fn new(value: impl Into<String>) -> Result<Self, ModelError> {
                let value = value.into();
                if value.is_empty() {
                    return Err(ModelError::EmptyIdentifier { kind: $kind });
                }
                $( $extra_rules(value.as_str())?; )?
                Ok(Self(value))
            }

            /// The identifier as written.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.pad(&self.0)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl Borrow<str> for $name {
            fn borrow(&self) -> &str {
                &self.0
            }
        }

        impl FromStr for $name {
            type Err = ModelError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }

        impl TryFrom<String> for $name {
            type Error = ModelError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl TryFrom<&str> for $name {
            type Error = ModelError;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl From<$name> for String {
            fn from(id: $name) -> Self {
                id.0
            }
        }
    };
}

identifier!(
    /// Identifies a room (RFC §5.1).
    RoomId,
    "room id"
);

identifier!(
    /// Identifies an event in its room's timeline (RFC §5.2).
    EventId,
    "event id"
);

identifier!(
    /// Identifies a participant (RFC §5.5).
    ParticipantId,
    "participant id"
);

identifier!(
    /// Identifies a cross-channel identity (RFC §5.6).
    IdentityId,
    "identity id"
);

identifier!(
    /// Identifies a task (RFC §5.10).
    TaskId,
    "task id"
);

identifier!(
    /// Identifies an observation (RFC §5.11).
    ObservationId,
    "observation id"
);

identifier!(
    /// Identifies a channel (RFC §6.1).
    ///
    /// A channel id is written in visibility lists (RFC §7.3), so it may not be
    /// one of the grammar's keywords, contain a comma, or start or end with
    /// whitespace (gap G-002).
    ChannelId,
    "channel id",
    channel_id_rules
);

/// The id of the kit's own host channel, which a post that names no
/// channel speaks from, registered on first use (RFC §10.5).
pub const HOST_CHANNEL_ID: &str = "host";

impl ChannelId {
    /// The source of the records the framework writes itself, a lifecycle
    /// change or a task's creation.
    pub fn system() -> Self {
        Self("system".to_owned())
    }

    /// The id of the kit's own host channel, [`HOST_CHANNEL_ID`], which a
    /// post that names no channel speaks from (RFC §10.5).
    pub fn host() -> Self {
        Self(HOST_CHANNEL_ID.to_owned())
    }
}

impl RoomId {
    /// The room id written from a random 128-bit value, as the 32 hex digits
    /// of a UUID's simple form. It cannot fail: the text is never empty.
    pub fn from_u128(value: u128) -> Self {
        Self(format!("{value:032x}"))
    }

    /// A room id the crate generated and knows to be valid.
    pub(crate) fn generated(value: String) -> Self {
        debug_assert!(!value.is_empty(), "generated an empty room id");
        Self(value)
    }
}

impl EventId {
    /// The event id written from a random 128-bit value, as the 32 hex digits
    /// of a UUID's simple form. It cannot fail: the text is never empty.
    pub fn from_u128(value: u128) -> Self {
        Self(format!("{value:032x}"))
    }

    /// An event id the crate generated and knows to be valid.
    pub(crate) fn generated(value: String) -> Self {
        debug_assert!(!value.is_empty(), "generated an empty event id");
        Self(value)
    }
}

impl ObservationId {
    /// The observation id written from a random 128-bit value, as the 32 hex
    /// digits of a UUID's simple form. It cannot fail: the text is never empty.
    pub fn from_u128(value: u128) -> Self {
        Self(format!("{value:032x}"))
    }

    /// An observation id the crate generated and knows to be valid.
    pub(crate) fn generated(value: String) -> Self {
        debug_assert!(!value.is_empty(), "generated an empty observation id");
        Self(value)
    }
}

fn channel_id_rules(value: &str) -> Result<(), ModelError> {
    let refuse = |reason| {
        Err(ModelError::InvalidChannelId {
            value: value.to_owned(),
            reason,
        })
    };
    if VISIBILITY_KEYWORDS.contains(&value) {
        return refuse("it is a visibility keyword");
    }
    if value.contains(',') {
        return refuse("it contains a comma, which separates ids in a visibility");
    }
    if value.trim() != value {
        return refuse("it starts or ends with whitespace");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    #[test]
    fn an_id_from_a_u128_is_its_32_hex_digits() {
        assert_eq!(
            EventId::from_u128(0xff).as_str(),
            "000000000000000000000000000000ff"
        );
    }

    #[test]
    fn an_identifier_honours_the_requested_width() {
        assert_eq!(format!("{:<5}|", RoomId::new("r1").unwrap()), "r1   |");
    }

    #[test]
    fn an_empty_identifier_is_refused() {
        assert_eq!(
            RoomId::new(""),
            Err(ModelError::EmptyIdentifier { kind: "room id" })
        );
    }

    #[test]
    fn an_identifier_keeps_its_value() {
        assert_eq!(EventId::new("evt_1").unwrap().as_str(), "evt_1");
    }

    #[test]
    fn a_channel_id_may_not_be_a_visibility_keyword() {
        assert!(matches!(
            ChannelId::new("transport"),
            Err(ModelError::InvalidChannelId { .. })
        ));
    }

    #[test]
    fn a_channel_id_may_differ_from_a_keyword_by_case() {
        assert!(ChannelId::new("All").is_ok());
    }

    #[test]
    fn a_channel_id_may_not_contain_a_comma() {
        assert!(matches!(
            ChannelId::new("sms,ws"),
            Err(ModelError::InvalidChannelId { .. })
        ));
    }

    #[test]
    fn a_channel_id_may_not_start_with_whitespace() {
        assert!(matches!(
            ChannelId::new(" sms"),
            Err(ModelError::InvalidChannelId { .. })
        ));
    }

    #[test]
    fn a_channel_id_may_contain_inner_whitespace() {
        assert!(ChannelId::new("front desk").is_ok());
    }

    #[test]
    fn an_identifier_serializes_as_its_string() {
        let id = ParticipantId::new("p_jean").unwrap();
        assert_eq!(serde_json::to_value(&id).unwrap(), json!("p_jean"));
    }

    #[test]
    fn deserialization_applies_the_identifier_rules() {
        let parsed = serde_json::from_value::<ChannelId>(json!("none"));
        assert!(
            parsed.is_err(),
            "a keyword must not deserialize as a channel id"
        );
    }
}

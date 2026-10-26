//! The error a model value refuses to be built with.

use thiserror::Error;

/// Why a value of the model could not be built.
///
/// Every invariant the RFC states on a model type is checked when the value is
/// built, by its constructor or by deserialization, so a value that exists is
/// valid. This is the error either path returns.
#[derive(Clone, Debug, PartialEq, Error)]
#[non_exhaustive]
pub enum ModelError {
    /// An identifier was empty.
    #[error("{kind} must not be empty")]
    EmptyIdentifier {
        /// The identifier type, for example `room id`.
        kind: &'static str,
    },

    /// A channel id cannot be written in a visibility list (RFC §7.3, gap G-002).
    #[error("channel id {value:?} is not allowed: {reason}")]
    InvalidChannelId {
        /// The refused id.
        value: String,
        /// Why it is refused.
        reason: &'static str,
    },

    /// A channel type's name holds a character other than a lowercase ASCII
    /// letter, a digit, `_` or `-`.
    #[error(
        "channel type {value:?} is not allowed: a name is lowercase ASCII letters, digits, `_` \
         and `-`"
    )]
    InvalidChannelType {
        /// The refused name.
        value: String,
    },

    /// A visibility does not follow the grammar of RFC §7.3 (gap G-002).
    #[error("visibility {value:?} is not valid: {reason}")]
    InvalidVisibility {
        /// The refused visibility, as written.
        value: String,
        /// Why it is refused.
        reason: String,
    },

    /// A timestamp is not an RFC 3339 instant, or is out of range.
    #[error("timestamp {value:?} is not valid: {reason}")]
    InvalidTimestamp {
        /// The refused timestamp, as written.
        value: String,
        /// Why it is refused.
        reason: String,
    },

    /// A composite content has no part (RFC §5.3: "at least one").
    #[error("a composite content needs at least one part")]
    EmptyComposite,

    /// A composite content nests deeper than RFC §5.3 allows (gap G-006).
    #[error("composite content nests {depth} levels, more than the {max} allowed")]
    CompositeTooDeep {
        /// The nesting depth of the refused content.
        depth: usize,
        /// The maximum depth.
        max: usize,
    },

    /// A number is outside the range its field allows.
    #[error("{name} {value} is outside {min}..={max}")]
    OutOfRange {
        /// The field.
        name: &'static str,
        /// The refused value.
        value: f64,
        /// The lowest allowed value.
        min: f64,
        /// The highest allowed value.
        max: f64,
    },

    /// A hook result says `modify` without the payload that replaces the
    /// trigger's (gap G-010).
    #[error("a modify hook result carries no replacement payload")]
    ModifyWithoutPayload,
}

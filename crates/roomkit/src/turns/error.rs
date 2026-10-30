//! What crosses the turn backend in place of an error, and back (RFC
//! §10.6.8): each side writes an error as a record, its kind, its message,
//! what classifies it and its causes, and rebuilds the errors it knows.

use std::error::Error;

use roomkit_core::model::{ChannelId, ErrorRecord, Metadata};
use serde_json::Value;
use thiserror::Error;

use super::pipe::LinkFailure;
use super::wire::WireError;
use crate::error::RoomKitError;

/// At most this many causes cross with an error.
const CAUSES_KEPT: usize = 16;

/// The kind a channel's error crosses as: the name the core records a
/// channel's failure under.
pub(crate) const CHANNEL_ERROR: &str = "ChannelError";

/// The kind a kit's error crosses as, its variant among its attributes.
const KIT_ERROR: &str = "RoomKitError";

/// The attribute that names a kit error's variant.
const VARIANT: &str = "variant";

/// The kind a turn's link failure is recorded under on the owner, its
/// reason heading its message.
pub(crate) const TURN_LINK_ERROR: &str = "TurnLinkError";

/// A failure carried across the turn backend (RFC §10.6.8): the link of
/// a turn that failed, a verb its owner refused, or an error raised on the
/// other side that this side does not rebuild.
///
/// `kind` is a link's failure reason (`no_executor`, `executor_lost`,
/// `owner_lost`, `frame_lost`, `frame_invalid`, `backend_closed`), or the
/// kind of the error the other side raised as it named it (`RoomKitError`,
/// `PermissionError`, ...); `message` says what failed, for logs and hooks,
/// never for a model.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[error("{message}")]
#[non_exhaustive]
pub struct TurnError {
    /// The failure's reason, or the kind of the error it stands for.
    pub kind: String,
    /// What it says.
    pub message: String,
}

impl TurnError {
    pub(crate) fn new(kind: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            message: message.into(),
        }
    }

    /// The failure of a link, by its reason.
    pub(crate) fn link(failure: &LinkFailure) -> Self {
        Self::new(failure.reason(), failure.message())
    }

    /// The error a turn's failure is recorded as on the owner: a link's
    /// under [`TURN_LINK_ERROR`], its reason heading its message; another
    /// under its kind.
    pub(crate) fn to_record(&self) -> ErrorRecord {
        if is_link_reason(&self.kind) {
            return ErrorRecord::new(TURN_LINK_ERROR, format!("{}: {}", self.kind, self.message));
        }
        ErrorRecord::new(self.kind.clone(), self.message.clone())
    }
}

/// Whether `kind` is one of RFC §10.6.8's link failure reasons, or the
/// cancel that cut the turn.
fn is_link_reason(kind: &str) -> bool {
    matches!(
        kind,
        "no_executor"
            | "executor_lost"
            | "owner_lost"
            | "frame_lost"
            | "frame_invalid"
            | "backend_closed"
            | "cancelled"
    )
}

/// An error raised on the other side, as this side reads it: its record,
/// shown by its message.
#[derive(Clone, Debug, PartialEq, Error)]
#[error("{}", .0.message)]
pub(crate) struct RemoteError(pub(crate) WireError);

/// The core record of `error`, a channel's or one that crossed: one that
/// crossed keeps its kind, a link's failure is recorded under
/// [`TURN_LINK_ERROR`], any other is a channel's.
pub(crate) fn record_of(error: &(dyn Error + Send + Sync + 'static)) -> ErrorRecord {
    if let Some(turn) = error.downcast_ref::<TurnError>() {
        return turn.to_record();
    }
    if let Some(RemoteError(crossed)) = error.downcast_ref::<RemoteError>() {
        return to_record(crossed);
    }
    ErrorRecord::new(CHANNEL_ERROR, error.to_string())
}

/// `error`, a channel's, as it crosses, its causes after it.
pub(crate) fn channel_error(error: &(dyn Error + 'static)) -> WireError {
    let mut record = one(CHANNEL_ERROR, error.to_string());
    let mut cause = error.source();
    while let Some(next) = cause {
        if record.causes.len() >= CAUSES_KEPT {
            break;
        }
        record.causes.push(one("Error", next.to_string()));
        cause = next.source();
    }
    record
}

/// A core error record as it crosses.
pub(crate) fn from_record(record: &ErrorRecord) -> WireError {
    one(record.error_type.clone(), record.message.clone())
}

/// The core error record a crossed error is, on the owner.
pub(crate) fn to_record(error: &WireError) -> ErrorRecord {
    ErrorRecord::new(error.kind.clone(), error.message.clone())
}

/// `error`, a kit's, as it crosses: its variant among its attributes.
pub(crate) fn kit_error(error: &RoomKitError) -> WireError {
    let mut record = one(KIT_ERROR, error.to_string());
    let (variant, fields) = variant_of(error);
    record
        .attributes
        .insert(VARIANT.to_owned(), Value::from(variant));
    record.attributes.extend(fields);
    record
}

/// The kit error a crossed error is: a variant that carries no data, or an
/// unknown channel, as itself; any other as a [`TurnError`] keeping its
/// kind and message.
pub(crate) fn rebuild_kit_error(error: &WireError) -> RoomKitError {
    if error.kind == KIT_ERROR
        && let Some(rebuilt) = rebuilt_variant(error)
    {
        return rebuilt;
    }
    RoomKitError::Turn(TurnError::new(error.kind.clone(), error.message.clone()))
}

fn rebuilt_variant(error: &WireError) -> Option<RoomKitError> {
    let variant = error.attributes.get(VARIANT)?.as_str()?;
    Some(match variant {
        "KitClosed" => RoomKitError::KitClosed,
        "ShutdownCancelled" => RoomKitError::ShutdownCancelled,
        "Cancelled" => RoomKitError::Cancelled,
        "Internal" => RoomKitError::Internal(
            error
                .message
                .strip_prefix("internal error: ")
                .unwrap_or(&error.message)
                .to_owned(),
        ),
        "ChannelNotRegistered" => {
            let id = error.attributes.get("channel_id")?.as_str()?;
            RoomKitError::ChannelNotRegistered(ChannelId::new(id).ok()?)
        }
        _ => return None,
    })
}

/// A kit error's variant name, and the fields a rebuild reads.
/// The variant `error` is, by its name: the name of its Python class but
/// for the `Error` suffix (decision 9 of step 5).
pub(crate) fn variant_name(error: &RoomKitError) -> &'static str {
    variant_of(error).0
}

fn variant_of(error: &RoomKitError) -> (&'static str, Metadata) {
    let mut fields = Metadata::new();
    let variant = match error {
        RoomKitError::RoomNotFound(_) => "RoomNotFound",
        RoomKitError::RoomAlreadyExists(_) => "RoomAlreadyExists",
        RoomKitError::RoomClosed { .. } => "RoomClosed",
        RoomKitError::TransitionRefused { .. } => "TransitionRefused",
        RoomKitError::ProcessTimeout { .. } => "ProcessTimeout",
        RoomKitError::ChangeRefused { .. } => "ChangeRefused",
        RoomKitError::ReentrantCall { .. } => "ReentrantCall",
        RoomKitError::ReentryLoopCap(_) => "ReentryLoopCap",
        RoomKitError::ChannelNotFound { .. } => "ChannelNotFound",
        RoomKitError::ChannelNotRegistered(channel_id) => {
            fields.insert("channel_id".to_owned(), Value::from(channel_id.as_str()));
            "ChannelNotRegistered"
        }
        RoomKitError::ChannelAlreadyRegistered(_) => "ChannelAlreadyRegistered",
        RoomKitError::ParticipantNotFound { .. } => "ParticipantNotFound",
        RoomKitError::IdentityNotFound(_) => "IdentityNotFound",
        RoomKitError::AddressLinked { .. } => "AddressLinked",
        RoomKitError::EventNotFound(_) => "EventNotFound",
        RoomKitError::IdempotencyKeyTaken { .. } => "IdempotencyKeyTaken",
        RoomKitError::ProviderDelivery { .. } => "ProviderDelivery",
        RoomKitError::Value(_) => "Value",
        RoomKitError::Cancelled => "Cancelled",
        RoomKitError::ExceptionGroup { .. } => "ExceptionGroup",
        RoomKitError::KitClosed => "KitClosed",
        RoomKitError::ShutdownCancelled => "ShutdownCancelled",
        RoomKitError::Store(_) => "Store",
        RoomKitError::Lock(_) => "Lock",
        RoomKitError::Channel { .. } => "Channel",
        RoomKitError::Router(_) => "Router",
        RoomKitError::Turn(_) => "Turn",
        RoomKitError::Internal(_) => "Internal",
    };
    (variant, fields)
}

/// An error record of `kind` saying `message`, its message its one
/// argument.
pub(crate) fn one(kind: impl Into<String>, message: String) -> WireError {
    WireError {
        kind: kind.into(),
        args: vec![Value::String(message.clone())],
        message,
        attributes: Metadata::new(),
        causes: Vec::new(),
        traceback: None,
        bases: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[derive(Debug, Error)]
    #[error("the provider failed")]
    struct Outer(#[source] std::io::Error);

    /// RFC §10.6.8: an error crosses with its causes, in order.
    #[test]
    fn a_channel_s_error_crosses_with_its_causes() {
        let error = Outer(std::io::Error::other("connection refused"));
        let record = channel_error(&error);
        assert_eq!(
            (
                record.kind.as_str(),
                record.message.as_str(),
                record
                    .causes
                    .iter()
                    .map(|cause| cause.message.as_str())
                    .collect::<Vec<_>>()
            ),
            (
                "ChannelError",
                "the provider failed",
                vec!["connection refused"]
            )
        );
    }

    /// RFC §10.6.8: an error of a class the side rebuilds comes back of that
    /// class; any other as a stand-in keeping its kind and message.
    #[test]
    fn a_kit_error_comes_back_as_itself_or_as_a_turn_error() {
        let unknown = || RoomKitError::ChannelNotRegistered(ChannelId::new("sms").unwrap());
        let reentrant = || RoomKitError::ReentrantCall {
            room_id: roomkit_core::model::RoomId::new("room_1").unwrap(),
        };
        let rebuilt = [RoomKitError::KitClosed, unknown(), reentrant()]
            .map(|error| rebuild_kit_error(&kit_error(&error)));
        assert_eq!(
            rebuilt,
            [
                RoomKitError::KitClosed,
                unknown(),
                RoomKitError::Turn(TurnError::new("RoomKitError", reentrant().to_string()))
            ]
        );
    }
}

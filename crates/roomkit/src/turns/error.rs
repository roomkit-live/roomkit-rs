//! What crosses the turn backend in place of an error, and back (RFC
//! §10.6.8): each side writes an error as a record, its kind, its message,
//! what classifies it and its causes, and rebuilds the errors it knows.

use std::error::Error;

use roomkit_core::model::{ChannelId, DeliveryError, ErrorRecord, Metadata, RoomId};
use roomkit_core::ops::{ChannelOutcome, Outcome, Raised};
use serde_json::Value;
use thiserror::Error;

use super::pipe::LinkFailure;
use super::wire::WireError;
use crate::classed::ClassedError;
use crate::error::RoomKitError;

/// At most this many causes cross with an error.
const CAUSES_KEPT: usize = 16;

/// The kind a channel's error crosses as: the name the core records a
/// channel's failure under.
pub(crate) const CHANNEL_ERROR: &str = "ChannelError";

/// The kind a kit's error crosses as, its variant among its attributes.
const KIT_ERROR: &str = "RoomKitError";

/// The kind a builtin `ValueError` crosses as.
const VALUE_ERROR: &str = "ValueError";

/// Python's builtin `RuntimeError`, which crosses as itself, as `ValueError`.
const RUNTIME_ERROR: &str = "RuntimeError";

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
pub(crate) struct TurnError {
    /// The failure's reason, or the kind of the error it stands for.
    pub(crate) kind: String,
    /// What it says.
    pub(crate) message: String,
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

    /// The kit's error it is: Python's `TurnLinkError` for a link's
    /// failure, its `RemoteTurnError` for another.
    pub(crate) fn into_kit_error(self) -> RoomKitError {
        if is_link_reason(&self.kind) {
            return RoomKitError::TurnLink {
                reason: self.kind,
                message: Some(self.message),
            };
        }
        RoomKitError::RemoteTurn {
            message: self.message,
            kind: self.kind,
            bases: Vec::new(),
        }
    }

    /// The failure of a link for `reason`, in the words Python's
    /// `TurnLinkError(reason)` writes when given none (RMK-1106): its turn
    /// backend closed, or the turn ended without its executor.
    pub(crate) fn link_reason(reason: &str) -> Self {
        let message = if reason == "backend_closed" {
            "The turn ended: its turn backend closed".to_owned()
        } else {
            format!("The turn ended without its executor: {reason}")
        };
        Self::new(reason, message)
    }

    /// The error a turn's failure is recorded as on the owner: a link's
    /// under [`TURN_LINK_ERROR`], its message as Python's `TurnLinkError`
    /// says it (RMK-1106), its reason among its attributes as Python's
    /// `error_record` keeps it; another under its kind.
    pub(crate) fn to_record(&self) -> ErrorRecord {
        if is_link_reason(&self.kind) {
            let mut attributes = Metadata::new();
            attributes.insert("reason".to_owned(), Value::from(self.kind.clone()));
            return ErrorRecord {
                attributes: Box::new(attributes),
                ..ErrorRecord::new(TURN_LINK_ERROR, self.message.clone())
            };
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
/// [`TURN_LINK_ERROR`], any other is a channel's, under the class a
/// [`ClassedError`] names.
pub(crate) fn record_of(error: &(dyn Error + Send + Sync + 'static)) -> ErrorRecord {
    if let Some(turn) = error.downcast_ref::<TurnError>() {
        return turn.to_record();
    }
    if let Some(RemoteError(crossed)) = error.downcast_ref::<RemoteError>() {
        return to_record(crossed);
    }
    let classed = error.downcast_ref::<ClassedError>();
    ErrorRecord {
        attributes: Box::new(
            classed
                .map(ClassedError::attributes)
                .cloned()
                .unwrap_or_default(),
        ),
        ..ErrorRecord::new(
            classed.map_or(CHANNEL_ERROR, ClassedError::class),
            error.to_string(),
        )
    }
}

/// `error`, a channel's, as it crosses, its causes after it.
pub(crate) fn channel_error(error: &(dyn Error + 'static)) -> WireError {
    let classed = error.downcast_ref::<ClassedError>();
    let mut record = one(
        classed.map_or(CHANNEL_ERROR, ClassedError::class),
        error.to_string(),
    );
    if let Some(classed) = classed {
        record.attributes.clone_from(classed.attributes());
    }
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

/// What a turn whose work ended cancelled before its output reads, as
/// Python's turn backend reports it.
const ENDED_BEFORE_ITS_OUTPUT: &str = "The turn ended before its output";

/// What a turn whose stream ended cancelled reads, as Python's turn backend
/// reports it.
const ENDED_IN_ITS_STREAM: &str = "The turn ended in the middle of its stream";

/// `error`, an agent's `on_event()` failing, as it crosses, as Python's
/// turn backend reports an agent's raised error (RMK-1047): see
/// [`failure`].
pub(crate) fn turn_failure(error: &(dyn Error + Send + Sync + 'static)) -> WireError {
    failure(error, ENDED_BEFORE_ITS_OUTPUT)
}

/// `error`, an agent's response stream failing, as it crosses: see
/// [`failure`], a cancel as the turn ending in the middle of its stream.
pub(crate) fn stream_failure(error: &(dyn Error + Send + Sync + 'static)) -> WireError {
    failure(error, ENDED_IN_ITS_STREAM)
}

/// `error` as it crosses: a kit error under its Python class's name, as
/// a transport's failure reads (`RoomKitError::Value` as `ValueError`, a
/// provider's refusal as `ProviderDeliveryError`), which `ON_ERROR` names
/// as its `error_type` as Python's `error_type_name` does (RMK-1128), the
/// `code` and `retryable` it declares among its attributes, as Python's
/// error crosses with its JSON attributes (RMK-1085); a cancel as Python's `TurnLinkError`, saying `cancelled`,
/// its reason `executor_lost`; any other as a channel's.
fn failure(error: &(dyn Error + Send + Sync + 'static), cancelled: &str) -> WireError {
    match error.downcast_ref::<RoomKitError>() {
        Some(RoomKitError::Cancelled) => {
            let mut crossed = one(TURN_LINK_ERROR, cancelled.to_owned());
            crossed
                .attributes
                .insert("reason".to_owned(), Value::from("executor_lost"));
            crossed
        }
        Some(kit_error) => {
            let mut crossed = one(kit_error.class_name(), error.to_string());
            if let (Some(code), Some(retryable)) = (kit_error.code(), kit_error.retryable()) {
                crossed
                    .attributes
                    .insert(CODE.to_owned(), Value::from(code));
                crossed
                    .attributes
                    .insert(RETRYABLE.to_owned(), Value::Bool(retryable));
            }
            crossed
        }
        None => channel_error(error),
    }
}

/// The attribute naming the code an error declares.
const CODE: &str = "code";

/// The attribute saying whether a retry of what an error failed may
/// succeed.
const RETRYABLE: &str = "retryable";

/// A turn's failure as its owner reads it: its record, and, for an error
/// that crossed declaring its code and retryability, the delivery error it
/// declares (RMK-1085).
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct TurnFailure {
    pub(crate) record: ErrorRecord,
    pub(crate) declared: Option<DeliveryError>,
}

impl From<ErrorRecord> for TurnFailure {
    fn from(record: ErrorRecord) -> Self {
        Self {
            record,
            declared: None,
        }
    }
}

impl TurnFailure {
    /// The failure of `error`, an error that crossed: what it declares
    /// read from its `code` and `retryable`, as Python's `DeliveryResult`
    /// reads them off the error its turn backend rebuilds.
    pub(crate) fn crossed(error: &WireError) -> Self {
        let declared = error
            .attributes
            .get(RETRYABLE)
            .and_then(Value::as_bool)
            .map(|retryable| DeliveryError {
                code: error
                    .attributes
                    .get(CODE)
                    .and_then(Value::as_str)
                    .map_or_else(|| error.kind.clone(), str::to_owned),
                message: error.message.clone(),
                retryable,
            });
        Self {
            record: to_record(error),
            declared,
        }
    }

    /// The core's outcome of the turn: an error declaring its delivery
    /// error as [`ChannelOutcome::Raised`], any other as failed.
    pub(crate) fn into_outcome(self) -> Outcome {
        match self.declared {
            Some(delivery) => ChannelOutcome::Raised(Box::new(Raised {
                error: self.record,
                delivery,
            }))
            .into(),
            None => Outcome::Failed(self.record),
        }
    }
}

/// A core error record as it crosses.
pub(crate) fn from_record(record: &ErrorRecord) -> WireError {
    WireError {
        attributes: (*record.attributes).clone(),
        ..one(record.error_type.clone(), record.message.clone())
    }
}

/// The core error record a crossed error is, on the owner, its attributes
/// kept but a kit error's, which name its variant.
pub(crate) fn to_record(error: &WireError) -> ErrorRecord {
    let record = ErrorRecord::new(error.kind.clone(), error.message.clone());
    if error.kind == KIT_ERROR {
        return record;
    }
    ErrorRecord {
        attributes: Box::new(error.attributes.clone()),
        ..record
    }
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
/// unknown channel, as itself; a link's failure as Python's
/// `TurnLinkError`; any other as its `RemoteTurnError`, keeping its kind,
/// its message and its bases.
pub(crate) fn rebuild_kit_error(error: &WireError) -> RoomKitError {
    if error.kind == KIT_ERROR
        && let Some(rebuilt) = rebuilt_variant(error)
    {
        return rebuilt;
    }
    // A builtin crosses as itself, as the Python implementation's codec
    // rebuilds it: a `ValueError` is the kit's.
    if error.kind == VALUE_ERROR {
        return RoomKitError::Value(error.message.clone());
    }
    if error.kind == RUNTIME_ERROR {
        return RoomKitError::Runtime(error.message.clone());
    }
    if is_link_reason(&error.kind) {
        return TurnError::new(error.kind.clone(), error.message.clone()).into_kit_error();
    }
    RoomKitError::RemoteTurn {
        message: error.message.clone(),
        kind: error.kind.clone(),
        bases: error.bases.clone(),
    }
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
            RoomKitError::ChannelNotRegistered {
                channel_id: ChannelId::new(id).ok()?,
                message: error.message.clone(),
            }
        }
        "RoomNotFound" => {
            let id = error.attributes.get("room_id")?.as_str()?;
            RoomKitError::RoomNotFound(RoomId::new(id).ok()?)
        }
        "Value" => RoomKitError::Value(error.message.clone()),
        "Runtime" => RoomKitError::Runtime(error.message.clone()),
        "Type" => RoomKitError::Type(error.message.clone()),
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
        RoomKitError::RoomNotFound(room_id) => {
            fields.insert("room_id".to_owned(), Value::from(room_id.as_str()));
            "RoomNotFound"
        }
        RoomKitError::RoomAlreadyExists(_) => "RoomAlreadyExists",
        RoomKitError::RoomClosed { .. } => "RoomClosed",
        RoomKitError::TransitionRefused { .. } => "TransitionRefused",
        RoomKitError::ProcessTimeout { .. } => "ProcessTimeout",
        RoomKitError::ChangeRefused { .. } => "ChangeRefused",
        RoomKitError::ReentrantCall { .. } => "ReentrantCall",
        RoomKitError::ReentryLoopCap(_) => "ReentryLoopCap",
        RoomKitError::ChannelNotFound { .. } => "ChannelNotFound",
        RoomKitError::ChannelNotRegistered { channel_id, .. } => {
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
        RoomKitError::Timeout => "Timeout",
        RoomKitError::ExceptionGroup { .. } => "ExceptionGroup",
        RoomKitError::KitClosed => "KitClosed",
        RoomKitError::ShutdownCancelled => "ShutdownCancelled",
        RoomKitError::Store(_) => "Store",
        RoomKitError::Lock(_) => "Lock",
        RoomKitError::Channel { .. } => "Channel",
        RoomKitError::Router(_) => "Router",
        RoomKitError::Identity(_) => "Identity",
        RoomKitError::RemoteTurn { .. } => "RemoteTurn",
        RoomKitError::TurnLink { .. } => "TurnLink",
        RoomKitError::Internal(_) => "Internal",
        RoomKitError::HumanInputRejected(_) => "HumanInputRejected",
        RoomKitError::Runtime(_) => "Runtime",
        RoomKitError::ToolNameCollision(_) => "ToolNameCollision",
        RoomKitError::VerificationRefused { .. } => "VerificationRefused",
        RoomKitError::Type(_) => "Type",
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

    /// An agent's turn cancelled on its executor crosses as Python's
    /// `TurnLinkError("executor_lost", ...)` (roomkit
    /// `src/roomkit/turns/_owner_turn.py`), its reason among its
    /// attributes, which the owner's record keeps (RMK-1239).
    #[test]
    fn a_cancelled_turn_crosses_as_its_executor_lost() {
        let records = [
            turn_failure(&RoomKitError::Cancelled),
            stream_failure(&RoomKitError::Cancelled),
        ]
        .map(|crossed| {
            let record = to_record(&crossed);
            (
                record.error_type,
                record.message,
                record.attributes.get("reason").cloned(),
            )
        });
        let lost = |message: &str| {
            (
                TURN_LINK_ERROR.to_owned(),
                message.to_owned(),
                Some(Value::from("executor_lost")),
            )
        };
        assert_eq!(
            records,
            [lost(ENDED_BEFORE_ITS_OUTPUT), lost(ENDED_IN_ITS_STREAM)]
        );
    }

    /// RMK-1106: a link's failure is recorded under `TurnLinkError` in the
    /// words Python's `TurnLinkError` writes for its reason, no reason
    /// heading them (roomkit `src/roomkit/core/exceptions.py`,
    /// `turns/base.py`, `turns/_owner_turn.py`).
    #[test]
    fn a_link_failure_is_recorded_in_python_s_words() {
        use crate::turns::pipe::LinkFailure;
        let records: Vec<(String, String, Option<Value>)> = [
            TurnError::link_reason("backend_closed"),
            TurnError::link_reason("cancelled"),
            TurnError::link(&LinkFailure::Closed),
            TurnError::link(&LinkFailure::Lost {
                expected: 4,
                got: 6,
            }),
            TurnError::link(&LinkFailure::Invalid("no kind".to_owned())),
        ]
        .iter()
        .map(|error| {
            let record = error.to_record();
            let reason = record.attributes.get("reason").cloned();
            (record.error_type, record.message, reason)
        })
        .collect();
        // Its reason among its attributes, as Python's `error_record`
        // keeps a `TurnLinkError`'s (RMK-1239).
        let link = |message: &str, reason: &str| {
            (
                TURN_LINK_ERROR.to_owned(),
                message.to_owned(),
                Some(Value::from(reason)),
            )
        };
        assert_eq!(
            records,
            [
                link("The turn ended: its turn backend closed", "backend_closed"),
                link(
                    "The turn ended without its executor: cancelled",
                    "cancelled"
                ),
                link("The turn ended: its turn backend closed", "backend_closed"),
                link("Message 4 of the turn was lost", "frame_lost"),
                link(
                    "The turn's frames could not be read: no kind",
                    "frame_invalid"
                ),
            ]
        );
    }

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

    /// RFC §10.6.8, as Python's codec rebuilds a builtin as itself
    /// (`turns/_error_codec.py` `_REBUILT_PACKAGES`): a `RuntimeError`
    /// crossing comes back as the kit's `Runtime`, and the kit's `Runtime`
    /// crosses and comes back as itself.
    #[test]
    fn a_runtime_error_comes_back_as_itself() {
        let crossed = one(
            "RuntimeError",
            "setup_background() already called".to_owned(),
        );
        let sent = || RoomKitError::Runtime("needs a turn".to_owned());
        assert_eq!(
            [
                rebuild_kit_error(&crossed),
                rebuild_kit_error(&kit_error(&sent()))
            ],
            [
                RoomKitError::Runtime("setup_background() already called".to_owned()),
                sent(),
            ]
        );
    }

    /// RFC §10.6.8: an error of a class the side rebuilds comes back of that
    /// class; any other as a stand-in keeping its kind and message.
    #[test]
    fn a_kit_error_comes_back_as_itself_or_as_a_turn_error() {
        let unknown = || RoomKitError::channel_not_registered(ChannelId::new("sms").unwrap());
        let reentrant = || RoomKitError::ReentrantCall {
            room_id: RoomId::new("room_1").unwrap(),
        };
        let rebuilt = [RoomKitError::KitClosed, unknown(), reentrant()]
            .map(|error| rebuild_kit_error(&kit_error(&error)));
        assert_eq!(
            rebuilt,
            [
                RoomKitError::KitClosed,
                unknown(),
                RoomKitError::RemoteTurn {
                    message: reentrant().to_string(),
                    kind: "RoomKitError".to_owned(),
                    bases: Vec::new(),
                }
            ]
        );
    }
}

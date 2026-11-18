//! What crosses the turn backend in place of an error, and back (RFC
//! §10.6.8): each side writes an error as a record, its kind, its message,
//! what classifies it and its causes, and rebuilds the errors it knows.

mod store;

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
pub(super) const CAUSES_KEPT: usize = 16;

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
            detail: None,
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

/// The JSON attributes of the application's pre-send refusal. The structured
/// provider result stays on the local delivery. Python's error record carries
/// the strings and retry flag of the exception itself.
fn recipient_error(error: &(dyn Error + 'static)) -> Option<WireError> {
    let RoomKitError::NoRecipient {
        channel_id,
        recipient_key,
        ..
    } = error.downcast_ref::<RoomKitError>()?
    else {
        return None;
    };
    let mut crossed = one("NoRecipientError", error.to_string());
    crossed.attributes = Metadata::from_iter([
        (CODE.into(), "no_recipient".into()),
        (RETRYABLE.into(), false.into()),
        ("channel_id".into(), channel_id.as_str().into()),
        ("recipient_key".into(), recipient_key.as_str().into()),
    ]);
    Some(crossed)
}

/// The core record of `error`, a channel's or one that crossed: one that
/// crossed keeps its kind, a link's failure is recorded under
/// [`TURN_LINK_ERROR`], any other is a channel's, under the class a
/// [`ClassedError`] names.
pub(crate) fn record_of(error: &(dyn Error + Send + Sync + 'static)) -> ErrorRecord {
    if let Some(missing) = recipient_error(error) {
        return to_record(&missing);
    }
    if let Some(turn) = error.downcast_ref::<TurnError>() {
        return turn.to_record();
    }
    if let Some(RemoteError(crossed)) = error.downcast_ref::<RemoteError>() {
        return to_record(crossed);
    }
    if let Some(record) = crate::text_errors::text_record(error) {
        return record;
    }
    let classed = error.downcast_ref::<ClassedError>();
    ErrorRecord {
        causes: cause_records(error),
        attributes: Box::new(
            classed
                .map(ClassedError::attributes)
                .cloned()
                .unwrap_or_default(),
        ),
        ..ErrorRecord::new(
            classed.map_or(CHANNEL_ERROR, ClassedError::record_class),
            error.to_string(),
        )
    }
}

/// `error`, a channel's, as it crosses, its causes after it.
pub(crate) fn channel_error(error: &(dyn Error + 'static)) -> WireError {
    if let Some(record) = crate::text_errors::text_record(error) {
        return from_record(&record);
    }
    let mut record = channel_record(error);
    record.causes = cause_records(error).iter().map(wire_record).collect();
    record
}

/// The native source chain, nearest first, bounded just like Python's error codec.
pub(crate) fn cause_records(error: &(dyn Error + 'static)) -> Vec<ErrorRecord> {
    let mut causes = Vec::new();
    let mut parent = error;
    while let Some(cause) = parent.source() {
        if causes.len() == CAUSES_KEPT {
            break;
        }
        let record = if let Some(wrapper) = cause.downcast_ref::<crate::TaskTurnFailedError>() {
            // The enclosing chain already walks the wrapper's source.
            wrapper.record_head()
        } else if let Some(known) = cause.downcast_ref::<RoomKitError>() {
            to_record(&described_kit_error(known))
        } else if let Some(known) = crate::text_errors::text_record(cause) {
            known
        } else if let Some(classed) = cause.downcast_ref::<ClassedError>() {
            ErrorRecord {
                attributes: Box::new(classed.attributes().clone()),
                ..ErrorRecord::new(classed.record_class(), classed.to_string())
            }
        } else {
            // A wrapper's original failure is the worker channel's error.
            let kind = if parent.is::<crate::TaskTurnFailedError>() {
                CHANNEL_ERROR
            } else {
                "Error"
            };
            ErrorRecord::new(kind, cause.to_string())
        };
        causes.push(record);
        parent = cause;
    }
    causes
}

/// The top-level channel error before the transport includes its causes.
fn channel_record(error: &(dyn Error + 'static)) -> WireError {
    if let Some(missing) = recipient_error(error) {
        return missing;
    }
    let classed = error.downcast_ref::<ClassedError>();
    let mut record = one(
        classed.map_or(CHANNEL_ERROR, ClassedError::record_class),
        error.to_string(),
    );
    if let Some(classed) = classed {
        record.attributes.clone_from(classed.attributes());
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
        Some(kit_error) => described_kit_error(kit_error),
        None => channel_error(error),
    }
}

/// The public class and JSON attributes of a kit error, including when a
/// delegated worker supplies it as the cause of its own failure.
fn described_kit_error(error: &RoomKitError) -> WireError {
    if let Some(missing) = recipient_error(error) {
        return missing;
    }
    let mut crossed = one(error.class_name(), error.to_string());
    if let (Some(code), Some(retryable)) = (error.code(), error.retryable()) {
        crossed
            .attributes
            .insert(CODE.to_owned(), Value::from(code));
        crossed
            .attributes
            .insert(RETRYABLE.to_owned(), Value::Bool(retryable));
    }
    crossed
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
    declared: Option<Box<DeliveryError>>,
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
            declared: declared.map(Box::new),
        }
    }

    /// The core's outcome of the turn: an error declaring its delivery
    /// error as [`ChannelOutcome::Raised`], any other as failed.
    pub(crate) fn into_outcome(self) -> Outcome {
        match self.declared {
            Some(delivery) => ChannelOutcome::Raised(Box::new(Raised {
                error: self.record,
                delivery: *delivery,
            }))
            .into(),
            None => Outcome::Failed(self.record),
        }
    }
}

/// A core error record as it crosses.
pub(crate) fn from_record(record: &ErrorRecord) -> WireError {
    let mut error = wire_record(record);
    append_causes(&record.causes, &mut error.causes);
    error
}

/// Rust callers can build records recursively. The wire uses one flat chain
/// and Python bounds it at 16, which also bounds the recursion here.
fn append_causes(records: &[ErrorRecord], causes: &mut Vec<WireError>) {
    for record in records {
        if causes.len() == CAUSES_KEPT {
            return;
        }
        causes.push(wire_record(record));
        append_causes(&record.causes, causes);
    }
}

fn wire_record(record: &ErrorRecord) -> WireError {
    let mut error = one(record.error_type.clone(), record.message.clone());
    error.attributes.clone_from(&record.attributes);
    error
}

/// The core record of a failure from a turn, including its flat cause chain.
pub(crate) fn to_record(error: &WireError) -> ErrorRecord {
    let mut record = local_record(error);
    if error.kind == KIT_ERROR {
        // The outer kit envelope uses attributes to encode its private variant.
        record.attributes.clear();
    }
    record.causes = error
        .causes
        .iter()
        .take(CAUSES_KEPT)
        .map(local_record)
        .collect();
    record
}

fn local_record(error: &WireError) -> ErrorRecord {
    ErrorRecord {
        attributes: Box::new(error.attributes.clone()),
        ..ErrorRecord::new(error.kind.clone(), error.message.clone())
    }
}

/// `error`, a kit's, as it crosses: its variant among its attributes.
pub(crate) fn kit_error(error: &RoomKitError) -> WireError {
    if let RoomKitError::RemoteTurn {
        kind,
        message,
        bases,
        detail,
    } = error
    {
        if let Some(detail) = detail {
            return detail.record().clone();
        }
        let mut record = one(kind, message.clone());
        record.bases.clone_from(bases);
        return record;
    }
    let mut record = one(KIT_ERROR, error.to_string());
    let (variant, fields) = variant_of(error);
    record
        .attributes
        .insert(VARIANT.to_owned(), Value::from(variant));
    record.attributes.extend(fields);
    if let RoomKitError::Store(failure) = error {
        record.causes = store::causes(&**failure);
    } else if let RoomKitError::Realtime(failure) = error {
        record.causes = store::causes(&**failure);
    }
    record
}

/// The kit error a crossed error is: a variant that carries no data, or an
/// unknown channel, as itself; a link's failure as Python's
/// `TurnLinkError`; any other as its `RemoteTurnError`, keeping its kind,
/// its message and its bases.
pub(crate) fn rebuild_kit_error(error: &WireError) -> RoomKitError {
    if matches!(
        error.kind.as_str(),
        KIT_ERROR | "roomkit.core.exceptions:RoomKitError"
    ) && let Some(rebuilt) = error
        .attributes
        .get(VARIANT)
        .and_then(Value::as_str)
        .and_then(|variant| rebuilt_variant(error, variant))
    {
        return rebuilt;
    }
    // A builtin crosses as itself, as the Python implementation's codec
    // rebuilds it: a `ValueError` is the kit's.
    if matches!(error.kind.as_str(), VALUE_ERROR | "builtins:ValueError") {
        return RoomKitError::Value(error.message.clone());
    }
    if matches!(error.kind.as_str(), RUNTIME_ERROR | "builtins:RuntimeError") {
        return RoomKitError::Runtime(error.message.clone());
    }
    match error.kind.as_str() {
        "builtins:TypeError" => return RoomKitError::Type(error.message.clone()),
        "roomkit.core.exceptions:TurnLinkError" => {
            if let Some(reason) = error.attributes.get("reason").and_then(Value::as_str) {
                return RoomKitError::TurnLink {
                    reason: reason.to_owned(),
                    message: Some(error.message.clone()),
                };
            }
        }
        kind => {
            if let Some(variant) = kind
                .strip_prefix("roomkit.core.exceptions:")
                .and_then(|name| name.strip_suffix("Error"))
                && let Some(rebuilt) = rebuilt_variant(error, variant)
                && rebuilt.to_string() == error.message
            {
                return rebuilt;
            }
        }
    }
    if is_link_reason(&error.kind) {
        return TurnError::new(error.kind.clone(), error.message.clone()).into_kit_error();
    }
    super::error_classes::remote_error(error, None)
}

fn rebuilt_variant(error: &WireError, variant: &str) -> Option<RoomKitError> {
    Some(match variant {
        "NoRecipient" => RoomKitError::no_recipient(
            error.attributes.get("channel_id")?.as_str()?,
            error.attributes.get("recipient_key")?.as_str()?,
        ),
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
        "Store" => store::rebuild(error)?,
        "Realtime" => store::rebuild_realtime(error)?,
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
        RoomKitError::NoRecipient {
            channel_id,
            recipient_key,
            ..
        } => {
            fields.insert("channel_id".into(), channel_id.as_str().into());
            fields.insert("recipient_key".into(), recipient_key.as_str().into());
            "NoRecipient"
        }
        RoomKitError::Value(_) => "Value",
        RoomKitError::Cancelled => "Cancelled",
        RoomKitError::Timeout => "Timeout",
        RoomKitError::ExceptionGroup { .. } => "ExceptionGroup",
        RoomKitError::KitClosed => "KitClosed",
        RoomKitError::ShutdownCancelled => "ShutdownCancelled",
        RoomKitError::Store(_) => "Store",
        RoomKitError::Realtime(_) => "Realtime",
        RoomKitError::Lock(_) => "Lock",
        RoomKitError::Channel { .. } => "Channel",
        RoomKitError::Router(_) => "Router",
        RoomKitError::Identity(_) => "Identity",
        RoomKitError::RemoteTurn { .. } => "RemoteTurn",
        RoomKitError::TurnLink { reason, .. } => {
            fields.insert("reason".into(), Value::from(reason.clone()));
            "TurnLink"
        }
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

    #[test]
    fn a_worker_keeps_the_attributes_of_its_cause_named_roomkit_error() {
        let mut worker = ErrorRecord::new("TaskTurnFailedError", "failure");
        let mut cause = ErrorRecord::new("RoomKitError", "failure");
        cause
            .attributes
            .insert("request_id".into(), Value::from("r1"));
        worker.causes.push(cause);
        assert_eq!(to_record(&from_record(&worker)), worker);
    }

    #[test]
    fn the_wire_retains_the_sixteen_nearest_causes() {
        let mut record = ErrorRecord::new("outer", "");
        let mut tail = &mut record;
        for number in 1..=20 {
            tail.causes
                .push(ErrorRecord::new(format!("error{number}"), ""));
            tail = tail.causes.last_mut().unwrap();
        }
        let crossed = from_record(&record);
        let cause_types = (1..=16).map(|n| format!("error{n}")).collect::<Vec<_>>();
        assert_eq!(
            crossed
                .causes
                .iter()
                .map(|item| &item.kind)
                .collect::<Vec<_>>(),
            cause_types.iter().collect::<Vec<_>>()
        );
        assert!(crossed.causes.iter().all(|item| item.causes.is_empty()));
        let mut oversized = crossed;
        oversized.causes.push(one("discarded", String::new()));
        let restored = to_record(&oversized);
        assert_eq!(
            restored
                .causes
                .into_iter()
                .map(|item| item.error_type)
                .collect::<Vec<_>>(),
            cause_types
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
        assert_eq!(
            rebuild_kit_error(&kit_error(&RoomKitError::KitClosed)),
            RoomKitError::KitClosed
        );
        assert_eq!(rebuild_kit_error(&kit_error(&unknown())), unknown());
        let original = kit_error(&reentrant());
        let RoomKitError::RemoteTurn {
            kind,
            message,
            detail: Some(detail),
            ..
        } = rebuild_kit_error(&original)
        else {
            panic!("expected a remote error preserving the original record");
        };
        assert_eq!(kind, original.kind);
        assert_eq!(message, original.message);
        assert_eq!(detail.record(), &original);
        assert!(detail.native_error().is_none());
    }

    #[test]
    fn a_delegated_provider_failure_keeps_its_declared_retry_policy() {
        let provider = RoomKitError::ProviderDelivery {
            result: roomkit_core::model::ProviderResult {
                error: Some("unreachable".to_owned()),
                metadata: Metadata::from_iter([
                    ("code".to_owned(), Value::from("E42")),
                    ("retryable".to_owned(), Value::Bool(true)),
                ]),
                ..roomkit_core::model::ProviderResult::new(false)
            },
        };
        let wrapped = crate::TaskTurnFailedError::new(std::sync::Arc::new(provider), "error", None);
        let record = ErrorRecord::from(&wrapped);
        assert_eq!(record.causes.len(), 1);
        let cause = &record.causes[0];
        assert_eq!(cause.error_type, "ProviderDeliveryError");
        assert_eq!(cause.attributes["code"], "E42");
        assert_eq!(cause.attributes["retryable"], true);
        assert_eq!(to_record(&channel_error(&wrapped)), record);
    }

    #[test]
    fn a_missing_recipient_keeps_its_class_and_json_attributes() {
        #[derive(Debug, thiserror::Error)]
        #[error("delivery callback failed")]
        struct Wrapped(#[source] RoomKitError);

        let error = RoomKitError::no_recipient("app", "customer");
        let record = record_of(&error);
        assert_eq!(record.error_type, "NoRecipientError");
        assert_eq!(record.message, "no_recipient");
        assert_eq!(
            serde_json::json!(*record.attributes),
            serde_json::json!({
                "code":"no_recipient","retryable":false,"channel_id":"app","recipient_key":"customer"
            })
        );
        assert_eq!(to_record(&turn_failure(&error)), record);
        assert_eq!(to_record(&channel_error(&error)), record);
        assert_eq!(rebuild_kit_error(&kit_error(&error)), error);

        let worker = crate::TaskTurnFailedError::new(
            std::sync::Arc::new(RoomKitError::no_recipient("app", "customer")),
            "interrupted",
            None,
        );
        assert_eq!(
            ErrorRecord::from(&worker).causes.as_slice(),
            std::slice::from_ref(&record)
        );
        assert_eq!(to_record(&channel_error(&worker).causes[0]), record);

        let wrapped = channel_error(&Wrapped(error));
        assert_eq!(wrapped.causes.len(), 1);
        assert_eq!(to_record(&wrapped.causes[0]), record);
    }
}

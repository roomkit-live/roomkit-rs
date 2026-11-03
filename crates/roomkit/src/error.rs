//! What a kit's calls fail with: [`RoomKitError`], the Python
//! implementation's `RoomKitError`, each variant one of its exception
//! classes (step 5, decision 9).

use std::fmt;

use roomkit_core::model::{
    ChannelId, ChannelType, DeliveryError, ErrorRecord, EventId, IdentityId, ModelError,
    ParticipantId, ProviderResult, RoomId, RoomStatus,
};
use roomkit_core::ops::{
    ConfigError, InboundError, KIT_CLOSED, LifecycleError, MemberError, MutationError,
    ProtocolError,
};
use roomkit_core::rules::hook_registry::RegistrationError;
use roomkit_core::rules::lifecycle::Transition;
/// A name as Python's `repr` of a `str` writes it, which the Python
/// implementation's messages quote their names with (`{name!r}`).
pub(crate) use roomkit_core::rules::python_repr as repr;
use thiserror::Error;

use crate::store::{StoreError, StoreFailure};
use crate::turns;

/// Why a kit call failed: the Python implementation's `RoomKitError`, each
/// variant named after one of its exception classes without the `Error`
/// suffix, which clippy refuses on every variant of an enum.
///
/// The per-operation errors of `roomkit-core` (`InboundError`,
/// `LifecycleError`, ...) convert into it. A room of another organization
/// reads as [`RoomNotFound`](Self::RoomNotFound), as RFC §17.2 requires.
#[derive(Debug, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum RoomKitError {
    /// Python's `RoomNotFoundError`: the room does not exist, or belongs to
    /// another organization than the one the call acts for (RFC §17.2).
    #[error("room {0} not found")]
    RoomNotFound(RoomId),
    /// Python's `RoomAlreadyExistsError`: a room was created under an id the
    /// store holds (RFC §14.3). Nothing was written.
    #[error("room {0} exists already")]
    RoomAlreadyExists(RoomId),
    /// Python's `RoomClosedError`: the room's status refuses new events
    /// (RFC §5.1). Nothing was written.
    #[error("room {room_id} does not accept new events")]
    RoomClosed {
        /// The room.
        room_id: RoomId,
    },
    /// Python's `TransitionRefusedError`: the room's status is neither one
    /// the transition leaves from nor the one it leads to (RFC §5.1).
    /// Nothing was written.
    #[error("room {room_id} is {}, which {transition} does not leave from", status_name(*.status))]
    TransitionRefused {
        /// The room.
        room_id: RoomId,
        /// The room's status, as the transition found it.
        status: RoomStatus,
        /// The transition refused: `pause`, `resume`, `reopen`, `close` or
        /// `archive`.
        transition: String,
    },
    /// Python's `ProcessTimeoutError`: `process_timeout` expired before the
    /// commit point (RFC §13.6). Nothing was written.
    #[error("room {room_id}: process_timeout expired before the event was committed")]
    ProcessTimeout {
        /// The room.
        room_id: RoomId,
    },
    /// Python's `ChangeRefusedError(reason)`: an edit or a delete was
    /// refused at RFC §10.1 step 8a, nothing written. `reason` is
    /// `target_event_not_found`, `identity_required_for_edit`,
    /// `not_authorized` or `not_original_author`.
    #[error("edit or delete refused: {reason}")]
    ChangeRefused {
        /// Why.
        reason: String,
    },
    /// Python's `ReentrantCallError`: a call made from inside a room's own
    /// work, under its lock or in its admission order, could not be adopted
    /// (RFC §13.5, §9.5.1 Reentrance): a message routed back into the room,
    /// whose room is known only once routed. Nothing was written. Its
    /// message is Python's, which names the room alone.
    #[error(
        "A message routed into room {room_id} from its own work (under its lock, in an off-lock \
         hook or a parse) cannot be adopted: name the room to send into it"
    )]
    ReentrantCall {
        /// The room whose lock or admission order the caller holds.
        room_id: RoomId,
    },
    /// Python's `ReentryLoopCapError`: a change from a hook's body is past
    /// the reentry budget it runs on (RFC §8.3, §9.5.1). Nothing was
    /// written.
    #[error("room {0}: the change is past its reentry budget")]
    ReentryLoopCap(RoomId),
    /// Python's `ChannelNotFoundError`: the room does not bind the channel.
    #[error("channel {channel_id} not in room {room_id}")]
    ChannelNotFound {
        /// The room.
        room_id: RoomId,
        /// The channel.
        channel_id: ChannelId,
    },
    /// Python's `ChannelNotRegisteredError`: no channel is registered under
    /// the id. Its message is the site's, as Python's exception carries
    /// its own: `Channel sms not registered` for most,
    /// `Agent channel 'worker' not registered` for a delegation's agent.
    #[error("{message}")]
    ChannelNotRegistered {
        /// The id no channel is registered under.
        channel_id: ChannelId,
        /// What the error says.
        message: String,
    },
    /// Python's `ChannelAlreadyRegisteredError`: another channel is
    /// registered under the id.
    #[error(
        "Channel {} is already registered. Call unregister_channel({}) first to replace it \
         deliberately.",
        repr(.0.as_str()),
        repr(.0.as_str())
    )]
    ChannelAlreadyRegistered(ChannelId),
    /// Python's `ParticipantNotFoundError`: a member call names no
    /// participant of the room (RFC §5.5), `channel_id` `None`; or a
    /// post's speaker names no participant of the room the host channel
    /// `channel_id` reaches, by their id or their identity's (RFC §10.5,
    /// §5.5). Nothing was written.
    #[error(
        "{}",
        participant_not_found(room_id, participant_id, channel_id.as_ref(), *reached_elsewhere)
    )]
    ParticipantNotFound {
        /// The room.
        room_id: RoomId,
        /// The participant named: a post's speaker, a member call's.
        participant_id: ParticipantId,
        /// The host channel a post speaks from; `None` for a member call.
        channel_id: Option<ChannelId>,
        /// Whether a post's id names a participant of the room reached
        /// through other channels only, or nobody of the room.
        reached_elsewhere: bool,
    },
    /// Python's `IdentityNotFoundError`.
    #[error("no identity {0} in the organization")]
    IdentityNotFound(IdentityId),
    /// Python's `AddressLinkedError`: another identity of the organization
    /// uses the address on channels of the type already (RFC §5.6). As
    /// Python's, it leaves out the address, which the caller passed, and
    /// the identity that holds it, another's.
    #[error("This {channel_type} address is linked to another identity of the organization")]
    AddressLinked {
        /// The channel type.
        channel_type: ChannelType,
    },
    /// Python's `EventNotFoundError`: the event, or a change's target, is
    /// not in its room (RFC §14.3).
    #[error("event {0} does not exist")]
    EventNotFound(EventId),
    /// Python's `IdempotencyKeyTakenError`: the source channel holds the key
    /// in the room for another event (RFC §13.4, §14.3). Nothing was
    /// written.
    #[error("room {room_id} holds an event of channel {channel_id} with the key {key:?}")]
    IdempotencyKeyTaken {
        /// The room.
        room_id: RoomId,
        /// The event's source channel.
        channel_id: ChannelId,
        /// The key.
        key: String,
    },
    /// Python's `ProviderDeliveryError(result)`: a provider's explicit
    /// refusal, which a channel's `on_event()` or `deliver()` returns as its
    /// error, boxed as a [`ChannelError`](crate::ChannelError). The
    /// delivery's result reads it as Python's does (RFC §5.13): the code
    /// `result.metadata["code"]`, else `result.error`, else
    /// `ProviderDeliveryFailed`; retryable as `result.metadata["retryable"]`
    /// says when it is a bool, else retryable; `result` kept. Its message is
    /// `result.error`, else `provider_send_failed`.
    #[error("{}", DeliveryError::of_provider(result).message)]
    ProviderDelivery {
        /// The provider's answer.
        result: ProviderResult,
    },
    /// Python's builtin `ValueError`: an argument the call refuses, as the
    /// Python implementation's message says.
    #[error("{0}")]
    Value(String),
    /// Python's `asyncio.CancelledError`: the call's run was cancelled
    /// before it ended, by the runtime's shutdown, or the call that adopted
    /// it was dropped before deciding it: what it wrote by then is not
    /// known.
    #[error("the call was cancelled before it ended")]
    Cancelled,
    /// Python's `ExceptionGroup` of [`RoomKit::close`](crate::RoomKit::close):
    /// every channel closed but these, whose close panicked, each with what
    /// its panic said; the rest of the shutdown ran to its end (RFC
    /// §12.10.4), and every close is answered so.
    #[error("{} channel(s) failed to close; the rest of the shutdown ran to completion", failures.len())]
    ExceptionGroup {
        /// Each channel that failed to close, and what its panic said.
        failures: Vec<(ChannelId, String)>,
    },
    /// The kit is closing or closed ([`RoomKit::close`](crate::RoomKit::close),
    /// RFC §12.10.4): the call was made once the close started, or the
    /// close cut it before its commit, nothing of it stored. Python's
    /// `KitClosedError`, a `RoomKitError` whose code is `kit_closed`,
    /// retryable.
    #[error("the kit is closed")]
    KitClosed,
    /// The kit's shutdown never ended, its task dropped with its runtime or
    /// panicked: its channels and its store may not all be closed (RFC
    /// §12.10.4). Python's `ShutdownCancelledError`, a `RoomKitError`.
    #[error(
        "the kit's shutdown was cancelled before it ended: its channels and its store may not \
         all be closed"
    )]
    ShutdownCancelled,
    /// The store failed, as Python propagates its driver's exception: the
    /// error it met is the cause.
    #[error("the store failed")]
    Store(#[source] StoreFailure),
    /// The room's lock could not be taken, as Python propagates the lock
    /// manager's exception.
    #[error("the room lock failed: {}", .0.message)]
    Lock(ErrorRecord),
    /// A channel failed what the call asked of it, as Python propagates the
    /// channel's exception: a parse of the message, for instance.
    #[error("channel {channel_id} failed: {}", .error.message)]
    Channel {
        /// The channel.
        channel_id: ChannelId,
        /// What it raised.
        error: ErrorRecord,
    },
    /// The kit's router failed to route the message (RFC §10.4), as Python
    /// propagates the router's exception.
    #[error("the router failed: {}", .0.message)]
    Router(ErrorRecord),
    /// The kit's identity resolver failed (RFC §11.1), as Python propagates
    /// the resolver's exception. Nothing was written.
    #[error("the identity resolver failed: {}", .0.message)]
    Identity(ErrorRecord),
    /// Python's `RemoteTurnError(message, *, kind, bases=())`: an error
    /// raised across the turn backend (RFC §10.6.8), by a turn's host verb
    /// that its owner refused or by the owner's work, of a class this side
    /// does not rebuild. `kind` is the class it had, `bases` the classes
    /// that class derives from, nearest first.
    #[error("{message}")]
    RemoteTurn {
        /// What it says.
        message: String,
        /// The class it had on the other side.
        kind: String,
        /// The classes that class derives from, nearest first.
        bases: Vec<String>,
    },
    /// Python's `TurnLinkError(reason, message=None)`: a turn the turn
    /// backend could not carry to its end (RFC §10.6.8). `reason` is
    /// `no_executor`, `executor_lost`, `owner_lost`, `frame_lost`,
    /// `frame_invalid`, `backend_closed` or `cancelled`; `message`, when
    /// set, says what failed in place of Python's default.
    #[error("{}", turn_link_message(reason, message.as_deref()))]
    TurnLink {
        /// The link's failure reason.
        reason: String,
        /// What it says, `None` for Python's default.
        message: Option<String>,
    },
    /// A defect of this crate, where Python asserts: the driver broke the
    /// core's effect protocol, or a run ended with neither its event nor a
    /// refusal.
    #[error("internal error: {0}")]
    Internal(String),
    /// Python's builtin `TimeoutError`: a wait whose bound passed first, a
    /// delegated task's [`wait`](crate::tasks::DelegatedTask::wait).
    #[error("the wait timed out")]
    Timeout,
    /// Python's `HumanInputRejectedError`: a human-input request rejected,
    /// by a person or an `ON_USER_INPUT_REQUIRED` hook, with the reason
    /// given ([`HumanInputHandler::wait`](crate::human_input::HumanInputHandler::wait),
    /// RFC §9.3). The human-input tool reads it as a refusal.
    #[error("{0}")]
    HumanInputRejected(String),
    /// Python's builtin `RuntimeError`: a call the kit's state refuses, a
    /// tool set up twice on one agent
    /// ([`setup_background`](crate::RoomKit::setup_background)), a verb a
    /// turn's host makes after its turn that needs it, a human-input request
    /// its handler gave up.
    #[error("{0}")]
    Runtime(String),
    /// Python's `ToolNameCollisionError`, a `ValueError`: a tool given an
    /// agent under a name it serves already where the tool would be
    /// declared (RFC §21.1), its message naming the tool, what serves it
    /// and the agent ([`setup_task_tools`](crate::RoomKit::setup_task_tools)).
    #[error("{0}")]
    ToolNameCollision(String),
    /// Python's `VerificationRefusedError(reason)`: a verification request
    /// could not be completed (RFC §11.7.2). `reason` is `not_found` (no
    /// such request in the caller's organization), `not_open` (completed,
    /// failed, expired or cancelled already), `expired` (its time ran out
    /// now), `mismatch` (a wrong secret, counted as an attempt) or `failed`
    /// (the wrong secret that used up its attempts).
    #[error("verification refused: {reason}")]
    VerificationRefused {
        /// Why.
        reason: String,
    },
    /// Python's builtin `TypeError`: a value of the wrong kind, as the
    /// Python implementation's message says: a delivery strategy's
    /// serialized parameter it takes no such argument for
    /// ([`deserialize_strategy`](crate::delivery::deserialize_strategy)).
    #[error("{0}")]
    Type(String),
}

impl RoomKitError {
    /// The code the error's Python class carries as its `code` attribute:
    /// `kit_closed` for [`KitClosed`](Self::KitClosed), Python's
    /// `KitClosedError.code`; for [`ProviderDelivery`](Self::ProviderDelivery)
    /// the code its delivery result reads (RFC §5.13), Python's
    /// `ProviderDeliveryError.code`. `None` for any other: its Python class
    /// carries no code, and a delivery that fails with it reads the code its
    /// type gives.
    #[must_use]
    pub fn code(&self) -> Option<String> {
        self.declared().map(|declared| declared.code)
    }

    /// Whether a retry may succeed, as the error's Python class says in its
    /// `retryable` attribute: `true` for [`KitClosed`](Self::KitClosed), a
    /// call or a send the close cut succeeding on another kit, Python's
    /// `KitClosedError.retryable`; for
    /// [`ProviderDelivery`](Self::ProviderDelivery) what its provider
    /// declared, `true` when it declared nothing, Python's
    /// `ProviderDeliveryError.retryable`. `None` for any other: its Python
    /// class says nothing, and a delivery that fails with it reads
    /// retryable.
    #[must_use]
    pub fn retryable(&self) -> Option<bool> {
        self.declared().map(|declared| declared.retryable)
    }

    /// The name of the error's Python class, which a delivery failing with
    /// an error whose class carries no code reads as its code, as Python's
    /// `_delivery_error` reads `error_type_name` (`RoomNotFoundError`,
    /// `ShutdownCancelledError`, `ValueError`): a variant's class is its
    /// name with the `Error` suffix, a builtin's its own; a defect, where
    /// Python asserts, `AssertionError`; a variant that carries an
    /// exception the lock manager, a channel or the router raised names the
    /// class its record holds; a turn's, `TurnLinkError` for a link's
    /// failure. A store's failure keeps `ChannelError`: a boxed driver
    /// error, or a refusal Python's store has no class for, its
    /// `StoreError::Value` reading `ValueError` as `Value`.
    pub(crate) fn class_name(&self) -> String {
        match self {
            Self::Value(_) => "ValueError".to_owned(),
            Self::Cancelled => "CancelledError".to_owned(),
            Self::ExceptionGroup { .. } => "ExceptionGroup".to_owned(),
            Self::Lock(record)
            | Self::Router(record)
            | Self::Identity(record)
            | Self::Channel { error: record, .. } => record.error_type.clone(),
            Self::RemoteTurn { kind, .. } => remote_class_name(kind),
            Self::Internal(_) => "AssertionError".to_owned(),
            Self::Timeout => "TimeoutError".to_owned(),
            Self::Runtime(_) => "RuntimeError".to_owned(),
            Self::Type(_) => "TypeError".to_owned(),
            Self::Store(_) => turns::CHANNEL_ERROR.to_owned(),
            Self::RoomNotFound(_)
            | Self::RoomAlreadyExists(_)
            | Self::RoomClosed { .. }
            | Self::TransitionRefused { .. }
            | Self::ProcessTimeout { .. }
            | Self::ChangeRefused { .. }
            | Self::ReentrantCall { .. }
            | Self::ReentryLoopCap(_)
            | Self::ChannelNotFound { .. }
            | Self::ChannelNotRegistered { .. }
            | Self::ChannelAlreadyRegistered(_)
            | Self::ParticipantNotFound { .. }
            | Self::IdentityNotFound(_)
            | Self::AddressLinked { .. }
            | Self::EventNotFound(_)
            | Self::IdempotencyKeyTaken { .. }
            | Self::ProviderDelivery { .. }
            | Self::TurnLink { .. }
            | Self::KitClosed
            | Self::ShutdownCancelled
            | Self::HumanInputRejected(_)
            | Self::ToolNameCollision(_)
            | Self::VerificationRefused { .. } => format!("{}Error", turns::variant_name(self)),
        }
    }

    /// The code and the flag the error's Python class carries, as its
    /// delivery result reads them; `None` for a class that carries none.
    fn declared(&self) -> Option<DeliveryError> {
        if let Self::ProviderDelivery { result } = self {
            return Some(DeliveryError::of_provider(result));
        }
        matches!(self, Self::KitClosed).then(|| DeliveryError::new(KIT_CLOSED, self.to_string()))
    }

    /// `error`, an entry's into `room_id` from `channel_id`, read as the
    /// Python implementation's `_raise_unless_committed` raises a refusal
    /// before the commit point (RFC §10.5): `room_closed`, `process_timeout`
    /// and a change's refusal by their classes, a source the room does not
    /// bind as [`ChannelNotFound`](Self::ChannelNotFound); any other error as
    /// [`From`] reads it.
    pub(crate) fn of_entry(error: InboundError, room_id: &RoomId, channel_id: &ChannelId) -> Self {
        let InboundError::Refused { reason } = error else {
            return error.into();
        };
        refusal(reason, room_id, channel_id)
    }
}

/// A direct entry's refusal before its commit, by its reason (RFC §10.5).
fn refusal(reason: String, room_id: &RoomId, channel_id: &ChannelId) -> RoomKitError {
    match reason.as_str() {
        "room_closed" => RoomKitError::RoomClosed {
            room_id: room_id.clone(),
        },
        "process_timeout" => RoomKitError::ProcessTimeout {
            room_id: room_id.clone(),
        },
        "source_unbound" => RoomKitError::ChannelNotFound {
            room_id: room_id.clone(),
            channel_id: channel_id.clone(),
        },
        "reentry_loop_cap" => RoomKitError::ReentryLoopCap(room_id.clone()),
        "target_event_not_found"
        | "identity_required_for_edit"
        | "not_authorized"
        | "not_original_author" => RoomKitError::ChangeRefused { reason },
        "instruction_unaddressed" => RoomKitError::Value(
            "An INSTRUCTION must name an agent it directs (addressed_to): an intelligence \
             channel the kit registers"
                .to_owned(),
        ),
        "instruction_not_idempotent" => RoomKitError::Value(
            "An INSTRUCTION is never stored and takes no idempotency_key".to_owned(),
        ),
        _ => RoomKitError::Value(format!("the event was refused before its commit: {reason}")),
    }
}

/// How the Python implementation spells a room's status.
fn status_name(status: RoomStatus) -> String {
    serde_json::to_value(status)
        .ok()
        .and_then(|name| name.as_str().map(str::to_owned))
        .unwrap_or_else(|| format!("{status:?}"))
}

/// How the Python implementation spells a transition: `close`, `pause`...
pub(crate) fn transition_name(transition: Transition) -> String {
    serde_json::to_value(transition)
        .ok()
        .and_then(|name| name.as_str().map(str::to_owned))
        .unwrap_or_else(|| format!("{transition:?}"))
}

/// An error the core recorded, a store's, as a store failure's cause.
#[derive(Debug)]
struct Recorded(ErrorRecord);

impl fmt::Display for Recorded {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0.message)
    }
}

impl std::error::Error for Recorded {}

/// A store failure the core recorded: a store's `ValueError` as itself, as
/// Python propagates it, any other as the store's failure.
fn store_failure(record: ErrorRecord) -> RoomKitError {
    if record.error_type == STORE_VALUE {
        return RoomKitError::Value(record.message);
    }
    RoomKitError::Store(StoreFailure::new(Recorded(record)))
}

/// The record name of a store's `ValueError` ([`StoreError::Value`]).
const STORE_VALUE: &str = "Value";

impl RoomKitError {
    /// No channel is registered under `channel_id`, in the words of most of
    /// Python's sites (`core/mixins/channel_ops.py`, `core/mixins/inbound.py`,
    /// `core/_host_post.py`...): `Channel sms not registered`.
    pub(crate) fn channel_not_registered(channel_id: ChannelId) -> Self {
        let message = format!("Channel {channel_id} not registered");
        Self::ChannelNotRegistered {
            channel_id,
            message,
        }
    }

    /// The name the core recorded a store's refusal under, when this error
    /// is one: `RoomAlreadyExists`, `IdempotencyKeyTaken`... A door that
    /// knows what the refusal names rebuilds Python's class from it.
    pub(crate) fn recorded_refusal(&self) -> Option<&str> {
        let Self::Store(failure) = self else {
            return None;
        };
        let cause: &(dyn std::error::Error + Send + Sync + 'static) = &**failure;
        cause
            .downcast_ref::<Recorded>()
            .map(|recorded| recorded.0.error_type.as_str())
    }
}

// The core's per-operation errors convert into the kit's. Their impls are
// hidden, as roomkit does not re-export those errors (docs/architecture.md,
// Conventions).
#[doc(hidden)]
impl From<InboundError> for RoomKitError {
    fn from(error: InboundError) -> Self {
        match error {
            InboundError::UnknownChannel(channel_id) => Self::channel_not_registered(channel_id),
            InboundError::RoomNotFound(room_id) => Self::RoomNotFound(room_id),
            InboundError::InboundOnHostChannel(channel_id) => Self::Value(format!(
                "Channel {} is a host channel and receives no external payload: the host \
                 speaks through RoomKit.post() or send_event()",
                repr(channel_id.as_str())
            )),
            InboundError::NotAHostChannel(channel_id) => Self::Value(format!(
                "post() speaks from a channel of type HOST; {} is not one",
                repr(channel_id.as_str())
            )),
            InboundError::HostAsIntelligence(channel_id) => host_as_intelligence(&channel_id),
            InboundError::ParticipantNotFound {
                room_id,
                participant_id,
                channel_id,
                reached_elsewhere,
            } => Self::ParticipantNotFound {
                room_id,
                participant_id,
                channel_id: Some(channel_id),
                reached_elsewhere,
            },
            InboundError::Routing(error) => Self::Router(error),
            InboundError::Identity(error) => Self::Identity(error),
            InboundError::StandaloneOutsideInstruction => {
                Self::Value("standalone applies to an INSTRUCTION only".to_owned())
            }
            InboundError::InternalVisibility => internal_visibility(),
            InboundError::Refused { reason } => {
                Self::Value(format!("the event was refused before its commit: {reason}"))
            }
            InboundError::ChannelFailed { channel_id, error } => {
                Self::Channel { channel_id, error }
            }
            InboundError::Store(error) => store_failure(error),
            InboundError::Lock(error) | InboundError::Ticket(error) => Self::Lock(error),
            other => Self::Internal(other.to_string()),
        }
    }
}

fn host_as_intelligence(channel_id: &ChannelId) -> RoomKitError {
    RoomKitError::Value(format!(
        "Channel {} is a host channel: it can be attached as a transport only, never as an \
         intelligence channel",
        repr(channel_id.as_str())
    ))
}

/// Python's `parse_given_visibility` refusing `internal`.
fn internal_visibility() -> RoomKitError {
    RoomKitError::Value(
        "visibility 'internal' marks a record the framework writes (RFC §7.3): an event stored \
         for no channel takes 'none'"
            .to_owned(),
    )
}

/// Python's `ParticipantNotFoundError` message: a member call's, or, for
/// a post from the host channel `channel_id`, what the host does instead.
fn participant_not_found(
    room_id: &RoomId,
    participant_id: &ParticipantId,
    channel_id: Option<&ChannelId>,
    reached_elsewhere: bool,
) -> String {
    let Some(channel_id) = channel_id else {
        return format!("Participant {participant_id} not found in room {room_id}");
    };
    let channel = repr(channel_id.as_str());
    let named = if reached_elsewhere {
        "names a participant reached through other channels only"
    } else {
        "names nobody of the room"
    };
    format!(
        "post() speaks as a participant of room {room_id} the host channel {channel} reaches, \
         and {} {named}. The host's own speakers are members added on that channel: \
         kit.add_member(room_id, {channel}, speaker_id)",
        repr(participant_id.as_str()),
    )
}

#[doc(hidden)]
impl From<MemberError> for RoomKitError {
    fn from(error: MemberError) -> Self {
        match error {
            MemberError::RoomNotFound(room_id) => Self::RoomNotFound(room_id),
            MemberError::ParticipantNotFound {
                room_id,
                participant_id,
            } => Self::ParticipantNotFound {
                room_id,
                participant_id,
                channel_id: None,
                reached_elsewhere: false,
            },
            MemberError::IdentityNotFound(identity_id) => Self::IdentityNotFound(identity_id),
            MemberError::Store(error) => store_failure(error),
            MemberError::Lock(error) => Self::Lock(error),
            other => Self::Internal(other.to_string()),
        }
    }
}

#[doc(hidden)]
impl From<LifecycleError> for RoomKitError {
    fn from(error: LifecycleError) -> Self {
        match error {
            LifecycleError::RoomNotFound(room_id) => Self::RoomNotFound(room_id),
            LifecycleError::ChannelNotBound {
                room_id,
                channel_id,
            } => Self::ChannelNotFound {
                room_id,
                channel_id,
            },
            LifecycleError::InternalVisibility => internal_visibility(),
            LifecycleError::HostAsIntelligence(channel_id) => host_as_intelligence(&channel_id),
            LifecycleError::TransitionRefused {
                room_id,
                transition,
                status,
            } => Self::TransitionRefused {
                room_id,
                status,
                transition: transition_name(transition),
            },
            LifecycleError::Store(error) => store_failure(error),
            LifecycleError::Lock(error) => Self::Lock(error),
            LifecycleError::ChannelFailed { channel_id, error } => {
                Self::Channel { channel_id, error }
            }
            other => Self::Internal(other.to_string()),
        }
    }
}

#[doc(hidden)]
impl From<MutationError> for RoomKitError {
    fn from(error: MutationError) -> Self {
        match error {
            MutationError::RoomNotFound(room_id) => Self::RoomNotFound(room_id),
            MutationError::OtherRoom { room_id, record } => Self::Value(format!(
                "commit_event into {room_id} got an event of room {record}"
            )),
            MutationError::Refused { room_id, .. } => Self::RoomClosed { room_id },
            MutationError::Store(error) => store_failure(error),
            MutationError::Lock(error) => Self::Lock(error),
            MutationError::ReentryLoopCap(room_id) => Self::ReentryLoopCap(room_id),
            other => Self::Internal(other.to_string()),
        }
    }
}

#[doc(hidden)]
impl From<ConfigError> for RoomKitError {
    fn from(error: ConfigError) -> Self {
        match error {
            ConfigError::DuplicateChannel(channel_id) => Self::ChannelAlreadyRegistered(channel_id),
            ConfigError::UnknownChannel(channel_id) => Self::channel_not_registered(channel_id),
            other @ (ConfigError::ReservedChannelType { .. } | _) => Self::Value(other.to_string()),
        }
    }
}

#[doc(hidden)]
impl From<RegistrationError> for RoomKitError {
    /// The refusal as Python's `RoomKit.hook` raises it, a `ValueError`
    /// with its words; a handler of another payload than its trigger's,
    /// which only Rust's types can refuse, in its own.
    fn from(error: RegistrationError) -> Self {
        Self::Value(match error {
            RegistrationError::DuplicateName { name } => format!(
                "A hook named {} is already registered in this scope: a hook's name is unique \
                 within its scope (RFC §9.1)",
                repr(&name)
            ),
            RegistrationError::ReservedName { name } => format!(
                "Hook name {} is a value the framework writes in blocked_by; a block of this \
                 hook would read as the framework's",
                repr(&name)
            ),
            RegistrationError::FailClosedAsync { hook } => format!(
                "Hook {}: fail_closed=True needs a SYNC hook; an ASYNC hook cannot block",
                repr(&hook)
            ),
            RegistrationError::OffLockOutsideBeforeBroadcast {
                hook,
                execution,
                trigger,
            } => format!(
                "Hook {}: needs_lock=False is only supported on a SYNC BEFORE_BROADCAST hook, \
                 not {} {}",
                repr(&hook),
                wire_name(&execution),
                wire_name(&trigger)
            ),
            RegistrationError::LockOrdering {
                locked,
                locked_priority,
                off_lock,
                off_lock_priority,
            } => {
                let (locked, off_lock) = (repr(&locked), repr(&off_lock));
                format!(
                    "Hook {locked} (priority {locked_priority}) needs the room lock but is \
                     ordered before off-lock hook {off_lock} (priority {off_lock_priority}). \
                     Off-lock hooks run first (RFC §9.5.1): give {off_lock} a priority at or \
                     below {locked_priority}, or keep it needs_lock=True. Channel and event \
                     filters are not considered."
                )
            }
            other @ RegistrationError::PayloadMismatch { .. } | other => other.to_string(),
        })
    }
}

/// `value` as Python prints its enum member's value: its wire name.
fn wire_name(value: &impl serde::Serialize) -> String {
    match serde_json::to_value(value) {
        Ok(serde_json::Value::String(name)) => name,
        _ => String::new(),
    }
}

impl From<ModelError> for RoomKitError {
    fn from(error: ModelError) -> Self {
        Self::Value(error.to_string())
    }
}

#[doc(hidden)]
impl From<ProtocolError> for RoomKitError {
    fn from(error: ProtocolError) -> Self {
        Self::Internal(format!("the driver broke the effect protocol: {error}"))
    }
}

impl From<StoreError> for RoomKitError {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::RoomAlreadyExists(room_id) => Self::RoomAlreadyExists(room_id),
            StoreError::RoomNotFound(room_id) => Self::RoomNotFound(room_id),
            StoreError::IdentityNotFound(identity_id) => Self::IdentityNotFound(identity_id),
            StoreError::AddressLinked { channel_type } => Self::AddressLinked { channel_type },
            StoreError::EventNotFound(event_id) => Self::EventNotFound(event_id),
            StoreError::IdempotencyKeyTaken {
                room_id,
                channel_id,
                key,
            } => Self::IdempotencyKeyTaken {
                room_id,
                channel_id,
                key,
            },
            StoreError::Value(message) => Self::Value(message),
            StoreError::Unavailable(failure) => Self::Store(failure),
            // No call of the kit writes a participant: these refusals reach a
            // caller from the store itself, as its `StoreError`, which names
            // Python's `ParticipantExistsError` and
            // `ParticipantNotFoundError`.
            other @ (StoreError::ParticipantExists { .. }
            | StoreError::ParticipantNotFound { .. }
            | StoreError::TaskNotFound(_)) => Self::Store(StoreFailure::new(other)),
        }
    }
}

/// `error`'s message, then each of its causes', joined by `: `: what a log
/// line or a record shows of an error whose message leaves its cause to its
/// source, a store that could not be reached among them.
pub(crate) fn with_causes(error: &dyn std::error::Error) -> String {
    let mut message = error.to_string();
    let mut cause = error.source();
    while let Some(next) = cause {
        message.push_str(": ");
        message.push_str(&next.to_string());
        cause = next.source();
    }
    message
}

/// What a [`RoomKitError::TurnLink`] says without a message of its own, as
/// Python's `TurnLinkError` says it.
fn turn_link_message(reason: &str, message: Option<&str>) -> String {
    match message {
        Some(message) => message.to_owned(),
        None if reason == "backend_closed" => "The turn ended: its turn backend closed".to_owned(),
        None => format!("The turn ended without its executor: {reason}"),
    }
}

/// The class a [`RoomKitError::RemoteTurn`] is reported under, as Python's
/// `error_type_name` reads it: the last name of the class it had, else
/// `RemoteTurnError`.
fn remote_class_name(kind: &str) -> String {
    let name = kind.rsplit(':').next().unwrap_or(kind);
    let name = name.rsplit('.').next().unwrap_or(name);
    if name.is_empty() {
        "RemoteTurnError".to_owned()
    } else {
        name.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use pretty_assertions::assert_eq;
    use roomkit_core::ops::{
        CheckTimers, HoldOutcome, KitConfig, Next, Operation, SequentialEnvironment,
    };
    use roomkit_core::rules::hook_registry::{HookExecution, HookTrigger};

    use super::*;

    /// The `code` and `retryable` of each error whose Python class carries
    /// them, `KitClosedError` and `ProviderDeliveryError`, and none of any
    /// other (RMK-1030).
    #[test]
    fn the_errors_whose_class_carries_a_code_say_it_and_whether_a_retry_may_succeed() {
        let refused = |metadata: serde_json::Value| RoomKitError::ProviderDelivery {
            result: ProviderResult {
                error: Some("unreachable".to_owned()),
                metadata: serde_json::from_value(metadata).unwrap(),
                ..ProviderResult::new(false)
            },
        };
        let errors = [
            RoomKitError::KitClosed,
            refused(serde_json::json!({ "code": "E42", "retryable": false })),
            refused(serde_json::json!({})),
            RoomKitError::ShutdownCancelled,
            RoomKitError::RoomNotFound(RoomId::new("r").unwrap()),
        ];
        let read: Vec<(Option<String>, Option<bool>)> = errors
            .iter()
            .map(|error| (error.code(), error.retryable()))
            .collect();
        assert_eq!(
            read,
            [
                (Some("kit_closed".to_owned()), Some(true)),
                (Some("E42".to_owned()), Some(false)),
                (Some("unreachable".to_owned()), Some(true)),
                (None, None),
                (None, None),
            ]
        );
    }

    /// A store failure the kit meets shows its message alone; its chain
    /// carries the cause the store met.
    #[test]
    fn a_kit_error_s_chain_carries_the_store_s_cause() {
        let error = RoomKitError::from(StoreError::unavailable("connection refused"));
        assert_eq!(
            (error.to_string(), with_causes(&error)),
            (
                "the store failed".to_owned(),
                "the store failed: connection refused".to_owned()
            )
        );
    }

    /// A protocol error is a defect of the crate, shown once.
    #[test]
    fn a_protocol_error_is_internal() {
        let error = RoomKitError::from(answered_twice());
        assert_eq!(
            with_causes(&error),
            format!(
                "internal error: the driver broke the effect protocol: {}",
                answered_twice()
            )
        );
    }

    /// The error of an effect answered twice.
    fn answered_twice() -> ProtocolError {
        let room_id = RoomId::new("room_1").unwrap();
        let mut check = CheckTimers::new(Arc::new(KitConfig::new()), room_id, None);
        let step = check.poll(&SequentialEnvironment::default());
        let Next::Effects(effects) = step.next else {
            panic!("the check asks for the room's lock first");
        };
        let lock = effects[0].id;
        check.complete(lock, HoldOutcome::Locked.into()).unwrap();
        check
            .complete(lock, HoldOutcome::Locked.into())
            .unwrap_err()
    }

    /// RFC §10.5 and Python's `_raise_unless_committed`: a direct entry's
    /// refusal before its commit, by its class, for the reasons no kit test
    /// reaches through `of_entry`. `room_closed`, `process_timeout` and
    /// `source_unbound` are the kit's tests' (`host_post.rs`'s
    /// `a_refused_post_writes_nothing`, `reentrancy.rs`'s
    /// `into_a_closed_room_is_refused`,
    /// `a_turn_s_call_meets_its_own_process_timeout` and
    /// `on_a_channel_the_room_does_not_bind_is_refused`).
    #[test]
    fn an_entry_s_refusal_reads_as_python_s_class() {
        let room_id = RoomId::new("room_1").unwrap();
        let channel_id = ChannelId::new("ws").unwrap();
        let read = |reason: &str| {
            RoomKitError::of_entry(
                InboundError::Refused {
                    reason: reason.to_owned(),
                },
                &room_id,
                &channel_id,
            )
        };
        assert_eq!(
            ["not_original_author", "reentry_loop_cap"].map(read),
            [
                RoomKitError::ChangeRefused {
                    reason: "not_original_author".to_owned()
                },
                RoomKitError::ReentryLoopCap(room_id.clone()),
            ]
        );
    }

    /// Python's `TurnLinkError` says its default unless given a message, and
    /// a `RemoteTurnError` is reported under the last name of the class it
    /// had, as `error_type_name` reads it.
    /// A refused hook registration reads as Python's `ValueError`, word
    /// for word (probe `tasks/probes/step5-lot3/py_registration.out`).
    #[test]
    fn a_refused_registration_reads_as_python_s() {
        let refusals = [
            RegistrationError::DuplicateName {
                name: "a".to_owned(),
            },
            RegistrationError::FailClosedAsync {
                hook: "y".to_owned(),
            },
            RegistrationError::OffLockOutsideBeforeBroadcast {
                hook: "x".to_owned(),
                execution: HookExecution::Async,
                trigger: HookTrigger::OnRoomCreated,
            },
            RegistrationError::LockOrdering {
                locked: "locked".to_owned(),
                locked_priority: -1,
                off_lock: "scan".to_owned(),
                off_lock_priority: 0,
            },
        ];
        let read: Vec<String> = refusals
            .into_iter()
            .map(|refusal| RoomKitError::from(refusal).to_string())
            .collect();
        assert_eq!(
            read,
            [
                "A hook named 'a' is already registered in this scope: a hook's name is unique \
                 within its scope (RFC §9.1)",
                "Hook 'y': fail_closed=True needs a SYNC hook; an ASYNC hook cannot block",
                "Hook 'x': needs_lock=False is only supported on a SYNC BEFORE_BROADCAST hook, not \
                 async on_room_created",
                "Hook 'locked' (priority -1) needs the room lock but is ordered before off-lock \
                 hook 'scan' (priority 0). Off-lock hooks run first (RFC §9.5.1): give 'scan' a \
                 priority at or below -1, or keep it needs_lock=True. Channel and event filters \
                 are not considered.",
            ]
        );
    }

    #[test]
    fn a_turn_s_errors_read_as_python_s_classes() {
        let closed = RoomKitError::TurnLink {
            reason: "backend_closed".to_owned(),
            message: None,
        };
        let lost = RoomKitError::TurnLink {
            reason: "executor_lost".to_owned(),
            message: None,
        };
        let remote = RoomKitError::RemoteTurn {
            message: "the provider failed".to_owned(),
            kind: "shop.errors:ProviderError".to_owned(),
            bases: vec!["roomkit.providers:ProviderError".to_owned()],
        };
        assert_eq!(
            [&closed, &lost, &remote].map(|error| (error.to_string(), error.class_name())),
            [
                (
                    "The turn ended: its turn backend closed".to_owned(),
                    "TurnLinkError".to_owned()
                ),
                (
                    "The turn ended without its executor: executor_lost".to_owned(),
                    "TurnLinkError".to_owned()
                ),
                ("the provider failed".to_owned(), "ProviderError".to_owned()),
            ]
        );
    }

    /// `ReentrantCallError` and `AddressLinkedError` name only what
    /// Python's messages name (`_reentrance.py`, `store/base.py`'s
    /// `address_linked`): the room; the channel type, never the address
    /// the caller passed nor the identity that holds it.
    #[test]
    fn a_refusal_names_what_python_s_message_names() {
        let room_id = RoomId::new("room_1").unwrap();
        let told = [
            RoomKitError::ReentrantCall { room_id }.to_string(),
            RoomKitError::AddressLinked {
                channel_type: ChannelType::SMS,
            }
            .to_string(),
            StoreError::AddressLinked {
                channel_type: ChannelType::SMS,
            }
            .to_string(),
        ];
        let linked = "This sms address is linked to another identity of the organization";
        assert_eq!(
            told,
            [
                "A message routed into room room_1 from its own work (under its lock, in an \
                 off-lock hook or a parse) cannot be adopted: name the room to send into it",
                linked,
                linked
            ]
        );
    }
}

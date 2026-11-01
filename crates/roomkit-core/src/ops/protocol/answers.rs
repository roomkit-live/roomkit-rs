//! How an outcome answers its request: the check `Io` applies when the
//! driver completes an effect, and the readers an operation takes an answer
//! through once checked.

use super::{
    ChannelOutcome, ChannelRequest, EffectId, HoldOutcome, HoldRequest, HookAnswer, HookRequest,
    Outcome, ProtocolError, Request, StoreOutcome, StoreRequest,
};
use crate::model::{
    ChannelId, ChannelOutput, DeliveryError, ErrorRecord, Identity, RoomContext, RoomEvent, RoomId,
};
use crate::rules::hook_chain::HookOutcome;

/// The kind of a request, which `Io` keeps until the request is answered,
/// to check the answer against it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RequestKind {
    AcquireLock,
    ReleaseLock,
    EnterLane,
    AwaitLane,
    LeaveLane,
    TakeTicket,
    AwaitTicket,
    ReleaseTicket,
    LoadContext,
    GetEventByIdempotencyKey,
    GetEvent,
    ListEvents,
    DeleteEvent,
    Commit,
    PersistSideEffects,
    /// A write answered `Stored`: a room's creation or update, a binding's
    /// store or delete, an event's update.
    Write,
    ResolveIdentity,
    HandleInbound,
    /// A turn or a delivery: `on_event()` or `deliver()`.
    ChannelCall,
    StreamsIn,
    RunHook,
    RunObservers,
    Route,
}

impl Request {
    pub(crate) fn kind(&self) -> RequestKind {
        match self {
            Self::Hold(request) => request.kind(),
            Self::Store(request) => request.kind(),
            Self::Channel(ChannelRequest::HandleInbound { .. }) => RequestKind::HandleInbound,
            Self::Channel(
                ChannelRequest::OnEvent { .. }
                | ChannelRequest::Deliver { .. }
                | ChannelRequest::ReadStream { .. },
            ) => RequestKind::ChannelCall,
            Self::Channel(ChannelRequest::StreamsIn { .. }) => RequestKind::StreamsIn,
            Self::Hook(HookRequest::RunHook { .. }) => RequestKind::RunHook,
            Self::Hook(HookRequest::RunObservers { .. }) => RequestKind::RunObservers,
            Self::Route { .. } => RequestKind::Route,
        }
    }
}

impl HoldRequest {
    fn kind(&self) -> RequestKind {
        match self {
            Self::AcquireLock { .. } => RequestKind::AcquireLock,
            Self::ReleaseLock { .. } => RequestKind::ReleaseLock,
            Self::EnterLane { .. } => RequestKind::EnterLane,
            Self::AwaitLane { .. } => RequestKind::AwaitLane,
            Self::LeaveLane { .. } => RequestKind::LeaveLane,
            Self::TakeTicket { .. } => RequestKind::TakeTicket,
            Self::AwaitTicket { .. } => RequestKind::AwaitTicket,
            Self::ReleaseTicket { .. } => RequestKind::ReleaseTicket,
        }
    }
}

impl StoreRequest {
    fn kind(&self) -> RequestKind {
        match self {
            Self::LoadContext { .. } => RequestKind::LoadContext,
            Self::GetEventByIdempotencyKey { .. } => RequestKind::GetEventByIdempotencyKey,
            Self::GetEvent { .. } => RequestKind::GetEvent,
            Self::ListEvents { .. } => RequestKind::ListEvents,
            Self::DeleteEvent { .. } => RequestKind::DeleteEvent,
            Self::Commit { .. } => RequestKind::Commit,
            Self::PersistSideEffects { .. } => RequestKind::PersistSideEffects,
            Self::CreateRoom { .. }
            | Self::UpdateRoom { .. }
            | Self::StoreBinding { .. }
            | Self::DeleteBinding { .. }
            | Self::UpdateEvent { .. } => RequestKind::Write,
            Self::ResolveIdentity { .. } => RequestKind::ResolveIdentity,
        }
    }
}

/// What a room holds under an idempotency key (RFC §13.4), as the
/// Python implementation reads it: `check_idempotency`, then
/// `get_event_by_idempotency_key`.
#[derive(Debug)]
pub(crate) enum KeyHeld {
    /// No event of the room holds it.
    Free,
    /// The event the key committed.
    Event(Box<RoomEvent>),
    /// The key is held, its event unread.
    Seen,
}

impl RequestKind {
    /// The outcomes that answer this kind, for error messages.
    fn expected(self) -> &'static str {
        match self {
            Self::AcquireLock => "Locked",
            Self::ReleaseLock => "Released",
            Self::EnterLane => "Entered",
            Self::AwaitLane => "LaneTurn or LaneClosed",
            Self::LeaveLane => "Left",
            Self::TakeTicket => "Ticketed",
            Self::AwaitTicket => "Admitted",
            Self::ReleaseTicket => "TicketReleased",
            Self::LoadContext => "Context or RoomNotFound",
            Self::GetEventByIdempotencyKey => "Idempotent or KeySeen",
            Self::GetEvent => "Event",
            Self::ListEvents => "Events",
            Self::DeleteEvent => "Deleted",
            Self::Commit => "Committed",
            Self::PersistSideEffects => "Persisted",
            Self::Write => "Stored",
            Self::ResolveIdentity => "Resolved",
            Self::HandleInbound => "Parsed",
            Self::ChannelCall => "Output",
            Self::StreamsIn => "StreamsIn",
            Self::RunHook => "Ran",
            Self::RunObservers => "Observed",
            Self::Route => "Routed",
        }
    }

    /// Whether `outcome`, of the kind's family, answers the kind.
    fn answered_by(self, outcome: &Outcome) -> bool {
        match self {
            Self::AcquireLock => matches!(outcome, Outcome::Hold(HoldOutcome::Locked)),
            Self::ReleaseLock => matches!(outcome, Outcome::Hold(HoldOutcome::Released)),
            Self::EnterLane => matches!(
                outcome,
                Outcome::Hold(HoldOutcome::Entered | HoldOutcome::LaneClosed)
            ),
            Self::AwaitLane => matches!(
                outcome,
                Outcome::Hold(HoldOutcome::LaneTurn | HoldOutcome::LaneClosed)
            ),
            Self::LeaveLane => matches!(outcome, Outcome::Hold(HoldOutcome::Left)),
            Self::TakeTicket => matches!(outcome, Outcome::Hold(HoldOutcome::Ticketed)),
            Self::AwaitTicket => matches!(outcome, Outcome::Hold(HoldOutcome::Admitted)),
            Self::ReleaseTicket => matches!(outcome, Outcome::Hold(HoldOutcome::TicketReleased)),
            Self::LoadContext => matches!(
                outcome,
                Outcome::Store(StoreOutcome::Context(_) | StoreOutcome::RoomNotFound)
            ),
            Self::GetEventByIdempotencyKey => {
                matches!(
                    outcome,
                    Outcome::Store(StoreOutcome::Idempotent(_) | StoreOutcome::KeySeen)
                )
            }
            Self::GetEvent => matches!(outcome, Outcome::Store(StoreOutcome::Event(_))),
            Self::ListEvents => matches!(outcome, Outcome::Store(StoreOutcome::Events(_))),
            Self::DeleteEvent => matches!(outcome, Outcome::Store(StoreOutcome::Deleted(_))),
            Self::Commit => matches!(outcome, Outcome::Store(StoreOutcome::Committed(_))),
            Self::PersistSideEffects => matches!(outcome, Outcome::Store(StoreOutcome::Persisted)),
            Self::Write => matches!(outcome, Outcome::Store(StoreOutcome::Stored)),
            Self::ResolveIdentity => matches!(outcome, Outcome::Store(StoreOutcome::Resolved(_))),
            Self::HandleInbound => matches!(outcome, Outcome::Channel(ChannelOutcome::Parsed(_))),
            Self::ChannelCall => matches!(
                outcome,
                Outcome::Channel(
                    ChannelOutcome::Output(_)
                        | ChannelOutcome::Streamed(_)
                        | ChannelOutcome::ProviderDelivery(_)
                        | ChannelOutcome::Raised(_)
                )
            ),
            Self::StreamsIn => matches!(outcome, Outcome::Channel(ChannelOutcome::StreamsIn(_))),
            Self::RunHook => matches!(outcome, Outcome::Hook(HookAnswer::Ran(_))),
            Self::RunObservers => matches!(outcome, Outcome::Hook(HookAnswer::Observed)),
            Self::Route => matches!(outcome, Outcome::Routed(_)),
        }
    }

    /// Checks that `outcome` answers this kind; `Failed` answers every one,
    /// and `DeadlinePassed` every one whose effect carried a deadline
    /// (`bounded`).
    pub(crate) fn check(
        self,
        effect: EffectId,
        outcome: &Outcome,
        bounded: bool,
    ) -> Result<(), ProtocolError> {
        let fits = match outcome {
            Outcome::DeadlinePassed => bounded,
            Outcome::Failed(_) => true,
            Outcome::Hold(_)
            | Outcome::Store(_)
            | Outcome::Channel(_)
            | Outcome::Hook(_)
            | Outcome::Routed(_) => self.answered_by(outcome),
        };
        if fits {
            Ok(())
        } else {
            Err(ProtocolError::UnexpectedOutcome {
                effect,
                expected: self.expected(),
                got: outcome.name(),
            })
        }
    }
}

/// An outcome read as the answer of the request that asked for it.
///
/// `Io` checks every outcome against its request before an operation reads
/// it, so each reader meets only its own variants and `Failed`; another
/// variant would be a defect of the core, which a reader reports as a
/// failure rather than a panic.
#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "Io has checked the outcome against its request: other variants cannot arrive"
)]
impl Outcome {
    /// Whether the outcome is the hold `held`.
    fn held(self, held: HoldOutcome) -> Result<(), ErrorRecord> {
        match self {
            Self::Hold(outcome) if outcome == held => Ok(()),
            other => Err(other.into_failure()),
        }
    }

    pub(crate) fn into_locked(self) -> Result<(), ErrorRecord> {
        self.held(HoldOutcome::Locked)
    }

    pub(crate) fn into_released(self) -> Result<(), ErrorRecord> {
        self.held(HoldOutcome::Released)
    }

    pub(crate) fn into_entered(self) -> Result<(), ErrorRecord> {
        self.held(HoldOutcome::Entered)
    }

    pub(crate) fn into_lane_turn(self) -> Result<(), ErrorRecord> {
        self.held(HoldOutcome::LaneTurn)
    }

    pub(crate) fn into_left(self) -> Result<(), ErrorRecord> {
        self.held(HoldOutcome::Left)
    }

    pub(crate) fn into_ticketed(self) -> Result<(), ErrorRecord> {
        self.held(HoldOutcome::Ticketed)
    }

    pub(crate) fn into_admitted(self) -> Result<(), ErrorRecord> {
        self.held(HoldOutcome::Admitted)
    }

    pub(crate) fn into_ticket_released(self) -> Result<(), ErrorRecord> {
        self.held(HoldOutcome::TicketReleased)
    }

    pub(crate) fn into_observed(self) -> Result<(), ErrorRecord> {
        match self {
            Self::Hook(HookAnswer::Observed) => Ok(()),
            other => Err(other.into_failure()),
        }
    }

    /// The room's context, `None` when the room does not exist.
    pub(crate) fn into_context(self) -> Result<Option<Box<RoomContext>>, ErrorRecord> {
        match self {
            Self::Store(StoreOutcome::Context(context)) => Ok(Some(context)),
            Self::Store(StoreOutcome::RoomNotFound) => Ok(None),
            other => Err(other.into_failure()),
        }
    }

    /// What the room holds under the key read.
    pub(crate) fn into_idempotent(self) -> Result<KeyHeld, ErrorRecord> {
        match self {
            Self::Store(StoreOutcome::Idempotent(None)) => Ok(KeyHeld::Free),
            Self::Store(StoreOutcome::Idempotent(Some(event))) => Ok(KeyHeld::Event(event)),
            Self::Store(StoreOutcome::KeySeen) => Ok(KeyHeld::Seen),
            other => Err(other.into_failure()),
        }
    }

    /// The event read, `None` when the store holds none with the id.
    pub(crate) fn into_event(self) -> Result<Option<Box<RoomEvent>>, ErrorRecord> {
        match self {
            Self::Store(StoreOutcome::Event(event)) => Ok(event),
            other => Err(other.into_failure()),
        }
    }

    /// The events a delete removed, the target first.
    pub(crate) fn into_deleted(self) -> Result<Vec<RoomEvent>, ErrorRecord> {
        match self {
            Self::Store(StoreOutcome::Deleted(events)) => Ok(events),
            other => Err(other.into_failure()),
        }
    }

    /// The room's timeline, in index order.
    pub(crate) fn into_events(self) -> Result<Vec<RoomEvent>, ErrorRecord> {
        match self {
            Self::Store(StoreOutcome::Events(events)) => Ok(events),
            other => Err(other.into_failure()),
        }
    }

    /// The event as the store committed it, its index the store's.
    pub(crate) fn into_committed(self) -> Result<Box<RoomEvent>, ErrorRecord> {
        match self {
            Self::Store(StoreOutcome::Committed(event)) => Ok(event),
            other => Err(other.into_failure()),
        }
    }

    pub(crate) fn into_persisted(self) -> Result<(), ErrorRecord> {
        match self {
            Self::Store(StoreOutcome::Persisted) => Ok(()),
            other => Err(other.into_failure()),
        }
    }

    pub(crate) fn into_stored(self) -> Result<(), ErrorRecord> {
        match self {
            Self::Store(StoreOutcome::Stored) => Ok(()),
            other => Err(other.into_failure()),
        }
    }

    /// A write's answer, a commit's included: `Stored` or `Committed`.
    pub(crate) fn into_written(self) -> Result<(), ErrorRecord> {
        match self {
            Self::Store(StoreOutcome::Stored | StoreOutcome::Committed(_)) => Ok(()),
            other => Err(other.into_failure()),
        }
    }

    pub(crate) fn into_parsed(self) -> Result<Box<RoomEvent>, ErrorRecord> {
        match self {
            Self::Channel(ChannelOutcome::Parsed(event)) => Ok(event),
            other => Err(other.into_failure()),
        }
    }

    pub(crate) fn into_output(self) -> Result<Box<ChannelOutput>, ErrorRecord> {
        match self {
            Self::Channel(ChannelOutcome::Output(output)) => Ok(output),
            other => Err(other.into_failure()),
        }
    }

    /// What a turn returned, and whether the stream of its response
    /// follows (RFC §6.1 `response_stream`).
    pub(crate) fn into_turn_output(self) -> Result<(Box<ChannelOutput>, bool), ErrorRecord> {
        match self {
            Self::Channel(ChannelOutcome::Output(output)) => Ok((output, false)),
            Self::Channel(ChannelOutcome::Streamed(output)) => Ok((output, true)),
            other => Err(other.into_failure()),
        }
    }

    /// The channels that take a streamed response in the room asked.
    pub(crate) fn into_streams_in(self) -> Result<Vec<ChannelId>, ErrorRecord> {
        match self {
            Self::Channel(ChannelOutcome::StreamsIn(channel_ids)) => Ok(channel_ids),
            other => Err(other.into_failure()),
        }
    }

    /// The identity an address resolved to, if any.
    pub(crate) fn into_resolved(self) -> Result<Option<Identity>, ErrorRecord> {
        match self {
            Self::Store(StoreOutcome::Resolved(identity)) => Ok(identity),
            other => Err(other.into_failure()),
        }
    }

    /// The room a router chose, `None` for a new room.
    pub(crate) fn into_routed(self) -> Result<Option<RoomId>, ErrorRecord> {
        match self {
            Self::Routed(room_id) => Ok(room_id),
            other => Err(other.into_failure()),
        }
    }

    /// What a hook produced; a hook the driver could not run raised.
    pub(crate) fn into_hook(self) -> HookOutcome<RoomEvent> {
        match self {
            Self::Hook(HookAnswer::Ran(outcome)) => *outcome,
            other => HookOutcome::Raised(other.into_failure()),
        }
    }

    fn into_failure(self) -> ErrorRecord {
        match self {
            Self::Failed(error) => error,
            // A provider's refusal fails the call as Python's
            // `ProviderDeliveryError` raised.
            Self::Channel(ChannelOutcome::ProviderDelivery(result)) => ErrorRecord::new(
                "ProviderDeliveryError",
                DeliveryError::of_provider(&result).message,
            ),
            Self::Channel(ChannelOutcome::Raised(raised)) => raised.error,
            other => {
                debug_assert!(
                    false,
                    "an unchecked outcome reached an operation: {other:?}"
                );
                ErrorRecord::new(
                    "ProtocolError",
                    format!("unexpected outcome {}", other.name()),
                )
            }
        }
    }
}

impl Outcome {
    fn name(&self) -> &'static str {
        match self {
            Self::Hold(outcome) => match outcome {
                HoldOutcome::Locked => "Locked",
                HoldOutcome::Released => "Released",
                HoldOutcome::Entered => "Entered",
                HoldOutcome::LaneClosed => "LaneClosed",
                HoldOutcome::LaneTurn => "LaneTurn",
                HoldOutcome::Left => "Left",
                HoldOutcome::Ticketed => "Ticketed",
                HoldOutcome::Admitted => "Admitted",
                HoldOutcome::TicketReleased => "TicketReleased",
            },
            Self::Store(outcome) => match outcome {
                StoreOutcome::Context(_) => "Context",
                StoreOutcome::RoomNotFound => "RoomNotFound",
                StoreOutcome::Idempotent(_) => "Idempotent",
                StoreOutcome::KeySeen => "KeySeen",
                StoreOutcome::Event(_) => "Event",
                StoreOutcome::Deleted(_) => "Deleted",
                StoreOutcome::Events(_) => "Events",
                StoreOutcome::Committed(_) => "Committed",
                StoreOutcome::Persisted => "Persisted",
                StoreOutcome::Stored => "Stored",
                StoreOutcome::Resolved(_) => "Resolved",
            },
            Self::Channel(ChannelOutcome::Parsed(_)) => "Parsed",
            Self::Channel(ChannelOutcome::Output(_)) => "Output",
            Self::Channel(ChannelOutcome::Streamed(_)) => "Streamed",
            Self::Channel(ChannelOutcome::ProviderDelivery(_)) => "ProviderDelivery",
            Self::Channel(ChannelOutcome::StreamsIn(_)) => "StreamsIn",
            Self::Channel(ChannelOutcome::Raised(_)) => "Raised",
            Self::Hook(HookAnswer::Ran(_)) => "Ran",
            Self::Hook(HookAnswer::Observed) => "Observed",
            Self::Routed(_) => "Routed",
            Self::Failed(_) => "Failed",
            Self::DeadlinePassed => "DeadlinePassed",
        }
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn failed_answers_every_request() {
        let failed = Outcome::Failed(ErrorRecord::new("StoreError", "down"));
        assert_eq!(
            RequestKind::Commit.check(EffectId::first(), &failed, false),
            Ok(())
        );
    }

    #[test]
    fn an_outcome_of_another_family_is_a_protocol_error() {
        assert_eq!(
            RequestKind::Commit.check(
                EffectId::first(),
                &Outcome::Hold(HoldOutcome::Locked),
                false
            ),
            Err(ProtocolError::UnexpectedOutcome {
                effect: EffectId::first(),
                expected: "Committed",
                got: "Locked",
            })
        );
    }

    #[test]
    fn a_router_s_answer_answers_a_route_request_only() {
        assert_eq!(
            (
                RequestKind::Route.check(EffectId::first(), &Outcome::Routed(None), false),
                RequestKind::ResolveIdentity
                    .check(EffectId::first(), &Outcome::Routed(None), false)
                    .is_err()
            ),
            (Ok(()), true)
        );
    }

    #[test]
    fn an_event_or_none_answers_an_event_read_only() {
        assert_eq!(
            (
                RequestKind::GetEvent.check(
                    EffectId::first(),
                    &Outcome::Store(StoreOutcome::Event(None)),
                    false
                ),
                RequestKind::GetEvent
                    .check(
                        EffectId::first(),
                        &Outcome::Store(StoreOutcome::Stored),
                        false
                    )
                    .is_err()
            ),
            (Ok(()), true)
        );
    }

    #[test]
    fn the_events_removed_answer_a_delete_only() {
        assert_eq!(
            (
                RequestKind::DeleteEvent.check(
                    EffectId::first(),
                    &Outcome::Store(StoreOutcome::Deleted(Vec::new())),
                    false
                ),
                RequestKind::DeleteEvent
                    .check(
                        EffectId::first(),
                        &Outcome::Store(StoreOutcome::Stored),
                        false
                    )
                    .is_err(),
                RequestKind::Write
                    .check(
                        EffectId::first(),
                        &Outcome::Store(StoreOutcome::Deleted(Vec::new())),
                        false
                    )
                    .is_err()
            ),
            (Ok(()), true, true)
        );
    }

    /// RFC §13.6: a deadline passed answers an effect that carried one, of
    /// any family, and no other.
    #[test]
    fn a_passed_deadline_answers_a_bounded_effect_only() {
        assert_eq!(
            (
                RequestKind::RunHook.check(EffectId::first(), &Outcome::DeadlinePassed, true),
                RequestKind::AwaitTicket.check(EffectId::first(), &Outcome::DeadlinePassed, true),
                RequestKind::Commit.check(EffectId::first(), &Outcome::DeadlinePassed, false)
            ),
            (
                Ok(()),
                Ok(()),
                Err(ProtocolError::UnexpectedOutcome {
                    effect: EffectId::first(),
                    expected: "Committed",
                    got: "DeadlinePassed",
                })
            )
        );
    }

    #[test]
    fn a_ticket_s_answers_answer_their_own_requests_only() {
        assert_eq!(
            (
                RequestKind::TakeTicket.check(
                    EffectId::first(),
                    &Outcome::Hold(HoldOutcome::Ticketed),
                    false
                ),
                RequestKind::AwaitTicket.check(
                    EffectId::first(),
                    &Outcome::Hold(HoldOutcome::Admitted),
                    false
                ),
                RequestKind::ReleaseTicket.check(
                    EffectId::first(),
                    &Outcome::Hold(HoldOutcome::TicketReleased),
                    false
                ),
                RequestKind::AwaitTicket
                    .check(
                        EffectId::first(),
                        &Outcome::Hold(HoldOutcome::Ticketed),
                        false
                    )
                    .is_err()
            ),
            (Ok(()), Ok(()), Ok(()), true)
        );
    }

    #[test]
    fn a_missing_room_answers_a_context_request() {
        assert_eq!(
            RequestKind::LoadContext.check(
                EffectId::first(),
                &Outcome::Store(StoreOutcome::RoomNotFound),
                false
            ),
            Ok(())
        );
    }
}

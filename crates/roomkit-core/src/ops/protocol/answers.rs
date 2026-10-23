//! How an outcome answers its request: the check `Io` applies when the
//! driver completes an effect, and the readers an operation takes an answer
//! through once checked.

use super::{EffectId, Outcome, ProtocolError, Request};
use crate::model::{ChannelOutput, ErrorRecord, IdentityId, RoomContext, RoomEvent, RoomId};
use crate::rules::hook_chain::HookOutcome;

/// The families of requests, each answered by its own outcomes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RequestKind {
    AcquireLock,
    ReleaseLock,
    EnterLane,
    LeaveLane,
    TakeTicket,
    AwaitTicket,
    ReleaseTicket,
    LoadContext,
    FindByIdempotencyKey,
    GetEvent,
    DeleteEvent,
    Commit,
    PersistSideEffects,
    Store,
    HandleInbound,
    ChannelCall,
    RunHook,
    RunObservers,
    ResolveIdentity,
    Route,
}

impl Request {
    pub(crate) fn kind(&self) -> RequestKind {
        match self {
            Self::AcquireLock { .. } => RequestKind::AcquireLock,
            Self::ReleaseLock { .. } => RequestKind::ReleaseLock,
            Self::EnterLane { .. } => RequestKind::EnterLane,
            Self::LeaveLane { .. } => RequestKind::LeaveLane,
            Self::TakeTicket { .. } => RequestKind::TakeTicket,
            Self::AwaitTicket { .. } => RequestKind::AwaitTicket,
            Self::ReleaseTicket { .. } => RequestKind::ReleaseTicket,
            Self::LoadContext { .. } => RequestKind::LoadContext,
            Self::FindByIdempotencyKey { .. } => RequestKind::FindByIdempotencyKey,
            Self::GetEvent { .. } => RequestKind::GetEvent,
            Self::DeleteEvent { .. } => RequestKind::DeleteEvent,
            Self::Commit { .. } => RequestKind::Commit,
            Self::PersistSideEffects { .. } => RequestKind::PersistSideEffects,
            Self::CreateRoom { .. }
            | Self::UpdateRoom { .. }
            | Self::StoreBinding { .. }
            | Self::DeleteBinding { .. }
            | Self::UpdateEvent { .. } => RequestKind::Store,
            Self::HandleInbound { .. } => RequestKind::HandleInbound,
            Self::OnEvent { .. } | Self::Deliver { .. } => RequestKind::ChannelCall,
            Self::RunHook { .. } => RequestKind::RunHook,
            Self::RunObservers { .. } => RequestKind::RunObservers,
            Self::ResolveIdentity { .. } => RequestKind::ResolveIdentity,
            Self::Route { .. } => RequestKind::Route,
        }
    }
}

impl RequestKind {
    /// The outcomes that answer this family, for error messages.
    fn expected(self) -> &'static str {
        match self {
            Self::AcquireLock => "Locked",
            Self::ReleaseLock => "Released",
            Self::EnterLane => "Entered",
            Self::LeaveLane => "Left",
            Self::TakeTicket => "Ticketed",
            Self::AwaitTicket => "Admitted",
            Self::ReleaseTicket => "TicketReleased",
            Self::LoadContext => "Context or RoomNotFound",
            Self::FindByIdempotencyKey => "Idempotent",
            Self::GetEvent => "Event",
            Self::DeleteEvent => "Deleted",
            Self::Commit => "Committed",
            Self::PersistSideEffects => "Persisted",
            Self::Store => "Stored",
            Self::HandleInbound => "Parsed",
            Self::ChannelCall => "Output",
            Self::RunHook => "Hook",
            Self::RunObservers => "Observed",
            Self::ResolveIdentity => "Resolved",
            Self::Route => "Routed",
        }
    }

    /// Checks that `outcome` answers this family; `Failed` answers every
    /// one, and `DeadlinePassed` every one whose effect carried a deadline
    /// (`bounded`).
    pub(crate) fn check(
        self,
        effect: EffectId,
        outcome: &Outcome,
        bounded: bool,
    ) -> Result<(), ProtocolError> {
        let fits = match (self, outcome) {
            (_, Outcome::DeadlinePassed) => bounded,
            (_, Outcome::Failed(_))
            | (Self::AcquireLock, Outcome::Locked)
            | (Self::ReleaseLock, Outcome::Released)
            | (Self::EnterLane, Outcome::Entered)
            | (Self::LeaveLane, Outcome::Left)
            | (Self::TakeTicket, Outcome::Ticketed)
            | (Self::AwaitTicket, Outcome::Admitted)
            | (Self::ReleaseTicket, Outcome::TicketReleased)
            | (Self::LoadContext, Outcome::Context(_) | Outcome::RoomNotFound)
            | (Self::FindByIdempotencyKey, Outcome::Idempotent(_))
            | (Self::GetEvent, Outcome::Event(_))
            | (Self::DeleteEvent, Outcome::Deleted(_))
            | (Self::Commit, Outcome::Committed)
            | (Self::PersistSideEffects, Outcome::Persisted)
            | (Self::Store, Outcome::Stored)
            | (Self::HandleInbound, Outcome::Parsed(_))
            | (Self::ChannelCall, Outcome::Output(_))
            | (Self::RunHook, Outcome::Hook(_))
            | (Self::RunObservers, Outcome::Observed)
            | (Self::ResolveIdentity, Outcome::Resolved(_))
            | (Self::Route, Outcome::Routed(_)) => true,
            (
                Self::AcquireLock
                | Self::ReleaseLock
                | Self::EnterLane
                | Self::LeaveLane
                | Self::TakeTicket
                | Self::AwaitTicket
                | Self::ReleaseTicket
                | Self::LoadContext
                | Self::FindByIdempotencyKey
                | Self::GetEvent
                | Self::DeleteEvent
                | Self::Commit
                | Self::PersistSideEffects
                | Self::Store
                | Self::HandleInbound
                | Self::ChannelCall
                | Self::RunHook
                | Self::RunObservers
                | Self::ResolveIdentity
                | Self::Route,
                _,
            ) => false,
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
    pub(crate) fn into_locked(self) -> Result<(), ErrorRecord> {
        match self {
            Self::Locked => Ok(()),
            other => Err(other.into_failure()),
        }
    }

    pub(crate) fn into_released(self) -> Result<(), ErrorRecord> {
        match self {
            Self::Released => Ok(()),
            other => Err(other.into_failure()),
        }
    }

    pub(crate) fn into_entered(self) -> Result<(), ErrorRecord> {
        match self {
            Self::Entered => Ok(()),
            other => Err(other.into_failure()),
        }
    }

    pub(crate) fn into_left(self) -> Result<(), ErrorRecord> {
        match self {
            Self::Left => Ok(()),
            other => Err(other.into_failure()),
        }
    }

    pub(crate) fn into_ticketed(self) -> Result<(), ErrorRecord> {
        match self {
            Self::Ticketed => Ok(()),
            other => Err(other.into_failure()),
        }
    }

    pub(crate) fn into_admitted(self) -> Result<(), ErrorRecord> {
        match self {
            Self::Admitted => Ok(()),
            other => Err(other.into_failure()),
        }
    }

    pub(crate) fn into_ticket_released(self) -> Result<(), ErrorRecord> {
        match self {
            Self::TicketReleased => Ok(()),
            other => Err(other.into_failure()),
        }
    }

    pub(crate) fn into_observed(self) -> Result<(), ErrorRecord> {
        match self {
            Self::Observed => Ok(()),
            other => Err(other.into_failure()),
        }
    }

    /// The room's context, `None` when the room does not exist.
    pub(crate) fn into_context(self) -> Result<Option<Box<RoomContext>>, ErrorRecord> {
        match self {
            Self::Context(context) => Ok(Some(context)),
            Self::RoomNotFound => Ok(None),
            other => Err(other.into_failure()),
        }
    }

    pub(crate) fn into_idempotent(self) -> Result<Option<Box<RoomEvent>>, ErrorRecord> {
        match self {
            Self::Idempotent(event) => Ok(event),
            other => Err(other.into_failure()),
        }
    }

    /// The event read, `None` when the store holds none with the id.
    pub(crate) fn into_event(self) -> Result<Option<Box<RoomEvent>>, ErrorRecord> {
        match self {
            Self::Event(event) => Ok(event),
            other => Err(other.into_failure()),
        }
    }

    /// The events a delete removed, the target first.
    pub(crate) fn into_deleted(self) -> Result<Vec<RoomEvent>, ErrorRecord> {
        match self {
            Self::Deleted(events) => Ok(events),
            other => Err(other.into_failure()),
        }
    }

    pub(crate) fn into_committed(self) -> Result<(), ErrorRecord> {
        match self {
            Self::Committed => Ok(()),
            other => Err(other.into_failure()),
        }
    }

    pub(crate) fn into_persisted(self) -> Result<(), ErrorRecord> {
        match self {
            Self::Persisted => Ok(()),
            other => Err(other.into_failure()),
        }
    }

    pub(crate) fn into_stored(self) -> Result<(), ErrorRecord> {
        match self {
            Self::Stored => Ok(()),
            other => Err(other.into_failure()),
        }
    }

    pub(crate) fn into_parsed(self) -> Result<Box<RoomEvent>, ErrorRecord> {
        match self {
            Self::Parsed(event) => Ok(event),
            other => Err(other.into_failure()),
        }
    }

    pub(crate) fn into_output(self) -> Result<Box<ChannelOutput>, ErrorRecord> {
        match self {
            Self::Output(output) => Ok(output),
            other => Err(other.into_failure()),
        }
    }

    /// The identity an address resolved to, if any.
    pub(crate) fn into_resolved(self) -> Result<Option<IdentityId>, ErrorRecord> {
        match self {
            Self::Resolved(identity) => Ok(identity),
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
            Self::Hook(outcome) => *outcome,
            other => HookOutcome::Raised(other.into_failure()),
        }
    }

    fn into_failure(self) -> ErrorRecord {
        match self {
            Self::Failed(error) => error,
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
            Self::Locked => "Locked",
            Self::Released => "Released",
            Self::Entered => "Entered",
            Self::Left => "Left",
            Self::Ticketed => "Ticketed",
            Self::Admitted => "Admitted",
            Self::TicketReleased => "TicketReleased",
            Self::Context(_) => "Context",
            Self::RoomNotFound => "RoomNotFound",
            Self::Idempotent(_) => "Idempotent",
            Self::Event(_) => "Event",
            Self::Deleted(_) => "Deleted",
            Self::Committed => "Committed",
            Self::Persisted => "Persisted",
            Self::Stored => "Stored",
            Self::Parsed(_) => "Parsed",
            Self::Output(_) => "Output",
            Self::Hook(_) => "Hook",
            Self::Observed => "Observed",
            Self::Resolved(_) => "Resolved",
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
            RequestKind::Commit.check(EffectId::first(), &Outcome::Locked, false),
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
                RequestKind::GetEvent.check(EffectId::first(), &Outcome::Event(None), false),
                RequestKind::GetEvent
                    .check(EffectId::first(), &Outcome::Stored, false)
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
                    &Outcome::Deleted(Vec::new()),
                    false
                ),
                RequestKind::DeleteEvent
                    .check(EffectId::first(), &Outcome::Stored, false)
                    .is_err(),
                RequestKind::Store
                    .check(EffectId::first(), &Outcome::Deleted(Vec::new()), false)
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
                RequestKind::TakeTicket.check(EffectId::first(), &Outcome::Ticketed, false),
                RequestKind::AwaitTicket.check(EffectId::first(), &Outcome::Admitted, false),
                RequestKind::ReleaseTicket.check(
                    EffectId::first(),
                    &Outcome::TicketReleased,
                    false
                ),
                RequestKind::AwaitTicket
                    .check(EffectId::first(), &Outcome::Ticketed, false)
                    .is_err()
            ),
            (Ok(()), Ok(()), Ok(()), true)
        );
    }

    #[test]
    fn a_missing_room_answers_a_context_request() {
        assert_eq!(
            RequestKind::LoadContext.check(EffectId::first(), &Outcome::RoomNotFound, false),
            Ok(())
        );
    }
}

//! How an outcome answers its request: the check `Io` applies when the
//! driver completes an effect, and the readers an operation takes an answer
//! through once checked.

use super::{EffectId, Outcome, ProtocolError, Request};
use crate::model::{ChannelBinding, ChannelOutput, ErrorRecord, RoomContext, RoomEvent};
use crate::rules::hook_chain::HookOutcome;

/// The families of requests, each answered by its own outcomes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RequestKind {
    AcquireLock,
    ReleaseLock,
    EnterLane,
    LeaveLane,
    LoadContext,
    LoadBindings,
    FindByIdempotencyKey,
    Commit,
    PersistSideEffects,
    Store,
    HandleInbound,
    ChannelCall,
    RunHook,
    RunObservers,
}

impl Request {
    pub(crate) fn kind(&self) -> RequestKind {
        match self {
            Self::AcquireLock { .. } => RequestKind::AcquireLock,
            Self::ReleaseLock { .. } => RequestKind::ReleaseLock,
            Self::EnterLane { .. } => RequestKind::EnterLane,
            Self::LeaveLane { .. } => RequestKind::LeaveLane,
            Self::LoadContext { .. } => RequestKind::LoadContext,
            Self::LoadBindings { .. } => RequestKind::LoadBindings,
            Self::FindByIdempotencyKey { .. } => RequestKind::FindByIdempotencyKey,
            Self::Commit { .. } => RequestKind::Commit,
            Self::PersistSideEffects { .. } => RequestKind::PersistSideEffects,
            Self::CreateRoom { .. }
            | Self::UpdateRoom { .. }
            | Self::StoreBinding { .. }
            | Self::DeleteBinding { .. } => RequestKind::Store,
            Self::HandleInbound { .. } => RequestKind::HandleInbound,
            Self::OnEvent { .. } | Self::Deliver { .. } => RequestKind::ChannelCall,
            Self::RunHook { .. } => RequestKind::RunHook,
            Self::RunObservers { .. } => RequestKind::RunObservers,
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
            Self::LoadContext => "Context or RoomNotFound",
            Self::LoadBindings => "Bindings",
            Self::FindByIdempotencyKey => "Idempotent",
            Self::Commit => "Committed",
            Self::PersistSideEffects => "Persisted",
            Self::Store => "Stored",
            Self::HandleInbound => "Parsed",
            Self::ChannelCall => "Output",
            Self::RunHook => "Hook",
            Self::RunObservers => "Observed",
        }
    }

    /// Checks that `outcome` answers this family; `Failed` answers every one.
    pub(crate) fn check(self, effect: EffectId, outcome: &Outcome) -> Result<(), ProtocolError> {
        let fits = match (self, outcome) {
            (_, Outcome::Failed(_))
            | (Self::AcquireLock, Outcome::Locked)
            | (Self::ReleaseLock, Outcome::Released)
            | (Self::EnterLane, Outcome::Entered)
            | (Self::LeaveLane, Outcome::Left)
            | (Self::LoadContext, Outcome::Context(_) | Outcome::RoomNotFound)
            | (Self::LoadBindings, Outcome::Bindings(_))
            | (Self::FindByIdempotencyKey, Outcome::Idempotent(_))
            | (Self::Commit, Outcome::Committed)
            | (Self::PersistSideEffects, Outcome::Persisted)
            | (Self::Store, Outcome::Stored)
            | (Self::HandleInbound, Outcome::Parsed(_))
            | (Self::ChannelCall, Outcome::Output(_))
            | (Self::RunHook, Outcome::Hook(_))
            | (Self::RunObservers, Outcome::Observed) => true,
            (
                Self::AcquireLock
                | Self::ReleaseLock
                | Self::EnterLane
                | Self::LeaveLane
                | Self::LoadContext
                | Self::LoadBindings
                | Self::FindByIdempotencyKey
                | Self::Commit
                | Self::PersistSideEffects
                | Self::Store
                | Self::HandleInbound
                | Self::ChannelCall
                | Self::RunHook
                | Self::RunObservers,
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

    pub(crate) fn into_bindings(self) -> Result<Vec<ChannelBinding>, ErrorRecord> {
        match self {
            Self::Bindings(bindings) => Ok(bindings),
            other => Err(other.into_failure()),
        }
    }

    pub(crate) fn into_idempotent(self) -> Result<Option<Box<RoomEvent>>, ErrorRecord> {
        match self {
            Self::Idempotent(event) => Ok(event),
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
            Self::Context(_) => "Context",
            Self::RoomNotFound => "RoomNotFound",
            Self::Bindings(_) => "Bindings",
            Self::Idempotent(_) => "Idempotent",
            Self::Committed => "Committed",
            Self::Persisted => "Persisted",
            Self::Stored => "Stored",
            Self::Parsed(_) => "Parsed",
            Self::Output(_) => "Output",
            Self::Hook(_) => "Hook",
            Self::Observed => "Observed",
            Self::Failed(_) => "Failed",
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
            RequestKind::Commit.check(EffectId::first(), &failed),
            Ok(())
        );
    }

    #[test]
    fn an_outcome_of_another_family_is_a_protocol_error() {
        assert_eq!(
            RequestKind::Commit.check(EffectId::first(), &Outcome::Locked),
            Err(ProtocolError::UnexpectedOutcome {
                effect: EffectId::first(),
                expected: "Committed",
                got: "Locked",
            })
        );
    }

    #[test]
    fn a_missing_room_answers_a_context_request() {
        assert_eq!(
            RequestKind::LoadContext.check(EffectId::first(), &Outcome::RoomNotFound),
            Ok(())
        );
    }
}

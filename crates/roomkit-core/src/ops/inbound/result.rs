//! The results an inbound call builds: a message refused past its reentry
//! budget, and a direct injection's result as its caller receives it. The
//! types are the protocol's (`protocol::results`).

use super::{InboundError, InboundResult};
use crate::rules::chain_depth;

/// The result of a message refused past the reentry budget it draws on,
/// before its parse, with no event (RFC §8.3, gap G-070).
pub(super) fn past_the_budget() -> InboundResult {
    InboundResult {
        blocked: true,
        reason: Some(chain_depth::REENTRY_LOOP_CAP.to_owned()),
        refused_before_the_lock: true,
        ..InboundResult::default()
    }
}

/// A direct injection's result read as its caller receives it (RFC §10.5):
/// the committed event's result, a record stored `BLOCKED` included, or an
/// instruction's, uncommitted (RFC §10.1.1). A result with no event was
/// refused before its commit, with nothing written: it is raised as
/// [`InboundError::Refused`], with its reason.
///
/// # Errors
///
/// Returns [`InboundError::Refused`] for a result that carries no event.
pub(super) fn committed(result: InboundResult) -> Result<InboundResult, InboundError> {
    if result.event.is_some() {
        return Ok(result);
    }
    Err(InboundError::Refused {
        reason: result.reason.unwrap_or_else(|| "refused".to_owned()),
    })
}

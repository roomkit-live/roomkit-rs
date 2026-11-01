//! What a proactive delivery came to (RFC §22.1): Python's
//! `DeliveryOutcome`.

use std::collections::BTreeMap;

use roomkit_core::model::{DeliveryError, EventId};
use serde::{Deserialize, Serialize};

use crate::inbound::InboundResult;

/// Where a proactive delivery stands (RFC §22.1): the Python
/// implementation's `Literal` of `DeliveryOutcome.status`, named after its
/// field.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DeliveryOutcomeStatus {
    /// Accepted by the delivery backend; not sent yet.
    Queued,
    /// Published into the room, or accepted by the voice provider. It does
    /// not say the agent completed its turn.
    Sent,
    /// Refused: by a hook, the room, the request's own form.
    Blocked,
    /// The destination cannot be reached.
    Unavailable,
    /// Its execution failed.
    Failed,
    /// Its outcome is not known: a custom strategy that reported none.
    Unknown,
}

/// What a proactive delivery came to (RFC §22.1): Python's
/// `DeliveryOutcome(*, status, reason=None, delivery_item_id=None,
/// event_id=None, duplicate=False, unavailable_targets=<factory>,
/// session_ids=<factory>, session_outcomes=<factory>, error=None,
/// turn_complete=False, inbound=None)`. An unsuccessful outcome carries its
/// reason; `sent` does not imply that the agent completed its turn, which
/// `turn_complete` says.
///
/// It serializes without `inbound`, which holds the live inbound result, as
/// Python's model excludes it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DeliveryOutcome {
    /// Where it stands.
    pub status: DeliveryOutcomeStatus,
    /// Why it is not `sent`, or what a `sent` one was (`duplicate_publication`).
    #[serde(default)]
    pub reason: Option<String>,
    /// The id of its item in the delivery backend, or of the request
    /// executed.
    #[serde(default)]
    pub delivery_item_id: Option<String>,
    /// The event it committed, or the one its key replays.
    #[serde(default)]
    pub event_id: Option<EventId>,
    /// Whether its key named a publication the room already held.
    #[serde(default)]
    pub duplicate: bool,
    /// The targets it could not reach: an addressed agent, a channel.
    #[serde(default)]
    pub unavailable_targets: Vec<String>,
    /// The realtime sessions it was injected into.
    #[serde(default)]
    pub session_ids: Vec<String>,
    /// Each selected realtime session's own outcome.
    #[serde(default)]
    pub session_outcomes: BTreeMap<String, DeliveryOutcome>,
    /// What failed, and whether a retry may succeed.
    #[serde(default)]
    pub error: Option<DeliveryError>,
    /// Whether the turn its publication asked for is over.
    #[serde(default)]
    pub turn_complete: bool,
    /// The inbound result of its publication, the turn's details among
    /// them; never serialized.
    #[serde(skip)]
    pub inbound: Option<InboundResult>,
}

impl DeliveryOutcome {
    /// An outcome standing at `status`, with no reason, event, error or
    /// target.
    pub fn new(status: DeliveryOutcomeStatus) -> Self {
        Self {
            status,
            reason: None,
            delivery_item_id: None,
            event_id: None,
            duplicate: false,
            unavailable_targets: Vec::new(),
            session_ids: Vec::new(),
            session_outcomes: BTreeMap::new(),
            error: None,
            turn_complete: false,
            inbound: None,
        }
    }

    /// The request refused for `reason`.
    pub(crate) fn blocked(reason: impl Into<String>) -> Self {
        Self {
            reason: Some(reason.into()),
            ..Self::new(DeliveryOutcomeStatus::Blocked)
        }
    }

    /// A destination that may become available on a later attempt, Python's
    /// `unavailable(reason, targets)`: `targets` unreachable, a retryable
    /// error of `reason`.
    pub(crate) fn unavailable(reason: &str, targets: Vec<String>) -> Self {
        Self {
            reason: Some(reason.to_owned()),
            unavailable_targets: targets,
            error: Some(DeliveryError::new(reason, reason)),
            ..Self::new(DeliveryOutcomeStatus::Unavailable)
        }
    }

    /// An execution that failed for `reason` with `error`.
    pub(crate) fn failed(reason: &str, error: DeliveryError) -> Self {
        Self {
            reason: Some(reason.to_owned()),
            error: Some(error),
            ..Self::new(DeliveryOutcomeStatus::Failed)
        }
    }

    /// Whether it is an unsuccessful outcome: blocked, unavailable, failed
    /// or unknown.
    pub(crate) fn unsuccessful(&self) -> bool {
        !matches!(
            self.status,
            DeliveryOutcomeStatus::Queued | DeliveryOutcomeStatus::Sent
        )
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    // Python: tests/test_proactive_delivery.py::test_result_is_public_and_serializable_without_runtime_handle
    #[test]
    fn an_outcome_serializes_without_its_runtime_handle() {
        let outcome = DeliveryOutcome {
            inbound: Some(InboundResult::default()),
            ..DeliveryOutcome::unavailable("no_transport", Vec::new())
        };
        let written = serde_json::to_value(&outcome).unwrap();
        assert_eq!(
            written,
            json!({
                "status": "unavailable",
                "reason": "no_transport",
                "delivery_item_id": null,
                "event_id": null,
                "duplicate": false,
                "unavailable_targets": [],
                "session_ids": [],
                "session_outcomes": {},
                "error": {"code": "no_transport", "message": "no_transport", "retryable": true},
                "turn_complete": false,
            })
        );
        let read: DeliveryOutcome = serde_json::from_value(written).unwrap();
        assert_eq!(read.inbound, None);
        assert_eq!(read.reason.as_deref(), Some("no_transport"));
    }
}

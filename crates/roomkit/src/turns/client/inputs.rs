//! Application access to the owner's pending human-input requests.
use crate::turns::{TurnRequest, Turns, wire::InputRequest};
use std::sync::Arc;

/// The opening turn of a standalone client's human-input request (§10.6.5).
#[derive(Clone, Debug)]
pub struct TurnClientInputTurn {
    /// The request carried by the turn's link. Requests opened by the same
    /// turn share this snapshot.
    pub request: Arc<TurnRequest>,
}

/// The inputs held by one standalone client (RFC §10.6.5).
#[derive(Clone, Copy, Debug)]
pub struct TurnClientInputs<'a>(pub(super) &'a Turns);
impl TurnClientInputs<'_> {
    /// The opening turn and the executor's original request, while it is pending.
    /// For a tenant-filtered event with its room and channel resolved, use
    /// [`TurnClient::input_request`](super::TurnClient::input_request).
    pub fn request(&self, request_id: &str) -> Option<(TurnClientInputTurn, InputRequest)> {
        let (request, input) = self.0.inputs.client_request(request_id)?;
        Some((TurnClientInputTurn { request }, input))
    }
    /// Send the answer, reporting whether the executor accepted it.
    pub async fn answer(&self, id: &str, value: &str) -> bool {
        self.0.answer_input(id, value).await
    }
    /// Reject the request, reporting whether the executor accepted the rejection.
    pub async fn reject(&self, id: &str, reason: &str) -> bool {
        self.0.reject_input(id, reason).await
    }
}

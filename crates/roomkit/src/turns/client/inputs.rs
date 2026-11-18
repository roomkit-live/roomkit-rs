//! Application access to the owner's pending human-input requests.
use crate::human_input::PendingInputEvent;
use crate::turns::Turns;

/// The inputs held by one standalone client (RFC §10.6.5).
#[derive(Clone, Copy, Debug)]
pub struct TurnClientInputs<'a>(pub(super) &'a Turns);
impl TurnClientInputs<'_> {
    /// An owned snapshot with the room, channel, actor and tool arguments.
    pub fn request(&self, id: &str) -> Option<PendingInputEvent> {
        self.0.inputs.request(id, None)
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

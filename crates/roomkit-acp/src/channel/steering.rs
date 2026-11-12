//! A steering Cancel addresses the turn, never an idle room session.

use roomkit::model::SteeringDirective;
use std::sync::PoisonError;

use super::{AcpChannel, preparation::Cancellation};
use crate::{AcpError, AcpTransport};

impl<T: AcpTransport> AcpChannel<T> {
    /// Steer the named turn, as Python's `steer(directive, *, turn_id)`.
    /// A Cancel stops its preparation or asks its agent to end the prompt,
    /// returning one. Before the turn reaches the agent, returns `None` so
    /// its owner can cut it. Another directive reaches zero loops.
    ///
    /// # Errors
    /// The transport failed to write the cancellation notification.
    pub async fn steer(
        &self,
        directive: &SteeringDirective,
        turn_id: &str,
    ) -> Result<Option<usize>, AcpError> {
        if !matches!(directive, SteeringDirective::Cancel(_)) {
            return Ok(Some(0));
        }
        let target = self
            .shared
            .preparations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .find_map(|preparation| preparation.cancel_turn(turn_id));
        let Some(target) = target else {
            return Ok(None);
        };
        if let Cancellation::Prompting(session) = target {
            self.shared.connector.cancel_session(session).await?;
        }
        Ok(Some(1))
    }
}

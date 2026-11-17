//! Owner calls and inline completions share the turn's original cut.

use std::sync::atomic::Ordering;

use tokio::time::Instant;

use super::{Ending, Pump, until};

impl Pump {
    /// The executor ended. Stop this turn's owner calls before reading its
    /// inline registry: an aborted call cannot register a later child. Then
    /// wait for those children's existing completion, within the same cut.
    pub(super) async fn end_calls(&mut self) -> Ending {
        let mut calls = std::mem::take(&mut self.withdrawn);
        calls.extend(self.calls.drain().map(|(_, call)| call));
        calls.retain(|call| !self.kit.is_close_caller(call.id()));
        for call in &calls {
            call.abort();
        }
        let mut cancelled = self.state.cancel.subscribe();
        let mut deadline = self.cancel_deadline();
        let cleanup = async {
            for call in calls {
                drop(call.await);
            }
            self.kit.end_inline_calls(&self.state.turn.turn_id).await;
        };
        tokio::pin!(cleanup);
        loop {
            tokio::select! {
                biased;
                () = &mut cleanup => return Ending::Ended,
                () = until(deadline) => return Ending::GaveUp,
                Ok(()) = cancelled.changed(), if deadline.is_none() => {
                    deadline = self.cancel_deadline();
                }
            }
        }
    }

    fn cancel_deadline(&self) -> Option<Instant> {
        if self.state.steer_cut.load(Ordering::Acquire) {
            None
        } else {
            *self.state.cancel.borrow()
        }
    }
}

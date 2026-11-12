//! Detachment never waits for the agent; its room turn owns the remaining work.

use std::sync::{Arc, PoisonError};

use roomkit::model::RoomId;

use super::{AcpChannel, Shared};
use crate::transport::AcpTransport;

impl<T: AcpTransport> AcpChannel<T> {
    pub(super) fn detach_room(&self, room: RoomId) {
        let Some(session) = self.shared.connector.detach(&room) else {
            return;
        };
        let shared = Arc::clone(&self.shared);
        let mut closings = self
            .shared
            .closings
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if self.shared.connector.is_closed() {
            return;
        }
        while closings.try_join_next().is_some() {}
        closings.spawn(async move {
            let _room = shared.holding_room(&room).await;
            shared.connector.close_detached(&room, &session).await;
        });
    }
}

impl<T: AcpTransport> Shared<T> {
    pub(super) fn stop_closings(&self) {
        self.closings
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .abort_all();
    }
}

/// Cancelling channel shutdown must stop the detached closes too, as teardown does.
pub(super) struct ClosingTasks<'a, T: AcpTransport>(pub(super) &'a Shared<T>);

impl<T: AcpTransport> Drop for ClosingTasks<'_, T> {
    fn drop(&mut self) {
        self.0.stop_closings();
    }
}

//! Session-scoped acknowledgement, retired with the session itself.

use roomkit::model::RoomId;
use std::sync::Arc;

use super::Connector;
use crate::channel::context::Seen;
use crate::transport::AcpTransport;

impl<T: AcpTransport> Connector<T> {
    pub(in crate::channel) fn seen(&self, room: &RoomId) -> Seen {
        self.lock().seen.get(room).copied().unwrap_or_default()
    }

    pub(in crate::channel) fn acknowledged(
        &self,
        room: &RoomId,
        connection: &Arc<T::Connection>,
        session: &str,
        index: u64,
        labelled: bool,
    ) {
        let mut state = self.lock();
        // Each handshake gets a new Arc, even if the transport's connection
        // value shares state or reuses the wire's session identifiers.
        if state.closed
            || !state
                .connection
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, connection))
            || state.sessions.get(room).map(String::as_str) != Some(session)
        {
            return;
        }
        let seen = state.seen.entry(room.clone()).or_default();
        seen.index = Some(seen.index.map_or(index, |before| before.max(index)));
        seen.labelled |= labelled;
    }
}

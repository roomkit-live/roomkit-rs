//! Session-scoped acknowledgement, retired with the session itself.

use roomkit::model::RoomId;

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
        session: &str,
        index: u64,
        labelled: bool,
    ) {
        let mut state = self.lock();
        if state.sessions.get(room).map(String::as_str) != Some(session) {
            return;
        }
        let seen = state.seen.entry(room.clone()).or_default();
        seen.index = Some(seen.index.map_or(index, |before| before.max(index)));
        seen.labelled |= labelled;
    }
}

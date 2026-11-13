//! Per-room detach generations guard preparations and late host settings.

use roomkit::model::RoomId;

use super::{Connector, release::Unset};
use crate::transport::AcpTransport;

impl<T: AcpTransport> Connector<T> {
    pub(in crate::channel) fn generation(&self, room: &RoomId) -> u64 {
        self.lock().detached.get(room).copied().unwrap_or_default()
    }

    pub(in crate::channel) fn detach(&self, room: &RoomId) -> Option<String> {
        let mut state = self.lock();
        state.config.remove(room);
        let generation = state.detached.entry(room.clone()).or_default();
        *generation = generation.wrapping_add(1);
        state.sessions.get(room).cloned()
    }

    pub(in crate::channel) async fn close_detached(&self, room: &RoomId, session: &str) {
        let _opening = self.opening(room).await;
        if let Some(mut discarded) = self.discard_session(room, session) {
            discarded.release().await;
        }
    }

    /// Never discard a newer session that a reattachment opened meanwhile.
    pub(in crate::channel) fn discard_session(
        &self,
        room: &RoomId,
        session: &str,
    ) -> Option<Unset<T::Connection>> {
        let mut state = self.lock();
        if state.sessions.get(room).map(String::as_str) != Some(session) {
            return None;
        }
        state.options.remove(room);
        state.seen.remove(room);
        let session = state.sessions.remove(room)?;
        self.turns.forget(&session);
        Some(Unset {
            connection: state.connection.clone()?,
            session: Some(session),
            closes: state.closes_sessions,
        })
    }
}

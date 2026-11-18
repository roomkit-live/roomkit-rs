//! An adapter's publication belongs to one turn's room, channel and answer scope.

use super::TurnHost;
use crate::model::{RoomEvent, Visibility};
use crate::realtime::EphemeralEvent;

impl TurnHost {
    pub(crate) fn with_publication_scope(mut self, event: &RoomEvent) -> Self {
        self.answer_unscoped = event
            .response_visibility
            .as_ref()
            .is_none_or(|scope| *scope == Visibility::All);
        self
    }

    /// Publish an adapter notification only in the turn it names.
    /// Captured hosts retain this bound in tasks created with `keep_context`.
    #[doc(hidden)]
    pub fn publish_from_channel(&self, event: EphemeralEvent) {
        self.publish_scoped(None, event);
    }

    pub(crate) fn publish_scoped(&self, channel: Option<&str>, event: EphemeralEvent) {
        if !self.answer_unscoped
            || event.room_id != self.shared.room_id
            || event
                .channel_id
                .as_ref()
                .is_some_and(|id| id != &self.shared.channel_id)
        {
            return;
        }
        self.publish_on(channel, event);
    }
}

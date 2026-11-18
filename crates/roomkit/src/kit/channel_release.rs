//! Releasing the kit's locally held channels (RFC §10.6.3).

use roomkit_core::model::ChannelId;

use super::RoomKit;
use crate::{ConversationStore, RoomKitError};

impl<S: ConversationStore> RoomKit<S> {
    /// Release an executor's build of `channel_id`.
    ///
    /// For a declaration, each executor releases its own build after the
    /// turns using it finish. The next use builds the channel anew.
    /// The kit's local executor borrows its registered live channels:
    /// releasing one leaves that same channel registered and open, with
    /// its room state intact. The host replaces it through the registry.
    ///
    /// # Errors
    /// Returns [`RoomKitError::KitClosed`] once the kit starts closing,
    /// before checking the id; otherwise [`RoomKitError::ChannelNotRegistered`]
    /// when no channel is registered under it. A distributed backend can
    /// also fail to deliver the release.
    pub async fn release_channel(&self, channel_id: &ChannelId) -> Result<(), RoomKitError> {
        self.inner.open()?;
        let channel = self
            .inner
            .channel(channel_id)
            .ok_or_else(|| RoomKitError::channel_not_registered(channel_id.clone()))?;
        self.inner.turns.release(channel_id, channel).await
    }
}

#[cfg(test)]
#[path = "tests/channel_release.rs"]
mod tests;

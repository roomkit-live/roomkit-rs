//! Releasing the kit's locally held channels (RFC §10.6.3).

use roomkit_core::model::ChannelId;

use super::RoomKit;
use crate::{ConversationStore, RoomKitError};

impl<S: ConversationStore> RoomKit<S> {
    /// Release an executor's build of `channel_id`.
    ///
    /// The kit's local executor borrows its registered live channels:
    /// releasing one leaves that same channel registered and open, with
    /// its room state intact. The host replaces it through the registry.
    ///
    /// # Errors
    /// Returns [`RoomKitError::KitClosed`] once the kit starts closing,
    /// before checking the id; otherwise [`RoomKitError::ChannelNotRegistered`]
    /// when no channel is registered under it.
    #[expect(
        clippy::unused_async,
        clippy::unused_async_trait_impl,
        reason = "Python's public release_channel remains awaitable for local live channels"
    )]
    pub async fn release_channel(&self, channel_id: &ChannelId) -> Result<(), RoomKitError> {
        self.inner.open()?;
        if self.inner.channel(channel_id).is_none() {
            return Err(RoomKitError::channel_not_registered(channel_id.clone()));
        }
        // A live registry channel is borrowed, never an executor-owned build.
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests/channel_release.rs"]
mod tests;

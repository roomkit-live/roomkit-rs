//! A channel's speech output in a room (RFC §5.7 `output_muted`): muted, a
//! speech-to-speech provider's audio is not forwarded and injected text is
//! not spoken, while the channel still receives every event. roomkit-rs
//! has no speech channel yet, and a channel is not told of the change
//! (Python's `Channel.update_binding` is not in 0.x): the binding holds the
//! flag for whoever reads it.

use roomkit_core::model::{BindingUpdate, ChannelBinding, ChannelId, RoomId};

use super::RoomKit;
use crate::error::RoomKitError;
use crate::store::ConversationStore;

/// What [`RoomKit::mute_output`] takes beside the room and the channel:
/// Python's `mute_output(room_id, channel_id, *, organization_id=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MuteOutputOptions {
    /// Python `organization_id=None`: the organization the call acts for
    /// (RFC §17.2). A room of another organization, or of none, is not
    /// found; `None` reads rooms unscoped.
    pub organization_id: Option<String>,
}

/// What [`RoomKit::unmute_output`] takes beside the room and the channel:
/// Python's `unmute_output(room_id, channel_id, *, organization_id=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UnmuteOutputOptions {
    /// Python `organization_id=None`: the organization the call acts for
    /// (RFC §17.2). A room of another organization, or of none, is not
    /// found; `None` reads rooms unscoped.
    pub organization_id: Option<String>,
}

impl<S: ConversationStore> RoomKit<S> {
    /// Mutes a channel's speech output in a room (RFC §5.7): the binding's
    /// `output_muted`, as Python's `mute_output`. The channel still
    /// receives every event; no trigger fires and no record is written, as
    /// a mute of §7.2 would. A channel whose output is muted already is
    /// left as it is (RFC §7.2, "A binding change that changes nothing").
    /// Returns the binding as it now stands.
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::KitClosed`] once the kit's close started (RFC
    /// §12.10.4); [`RoomKitError::RoomNotFound`] when the room does not
    /// exist or is not of the organization `options` names; and
    /// [`RoomKitError::ChannelNotFound`] when the room does not bind the
    /// channel.
    pub async fn mute_output(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        options: MuteOutputOptions,
    ) -> Result<ChannelBinding, RoomKitError> {
        let update = BindingUpdate::mute_output();
        self.update_binding(room_id, channel_id, update, options.organization_id)
            .await
    }

    /// Unmutes a channel's speech output in a room (RFC §5.7), as Python's
    /// `unmute_output`: as [`mute_output`](Self::mute_output) does, the
    /// other way.
    ///
    /// # Errors
    ///
    /// As [`mute_output`](Self::mute_output).
    pub async fn unmute_output(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        options: UnmuteOutputOptions,
    ) -> Result<ChannelBinding, RoomKitError> {
        let update = BindingUpdate::unmute_output();
        self.update_binding(room_id, channel_id, update, options.organization_id)
            .await
    }
}

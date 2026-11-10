//! Steering an intelligence channel's running turns, wherever they run
//! (RFC §21.3, §10.6.8): Python's `RoomKit.steer`.

use roomkit_core::model::{ChannelId, RoomId};

use super::RoomKit;
use crate::GetRoomOptions;
use crate::error::RoomKitError;
use crate::steering::SteeringDirective;
use crate::store::ConversationStore;
use crate::turns::SteerAddress;

/// What [`RoomKit::steer`] takes beside the room, the channel and the
/// directive: Python's `steer(room_id, channel_id, directive, *,
/// turn_id=None, organization_id=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SteerOptions {
    /// Python `turn_id=None`: the turn of the room to reach, as the turn
    /// backend names it (RFC §10.6.8); a turn of another room is reached by
    /// nothing.
    pub turn_id: Option<String>,
    /// Python `organization_id=None`: the organization the caller acts for
    /// (RFC §17.2); the room is read with it first.
    pub organization_id: Option<String>,
}

impl<S: ConversationStore> RoomKit<S> {
    /// Steers the running turns of the intelligence channel `channel_id`
    /// in the room `room_id` (RFC §21.3, §10.6.8), wherever they run:
    /// Python's `steer(room_id, channel_id, directive, *, turn_id=None,
    /// organization_id=None)`.
    ///
    /// The directive reaches the room's turn `turn_id` alone when it is
    /// given, else every turn of the room for a `Cancel` and its most
    /// recently started one for any other directive; never a turn of
    /// another room. Each turn's loop reads it at its next stop
    /// (`roomkit_ai::AiChannel::steer`). A channel without steering takes a
    /// `Cancel` as its turn's cancel, counted as one. Only the turns this
    /// kit started are reached. To stop the turns one message starts,
    /// started or not, cancel its delivery instead
    /// ([`RoomKit::cancel_delivery`]).
    ///
    /// Returns how many loops it reached.
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::RoomNotFound`] when `organization_id` is
    /// given and the room is missing or another organization's; nothing is
    /// steered.
    pub async fn steer(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        directive: impl Into<SteeringDirective>,
        options: SteerOptions,
    ) -> Result<usize, RoomKitError> {
        let SteerOptions {
            turn_id,
            organization_id,
        } = options;
        if let Some(organization) = &organization_id {
            let options = GetRoomOptions {
                organization_id: Some(organization.clone()),
            };
            self.get_room(room_id, options).await?;
        }
        let address = SteerAddress {
            channel: channel_id,
            room: room_id,
            turn: turn_id.as_deref(),
            organization: organization_id.as_deref(),
        };
        let directive = directive.into();
        Ok(self.inner.turns.steer(&directive, address).await)
    }
}

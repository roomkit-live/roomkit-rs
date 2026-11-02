//! Steering an intelligence channel's running turns, wherever they run
//! (RFC §21.3, §10.6.8): Python's `RoomKit.steer`.

use roomkit_core::model::{ChannelId, RoomId};

use super::RoomKit;
use crate::GetRoomOptions;
use crate::error::RoomKitError;
use crate::steering::SteeringDirective;
use crate::store::ConversationStore;
use crate::turns::SteerAddress;

/// What [`RoomKit::steer`] takes beside the directive and the channel:
/// Python's `steer(directive, channel_id, *, room_id=None, turn_id=None,
/// organization_id=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SteerOptions {
    /// Python `room_id=None`: the room whose turns to reach. A `Cancel`
    /// reaches every turn of the room, any other directive its most
    /// recently started one still running.
    pub room_id: Option<RoomId>,
    /// Python `turn_id=None`: the turn to reach, as the turn backend names
    /// it (RFC §10.6.8).
    pub turn_id: Option<String>,
    /// Python `organization_id=None`: the organization the caller acts for
    /// (RFC §17.2); only the turns of its rooms are reached.
    pub organization_id: Option<String>,
}

impl<S: ConversationStore> RoomKit<S> {
    /// Steers the running turns of the intelligence channel `channel_id`
    /// (RFC §21.3, §10.6.8), wherever they run: Python's `steer(directive,
    /// channel_id, *, room_id=None, turn_id=None, organization_id=None)`.
    ///
    /// The directive reaches the turn `turn_id`; else the turns of
    /// `room_id`, every one for a `Cancel` and the most recently started
    /// for any other directive; else the channel's most recently started
    /// turn, whatever its room. Each turn's loop reads it at its next stop
    /// (`roomkit_ai::AiChannel::steer`). A channel without steering takes a
    /// `Cancel` as its turn's cancel, counted as one. Only the turns this
    /// kit started are reached, and with `organization_id` only those of
    /// that organization's rooms, a room of another organization's no more
    /// than one with no turn running.
    ///
    /// Returns how many loops it reached.
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::Value`], Python's `ValueError`, when both
    /// `room_id` and `turn_id` are given, and
    /// [`RoomKitError::RoomNotFound`] when `organization_id` and `room_id`
    /// are given and the room is missing or another organization's;
    /// nothing is steered.
    pub async fn steer(
        &self,
        directive: impl Into<SteeringDirective>,
        channel_id: &ChannelId,
        options: SteerOptions,
    ) -> Result<usize, RoomKitError> {
        let SteerOptions {
            room_id,
            turn_id,
            organization_id,
        } = options;
        if room_id.is_some() && turn_id.is_some() {
            return Err(RoomKitError::Value(
                "steer() addresses a room_id or a turn_id, not both".to_owned(),
            ));
        }
        if let (Some(organization), Some(room)) = (&organization_id, &room_id) {
            let options = GetRoomOptions {
                organization_id: Some(organization.clone()),
            };
            self.get_room(room, options).await?;
        }
        let address = SteerAddress {
            channel: channel_id,
            room: room_id.as_ref(),
            turn: turn_id.as_deref(),
            organization: organization_id.as_deref(),
        };
        let directive = directive.into();
        Ok(self.inner.turns.steer(&directive, address).await)
    }
}

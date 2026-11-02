//! The turns the owner started, which steering reaches (RFC §10.6.8,
//! §21.3): Python's `TurnClient._started` and `TurnClient.steer`.

use roomkit_core::model::{ChannelId, RoomId};

use super::owner::Steerable;
use crate::steering::SteeringDirective;

/// A turn the owner started: its address, and the way to steer it.
#[derive(Clone, Debug)]
struct Started {
    turn_id: String,
    room_id: RoomId,
    channel_id: ChannelId,
    organization_id: Option<String>,
    steerable: Steerable,
}

/// The turns the owner started and that are not over, in the order it
/// started them.
#[derive(Debug, Default)]
pub(crate) struct StartedTurns {
    turns: Vec<Started>,
}

/// Which turns of the channel `channel` a directive is for: the turn
/// `turn`, else the turns of `room`, else the channel's most recent; only
/// those of rooms of `organization` when it is given (RFC §17.2).
#[derive(Clone, Copy, Debug)]
pub(crate) struct SteerAddress<'a> {
    pub(crate) channel: &'a ChannelId,
    pub(crate) room: Option<&'a RoomId>,
    pub(crate) turn: Option<&'a str>,
    pub(crate) organization: Option<&'a str>,
}

impl StartedTurns {
    /// Records the turn `turn_id` of `channel_id` in `room_id`, started
    /// now; the turns that are over are forgotten.
    pub(crate) fn start(
        &mut self,
        (turn_id, room_id, channel_id): (&str, &RoomId, &ChannelId),
        organization_id: Option<&str>,
        steerable: Steerable,
    ) {
        self.turns.retain(|turn| !turn.steerable.over());
        self.turns.push(Started {
            turn_id: turn_id.to_owned(),
            room_id: room_id.clone(),
            channel_id: channel_id.clone(),
            organization_id: organization_id.map(str::to_owned),
            steerable,
        });
    }

    /// The turns `directive` reaches at `address` (RFC §10.6.8): the turn
    /// named; else the room's turns still running, all of them for a
    /// `Cancel` and the most recent for any other directive; else the
    /// channel's most recent turn, whatever its room.
    pub(crate) fn targets(
        &self,
        directive: &SteeringDirective,
        address: SteerAddress<'_>,
    ) -> Vec<Steerable> {
        let mut running = self.turns.iter().filter(|turn| {
            !turn.steerable.over()
                && &turn.channel_id == address.channel
                && address.organization.is_none_or(|organization| {
                    turn.organization_id.as_deref() == Some(organization)
                })
        });
        let addressed: Vec<&Started> = match (address.turn, address.room) {
            (Some(turn_id), _) => running.filter(|turn| turn.turn_id == turn_id).collect(),
            (None, Some(room_id)) => {
                let in_room: Vec<&Started> =
                    running.filter(|turn| &turn.room_id == room_id).collect();
                if directive.is_cancel() {
                    in_room
                } else {
                    in_room.into_iter().last().into_iter().collect()
                }
            }
            (None, None) => running.next_back().into_iter().collect(),
        };
        addressed
            .into_iter()
            .map(|turn| turn.steerable.clone())
            .collect()
    }
}

//! The turns the owner started, which steering reaches (RFC §10.6.8,
//! §21.3): Python's `TurnClient._started` and `TurnClient.steer`.

use roomkit_core::model::{ChannelId, RoomId};
use roomkit_core::rules::transcript::TurnAuthor;

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
    /// Who wrote the message the turn answers.
    author: TurnAuthor,
}

/// The turns the owner started and that are not over, in the order it
/// started them.
#[derive(Debug, Default)]
pub(crate) struct StartedTurns {
    turns: Vec<Started>,
}

/// Which turns of the channel `channel` a directive is for: those of
/// `room`, its turn `turn` alone when given; only those of rooms of
/// `organization` when it is given (RFC §17.2).
#[derive(Clone, Copy, Debug)]
pub(crate) struct SteerAddress<'a> {
    pub(crate) channel: &'a ChannelId,
    pub(crate) room: &'a RoomId,
    pub(crate) turn: Option<&'a str>,
    pub(crate) organization: Option<&'a str>,
}

impl StartedTurns {
    pub(super) fn abandon(&mut self) {
        for turn in self.turns.drain(..) {
            turn.steerable.abandon();
        }
    }

    /// Records the turn `turn_id` of `channel_id` in `room_id`, started
    /// now to answer a message of `author`; the turns that are over are
    /// forgotten.
    pub(crate) fn start(
        &mut self,
        (turn_id, room_id, channel_id): (&str, &RoomId, &ChannelId),
        organization_id: Option<&str>,
        steerable: Steerable,
        author: TurnAuthor,
    ) {
        self.turns.retain(|turn| !turn.steerable.over());
        self.turns.push(Started {
            turn_id: turn_id.to_owned(),
            room_id: room_id.clone(),
            channel_id: channel_id.clone(),
            organization_id: organization_id.map(str::to_owned),
            steerable,
            author,
        });
    }

    /// The turns of `channel_id` still running in `room_id`, oldest first:
    /// each one's id and who wrote the message it answers. What a directive
    /// steered there without a turn id may reach. Python's
    /// `TurnClient._running_in`.
    pub(crate) fn running_in(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Vec<(String, TurnAuthor)> {
        self.turns
            .iter()
            .filter(|turn| {
                !turn.steerable.over() && &turn.channel_id == channel_id && &turn.room_id == room_id
            })
            .map(|turn| (turn.turn_id.clone(), turn.author.clone()))
            .collect()
    }

    /// The turns `directive` reaches at `address` (RFC §10.6.8): the
    /// room's turn named, else the room's turns still running, all of them
    /// for a `Cancel` and the most recent for any other directive.
    pub(crate) fn targets(
        &self,
        directive: &SteeringDirective,
        address: SteerAddress<'_>,
    ) -> Vec<Steerable> {
        let mut in_room = self.turns.iter().filter(|turn| {
            !turn.steerable.over()
                && &turn.channel_id == address.channel
                && &turn.room_id == address.room
                && address.organization.is_none_or(|organization| {
                    turn.organization_id.as_deref() == Some(organization)
                })
        });
        let addressed: Vec<&Started> = match address.turn {
            Some(turn_id) => in_room.filter(|turn| turn.turn_id == turn_id).collect(),
            None if directive.is_cancel() => in_room.collect(),
            None => in_room.next_back().into_iter().collect(),
        };
        addressed
            .into_iter()
            .map(|turn| turn.steerable.clone())
            .collect()
    }
}

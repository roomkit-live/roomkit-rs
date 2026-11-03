//! The turn runners a room's owner keeps (RFC §10.6.1, §19.7): Python's
//! `core/_turn_runners.py`.
//!
//! A strategy that takes an agent's turns in a room (a loop's produce and
//! review) is kept here, by the room's owner, per channel and room, never
//! on the shared channel: the runner runs on the owner, and each
//! generation it asks of the agent is a turn of its own.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use roomkit_core::model::{
    ChannelBinding, ChannelId, ChannelOutput, RoomContext, RoomEvent, RoomId,
};

use crate::channel::{BoxFuture, ChannelError};
use crate::error::RoomKitError;
use crate::kit::RoomKit;

/// What a runner does with a turn of its channel: Python's runner either
/// asks the channel's own turn (`kit._strategy_turn`) or answers itself.
pub(crate) enum Taken {
    /// The channel's own turn, its event handed back.
    Own(Box<(RoomEvent, ChannelBinding, RoomContext)>),
    /// The runner's answer, once it is over, or what it failed with.
    Answered(BoxFuture<'static, Result<ChannelOutput, ChannelError>>),
}

/// What takes a channel's turns in a room in place of the channel's own
/// answer: Python's `TurnRunner`.
pub(crate) trait TurnRunner<S>: Send + Sync {
    /// Takes the turn of `binding`'s channel on `event`, or hands it back
    /// to the channel's own turn.
    fn take(
        &self,
        kit: RoomKit<S>,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> Taken;
}

/// A runner, and the install that took the turns: the address of the
/// strategy object, Python's `owner`.
struct Held<S> {
    runner: Arc<dyn TurnRunner<S>>,
    owner: usize,
}

/// The runners of the strategies installed in this owner's rooms.
pub(crate) struct TurnRunners<S> {
    held: Mutex<HashMap<(RoomId, ChannelId), Held<S>>>,
}

impl<S> Default for TurnRunners<S> {
    fn default() -> Self {
        Self {
            held: Mutex::default(),
        }
    }
}

impl<S> std::fmt::Debug for TurnRunners<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TurnRunners")
            .field("held", &self.held().len())
            .finish()
    }
}

impl<S> TurnRunners<S> {
    fn held(&self) -> MutexGuard<'_, HashMap<(RoomId, ChannelId), Held<S>>> {
        self.held.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Takes `channel_id`'s turns in `room_id` with `runner`, for the
    /// install of the strategy at `owner`: one runner per channel and
    /// room, which only the install that set it replaces.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::Value`] when another strategy's install took them.
    pub(crate) fn take(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        runner: Arc<dyn TurnRunner<S>>,
        owner: usize,
    ) -> Result<(), RoomKitError> {
        let mut held = self.held();
        let key = (room_id.clone(), channel_id.clone());
        if held.get(&key).is_some_and(|held| held.owner != owner) {
            return Err(RoomKitError::Value(format!(
                "Room '{room_id}' already has the turns of '{channel_id}' taken by another \
                 strategy"
            )));
        }
        held.insert(key, Held { runner, owner });
        Ok(())
    }

    /// The runner taking `channel_id`'s turns in `room_id`, if one does.
    pub(crate) fn get(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Option<Arc<dyn TurnRunner<S>>> {
        let key = (room_id.clone(), channel_id.clone());
        self.held().get(&key).map(|held| Arc::clone(&held.runner))
    }

    /// Stops taking `channel_id`'s turns in `room_id` (an uninstall, RFC
    /// §19.7).
    pub(crate) fn release(&self, room_id: &RoomId, channel_id: &ChannelId) {
        self.held().remove(&(room_id.clone(), channel_id.clone()));
    }

    /// The channels whose turns a runner takes in `room_id`.
    pub(crate) fn channels(&self, room_id: &RoomId) -> BTreeSet<ChannelId> {
        self.held()
            .keys()
            .filter(|(held_room, _)| held_room == room_id)
            .map(|(_, channel_id)| channel_id.clone())
            .collect()
    }
}

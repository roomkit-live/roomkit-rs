//! A host's setters of a room's settings (RFC §5.1, §19.3.1): its timers
//! and what an agent's own output asks in it, each written under the room's
//! lock, as Python's `set_room_timers` and `set_agent_response_policy`.

use roomkit_core::model::{AgentResponsePolicy, Room, RoomId, RoomTimers};
use roomkit_core::ops::{SetAgentResponsePolicy, SetRoomTimers};

use super::RoomKit;
use super::options::{SetAgentResponsePolicyOptions, SetRoomTimersOptions};
use crate::driver;
use crate::error::RoomKitError;
use crate::store::ConversationStore;

impl<S: ConversationStore> RoomKit<S> {
    /// Sets or replaces a room's lifecycle timers (RFC §5.1), under the
    /// room's lock, and returns the room as stored. When
    /// `timers.last_activity_at` is unset, the room's last activity is kept,
    /// so new thresholds never reset its idle clock; a room with no activity
    /// keeps none, its inactivity counting from its creation. The thresholds
    /// apply from the room's next locked section on, or its next
    /// [`check_room_timers`](Self::check_room_timers) or
    /// [`check_all_timers`](Self::check_all_timers). Python's
    /// `set_room_timers(room_id, timers, *, organization_id=None)`.
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::RoomNotFound`] for a room the store lacks or
    /// the organization `options` names does not reach, nothing written;
    /// [`RoomKitError::KitClosed`] once the kit's close started; the store's
    /// error when it fails.
    pub async fn set_room_timers(
        &self,
        room_id: &RoomId,
        timers: RoomTimers,
        options: SetRoomTimersOptions,
    ) -> Result<Room, RoomKitError> {
        let set = SetRoomTimers::new(
            self.config(),
            room_id.clone(),
            timers,
            options.organization_id,
        );
        Ok(driver::run(&self.inner, "set_room_timers", set).await??)
    }

    /// Sets what an agent's own output asks in a room (RFC §19.3.1), under
    /// the room's lock, and returns the room as stored. A room rarely knows
    /// at its creation how many agents it will hold: one that gains a
    /// second agent switches to [`AgentResponsePolicy::AddressedOnly`] so
    /// that its agents do not answer each other down to the chain-depth
    /// limit. It applies to the events processed after it, an event already
    /// broadcast is not reconsidered; a room that holds `policy` is left as
    /// it is. Python's `set_agent_response_policy(room_id, policy, *,
    /// organization_id=None)`.
    ///
    /// # Errors
    ///
    /// As [`set_room_timers`](Self::set_room_timers).
    pub async fn set_agent_response_policy(
        &self,
        room_id: &RoomId,
        policy: AgentResponsePolicy,
        options: SetAgentResponsePolicyOptions,
    ) -> Result<Room, RoomKitError> {
        let set = SetAgentResponsePolicy::new(
            self.config(),
            room_id.clone(),
            policy,
            options.organization_id,
        );
        Ok(driver::run(&self.inner, "set_agent_response_policy", set).await??)
    }
}

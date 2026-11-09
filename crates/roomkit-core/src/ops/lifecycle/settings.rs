//! A host's change of a room's settings (RFC §5.1, §19.3.1), as the Python
//! implementation's `set_room_timers` and `set_agent_response_policy` make
//! it: under the room's lock, the room read with the caller's organization
//! and written back with the setting, its due timers left as they are.

use std::collections::VecDeque;
use std::sync::Arc;

use super::LifecycleError;
use super::change::{Decision, SectionChange};
use super::locked::LockedChange;
use super::report::Reported;
use crate::model::{AgentResponsePolicy, Room, RoomContext, RoomId, RoomTimers, RoomUpdates};
use crate::ops::protocol::{EffectId, Operation, Outcome, ProtocolError, Step, StoreRequest};
use crate::ops::{Cx, Environment, KitConfig};

/// A room's timers replaced, its last activity kept when they name none.
#[derive(Clone, Debug)]
pub(crate) struct RoomTimersChange(RoomTimers);

impl SectionChange for RoomTimersChange {
    type Done = Box<RoomContext>;
    type Refusal = LifecycleError;

    /// Writes the timers and the room's last modification: the last
    /// activity the timers name, else the room's, so that new thresholds
    /// never reset the idle clock, and a room with no activity keeps none
    /// (its inactivity counting from its creation).
    fn decide(
        &self,
        mut context: Box<RoomContext>,
        cx: &Cx<'_>,
    ) -> Result<Decision<Box<RoomContext>>, LifecycleError> {
        let last_activity_at = self
            .0
            .last_activity_at
            .or(context.room.timers.last_activity_at);
        let updates = RoomUpdates {
            updated_at: Some(cx.env.now()),
            inactive_after_seconds: Some(self.0.inactive_after_seconds),
            closed_after_seconds: Some(self.0.closed_after_seconds),
            last_activity_at: Some(last_activity_at),
            ..RoomUpdates::default()
        };
        let write = write(&mut context, updates);
        Ok(Decision {
            writes: VecDeque::from([write]),
            done: context,
            telling: None,
        })
    }

    /// A change of timers reports nothing: no trigger and no framework event
    /// names it.
    fn reported(_done: &Box<RoomContext>) -> Option<(Reported<'_>, &RoomContext)> {
        None
    }

    /// The Python implementation applies no due timer before it writes.
    fn applies_timers(&self) -> bool {
        false
    }
}

/// A room's agent response policy set; nothing written when the room holds
/// it already.
#[derive(Clone, Debug)]
pub(crate) struct PolicyChange(AgentResponsePolicy);

impl SectionChange for PolicyChange {
    type Done = Box<RoomContext>;
    type Refusal = LifecycleError;

    /// Writes the policy and the room's last modification, logged; leaves a
    /// room that holds the policy as it is.
    fn decide(
        &self,
        mut context: Box<RoomContext>,
        cx: &Cx<'_>,
    ) -> Result<Decision<Box<RoomContext>>, LifecycleError> {
        if context.room.agent_response_policy == self.0 {
            return Ok(Decision {
                writes: VecDeque::new(),
                done: context,
                telling: None,
            });
        }
        let updates = RoomUpdates {
            updated_at: Some(cx.env.now()),
            agent_response_policy: Some(self.0),
            ..RoomUpdates::default()
        };
        let write = write(&mut context, updates);
        let policy = match self.0 {
            AgentResponsePolicy::AgentChain => "agent_chain",
            AgentResponsePolicy::AddressedOnly => "addressed_only",
        };
        tracing::info!(
            target: "roomkit::framework",
            room_id = %context.room.id,
            "Room {} agent response policy set to {policy}",
            context.room.id,
        );
        Ok(Decision {
            writes: VecDeque::from([write]),
            done: context,
            telling: None,
        })
    }

    /// A change of policy reports nothing: no trigger and no framework event
    /// names it.
    fn reported(_done: &Box<RoomContext>) -> Option<(Reported<'_>, &RoomContext)> {
        None
    }

    /// The Python implementation applies no due timer before it writes.
    fn applies_timers(&self) -> bool {
        false
    }
}

/// `updates` applied on `context`'s room, which the context then reflects,
/// and the write that stores them.
fn write(context: &mut RoomContext, updates: RoomUpdates) -> StoreRequest {
    updates.apply(&mut context.room);
    StoreRequest::UpdateRoom {
        room_id: context.room.id.clone(),
        updates,
    }
}

/// Replaces a room's timers under the room's lock (RFC §5.1), Python's
/// `set_room_timers(room_id, timers, *, organization_id=None)`: the last
/// activity `timers` names, else the room's, so that new thresholds never
/// reset the idle clock. The thresholds apply from the room's next locked
/// section on, or its next timer check. Its output is the room as stored.
#[derive(Debug)]
pub struct SetRoomTimers {
    change: LockedChange<RoomTimersChange>,
}

impl SetRoomTimers {
    /// The change of `room_id`'s timers to `timers`, for a caller acting for
    /// `organization_id`: a room of another organization, or of none, is
    /// not found, nothing written (RFC §17.2); `None` reads the room
    /// unscoped.
    pub fn new(
        config: Arc<KitConfig>,
        room_id: RoomId,
        timers: RoomTimers,
        organization_id: Option<String>,
    ) -> Self {
        Self {
            change: LockedChange::new(config, room_id, organization_id, RoomTimersChange(timers)),
        }
    }
}

impl Operation for SetRoomTimers {
    type Output = Result<Room, LifecycleError>;

    fn poll(&mut self, environment: &dyn Environment) -> Step<Self::Output> {
        self.change.poll(environment, |context, _| context.room)
    }

    fn complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError> {
        self.change.complete(id, outcome)
    }

    fn expire(&mut self) -> Vec<EffectId> {
        self.change.expire()
    }
}

/// Sets what an agent's own output asks in a room (RFC §19.3.1), under the
/// room's lock, Python's `set_agent_response_policy(room_id, policy, *,
/// organization_id=None)`: it applies to the events processed after it, and
/// a room that holds `policy` already is left as it is. Its output is the
/// room as stored.
#[derive(Debug)]
pub struct SetAgentResponsePolicy {
    change: LockedChange<PolicyChange>,
}

impl SetAgentResponsePolicy {
    /// The change of `room_id`'s policy to `policy`, for a caller acting for
    /// `organization_id`, scoped as [`SetRoomTimers::new`] is.
    pub fn new(
        config: Arc<KitConfig>,
        room_id: RoomId,
        policy: AgentResponsePolicy,
        organization_id: Option<String>,
    ) -> Self {
        Self {
            change: LockedChange::new(config, room_id, organization_id, PolicyChange(policy)),
        }
    }
}

impl Operation for SetAgentResponsePolicy {
    type Output = Result<Room, LifecycleError>;

    fn poll(&mut self, environment: &dyn Environment) -> Step<Self::Output> {
        self.change.poll(environment, |context, _| context.room)
    }

    fn complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError> {
        self.change.complete(id, outcome)
    }

    fn expire(&mut self) -> Vec<EffectId> {
        self.change.expire()
    }
}

//! Admit a turn, record its request, and connect its owner to the chosen executor.
use std::sync::{Arc, Weak};

use roomkit_core::model::{ChannelBinding, ChannelId, ErrorRecord, RoomContext, RoomEvent, RoomId};
use uuid::Uuid;

use super::error::TurnError;
use super::executor::{self, Running};
use super::memory::Request;
use super::owner;
use super::pipe::pipe;
use super::wire::PreviousTurn;
use super::{
    Canceller, Carried, CarriedRequest, MemoryTurn, Observers, OwnerTurn, Turns, closed, lock,
    request_room_hooks, uncarried,
};
use crate::telemetry::current_trace;
use crate::turn::TurnKit;

pub(super) struct Input {
    pub(super) event: RoomEvent,
    pub(super) context: RoomContext,
    pub(super) strategy_turn: bool,
    pub(super) channel_rows: Vec<RoomEvent>,
}

/// A placement can fail or be cancelled before the executor receives it.
pub(super) struct Handover {
    owner: Weak<Turns>,
    room: RoomId,
    channel: ChannelId,
    turn: Arc<str>,
    previous: Option<PreviousTurn>,
    pub(super) handed: bool,
}

impl Drop for Handover {
    fn drop(&mut self) {
        if !self.handed
            && let Some(owner) = self.owner.upgrade()
        {
            owner.records().undo_submission(
                &self.channel,
                &self.room,
                &self.turn,
                self.previous.take(),
            );
        }
    }
}

struct Prepared {
    carried: CarriedRequest,
    stale: bool,
    handover: Handover,
}

enum Destination {
    Local(Running),
    #[cfg(feature = "redis")]
    Remote(super::RedisTurnBackend),
}

impl Turns {
    fn destination(
        &self,
        turn_id: &Arc<str>,
        kit: &dyn TurnKit,
        binding: &ChannelBinding,
    ) -> Result<Destination, ErrorRecord> {
        if kit
            .channel(&binding.channel_id)
            .is_some_and(|c| c.is_declaration_only())
        {
            #[cfg(feature = "redis")]
            if let Some(backend) = self.remote.clone() {
                return Ok(Destination::Remote(backend));
            }
            return Err(TurnError::new(
                "no_executor",
                format!("No executor serves channel '{}'", binding.channel_id),
            )
            .to_record());
        }
        self.executor_lifecycle
            .admit(Arc::clone(turn_id))
            .map(Destination::Local)
            .ok_or_else(|| {
                TurnError::new("no_executor", "the kit's local executor has closed").to_record()
            })
    }

    fn prepare_request(
        self: &Arc<Self>,
        kit: &dyn TurnKit,
        binding: &ChannelBinding,
        input: Input,
        turn_id: &Arc<str>,
        local: bool,
        observers: &Observers,
    ) -> Result<Prepared, ErrorRecord> {
        let Input {
            event,
            mut context,
            strategy_turn,
            channel_rows,
        } = input;
        let room_hooks = request_room_hooks(kit, &event.room_id, &binding.channel_id, local)?;
        let owner_tools = kit.owner_tools(&binding.channel_id, &event.room_id);
        let trace = current_trace();
        let history = std::mem::take(&mut context.recent_events);
        let mut records = self.records();
        if records
            .room_mark(&event.room_id)
            .is_some_and(|mark| mark != context.room.created_at)
        {
            records.pop_room(&event.room_id);
        }
        let previous = records.get(&binding.channel_id, &event.room_id).cloned();
        let request = Request {
            strategy_turn,
            room_hooks: &room_hooks,
            turn_id,
            binding,
            event: &event,
            context: &context,
            history: &history,
            previous: previous.as_ref(),
            owner_tools: &owner_tools,
            trace: trace.as_ref(),
        };
        let rest = request.write(observers).map_err(|why| uncarried(&why))?;
        records.put(
            &binding.channel_id,
            &event.room_id,
            PreviousTurn::after(turn_id, previous.as_ref(), Some(&event)),
        );
        records.mark_room(&event.room_id, context.room.created_at);
        let stale = local
            && kit.channel(&binding.channel_id).is_some_and(|channel| {
                self.rooms.enter(
                    &channel,
                    (binding.channel_id.clone(), event.room_id.clone()),
                    turn_id,
                    previous.as_ref(),
                )
            });
        let handover = Handover {
            owner: Arc::downgrade(self),
            room: event.room_id.clone(),
            channel: binding.channel_id.clone(),
            turn: Arc::clone(turn_id),
            previous,
            handed: false,
        };
        Ok(Prepared {
            carried: CarriedRequest {
                rest,
                event,
                history,
                channel_rows,
            },
            stale,
            handover,
        })
    }

    pub(super) fn start_turn<G>(
        self: &Arc<Self>,
        kit: &Arc<dyn TurnKit>,
        binding: &ChannelBinding,
        input: Input,
        submitted: impl FnOnce(Canceller) -> G,
    ) -> Result<(OwnerTurn, G), ErrorRecord> {
        if self.is_closed() || *self.cut.borrow() {
            return Err(closed());
        }
        let turn_id: Arc<str> = Arc::from(format!("turn-{}", Uuid::new_v4().simple()));
        let destination = self.destination(&turn_id, &**kit, binding)?;
        let observers = self.observers();
        let facts = owner::Facts::of(binding, &input.event, &input.context.room);
        let Prepared {
            carried,
            stale,
            mut handover,
        } = self.prepare_request(
            &**kit,
            binding,
            input,
            &turn_id,
            matches!(destination, Destination::Local(_)),
            &observers,
        )?;
        let (frames, frames_read) = pipe(Arc::clone(&turn_id), Carried::Frame, observers.clone());
        let (controls, controls_read) = pipe(Arc::clone(&turn_id), Carried::Control, observers);
        let memory = Arc::new(MemoryTurn::new(turn_id, frames, controls, self.timeouts()));
        let (room_id, organization_id) = (facts.room_id.clone(), facts.organization_id.clone());
        let (turn, pump) = owner::open(
            Arc::clone(&memory),
            frames_read,
            facts,
            Arc::clone(kit),
            Arc::clone(self),
        );
        // Register a pending cut before an executor can take its first step.
        let following = submitted(turn.canceller());
        let taken = kit.spawn(pump)
            && match destination {
                Destination::Local(running) => {
                    handover.handed = executor::take(
                        self,
                        running,
                        Arc::clone(&memory),
                        controls_read,
                        carried,
                        stale,
                        kit,
                    );
                    handover.handed
                }
                #[cfg(feature = "redis")]
                Destination::Remote(backend) => {
                    let link = Arc::clone(&memory);
                    kit.spawn(Box::pin(async move {
                        backend
                            .submit_remote(carried, link, controls_read, handover)
                            .await;
                    }))
                }
            };
        if !taken {
            memory.shut();
            return Err(closed());
        }
        lock(&self.started).start(
            (&memory.turn_id, &room_id, &binding.channel_id),
            organization_id.as_deref(),
            turn.steerable(),
        );
        Ok((turn, following))
    }

    #[cfg(test)]
    pub(super) fn submit<G>(
        self: &Arc<Self>,
        kit: &Arc<dyn TurnKit>,
        binding: &ChannelBinding,
        event: RoomEvent,
        context: RoomContext,
        strategy_turn: bool,
        submitted: impl FnOnce(Canceller) -> G,
    ) -> Result<(OwnerTurn, G), ErrorRecord> {
        self.start_turn(
            kit,
            binding,
            Input {
                event,
                context,
                strategy_turn,
                channel_rows: Vec::new(),
            },
            submitted,
        )
    }
}

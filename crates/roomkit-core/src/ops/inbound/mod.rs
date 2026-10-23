//! The inbound pipeline (RFC §10.1), with the broadcast of every event it
//! commits (RFC §10.2), for a message and for an event injected directly
//! (RFC §10.5).
//!
//! [`Inbound`] runs one call into a room: a message into the room the
//! caller names or the room the kit's router finds for it (RFC §10.4), or
//! an event the integrator injects into a room ([`Inbound::inject`]), and
//! the whole cascade it starts. A message arrives first (steps 1 to 4): its
//! sender in the channel's form, its room, named or routed, created,
//! attached to or recorded on when it must be, the room's context and the
//! event the channel parses (`arrival`). A direct injection skips the
//! channel's parse and step 2 (`direct`). Then the cascade: the call's
//! pass, its delivery set, the responses that re-enter as passes of their
//! own, the events hooks inject, and the records the room stores in place of
//! responses it refuses (`cascade`).
//!
//! The room's delivery lane is entered at each commit (RFC §10.1 step 12,
//! §10.2): an event delivered takes its place under the room lock, and its
//! delivery set waits for that place's turn once the lock is released, so
//! one event's set completes before the next one's begins, whichever call
//! committed it. Nothing of the lane comes before the commit: two calls
//! into a room overlap up to the room lock, and commit in lock order. The
//! operation runs its passes and its sets as two queues, one of each at a
//! time (`queues`).
//!
//! A call into the room that the operation's own work makes under the
//! room's lock or in its admission order, a hook or a parse calling
//! `send_event` or `process_inbound` for the room, is adopted (`adoption`):
//! the operation runs it as an entry of its own, beside its work, or nested
//! in the pass whose hook made it, so that it commits ahead of that pass's
//! event; its caller receives its result at its commit, and its delivery
//! set takes its place in the lane at that commit (RFC §13.5, §10.1 step 18,
//! gap G-070). The queues thus carry several cascades: the call's, and one
//! per adopted entry. A turn's or a delivery's call is a new arrival, an
//! operation of its own (RFC §9.5.1 Reentrance), drawing on the reentry
//! budget of the cascade that delivers (gap G-070), whose caller may take
//! the call's decision, published once it is known (`Step::decided`),
//! rather than wait for its delivery.
//!
//! Every pass checks an edit or a delete against its target (RFC §10.1 step
//! 8a, §10.3), updates the target in its commit, and fires `ON_EVENT_UPDATED`
//! or `ON_EVENT_DELETED` with the target as the commit left it, once the lock
//! is released.
//!
//! An instruction (RFC §10.1.1) takes the same way without being stored:
//! refused before anything is read when it names no agent or carries a key,
//! decided by its pass without a commit, delivered to the agents it names,
//! and fingerprinted on every response it produces (`instructions`).
//!
//! The call's off-lock hooks run before the room lock, ordered by the
//! room's admission ticket, and step 9 applies their outcome (RFC §9.5.1,
//! `off_lock`). The kit's `process_timeout` bounds the call from its start
//! to its decision: past it, nothing of the event is written, what the call
//! holds is released, and the call is refused `process_timeout` (RFC §13.6,
//! `deadline`).

mod adoption;
mod arrival;
mod cascade;
mod chain;
mod commit;
mod deadline;
mod delivery;
mod direct;
mod edits;
mod entry;
mod holds;
mod instructions;
mod job;
mod named_room;
mod off_lock;
mod pass;
mod persist;
mod queues;
mod record;
mod report;
mod result;
mod stamp;

use std::collections::VecDeque;
use std::sync::Arc;

use self::adoption::Adoptions;
use self::arrival::Arrival;
pub use self::arrival::InboundTarget;
use self::direct::DirectRead;
use self::holds::AfterTicket;
use self::job::Job;
use self::off_lock::OffLockCheck;
use self::queues::Delivery;
pub use self::result::{InboundError, InboundResult};
pub use self::stamp::default_inbound_event;
use super::io::Io;
use super::protocol::{Decided, EffectId, Entry, EntryId, Operation, Outcome, ProtocolError, Step};
use super::{Cx, Environment, KitConfig};
use crate::model::{
    ChannelId, DirectEvent, InboundMessage, Metadata, Observation, RoomContext, RoomId, Task,
    Timestamp,
};
use crate::rules::chain_depth::ReentryBudget;

/// The inbound pipeline for one call (RFC §10.1, §10.5).
///
/// ```
/// # use std::sync::Arc;
/// # use roomkit_core::model::{ChannelId, EventContent, InboundMessage, RoomId};
/// # use roomkit_core::ops::{Inbound, InboundTarget, KitConfig, Next, Operation, SequentialEnvironment};
/// let config = Arc::new(KitConfig::new());
/// let message = InboundMessage::new(ChannelId::new("sms")?, "+15551234567", EventContent::text("Hi"));
/// let room = InboundTarget::Room(RoomId::new("room_1")?);
/// let mut inbound = Inbound::new(config, room, None, message);
/// let step = inbound.poll(&mut SequentialEnvironment::default());
/// // No channel `sms` is registered: the operation ends at once.
/// assert!(matches!(step.next, Next::Ready(Err(_))));
/// # Ok::<(), roomkit_core::model::ModelError>(())
/// ```
#[derive(Debug)]
pub struct Inbound {
    config: Arc<KitConfig>,
    io: Io,
    machine: Machine,
}

impl Inbound {
    /// The pipeline for `message`, into the room `target` names or the room
    /// it is routed to, for a caller acting for `organization_id` (RFC
    /// §17.2): a room of another organization fails the message, a room the
    /// call creates is that organization's, and routing reads that
    /// organization's rooms and identities only. A call that names no
    /// organization reads rooms unscoped (gap G-051).
    pub fn new(
        config: Arc<KitConfig>,
        target: InboundTarget,
        organization_id: Option<String>,
        message: InboundMessage,
    ) -> Self {
        let call = Call::Message {
            target,
            organization_id,
            message: Box::new(message),
        };
        Self::calling(config, call)
    }

    /// The pipeline for `event`, injected directly into `room_id` (RFC
    /// §10.5) by a caller acting for `organization_id` (RFC §17.2): the same
    /// pipeline as a message's from the room lock on, its off-lock check
    /// included, without `handle_inbound()`, routing, the room's creation or
    /// an attach (gap G-069). A call that names no organization reads the
    /// room unscoped.
    ///
    /// Its output is the committed event's result, a record stored
    /// `BLOCKED` included, once its delivery set is over. A refusal before
    /// the commit is an error with nothing written: a room of another
    /// organization, or of none for a caller that names one,
    /// [`InboundError::OtherOrganization`]; otherwise
    /// [`InboundError::Refused`]: a channel the room does not bind
    /// (`source_unbound`), a room whose status refuses events, the call's
    /// `process_timeout`, a refused edit or delete, a refused instruction.
    pub fn inject(
        config: Arc<KitConfig>,
        room_id: RoomId,
        organization_id: Option<String>,
        event: DirectEvent,
    ) -> Self {
        let call = Call::Direct {
            room_id,
            organization_id,
            event: Box::new(event),
        };
        Self::calling(config, call)
    }

    /// The same call, made by the code a delivery set runs, a turn or a
    /// transport's delivery, drawing on `budget`, the reentry budget of the
    /// cascade that delivers, rather than on one of its own (RFC §8.3, gap
    /// G-070). The call is charged as it starts, as an entry adopted from
    /// that cascade would be: a message as its arrival starts, before its
    /// parse, refused with no event past the budget; an injected event at
    /// its pass, stored `BLOCKED` with `reentry_loop_cap` past it, its hooks
    /// never run. Its own cascade draws on the same budget, so that a
    /// channel that calls into a room on every delivery stops at the cap.
    /// The call keeps its own admission ticket and `process_timeout`.
    #[must_use]
    pub fn drawing_on(mut self, budget: ReentryBudget) -> Self {
        self.machine.budget = budget;
        self.machine.drawn = true;
        self
    }

    fn calling(config: Arc<KitConfig>, call: Call) -> Self {
        let budget = ReentryBudget::new(config.max_chain_depth);
        let entry = match call {
            Call::Message { .. } => EntryKind::Message,
            Call::Direct { .. } => EntryKind::Direct,
        };
        Self {
            config,
            io: Io::new(),
            machine: Machine {
                room_id: None,
                entry,
                channel_id: None,
                direct: None,
                deadline: None,
                ticket: None,
                stage: Stage::Start(Box::new(call)),
                passes: VecDeque::new(),
                pass: None,
                sets: VecDeque::new(),
                delivery: None,
                adopted: Adoptions::default(),
                stopping: false,
                budget,
                drawn: false,
                decided: false,
                result: InboundResult::default(),
                tasks: Vec::new(),
                observations: Vec::new(),
                metadata_updates: Metadata::new(),
                task_context: None,
                outcome: None,
            },
        }
    }
}

impl Operation for Inbound {
    type Output = Result<InboundResult, InboundError>;

    fn poll(&mut self, environment: &mut dyn Environment) -> Step<Self::Output> {
        let Self {
            config,
            io,
            machine,
        } = self;
        let mut cx = Cx {
            config,
            env: environment,
            io,
        };
        machine.advance(&mut cx);
        let outcome = machine.take_outcome();
        self.io.step(outcome)
    }

    fn complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError> {
        self.io.complete(id, outcome)
    }

    fn adopt(&mut self, entry: Entry, origin: EffectId) -> Result<EntryId, Box<Entry>> {
        self.machine.adopt(&self.io, entry, origin)
    }
}

/// What the call brings into the room.
#[derive(Debug)]
enum Call {
    Message {
        target: InboundTarget,
        organization_id: Option<String>,
        message: Box<InboundMessage>,
    },
    Direct {
        room_id: RoomId,
        organization_id: Option<String>,
        event: Box<DirectEvent>,
    },
}

/// The kind of the call's own entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EntryKind {
    /// A message (RFC §10.1).
    Message,
    /// An event injected directly (RFC §10.5).
    Direct,
}

#[derive(Debug)]
struct Machine {
    /// The call's room, once it arrived there.
    room_id: Option<RoomId>,
    /// What the call brings: a direct injection's refusal before its commit
    /// is an error (RFC §10.5).
    entry: EntryKind,
    /// The call's channel, once resolved (RFC §10.1 step 1).
    channel_id: Option<ChannelId>,
    /// What a direct injection's trigger is checked for before its hooks
    /// (gap G-069); `None` for a message.
    direct: Option<direct::DirectCall>,
    /// The call's deadline, set at its start from `process_timeout` (RFC
    /// §13.6).
    deadline: Option<Timestamp>,
    /// The room whose admission ticket the call holds, from its off-lock
    /// check to its trigger's pass's end (RFC §9.5.1).
    ticket: Option<RoomId>,
    stage: Stage,
    /// The passes queued, and the one running.
    passes: VecDeque<Job>,
    pass: Option<Job>,
    /// The delivery sets queued, in the order of their places, and the one
    /// running.
    sets: VecDeque<Delivery>,
    delivery: Option<Delivery>,
    /// The entries adopted from the operation's own work (gap G-070).
    adopted: Adoptions,
    /// Whether the cascade stopped, failed or past its deadline: no pass
    /// runs any more, committed events are still delivered.
    stopping: bool,
    /// The reentry budget every cascade of the operation draws on, an
    /// adopted entry's on its caller's (RFC §8.3), and the calls its
    /// delivery sets' targets make into a room too (gap G-070).
    budget: ReentryBudget,
    /// Whether the call draws on the budget of the cascade whose delivery
    /// made it, which charges its trigger as it starts (gap G-070).
    drawn: bool,
    /// Whether the trigger is decided: committed or, for an instruction,
    /// which is never committed, delivered or blocked by its pass. A failure
    /// after it no longer voids the result (RFC §13.6, gap G-065).
    decided: bool,
    result: InboundResult,
    tasks: Vec<Task>,
    observations: Vec<Observation>,
    /// The room metadata updates the channels returned (RFC §6.1).
    metadata_updates: Metadata,
    /// The context of the last pass or delivery set that filed tasks, which
    /// their `ON_TASK_CREATED` observers read.
    task_context: Option<Box<RoomContext>>,
    outcome: Option<Result<InboundResult, InboundError>>,
}

#[derive(Debug)]
enum Stage {
    Start(Box<Call>),
    /// A message's steps 2 to 4.
    Arriving(Box<Arrival>),
    /// A direct injection's read of its room for its off-lock check.
    Reading(Box<DirectRead>),
    /// Step 5a: the trigger's off-lock check, holding the room's ticket.
    Checking(Box<OffLockCheck>),
    Cascade,
    /// The ticket's release, before the call goes on.
    ReleasingTicket {
        effect: EffectId,
        then: Box<AfterTicket>,
    },
    /// Step 15's writes, each outstanding until answered.
    Persisting {
        side_effects: Option<(EffectId, Vec<Task>)>,
        metadata: Option<EffectId>,
    },
    /// The call's outcome waits for every entry it adopted to be settled,
    /// and every event committed to be delivered.
    Ending(Box<Result<InboundResult, InboundError>>),
    Done,
}

impl Machine {
    /// Takes every answer in, starts the entries adopted since, and runs
    /// the queues, until nothing is left to take in: a section nested under
    /// a lock is answered at once, without the driver.
    fn advance(&mut self, cx: &mut Cx<'_>) {
        if matches!(self.stage, Stage::Start(_)) {
            self.begin(cx);
        }
        loop {
            while let Some((effect, outcome)) = cx.io.next_outcome() {
                self.dispatch(cx, effect, outcome);
            }
            self.start_adopted(cx);
            self.run_queues(cx);
            self.end_once_settled();
            if !cx.io.has_outcomes() {
                return;
            }
        }
    }

    /// The operation's outcome once it is over, as its caller receives it.
    fn take_outcome(&mut self) -> Option<Result<InboundResult, InboundError>> {
        let outcome = self.outcome.take()?;
        Some(self.for_caller(outcome))
    }

    /// `outcome` as the call's caller receives it: a direct injection's
    /// refusal before its commit raised (RFC §10.5).
    fn for_caller(
        &self,
        outcome: Result<InboundResult, InboundError>,
    ) -> Result<InboundResult, InboundError> {
        if self.entry == EntryKind::Direct {
            outcome.and_then(result::committed)
        } else {
            outcome
        }
    }

    /// Publishes the call's decision, once: `outcome`, the trigger's result
    /// at its commit or the call's end before it, with no delivery (RFC
    /// §10.1 step 18). It is published whatever is still to run: the
    /// delivery sets of the entries the call adopted, which may wait in the
    /// room's lane behind the work of the caller that reads it.
    fn publish(&self, cx: &mut Cx<'_>, outcome: &Result<InboundResult, InboundError>) {
        if cx.io.has_decided() {
            return;
        }
        cx.io.decide(Decided {
            room_id: self.room_id.clone(),
            result: self.for_caller(outcome.clone()),
        });
    }

    /// Hands an answer to whoever waits for it: an adopted entry, or the
    /// call's own stage.
    fn dispatch(&mut self, cx: &mut Cx<'_>, effect: EffectId, outcome: Outcome) {
        if self.adopted.defers(effect) {
            return self.adopted.defer(effect, outcome);
        }
        if self.adopted.awaits(effect) {
            return self.on_adopted_outcome(cx, effect, outcome);
        }
        if self.pass.as_ref().is_some_and(|job| job.awaits(effect)) {
            return self.on_pass_outcome(cx, effect, outcome);
        }
        if let Some(delivery) = &self.delivery
            && delivery.set.awaits(effect)
        {
            return self.on_set_outcome(cx, effect, outcome);
        }
        match std::mem::replace(&mut self.stage, Stage::Done) {
            Stage::Arriving(arrival) => self.on_arrival(cx, arrival, effect, outcome),
            Stage::Reading(read) => self.on_read(cx, read, effect, outcome),
            Stage::Checking(check) => self.on_check(cx, check, effect, outcome),
            Stage::ReleasingTicket {
                effect: expected,
                then,
            } if expected == effect => self.on_ticket_released(cx, *then, outcome),
            Stage::Persisting {
                side_effects,
                metadata,
            } => self.on_persisting(cx, side_effects, metadata, effect, outcome),
            stage @ (Stage::Start(_)
            | Stage::Cascade
            | Stage::ReleasingTicket { .. }
            | Stage::Ending(_)
            | Stage::Done) => {
                debug_assert!(false, "inbound received {effect:?} in {stage:?}");
                self.stage = stage;
            }
        }
    }
}

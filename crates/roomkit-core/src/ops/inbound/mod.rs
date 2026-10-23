//! The inbound pipeline (RFC §10.1), with the broadcast of every event it
//! commits (RFC §10.2).
//!
//! [`Inbound`] runs one message into a room the caller names, or the room
//! the kit's router finds for it (RFC §10.4), and the whole cascade it
//! starts. The message's arrival comes first (steps 1 to 4): its sender in
//! the channel's form, its room, named or routed, created, attached to or
//! recorded on when it must be, the room's context and the event the
//! channel parses.
//! Then the cascade: the trigger's pass, its delivery set, the responses
//! that re-enter as passes of their own, the events hooks inject, and the
//! records the room stores in place of responses it refuses.
//!
//! The operation holds the room's delivery lane from the moment the room is
//! known to its end, on every path: the lane is a FIFO of the cascade's
//! work, so one event's delivery set completes before the next one's
//! begins, and the next call into the room waits for it (RFC §10.2).
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
//! The trigger's off-lock hooks run before the room lock, ordered by the
//! room's admission ticket, and step 9 applies their outcome (RFC §9.5.1,
//! `off_lock`). The kit's `process_timeout` bounds the call from its start
//! to its decision: past it, nothing of the event is written, what the call
//! holds is released, and the call is refused `process_timeout` (RFC §13.6,
//! `deadline`).

mod arrival;
mod chain;
mod commit;
mod deadline;
mod delivery;
mod edits;
mod holds;
mod instructions;
mod job;
mod named_room;
mod off_lock;
mod pass;
mod persist;
mod record;
mod report;
mod result;
mod stamp;

use std::collections::VecDeque;
use std::sync::Arc;

use self::arrival::Arrival;
pub use self::arrival::InboundTarget;
use self::deadline::Stopped;
use self::delivery::{DeliverySet, Response, SetKind, SetOutcome};
use self::holds::AfterTicket;
use self::instructions::Admission;
use self::job::{Finished, Job, RecordNote, Run};
use self::off_lock::OffLockCheck;
use self::pass::{CommitPass, PassOrigin, PassReport, PassResult};
use self::record::{RecordPass, RecordReport, RecordResult};
pub use self::result::{InboundError, InboundResult};
pub use self::stamp::default_inbound_event;
use super::io::Io;
use super::protocol::{EffectId, Operation, Outcome, ProtocolError, Step};
use super::{Abort, Cx, Environment, KitConfig, Progress};
use crate::model::{
    ChannelCategory, ChannelId, ErrorRecord, EventStatus, InboundMessage, InjectedEvent, Metadata,
    Observation, RoomContext, RoomEvent, RoomId, Task, Timestamp,
};
use crate::rules::chain_depth::{self, ReentryBudget};
use crate::rules::hook_chain::HookChain;

/// The inbound pipeline for one message (RFC §10.1).
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
        let budget = ReentryBudget::new(config.max_chain_depth);
        Self {
            config,
            io: Io::new(),
            machine: Machine {
                room_id: None,
                target,
                organization_id,
                channel_id: None,
                deadline: None,
                holds_lane: false,
                ticket: None,
                stage: Stage::Start(Box::new(message)),
                lane: VecDeque::new(),
                active: None,
                budget,
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
        let outcome = machine.outcome.take();
        self.io.step(outcome)
    }

    fn complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError> {
        self.io.complete(id, outcome)
    }
}

#[derive(Debug)]
struct Machine {
    /// The message's room, once it arrived there.
    room_id: Option<RoomId>,
    target: InboundTarget,
    /// The organization the caller acts for (RFC §17.2).
    organization_id: Option<String>,
    /// The message's channel, once resolved (RFC §10.1 step 1).
    channel_id: Option<ChannelId>,
    /// The call's deadline, set at its start from `process_timeout` (RFC
    /// §13.6).
    deadline: Option<Timestamp>,
    /// Whether the operation holds the room's delivery lane, which it
    /// leaves before it ends.
    holds_lane: bool,
    /// The room whose admission ticket the call holds, from its off-lock
    /// check to its trigger's pass's end (RFC §9.5.1).
    ticket: Option<RoomId>,
    stage: Stage,
    lane: VecDeque<Job>,
    active: Option<Job>,
    budget: ReentryBudget,
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
    Start(Box<InboundMessage>),
    /// Steps 2 to 4, which enter the room's lane.
    Arriving(Box<Arrival>),
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
    /// The operation's outcome waits for the lane to be left.
    LeavingLane {
        effect: EffectId,
        outcome: Box<Result<InboundResult, InboundError>>,
    },
    Done,
}

impl Machine {
    fn advance(&mut self, cx: &mut Cx<'_>) {
        if matches!(self.stage, Stage::Start(_)) {
            self.begin(cx);
        }
        while let Some((effect, outcome)) = cx.io.next_outcome() {
            self.dispatch(cx, effect, outcome);
        }
        self.run_lane(cx);
    }

    /// Sets the call's deadline (RFC §13.6), resolves the channel and the
    /// sender's form (RFC §10.1 step 1), and refuses an instruction that
    /// cannot be one before anything is read (RFC §10.1.1), then starts the
    /// arrival, which enters the room's lane once the room is known.
    fn begin(&mut self, cx: &mut Cx<'_>) {
        let Stage::Start(message) = std::mem::replace(&mut self.stage, Stage::Done) else {
            return;
        };
        self.deadline = deadline::of_call(cx.config, cx.env.now());
        self.channel_id = Some(message.channel_id.clone());
        let (channel, message) = match arrival::resolve(cx.config, *message) {
            Ok(resolved) => resolved,
            Err(error) => return self.end(cx, Err(error)),
        };
        match instructions::admit(cx.config, &message) {
            Ok(Admission::Admitted) => {}
            Ok(Admission::Refused(refusal)) => {
                return self.end(cx, Ok(instructions::refused(refusal)));
            }
            Err(error) => return self.end(cx, Err(error)),
        }
        let mut arrival = Box::new(Arrival::new(
            self.target.clone(),
            self.organization_id.clone(),
            channel,
            message,
            self.deadline,
        ));
        arrival.start(cx);
        self.stage = Stage::Arriving(arrival);
    }

    fn dispatch(&mut self, cx: &mut Cx<'_>, effect: EffectId, outcome: Outcome) {
        match std::mem::replace(&mut self.stage, Stage::Done) {
            Stage::Arriving(arrival) => self.on_arrival(cx, arrival, effect, outcome),
            Stage::Checking(check) => self.on_check(cx, check, effect, outcome),
            Stage::ReleasingTicket {
                effect: expected,
                then,
            } if expected == effect => self.on_ticket_released(cx, *then, outcome),
            Stage::Persisting {
                side_effects,
                metadata,
            } => self.on_persisting(cx, side_effects, metadata, effect, outcome),
            Stage::Cascade => {
                self.stage = Stage::Cascade;
                self.dispatch_to_job(cx, effect, outcome);
            }
            Stage::LeavingLane {
                effect: expected,
                outcome: result,
            } if expected == effect => self.on_left(*result, outcome),
            stage @ (Stage::Start(_)
            | Stage::ReleasingTicket { .. }
            | Stage::LeavingLane { .. }
            | Stage::Done) => {
                debug_assert!(false, "inbound received {effect:?} in {stage:?}");
                self.stage = stage;
            }
        }
    }

    /// Takes an answer of the arrival; once it hands the event, the
    /// trigger's off-lock check or its pass starts, in the room whose lane
    /// the arrival entered.
    fn on_arrival(
        &mut self,
        cx: &mut Cx<'_>,
        mut arrival: Box<Arrival>,
        effect: EffectId,
        outcome: Outcome,
    ) {
        let Progress::Done(arrived) = arrival.accept(cx, effect, outcome) else {
            self.stage = Stage::Arriving(arrival);
            return;
        };
        self.holds_lane = arrival.lane().is_some();
        match arrived {
            Ok(arrived) => {
                self.room_id = Some(arrived.event.room_id.clone());
                self.after_arrival(cx, arrived);
            }
            Err(stopped) => {
                self.room_id = arrival.room().cloned();
                match stopped {
                    Stopped::Failed(error) => self.end(cx, Err(error)),
                    Stopped::Expired => self.expire(cx, None),
                }
            }
        }
    }

    /// Queues the trigger's pass, bounded by the call's deadline and going
    /// on with the chain its off-lock check left, if any.
    fn start_trigger(&mut self, event: RoomEvent, checked: Option<HookChain<RoomEvent>>) {
        let pass = CommitPass::trigger(event, self.deadline, checked);
        self.lane
            .push_back(Job::Pass(Box::new(pass), PassOrigin::Trigger));
        self.stage = Stage::Cascade;
    }

    fn dispatch_to_job(&mut self, cx: &mut Cx<'_>, effect: EffectId, outcome: Outcome) {
        let Some(job) = self.active.take() else {
            debug_assert!(false, "inbound received {effect:?} with no job running");
            return;
        };
        match job.accept(cx, effect, outcome) {
            Run::Running(job) => self.active = Some(job),
            Run::Finished(finished) => self.settle(cx, *finished),
        }
    }

    /// Starts the lane's next job while none runs; ends the cascade once the
    /// lane is empty.
    fn run_lane(&mut self, cx: &mut Cx<'_>) {
        while matches!(self.stage, Stage::Cascade) && self.active.is_none() {
            let Some(job) = self.lane.pop_front() else {
                self.persist_side_effects(cx);
                return;
            };
            match job.start(cx) {
                Run::Running(job) => self.active = Some(job),
                Run::Finished(finished) => self.settle(cx, *finished),
            }
        }
    }

    /// Settles a finished job; the trigger's pass once the call's ticket is
    /// released (RFC §9.5.1).
    fn settle(&mut self, cx: &mut Cx<'_>, finished: Finished) {
        if self.ticket.is_some() && matches!(finished, Finished::Pass(_, PassOrigin::Trigger)) {
            return self.release_ticket(cx, AfterTicket::Settle(Box::new(finished)));
        }
        match finished {
            Finished::Pass(Ok(report), origin) => self.on_pass(cx, report, &origin),
            Finished::Record(Ok(report), note) => self.on_record(cx, report, note),
            Finished::Set(outcome, set) => self.on_set(cx, outcome, &set),
            Finished::Pass(Err(abort), _) | Finished::Record(Err(abort), _) => {
                self.abort(cx, abort);
            }
        }
    }

    fn on_pass(&mut self, cx: &mut Cx<'_>, report: PassReport, origin: &PassOrigin) {
        if !report.tasks.is_empty() {
            self.task_context = match &report.result {
                PassResult::Delivered { context, .. } | PassResult::Blocked { context, .. } => {
                    Some(context.clone())
                }
                PassResult::Refused { .. }
                | PassResult::Duplicate { .. }
                | PassResult::EditRefused { .. } => self.task_context.take(),
            };
        }
        self.tasks.extend(report.tasks);
        self.observations.extend(report.observations);
        self.note_error(report.release_error);
        let trigger = match &report.result {
            PassResult::Delivered { event, .. } | PassResult::Blocked { event, .. } => {
                Some(event.clone())
            }
            PassResult::Refused { .. }
            | PassResult::Duplicate { .. }
            | PassResult::EditRefused { .. } => None,
        };
        if *origin == PassOrigin::Trigger {
            self.settle_trigger(&report.result);
        }
        if let PassResult::Delivered {
            event,
            plan,
            context,
            target,
        } = report.result
        {
            if let Some(target) = &target {
                report::target_changed(cx, &event, target, &context);
            }
            let kind = match origin {
                PassOrigin::Trigger => SetKind::Root,
                PassOrigin::Response => SetKind::Response,
                PassOrigin::Injected { .. } => SetKind::Injected,
            };
            self.lane.push_back(Job::Set(Box::new(DeliverySet::new(
                kind, *event, plan, context,
            ))));
        }
        if let Some(trigger) = trigger {
            self.queue_injected(cx, report.injected_events, &trigger);
        }
    }

    /// Writes what the trigger's pass decided into the result (RFC §10.1):
    /// the committed event, or the uncommitted instruction (RFC §10.1.1
    /// rule 5).
    fn settle_trigger(&mut self, result: &PassResult) {
        match result {
            PassResult::Delivered { event, .. } => {
                self.decided = true;
                self.result.event = Some((**event).clone());
            }
            PassResult::Blocked { event, reason, .. } => {
                // The result carries the stored record (gap G-026), or the
                // blocked instruction, which is not stored (gap G-065).
                self.decided = true;
                self.result.blocked = true;
                self.result.reason = reason.clone().or_else(|| event.blocked_by.clone());
                self.result.event = Some((**event).clone());
            }
            PassResult::Refused { .. } => {
                self.result.blocked = true;
                self.result.reason = Some("room_closed".to_owned());
            }
            PassResult::EditRefused { refusal } => {
                // Nothing was stored: the result carries no event (RFC §10.1
                // step 8a).
                self.result.blocked = true;
                self.result.reason = Some(refusal.reason().to_owned());
            }
            PassResult::Duplicate { event } => {
                // The replay answers what the first delivery did, the stored
                // record included (RFC §13.4, gap G-026).
                if event.status == EventStatus::Blocked {
                    self.result.blocked = true;
                    self.result.reason.clone_from(&event.blocked_by);
                }
                self.result.event = Some((**event).clone());
                self.lane.clear();
            }
        }
    }

    /// Queues the events a pass's hooks injected, each a pass of the
    /// cascade within its reentry budget (gap G-021), but an instruction
    /// (gap G-042).
    fn queue_injected(
        &mut self,
        cx: &mut Cx<'_>,
        injected: Vec<InjectedEvent>,
        trigger: &RoomEvent,
    ) {
        for injected in instructions::without_instructions(injected, trigger) {
            let targets = injected.target_channel_ids.clone();
            let event = stamp::stamp_injected(injected, trigger, cx.env.new_event_id());
            if self.budget.take() {
                self.lane.push_back(Job::Pass(
                    Box::new(CommitPass::new(
                        PassOrigin::Injected {
                            targets: targets.clone(),
                        },
                        event,
                    )),
                    PassOrigin::Injected { targets },
                ));
            } else {
                self.queue_refused(event, chain_depth::REENTRY_LOOP_CAP);
            }
        }
    }

    fn on_record(&mut self, cx: &mut Cx<'_>, report: RecordReport, note: RecordNote) {
        self.note_error(report.release_error);
        let (RecordResult::Committed(record), RecordNote::NotAsked { agent, trigger }) =
            (report.result, note)
        else {
            return;
        };
        let id = cx.env.new_observation_id();
        let now = cx.env.now();
        let max_chain_depth = cx.config.max_chain_depth;
        self.observations.push(chain_depth::not_asked_observation(
            id,
            &trigger,
            &agent,
            max_chain_depth,
            now,
        ));
        report::chain_depth_exceeded(cx, &record, &agent);
    }

    /// A delivery set finished: its observers run, its responses re-enter,
    /// and the records of the channels not asked are queued (RFC §10.1 steps
    /// 14 to 17).
    fn on_set(&mut self, cx: &mut Cx<'_>, outcome: SetOutcome, set: &DeliverySet) {
        let event = set.event();
        report::after_broadcast(cx, event, set.context());
        if set.kind() == SetKind::Root {
            self.settle_root(cx, event, &outcome);
        }
        if !outcome.tasks.is_empty() {
            self.task_context = Some(Box::new(set.context().clone()));
        }
        self.tasks.extend(outcome.tasks);
        self.observations.extend(outcome.observations);
        self.metadata_updates.extend(outcome.metadata_updates);
        for agent in outcome.not_asked {
            let id = cx.env.new_event_id();
            let record = chain_depth::not_asked_record(id, event, &agent, cx.env.now());
            self.lane.push_back(Job::Record(
                Box::new(RecordPass::new(record)),
                RecordNote::NotAsked {
                    agent: Box::new(agent),
                    trigger: Box::new(event.clone()),
                },
            ));
        }
        for response in outcome.responses {
            self.queue_response(cx, response, event);
        }
    }

    /// Reports the inbound event's deliveries and writes them into the
    /// result (RFC §10.1 step 18).
    fn settle_root(&mut self, cx: &mut Cx<'_>, event: &RoomEvent, outcome: &SetOutcome) {
        report::root_deliveries(cx, event, outcome);
        self.result
            .delivery_results
            .clone_from(&outcome.delivery_results);
        self.result
            .response_metadata
            .clone_from(&outcome.response_metadata);
        self.note_error(outcome.error.clone());
    }

    /// Queues one channel's responses to `trigger`: each re-enters as a pass,
    /// unless the room refuses it first (RFC §8.3). An instruction does not
    /// re-enter (gap G-066).
    fn queue_response(&mut self, cx: &mut Cx<'_>, response: Response, trigger: &RoomEvent) {
        let Response { responder, events } = response;
        let at_limit = chain_depth::reaches_limit(trigger.chain_depth, cx.config.max_chain_depth);
        let reentering = events
            .into_iter()
            .filter(|event| instructions::reenters(event, &responder.channel_id, trigger));
        for event in reentering {
            let event = stamp::stamp_response(event, &responder, trigger, cx.env.new_event_id());
            if at_limit && responder.category == ChannelCategory::Transport {
                // Only a transport is asked at the limit; what it answers is
                // stored refused (RFC §8.3).
                self.queue_refused(event, chain_depth::CHAIN_DEPTH_LIMIT);
            } else if self.budget.take() {
                self.lane.push_back(Job::Pass(
                    Box::new(CommitPass::new(PassOrigin::Response, event)),
                    PassOrigin::Response,
                ));
            } else {
                self.queue_refused(event, chain_depth::REENTRY_LOOP_CAP);
            }
        }
    }

    fn queue_refused(&mut self, mut event: RoomEvent, blocked_by: &str) {
        event.blocked_by = Some(blocked_by.to_owned());
        self.lane.push_back(Job::Record(
            Box::new(RecordPass::new(event)),
            RecordNote::Refused,
        ));
    }

    /// Keeps the first error met once the trigger was decided: the result
    /// reports one (RFC §13.6).
    fn note_error(&mut self, error: Option<ErrorRecord>) {
        if self.result.error.is_none() {
            self.result.error = error;
        }
    }

    /// A pass or a record failed. Before the trigger was decided nothing was
    /// written and the message fails, or is refused once past its deadline;
    /// after it, the event stands, the cascade stops, and the result carries
    /// the error (RFC §13.6).
    fn abort(&mut self, cx: &mut Cx<'_>, abort: Abort) {
        self.lane.clear();
        if let Abort::DeadlinePassed(locked_event) = abort {
            // Only the trigger's pass carries a deadline, and it is undecided.
            return self.expire(cx, locked_event);
        }
        if self.decided {
            return self.note_error(Some(abort.into_record()));
        }
        let error = match (abort, self.room_id.clone()) {
            (Abort::RoomNotFound, Some(room_id)) => InboundError::RoomNotFound(room_id),
            (Abort::Store(error), _) => InboundError::Store(error),
            (Abort::Lock(error), _) => InboundError::Lock(error),
            (abort @ (Abort::RoomNotFound | Abort::DeadlinePassed(_)), _) => {
                InboundError::Store(abort.into_record())
            }
        };
        self.end(cx, Err(error));
    }
}

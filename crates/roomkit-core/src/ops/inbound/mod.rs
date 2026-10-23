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
//! Not covered yet, and refused rather than half done: instructions (RFC
//! §10.1.1), edits and deletes (RFC §10.3), off-lock hooks (RFC §9.5.1).

mod arrival;
mod delivery;
mod job;
mod named_room;
mod pass;
mod record;
mod report;
mod result;
mod stamp;

use std::collections::VecDeque;
use std::sync::Arc;

use self::arrival::Arrival;
pub use self::arrival::InboundTarget;
use self::delivery::{DeliverySet, Response, SetKind, SetOutcome};
use self::job::{Finished, Job, RecordNote, Run};
use self::pass::{CommitPass, PassOrigin, PassReport, PassResult};
use self::record::{RecordPass, RecordReport, RecordResult};
pub use self::result::{InboundError, InboundResult};
pub use self::stamp::default_inbound_event;
use super::io::Io;
use super::protocol::{EffectId, Operation, Outcome, ProtocolError, Request, Step};
use super::{Abort, Cx, Environment, KitConfig, Progress};
use crate::model::{
    ChannelCategory, ErrorRecord, EventContent, EventStatus, EventType, InboundMessage,
    InjectedEvent, Metadata, Observation, RoomContext, RoomEvent, RoomId, RoomUpdates, Task,
};
use crate::rules::chain_depth::{self, ReentryBudget};

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
                holds_lane: false,
                stage: Stage::Start(Box::new(message)),
                lane: VecDeque::new(),
                active: None,
                budget,
                committed: false,
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
    /// Whether the operation holds the room's delivery lane, which it
    /// leaves before it ends.
    holds_lane: bool,
    stage: Stage,
    lane: VecDeque<Job>,
    active: Option<Job>,
    budget: ReentryBudget,
    /// Whether the trigger is committed: a failure after it no longer
    /// voids the result (RFC §13.6).
    committed: bool,
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
    Cascade,
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

    /// Resolves the channel and the sender's form, and refuses what this
    /// step does not provide (RFC §10.1 step 1), then starts the arrival,
    /// which enters the room's lane once the room is known.
    fn begin(&mut self, cx: &mut Cx<'_>) {
        let Stage::Start(message) = std::mem::replace(&mut self.stage, Stage::Done) else {
            return;
        };
        let (channel, message) = match arrival::resolve(cx.config, *message) {
            Ok(resolved) => resolved,
            Err(error) => return self.end(cx, Err(error)),
        };
        if let Err(error) = check_supported(&message) {
            return self.end(cx, Err(error));
        }
        let mut arrival = Box::new(Arrival::new(
            self.target.clone(),
            self.organization_id.clone(),
            channel,
            message,
        ));
        arrival.start(cx);
        self.stage = Stage::Arriving(arrival);
    }

    fn dispatch(&mut self, cx: &mut Cx<'_>, effect: EffectId, outcome: Outcome) {
        match std::mem::replace(&mut self.stage, Stage::Done) {
            Stage::Arriving(arrival) => self.on_arrival(cx, arrival, effect, outcome),
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
            stage @ (Stage::Start(_) | Stage::LeavingLane { .. } | Stage::Done) => {
                debug_assert!(false, "inbound received {effect:?} in {stage:?}");
                self.stage = stage;
            }
        }
    }

    /// Takes an answer of the arrival; once it hands the event, the cascade
    /// starts with its pass, in the room whose lane the arrival entered.
    fn on_arrival(
        &mut self,
        cx: &mut Cx<'_>,
        mut arrival: Box<Arrival>,
        effect: EffectId,
        outcome: Outcome,
    ) {
        match arrival.accept(cx, effect, outcome) {
            Progress::Waiting => self.stage = Stage::Arriving(arrival),
            Progress::Done(Ok(event)) => {
                self.room_id = Some(event.room_id.clone());
                self.holds_lane = arrival.lane().is_some();
                self.lane.push_back(Job::Pass(
                    Box::new(CommitPass::new(PassOrigin::Trigger, event)),
                    PassOrigin::Trigger,
                ));
                self.stage = Stage::Cascade;
            }
            Progress::Done(Err(error)) => {
                self.room_id = arrival.lane().cloned();
                self.holds_lane = self.room_id.is_some();
                self.end(cx, Err(error));
            }
        }
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

    fn settle(&mut self, cx: &mut Cx<'_>, finished: Finished) {
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
                PassResult::Refused { .. } | PassResult::Duplicate { .. } => {
                    self.task_context.take()
                }
            };
        }
        self.tasks.extend(report.tasks);
        self.observations.extend(report.observations);
        self.note_error(report.release_error);
        let trigger = match &report.result {
            PassResult::Delivered { event, .. } | PassResult::Blocked { event, .. } => {
                Some(event.clone())
            }
            PassResult::Refused { .. } | PassResult::Duplicate { .. } => None,
        };
        if *origin == PassOrigin::Trigger {
            self.settle_trigger(&report.result);
        }
        if let PassResult::Delivered {
            event,
            plan,
            context,
        } = report.result
        {
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

    /// Writes what the trigger's pass decided into the result (RFC §10.1).
    fn settle_trigger(&mut self, result: &PassResult) {
        match result {
            PassResult::Delivered { event, .. } => {
                self.committed = true;
                self.result.event = Some((**event).clone());
            }
            PassResult::Blocked { event, reason, .. } => {
                // The result carries the stored record (gap G-026).
                self.committed = true;
                self.result.blocked = true;
                self.result.reason = reason.clone().or_else(|| event.blocked_by.clone());
                self.result.event = Some((**event).clone());
            }
            PassResult::Refused { .. } => {
                self.result.blocked = true;
                self.result.reason = Some("room_closed".to_owned());
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
    /// cascade within its reentry budget (gap G-021).
    fn queue_injected(
        &mut self,
        cx: &mut Cx<'_>,
        injected: Vec<InjectedEvent>,
        trigger: &RoomEvent,
    ) {
        for injected in injected {
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
    /// unless the room refuses it first (RFC §8.3).
    fn queue_response(&mut self, cx: &mut Cx<'_>, response: Response, trigger: &RoomEvent) {
        let Response { responder, events } = response;
        let at_limit = chain_depth::reaches_limit(trigger.chain_depth, cx.config.max_chain_depth);
        for event in events {
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

    /// Stores the side effects every pass and set collected, and applies the
    /// room metadata updates the channels returned (RFC §10.1 step 15, gap
    /// G-049), then ends. The update names its keys only, so it needs no
    /// lock: it never overwrites the counters of an event committed
    /// meanwhile.
    fn persist_side_effects(&mut self, cx: &mut Cx<'_>) {
        let side_effects = self.issue_side_effects(cx);
        let metadata = self.issue_metadata_updates(cx);
        self.await_persisted(cx, side_effects, metadata);
    }

    /// Asks for the collected tasks and observations to be stored, if any.
    fn issue_side_effects(&mut self, cx: &mut Cx<'_>) -> Option<(EffectId, Vec<Task>)> {
        if self.tasks.is_empty() && self.observations.is_empty() {
            return None;
        }
        let room_id = self.room_id.clone()?;
        let tasks = std::mem::take(&mut self.tasks);
        let effect = cx.io.issue(Request::PersistSideEffects {
            room_id,
            tasks: tasks.clone(),
            observations: std::mem::take(&mut self.observations),
        });
        Some((effect, tasks))
    }

    /// Asks for the collected room metadata updates to be written, if any.
    fn issue_metadata_updates(&mut self, cx: &mut Cx<'_>) -> Option<EffectId> {
        if self.metadata_updates.is_empty() {
            return None;
        }
        let room_id = self.room_id.clone()?;
        Some(cx.io.issue(Request::UpdateRoom {
            room_id,
            updates: RoomUpdates {
                updated_at: Some(cx.env.now()),
                metadata: std::mem::take(&mut self.metadata_updates),
                ..RoomUpdates::default()
            },
        }))
    }

    /// Takes the answer of one of step 15's writes.
    fn on_persisting(
        &mut self,
        cx: &mut Cx<'_>,
        side_effects: Option<(EffectId, Vec<Task>)>,
        metadata: Option<EffectId>,
        effect: EffectId,
        outcome: Outcome,
    ) {
        match (side_effects, metadata) {
            (Some((expected, tasks)), metadata) if expected == effect => {
                self.on_persisted(cx, outcome, &tasks);
                self.await_persisted(cx, None, metadata);
            }
            (side_effects, Some(expected)) if expected == effect => {
                if let Err(error) = outcome.into_stored() {
                    self.note_error(Some(error));
                }
                self.await_persisted(cx, side_effects, None);
            }
            (side_effects, metadata) => {
                debug_assert!(false, "inbound received {effect:?} while persisting");
                self.stage = Stage::Persisting {
                    side_effects,
                    metadata,
                };
            }
        }
    }

    /// Waits for step 15's outstanding writes, or ends once none is left.
    fn await_persisted(
        &mut self,
        cx: &mut Cx<'_>,
        side_effects: Option<(EffectId, Vec<Task>)>,
        metadata: Option<EffectId>,
    ) {
        if side_effects.is_none() && metadata.is_none() {
            let result = std::mem::take(&mut self.result);
            return self.end(cx, Ok(result));
        }
        self.stage = Stage::Persisting {
            side_effects,
            metadata,
        };
    }

    /// The side effects are stored: each task's `ON_TASK_CREATED` observers
    /// run (RFC §10.1 step 15).
    fn on_persisted(&mut self, cx: &mut Cx<'_>, outcome: Outcome, tasks: &[Task]) {
        match outcome.into_persisted() {
            Ok(()) => {
                if let Some(context) = &self.task_context {
                    report::tasks_created(cx, tasks, context);
                }
            }
            Err(error) => self.note_error(Some(error)),
        }
    }

    /// Keeps the first error met once the trigger was decided: the result
    /// reports one (RFC §13.6).
    fn note_error(&mut self, error: Option<ErrorRecord>) {
        if self.result.error.is_none() {
            self.result.error = error;
        }
    }

    /// A pass or a record failed. Before the trigger's commit nothing was
    /// written and the message fails; after it, the event stands, the
    /// cascade stops, and the result carries the error (RFC §13.6).
    fn abort(&mut self, cx: &mut Cx<'_>, abort: Abort) {
        self.lane.clear();
        if !self.committed {
            let error = match (abort, self.room_id.clone()) {
                (Abort::RoomNotFound, Some(room_id)) => InboundError::RoomNotFound(room_id),
                (Abort::RoomNotFound, None) => {
                    InboundError::Store(ErrorRecord::new("RoomNotFound", "the room disappeared"))
                }
                (Abort::Store(error), _) => InboundError::Store(error),
                (Abort::Lock(error), _) => InboundError::Lock(error),
            };
            return self.end(cx, Err(error));
        }
        self.note_error(Some(match abort {
            Abort::RoomNotFound => ErrorRecord::new("RoomNotFound", "the room disappeared"),
            Abort::Store(error) | Abort::Lock(error) => error,
        }));
    }

    /// Ends the operation with `outcome`, once the room's lane is left when
    /// the operation holds it: every path that entered the lane leaves it.
    fn end(&mut self, cx: &mut Cx<'_>, outcome: Result<InboundResult, InboundError>) {
        self.lane.clear();
        self.active = None;
        let held = self.room_id.clone().filter(|_| self.holds_lane);
        let Some(room_id) = held else {
            self.stage = Stage::Done;
            self.outcome = Some(outcome);
            return;
        };
        let effect = cx.io.issue(Request::LeaveLane { room_id });
        self.stage = Stage::LeavingLane {
            effect,
            outcome: Box::new(outcome),
        };
    }

    /// The lane is left. A failed leave changes nothing the call decided: it
    /// is logged, and a result carries it when no earlier error was met, as
    /// a failed release of the room lock (gap G-025).
    fn on_left(&mut self, outcome: Result<InboundResult, InboundError>, left: Outcome) {
        self.holds_lane = false;
        self.stage = Stage::Done;
        let outcome = match (left.into_left(), outcome) {
            (Ok(()), outcome) => outcome,
            (Err(error), outcome) => {
                tracing::warn!(
                    target: "roomkit::core::inbound",
                    room_id = ?self.room_id,
                    error_type = %error.error_type,
                    message = %error.message,
                    "leaving the room's delivery lane failed; what the call decided stands"
                );
                outcome.map(|mut result| {
                    result.error.get_or_insert(error);
                    result
                })
            }
        };
        self.outcome = Some(outcome);
    }
}

/// Refuses what this step does not provide yet, before anything is read.
fn check_supported(message: &InboundMessage) -> Result<(), InboundError> {
    if message.standalone && message.event_type != EventType::Instruction {
        return Err(InboundError::StandaloneOutsideInstruction);
    }
    if message.event_type == EventType::Instruction {
        return Err(InboundError::Unsupported("an instruction (RFC §10.1.1)"));
    }
    let edits = matches!(message.event_type, EventType::Edit | EventType::Delete)
        || matches!(
            message.content,
            EventContent::Edit(_) | EventContent::Delete(_)
        );
    if edits {
        return Err(InboundError::Unsupported("an edit or a delete (RFC §10.3)"));
    }
    Ok(())
}

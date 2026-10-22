//! The inbound pipeline (RFC §10.1), with the broadcast of every event it
//! commits (RFC §10.2).
//!
//! [`Inbound`] runs one message into a room the caller names, and the whole
//! cascade it starts: the trigger's pass, its delivery set, the responses
//! that re-enter as passes of their own, the events hooks inject, and the
//! records the room stores in place of responses it refuses. The room's
//! delivery lane is a FIFO of that work, so one event's delivery set
//! completes before the next one's begins.
//!
//! Not covered yet, and refused rather than half done: routing a message to a
//! room the caller does not name (RFC §10.4), instructions (RFC §10.1.1),
//! edits and deletes (RFC §10.3), off-lock hooks (RFC §9.5.1).

mod delivery;
mod job;
mod pass;
mod record;
mod report;
mod result;
mod stamp;

use std::collections::VecDeque;
use std::sync::Arc;

use self::delivery::{DeliverySet, Response, SetKind, SetOutcome};
use self::job::{Finished, Job, RecordNote, Run};
use self::pass::{CommitPass, PassOrigin, PassReport, PassResult};
use self::record::{RecordPass, RecordReport, RecordResult};
pub use self::result::{InboundError, InboundResult};
pub use self::stamp::default_inbound_event;
use super::io::Io;
use super::protocol::{EffectId, Operation, Outcome, ProtocolError, Request, Step};
use super::{Abort, ChannelDeclaration, Cx, Environment, KitConfig};
use crate::model::{
    ChannelCategory, ErrorRecord, EventContent, EventStatus, EventType, InboundMessage,
    InjectedEvent, Observation, RoomContext, RoomEvent, RoomId, Task,
};
use crate::rules::chain_depth::{self, ReentryBudget};
use crate::rules::permissions;

/// The inbound pipeline for one message (RFC §10.1).
///
/// ```
/// # use std::sync::Arc;
/// # use roomkit_core::model::{ChannelId, EventContent, InboundMessage, RoomId};
/// # use roomkit_core::ops::{Inbound, KitConfig, Next, Operation, SequentialEnvironment};
/// let config = Arc::new(KitConfig::new());
/// let message = InboundMessage::new(ChannelId::new("sms")?, "+15551234567", EventContent::text("Hi"));
/// let mut inbound = Inbound::new(config, RoomId::new("room_1")?, message);
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
    /// The pipeline for `message`, into `room_id`.
    pub fn new(config: Arc<KitConfig>, room_id: RoomId, message: InboundMessage) -> Self {
        let budget = ReentryBudget::new(config.max_chain_depth);
        Self {
            config,
            io: Io::new(),
            machine: Machine {
                room_id,
                stage: Stage::Start(Box::new(message)),
                lane: VecDeque::new(),
                active: None,
                budget,
                committed: false,
                result: InboundResult::default(),
                tasks: Vec::new(),
                observations: Vec::new(),
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
    room_id: RoomId,
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
    outcome: Option<Result<InboundResult, InboundError>>,
}

#[derive(Debug)]
enum Stage {
    Start(Box<InboundMessage>),
    LoadingContext {
        effect: EffectId,
        message: Box<InboundMessage>,
        channel: Box<ChannelDeclaration>,
    },
    Parsing {
        effect: EffectId,
        message: Box<InboundMessage>,
        channel: Box<ChannelDeclaration>,
    },
    Cascade,
    Persisting(EffectId),
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

    /// Resolves the channel and refuses what this step does not provide
    /// (RFC §10.1 step 1), then reads the room (step 3).
    fn begin(&mut self, cx: &mut Cx<'_>) {
        let Stage::Start(message) = std::mem::replace(&mut self.stage, Stage::Done) else {
            return;
        };
        let Some(channel) = cx.config.channel(&message.channel_id).cloned() else {
            return self.fail(InboundError::UnknownChannel(message.channel_id.clone()));
        };
        if let Err(error) = check_supported(&message) {
            return self.fail(error);
        }
        let effect = cx.io.issue(Request::LoadContext {
            room_id: self.room_id.clone(),
            recent_events: cx.config.context_window,
        });
        self.stage = Stage::LoadingContext {
            effect,
            message,
            channel: Box::new(channel),
        };
    }

    fn dispatch(&mut self, cx: &mut Cx<'_>, effect: EffectId, outcome: Outcome) {
        match std::mem::replace(&mut self.stage, Stage::Done) {
            Stage::LoadingContext {
                effect: expected,
                message,
                channel,
            } if expected == effect => self.on_context(cx, message, channel, outcome),
            Stage::Parsing {
                effect: expected,
                message,
                channel,
            } if expected == effect => self.on_parsed(&message, &channel, outcome),
            Stage::Persisting(expected) if expected == effect => self.on_persisted(outcome),
            Stage::Cascade => {
                self.stage = Stage::Cascade;
                self.dispatch_to_job(cx, effect, outcome);
            }
            stage @ (Stage::Start(_)
            | Stage::LoadingContext { .. }
            | Stage::Parsing { .. }
            | Stage::Persisting(_)
            | Stage::Done) => {
                debug_assert!(false, "inbound received {effect:?} in {stage:?}");
                self.stage = stage;
            }
        }
    }

    /// Hands the room's context to the channel's `handle_inbound()` (RFC
    /// §10.1 step 4).
    fn on_context(
        &mut self,
        cx: &mut Cx<'_>,
        message: Box<InboundMessage>,
        channel: Box<ChannelDeclaration>,
        outcome: Outcome,
    ) {
        let context = match outcome.into_context() {
            Ok(Some(context)) => context,
            Ok(None) => return self.fail(InboundError::RoomNotFound(self.room_id.clone())),
            Err(error) => return self.fail(InboundError::Store(error)),
        };
        // The receiving channel sees only what it may (RFC §7.5 rule 8). One
        // with no binding in the room sees no event; its pass stores the
        // message refused (gap G-015).
        let receiver = context
            .bindings
            .iter()
            .find(|binding| binding.channel_id == message.channel_id);
        let context = Box::new(match receiver {
            Some(receiver) => permissions::context_for(receiver, &context),
            None => RoomContext {
                recent_events: Vec::new(),
                ..*context
            },
        });
        let effect = cx.io.issue(Request::HandleInbound {
            channel_id: message.channel_id.clone(),
            message: (*message).clone(),
            context,
        });
        self.stage = Stage::Parsing {
            effect,
            message,
            channel,
        };
    }

    /// Stamps the parsed event and starts the cascade with its pass.
    fn on_parsed(
        &mut self,
        message: &InboundMessage,
        channel: &ChannelDeclaration,
        outcome: Outcome,
    ) {
        let parsed = match outcome.into_parsed() {
            Ok(event) => *event,
            Err(error) => {
                return self.fail(InboundError::ChannelFailed {
                    channel_id: message.channel_id.clone(),
                    error,
                });
            }
        };
        let mut event = stamp::stamp_trigger(parsed, message, channel);
        event.room_id = self.room_id.clone();
        self.lane.push_back(Job::Pass(
            Box::new(CommitPass::new(PassOrigin::Trigger, event)),
            PassOrigin::Trigger,
        ));
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

    fn settle(&mut self, cx: &mut Cx<'_>, finished: Finished) {
        match finished {
            Finished::Pass(Ok(report), origin) => self.on_pass(cx, report, &origin),
            Finished::Record(Ok(report), note) => self.on_record(cx, report, note),
            Finished::Set(outcome, set) => self.on_set(cx, outcome, &set),
            Finished::Pass(Err(abort), _) | Finished::Record(Err(abort), _) => self.abort(abort),
        }
    }

    fn on_pass(&mut self, cx: &mut Cx<'_>, report: PassReport, origin: &PassOrigin) {
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
            PassResult::Blocked { event, reason } => {
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
        self.tasks.extend(outcome.tasks);
        self.observations.extend(outcome.observations);
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

    /// Stores the side effects every pass and set collected (RFC §10.1 step
    /// 15), then ends.
    fn persist_side_effects(&mut self, cx: &mut Cx<'_>) {
        if self.tasks.is_empty() && self.observations.is_empty() {
            return self.finish();
        }
        let effect = cx.io.issue(Request::PersistSideEffects {
            room_id: self.room_id.clone(),
            tasks: std::mem::take(&mut self.tasks),
            observations: std::mem::take(&mut self.observations),
        });
        self.stage = Stage::Persisting(effect);
    }

    fn on_persisted(&mut self, outcome: Outcome) {
        self.note_error(outcome.into_persisted().err());
        self.finish();
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
    fn abort(&mut self, abort: Abort) {
        self.lane.clear();
        if !self.committed {
            let error = match abort {
                Abort::RoomNotFound => InboundError::RoomNotFound(self.room_id.clone()),
                Abort::Store(error) => InboundError::Store(error),
                Abort::Lock(error) => InboundError::Lock(error),
            };
            return self.fail(error);
        }
        self.note_error(Some(match abort {
            Abort::RoomNotFound => ErrorRecord::new("RoomNotFound", "the room disappeared"),
            Abort::Store(error) | Abort::Lock(error) => error,
        }));
    }

    fn finish(&mut self) {
        self.stage = Stage::Done;
        self.outcome = Some(Ok(std::mem::take(&mut self.result)));
    }

    fn fail(&mut self, error: InboundError) {
        self.stage = Stage::Done;
        self.lane.clear();
        self.active = None;
        self.outcome = Some(Err(error));
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

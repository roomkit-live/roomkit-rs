//! The room's agents run again on a stored event, as on a new one (RFC
//! §11.7.6, §10.1 step 14): the Python implementation's
//! `_rerun_agents_on`, which a held message's release calls once it is
//! restored, and `regenerate_response`, which chooses the message under the
//! lock first (`_plan_regeneration`).
//!
//! The call takes the room's lock and reads, under it, the room as the
//! lock finds it, both within its `process_timeout`; the room's due timers
//! are applied, a release reads its event again, and a room whose status
//! refuses new events is refused, as the inbound gate does, before any
//! agent runs (RFC §5.1). An event that is no longer one
//! the agents may answer runs nothing: not in the room, deleted or itself
//! a change (RFC §10.3), or its source binding gone, not a transport, or
//! unable to write. Otherwise its delivery set is planned for the room's
//! agents alone, its scope intersected with `intelligence` so that a
//! transport is never handed it again, at least as deep as the work the
//! call is made in (RFC §8.3), its place taken in the room's lane behind
//! every event committed, and the lock released. From there it is the
//! cascade of a new message: the agents' answers re-enter as passes, are
//! committed and delivered, and come back in the call's result. Nothing
//! of the event's first pass is repeated: its deliveries are not
//! reported, its `AFTER_BROADCAST` hooks do not run again, and no
//! `event_processed` is emitted.

use super::delivery::{DeliverySet, Placement, SetKind, SetOutcome};
use super::job::{Job, RecordNote};
use super::queues::{Delivery, Lineage};
use super::record::RecordPass;
use super::{InboundResult, Machine, Stage, cascade, report};
use crate::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelType, EventId, Room, RoomContext,
    RoomEvent, RoomId, Visibility,
};
use crate::ops::protocol::{
    DiscussionRequest, DiscussionTurn, EffectId, HoldOutcome, HoldRequest, Outcome, StoppedTurn,
    StoreRequest,
};
use crate::ops::section::{Failure, Section, SectionWork};
use crate::ops::{Abort, Cx, Progress, RefusedOperation};
use crate::rules::chain_depth::{self, ReentryBudget};
use crate::rules::discussion;
use crate::rules::permissions::{self, DeliveryPlan};
use crate::rules::regeneration;
use crate::rules::verification::release;

/// What a rerun's section came to.
#[derive(Debug)]
pub(super) enum Reran {
    /// The event is no longer one the agents may answer: nothing runs.
    Nothing,
    /// The room's status refuses new events: refused before any agent
    /// runs (RFC §5.1).
    Refused,
    /// A regeneration's caller named a trigger the choice under the lock
    /// is not (RFC §13.6): refused before any agent runs.
    Moved,
    /// The read of the room under the lock passed the call's deadline (RFC
    /// §13.6): refused `process_timeout` before any agent runs.
    Expired,
    /// The event's delivery set, planned for the agents and placed in the
    /// room's lane.
    Planned(Box<Planned>),
    /// No turn, but turns the discussion's depth limit stopped, to record
    /// (RFC §8.3).
    Stopped(Vec<StoppedTurn>, Box<RoomContext>),
}

/// A rerun's delivery set, as planned under the lock.
#[derive(Debug)]
pub(super) struct Planned {
    /// The event as the store holds it, what the call's result names.
    trigger: RoomEvent,
    /// The event as the agents are handed it.
    event: RoomEvent,
    plan: DeliveryPlan,
    context: Box<RoomContext>,
    placement: Placement,
    /// A discussion's turn: its own chain-depth limit (RFC §19.7.5).
    limit: Option<u32>,
    /// The turns its depth limit stopped on the way, recorded first.
    stopped: Vec<StoppedTurn>,
}

/// The rerun's work under the room's lock.
#[derive(Debug)]
pub(super) struct RerunWork {
    asked: Asked,
    context: Option<Box<RoomContext>>,
    waiting: Waiting,
}

/// What the call reruns.
#[derive(Debug)]
enum Asked {
    /// The stored event `event_id`, for the room's agents; `floor` is the
    /// least depth of their answers: the depth of the work the call is made
    /// in (RFC §8.3).
    Event { event_id: EventId, floor: u32 },
    /// The room's last message, as [`regeneration::target`] chooses it,
    /// at least `floor` deep; when `trigger_id` names one, only that one.
    Newest {
        trigger_id: Option<EventId>,
        floor: u32,
    },
    /// The turn the discussion holding the room gives now, if any (RFC
    /// §19.7.5).
    Turn,
}

#[derive(Debug)]
enum Waiting {
    Nothing,
    Event(EffectId),
    Turn(EffectId),
    Placing {
        effect: EffectId,
        planned: Box<Planned>,
    },
}

impl RerunWork {
    /// The trigger is read again, under the lock: the room's status refuses
    /// it first, then what makes it no event the agents answer, then its
    /// set is planned and placed.
    fn on_event(&mut self, cx: &mut Cx<'_>, outcome: Outcome) -> Progress<Result<Reran, Abort>> {
        let trigger = match outcome.into_event() {
            Ok(trigger) => trigger,
            Err(error) => return Progress::Done(Err(Abort::Store(error))),
        };
        let Some(context) = self.context.take() else {
            return Progress::Done(Ok(Reran::Nothing));
        };
        let status = context.room.status;
        if !status.accepts_events() {
            if let Some(trigger) = &trigger {
                report::room_refused_as(cx, trigger, RefusedOperation::Regenerate, status);
            }
            return Progress::Done(Ok(Reran::Refused));
        }
        let Some(trigger) = trigger.filter(|event| answerable(event, &context)) else {
            return Progress::Done(Ok(Reran::Nothing));
        };
        let Asked::Event { floor, .. } = self.asked else {
            return Progress::Done(Ok(Reran::Nothing));
        };
        self.placing(cx, plan(cx, *trigger, context, floor))
    }

    /// The turn the discussion gives, if any, is planned for its agent
    /// alone; one the agent cannot read is no turn.
    fn on_turn(&mut self, cx: &mut Cx<'_>, outcome: Outcome) -> Progress<Result<Reran, Abort>> {
        let Some(context) = self.context.take() else {
            return Progress::Done(Ok(Reran::Nothing));
        };
        let next = match outcome.into_discussion_turn() {
            Ok(next) => next,
            Err(error) => return Progress::Done(Err(Abort::Store(error))),
        };
        let planned = next
            .turn
            .and_then(|turn| plan_turn(*turn, context.clone(), next.stopped.clone()));
        match planned {
            Some(planned) => self.placing(cx, planned),
            None if next.stopped.is_empty() => Progress::Done(Ok(Reran::Nothing)),
            None => Progress::Done(Ok(Reran::Stopped(next.stopped, context))),
        }
    }

    /// A regeneration's choice, under the lock (Python's
    /// `_refuse_regeneration`): a room whose status refuses new events
    /// refuses it first, naming the message it would have answered again,
    /// if any; then a named trigger that is not the choice; then no message
    /// runs nothing.
    fn choose(
        &mut self,
        cx: &mut Cx<'_>,
        context: Box<RoomContext>,
        trigger_id: Option<&EventId>,
        floor: u32,
    ) -> Progress<Result<Reran, Abort>> {
        let trigger = regeneration::target(&context).cloned();
        let status = context.room.status;
        if !status.accepts_events() {
            report::regeneration_refused(cx, &context.room.id, trigger.as_ref(), status);
            return Progress::Done(Ok(Reran::Refused));
        }
        if let Some(trigger_id) = trigger_id
            && trigger.as_ref().map(|trigger| &trigger.id) != Some(trigger_id)
        {
            tracing::info!(
                target: "roomkit::core::inbound",
                room_id = %context.room.id,
                event_id = %trigger_id,
                "regeneration refused: its trigger is no longer the room's choice"
            );
            return Progress::Done(Ok(Reran::Moved));
        }
        let Some(trigger) = trigger else {
            return Progress::Done(Ok(Reran::Nothing));
        };
        self.placing(cx, plan(cx, trigger, context, floor))
    }

    /// The set takes its place in the room's lane, behind every event
    /// committed.
    fn placing(&mut self, cx: &mut Cx<'_>, planned: Planned) -> Progress<Result<Reran, Abort>> {
        let request = HoldRequest::EnterLane {
            room_id: planned.trigger.room_id.clone(),
            event_id: planned.trigger.id.clone(),
        };
        let effect = cx.io.issue_locked(request, planned.trigger.room_id.clone());
        self.waiting = Waiting::Placing {
            effect,
            planned: Box::new(planned),
        };
        Progress::Waiting
    }
}

impl SectionWork for RerunWork {
    type Output = Reran;
    type Error = Abort;

    fn failure(failure: Failure) -> Abort {
        match failure {
            Failure::Store(error) => Abort::Store(error),
            Failure::Lock(error) => Abort::Lock(error),
        }
    }

    /// The agents read the room's history as a turn does.
    fn reads_history(&self) -> bool {
        true
    }

    fn missing(&self, _room_id: &RoomId) -> Result<Reran, Abort> {
        Err(Abort::RoomNotFound)
    }

    fn refuses(&self, _room: &Room) -> Option<Result<Reran, Abort>> {
        None
    }

    fn begin(
        &mut self,
        cx: &mut Cx<'_>,
        context: Box<RoomContext>,
    ) -> Progress<Result<Reran, Abort>> {
        let room_id = context.room.id.clone();
        match &self.asked {
            Asked::Newest { trigger_id, floor } => {
                let (trigger_id, floor) = (trigger_id.clone(), *floor);
                return self.choose(cx, context, trigger_id.as_ref(), floor);
            }
            Asked::Event { event_id, .. } => {
                let request = StoreRequest::GetEvent {
                    event_id: event_id.clone(),
                };
                // Read once the due timers are applied, unbounded: Python reads
                // the event within the call's bound, before applying them
                // (RMK-1491).
                let effect = cx.io.issue_locked(request, room_id);
                self.waiting = Waiting::Event(effect);
            }
            Asked::Turn => {
                // A room whose status refuses new events gives no turn.
                if !context.room.status.accepts_events() {
                    return Progress::Done(Ok(Reran::Nothing));
                }
                let request = DiscussionRequest::Turn {
                    room_id: room_id.clone(),
                    context: context.clone(),
                };
                self.waiting = Waiting::Turn(cx.io.issue_locked(request, room_id));
            }
        }
        self.context = Some(context);
        Progress::Waiting
    }

    fn accept(
        &mut self,
        cx: &mut Cx<'_>,
        _effect: EffectId,
        outcome: Outcome,
    ) -> Progress<Result<Reran, Abort>> {
        match std::mem::replace(&mut self.waiting, Waiting::Nothing) {
            Waiting::Event(_) => self.on_event(cx, outcome),
            Waiting::Turn(_) => self.on_turn(cx, outcome),
            Waiting::Placing { mut planned, .. } => {
                planned.placement = placement(&planned.trigger, outcome);
                Progress::Done(Ok(Reran::Planned(planned)))
            }
            Waiting::Nothing => Progress::Waiting,
        }
    }

    fn awaits(&self, effect: EffectId) -> bool {
        match &self.waiting {
            Waiting::Event(awaited)
            | Waiting::Turn(awaited)
            | Waiting::Placing {
                effect: awaited, ..
            } => *awaited == effect,
            Waiting::Nothing => false,
        }
    }

    fn report(&self, _cx: &mut Cx<'_>, _result: &Result<Reran, Abort>) {}

    /// Python's `_plan_regeneration` and `_rerun_agents_on` bound their
    /// read of the room with the wait for the lock.
    fn read_expired(&self) -> Option<Result<Reran, Abort>> {
        Some(Ok(Reran::Expired))
    }
}

/// Whether `trigger` is an event of the room the agents may answer: not
/// deleted nor itself a change (RFC §10.3), its source binding a transport
/// that can write.
fn answerable(trigger: &RoomEvent, context: &RoomContext) -> bool {
    let Some(source) = context.get_binding(&trigger.source.channel_id) else {
        return false;
    };
    trigger.room_id == context.room.id
        && !release::withdrawn(trigger)
        && source.category == ChannelCategory::Transport
        && permissions::write_refusal(source).is_none()
}

/// `trigger`'s delivery set for the room's agents alone: its scope within
/// `intelligence`, so that no transport is handed it again, and still
/// within its own (RFC §7.3); at least `floor` deep (RFC §8.3); its
/// answers bound by its `response_visibility`, which it keeps.
fn plan(cx: &Cx<'_>, trigger: RoomEvent, context: Box<RoomContext>, floor: u32) -> Planned {
    let mut event = trigger.clone();
    event.visibility = trigger
        .visibility
        .intersect(&Visibility::Intelligence, |channel_id| {
            context
                .get_binding(channel_id)
                .map(|binding| binding.category)
        });
    event.chain_depth = event.chain_depth.max(floor);
    let plan = permissions::plan_in_room(&event, &context, cx.config.max_chain_depth);
    Planned {
        trigger,
        event,
        plan,
        context,
        placement: Placement::Missing,
        limit: None,
        stopped: Vec::new(),
    }
}

/// A discussion's turn on the event it answers: for its agent alone, the
/// event carrying the turn's mark, its answers bound by the event's
/// `response_visibility` (RFC §19.7.5 rules 9 and 11); `None` when the agent
/// cannot read the event.
fn plan_turn(
    turn: DiscussionTurn,
    context: Box<RoomContext>,
    stopped: Vec<StoppedTurn>,
) -> Option<Planned> {
    let DiscussionTurn {
        agent,
        answered,
        mark,
        max_depth,
    } = turn;
    let mut event = permissions::turn_event(&answered, &context)?;
    let plan = permissions::plan_turn(&event, &context.bindings, &agent, max_depth)?;
    event.metadata.insert(discussion::TURN_KEY.to_owned(), mark);
    Some(Planned {
        trigger: *answered,
        event,
        plan,
        context,
        placement: Placement::Missing,
        limit: Some(max_depth),
        stopped,
    })
}

/// What became of the rerun's place in the room's lane: a place that could
/// not be taken is logged, and the agents run all the same, out of the
/// lane's order.
fn placement(trigger: &RoomEvent, outcome: Outcome) -> Placement {
    if outcome == Outcome::Hold(HoldOutcome::LaneClosed) {
        return Placement::Closed;
    }
    match outcome.into_entered() {
        Ok(()) => Placement::Taken,
        Err(error) => {
            tracing::warn!(
                target: "roomkit::core::inbound",
                room_id = %trigger.room_id,
                event_id = %trigger.id,
                error_type = %error.error_type,
                message = %error.message,
                "taking a rerun's place in the room's delivery lane failed; its agents run \
                 all the same"
            );
            Placement::Missing
        }
    }
}

impl Machine {
    /// Starts the rerun of `event_id` in `room_id`: its section waits for
    /// the room's lock within the call's bound.
    pub(super) fn begin_rerun(
        &mut self,
        cx: &mut Cx<'_>,
        room_id: RoomId,
        event_id: EventId,
        floor: u32,
    ) {
        self.begin_section(cx, room_id, Asked::Event { event_id, floor });
    }

    /// Starts the regeneration of `room_id`'s last message, the one named
    /// `trigger_id` when it is given: its section waits for the room's lock
    /// within the call's bound.
    pub(super) fn begin_regenerate(
        &mut self,
        cx: &mut Cx<'_>,
        room_id: RoomId,
        trigger_id: Option<EventId>,
        floor: u32,
    ) {
        self.begin_section(cx, room_id, Asked::Newest { trigger_id, floor });
    }

    /// Starts the turn the discussion holding `room_id` gives now, if any.
    pub(super) fn begin_turn(&mut self, cx: &mut Cx<'_>, room_id: RoomId) {
        self.begin_section(cx, room_id, Asked::Turn);
    }

    fn begin_section(&mut self, cx: &mut Cx<'_>, room_id: RoomId, asked: Asked) {
        self.call.room_id = Some(room_id.clone());
        // A discussion's turn waits for the room's lock as long as it takes:
        // nobody is waiting on its call, whose driver would only ask again.
        let bound = (!matches!(asked, Asked::Turn)).then_some(self.call.bound);
        let work = RerunWork {
            asked,
            context: None,
            waiting: Waiting::Nothing,
        };
        let section = Section::new(room_id, work);
        // Its read of the room is bounded with its wait for the lock, as
        // Python's `_plan_regeneration` and `_rerun_agents_on` bound it.
        let mut section = Box::new(match bound {
            Some(bound) => section.waiting_within(bound).reading_within(bound),
            None => section,
        });
        section.start(cx);
        self.stage.enter(Stage::Rerunning(section));
    }

    /// Takes an answer of the rerun's section; once it ends, the call ends,
    /// or its delivery set is queued as the call's cascade.
    pub(super) fn on_rerun(
        &mut self,
        cx: &mut Cx<'_>,
        mut section: Box<Section<RerunWork>>,
        effect: EffectId,
        outcome: Outcome,
    ) {
        if outcome == Outcome::DeadlinePassed && section.locking() {
            // The wait for the lock passed the deadline: no lock is held,
            // nothing was written (RFC §13.6). Its `process_timeout` names
            // the operation, as Python's `regenerate` does.
            return self.expire_as(cx, None, Some(RefusedOperation::Regenerate));
        }
        let ended = match section.accept(cx, effect, outcome) {
            Progress::Waiting => return self.stage.enter(Stage::Rerunning(section)),
            Progress::Done(ended) => ended,
        };
        match ended.result {
            Ok(Reran::Nothing) => self.end(cx, Ok(InboundResult::default())),
            // A refusal carries the section's failed release as its error,
            // as Python's `regenerate_response` collects it (RFC §13.5).
            Ok(Reran::Refused) => self.end(cx, Ok(with_error(room_closed(), ended.release_error))),
            Ok(Reran::Moved) => {
                let moved = refused("trigger_moved");
                self.end(cx, Ok(with_error(moved, ended.release_error)));
            }
            Ok(Reran::Expired) => {
                // The read passed the deadline under the lock, now released.
                self.trigger.note_error(ended.release_error);
                self.expire_as(cx, None, Some(RefusedOperation::Regenerate));
            }
            Ok(Reran::Planned(planned)) => {
                self.trigger.note_error(ended.release_error);
                self.queue_rerun(cx, *planned);
            }
            Ok(Reran::Stopped(stopped, context)) => {
                self.trigger.note_error(ended.release_error);
                self.queue_stopped(cx, stopped, &context);
                self.trigger.decided = true;
                self.stage.enter(Stage::Cascade);
                let decided = Ok(self.trigger.result.clone());
                self.publish(cx, &decided);
            }
            Err(abort) => {
                let error = cascade::failure(abort, self.call.room_id.clone());
                self.end(cx, Err(error));
            }
        }
    }

    /// The rerun's delivery set joins the call's cascade, the call decided:
    /// its result names the event.
    fn queue_rerun(&mut self, cx: &mut Cx<'_>, planned: Planned) {
        let Planned {
            trigger,
            event,
            plan,
            context,
            placement,
            limit,
            stopped,
        } = planned;
        self.queue_stopped(cx, stopped, &context);
        self.trigger.decided = true;
        self.trigger.result.event = Some(trigger);
        // A discussion's turn, and all that answers it, draws on one budget
        // its own limit bounds (RFC §19.7.5 rule 11).
        if let Some(limit) = limit {
            self.cascade.budget = ReentryBudget::new(limit);
        }
        let mut set = DeliverySet::new(
            SetKind::Rerun,
            event,
            plan,
            context,
            placement,
            self.cascade.budget.clone(),
            Lineage::Call,
        );
        if let Some(limit) = limit {
            set.held_to(limit);
        }
        // Its delivery cancelled before it was planned, the set asks no
        // agent (RFC §10.1 "Explicit delivery cancellation").
        if self.cascade.is_cut(Lineage::Call) {
            set.cut();
        }
        self.cascade.queue_set(Delivery {
            set: Box::new(set),
            then: Vec::new(),
        });
        self.stage.enter(Stage::Cascade);
        let decided = Ok(self.trigger.result.clone());
        self.publish(cx, &decided);
    }

    /// The record of each turn a discussion's depth limit stopped, once, with
    /// that limit (RFC §8.3, §19.7.5 rule 11).
    fn queue_stopped(&mut self, cx: &mut Cx<'_>, stopped: Vec<StoppedTurn>, context: &RoomContext) {
        for StoppedTurn {
            agent,
            answered,
            max_depth,
        } in stopped
        {
            // An agent detached while it stayed queued is still recorded.
            let binding = context.get_binding(&agent).cloned().unwrap_or_else(|| {
                ChannelBinding::at(
                    agent,
                    context.room.id.clone(),
                    ChannelType::AI,
                    ChannelCategory::Intelligence,
                    ChannelDirection::Bidirectional,
                    cx.env.now(),
                )
            });
            let id = cx.env.new_event_id();
            let record = chain_depth::not_asked_record(id, &answered, &binding, cx.env.now());
            self.cascade.queue(Job::Record(
                Box::new(RecordPass::new(record).within(Lineage::Call)),
                RecordNote::NotAsked {
                    agent: Box::new(binding),
                    trigger: answered,
                    limit: max_depth,
                },
            ));
        }
    }

    /// A rerun's set is over: what its agents answered is the call's, as a
    /// new message's (RFC §10.1 step 18), its deliveries never reported
    /// again.
    pub(super) fn settle_rerun(&mut self, outcome: &SetOutcome) {
        // The room closed while the set waited in its lane: refused as at
        // its start, before any agent ran (RFC §5.1).
        if outcome.refused {
            // The planning lock's failed release, noted, stays the error.
            let error = self.trigger.result.error.take();
            self.trigger.result = with_error(room_closed(), error);
            return;
        }
        self.trigger
            .result
            .response_metadata
            .clone_from(&outcome.response_metadata);
        self.trigger.note_error(outcome.error.clone());
    }
}

/// What a rerun refused by the room's status reads: blocked `room_closed`,
/// nothing written.
fn room_closed() -> InboundResult {
    refused("room_closed")
}

/// `result`, carrying `error`, a failed release of the room's lock.
fn with_error(
    mut result: InboundResult,
    error: Option<crate::model::ErrorRecord>,
) -> InboundResult {
    result.error = error;
    result
}

/// A rerun refused for `reason` before any agent ran: blocked, no event,
/// nothing written.
fn refused(reason: &str) -> InboundResult {
    InboundResult {
        blocked: true,
        reason: Some(reason.to_owned()),
        ..InboundResult::default()
    }
}

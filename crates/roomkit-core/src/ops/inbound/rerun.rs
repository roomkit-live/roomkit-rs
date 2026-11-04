//! The room's agents run again on a stored event, as on a new one (RFC
//! §11.7.6, §10.1 step 14): the Python implementation's
//! `_rerun_agents_on`, which a held message's release calls once it is
//! restored, and `regenerate_response` would share.
//!
//! The call takes the room's lock within its `process_timeout`, reads the
//! room as the lock finds it (its due timers applied) and the event again,
//! and refuses a room whose status refuses new events, as the inbound gate
//! does, before any agent runs (RFC §5.1). An event that is no longer one
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
use super::queues::{Delivery, Lineage};
use super::{InboundResult, Machine, Stage, cascade, report};
use crate::model::{ChannelCategory, EventId, Room, RoomContext, RoomEvent, RoomId, Visibility};
use crate::ops::protocol::{
    DiscussionRequest, DiscussionTurn, EffectId, HoldOutcome, HoldRequest, Outcome, StoreRequest,
};
use crate::ops::section::{Failure, Section, SectionWork};
use crate::ops::{Abort, Cx, Progress, RefusedOperation};
use crate::rules::chain_depth::ReentryBudget;
use crate::rules::discussion;
use crate::rules::permissions::{self, DeliveryPlan};
use crate::rules::verification::release;

/// What a rerun's section came to.
#[derive(Debug)]
pub(super) enum Reran {
    /// The event is no longer one the agents may answer: nothing runs.
    Nothing,
    /// The room's status refuses new events: refused before any agent
    /// runs (RFC §5.1).
    Refused,
    /// The event's delivery set, planned for the agents and placed in the
    /// room's lane.
    Planned(Box<Planned>),
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
        let turn = match outcome.into_discussion_turn() {
            Ok(turn) => turn,
            Err(error) => return Progress::Done(Err(Abort::Store(error))),
        };
        let Some(turn) = turn else {
            return Progress::Done(Ok(Reran::Nothing));
        };
        match plan_turn(*turn, context) {
            Some(planned) => self.placing(cx, planned),
            None => Progress::Done(Ok(Reran::Nothing)),
        }
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
            Asked::Event { event_id, .. } => {
                let request = StoreRequest::GetEvent {
                    event_id: event_id.clone(),
                };
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
    }
}

/// A discussion's turn on the event it answers: for its agent alone, the
/// event carrying the turn's mark, its answers bound by the event's
/// `response_visibility` (RFC §19.7.5 rules 9 and 11); `None` when the agent
/// cannot read the event.
fn plan_turn(turn: DiscussionTurn, context: Box<RoomContext>) -> Option<Planned> {
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

    /// Starts the turn the discussion holding `room_id` gives now, if any.
    pub(super) fn begin_turn(&mut self, cx: &mut Cx<'_>, room_id: RoomId) {
        self.begin_section(cx, room_id, Asked::Turn);
    }

    fn begin_section(&mut self, cx: &mut Cx<'_>, room_id: RoomId, asked: Asked) {
        self.call.room_id = Some(room_id.clone());
        let work = RerunWork {
            asked,
            context: None,
            waiting: Waiting::Nothing,
        };
        let mut section = Box::new(Section::new(room_id, work).waiting_within(self.call.bound));
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
        if outcome == Outcome::DeadlinePassed {
            // Only the wait for the lock carries the deadline: no lock is
            // held, nothing was written (RFC §13.6). Its `process_timeout`
            // names the operation, as Python's `regenerate` does.
            return self.expire_as(cx, None, Some(RefusedOperation::Regenerate));
        }
        let ended = match section.accept(cx, effect, outcome) {
            Progress::Waiting => return self.stage.enter(Stage::Rerunning(section)),
            Progress::Done(ended) => ended,
        };
        match ended.result {
            Ok(Reran::Nothing) => self.end(cx, Ok(InboundResult::default())),
            Ok(Reran::Refused) => self.end(cx, Ok(room_closed())),
            Ok(Reran::Planned(planned)) => {
                self.trigger.note_error(ended.release_error);
                self.queue_rerun(cx, *planned);
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
        } = planned;
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

    /// A rerun's set is over: what its agents answered is the call's, as a
    /// new message's (RFC §10.1 step 18), its deliveries never reported
    /// again.
    pub(super) fn settle_rerun(&mut self, outcome: &SetOutcome) {
        // The room closed while the set waited in its lane: refused as at
        // its start, before any agent ran (RFC §5.1).
        if outcome.refused {
            self.trigger.result = room_closed();
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
    InboundResult {
        blocked: true,
        reason: Some("room_closed".to_owned()),
        ..InboundResult::default()
    }
}

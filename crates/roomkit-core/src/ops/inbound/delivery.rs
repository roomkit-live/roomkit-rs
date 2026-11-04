//! The execution of one event's delivery set (RFC §10.2 step 3, §10.1 step
//! 14).
//!
//! The set waits for its place's turn in the room's delivery lane, taken
//! at its event's commit, and leaves its place once its last target
//! answered, so that one event's set completes before the next one's
//! begins (RFC §10.2). A place that could not be taken, or a turn that
//! could not be awaited, is logged and the event delivered all the same:
//! an event stored is delivered (RFC §13.6).
//!
//! Every target is called at once, so a driver may run them concurrently. A
//! transport target is told through `on_event()`, then delivered to when it
//! sends; an intelligence target is asked through `on_event()`, which is a
//! turn. Each target receives the event transcoded for its capabilities and
//! a context holding only what it may see (RFC §7.5 rule 8).
//!
//! A binding stays in its room when its channel is unregistered (gap G-048):
//! a transport target whose channel the kit does not hold is skipped, as one
//! served by another process may be, and an intelligence target is reported
//! `delivery_failed`, since the turn it was owed is never taken.

use std::collections::BTreeMap;

use super::delivery_record::DeliveryRecord;
use super::queues::Lineage;
use super::{reply_record, report, stamp};
use crate::model::{
    ChannelBinding, ChannelId, ChannelOutput, DeliveryError, DeliveryResult, DeliveryResultStatus,
    ErrorRecord, Metadata, Observation, ProviderResult, RoomContext, RoomEvent, Task, Visibility,
};
use crate::ops::protocol::{
    CHANNEL_NOT_REGISTERED, ChannelOutcome, ChannelRequest, EffectId, EntryId, HoldOutcome,
    HoldRequest, KIT_CLOSED, Notice, Outcome, Raised, SpanNote, StoreRequest, TURN_CUT,
};
use crate::ops::{Cx, Progress, RefusedOperation};
use crate::rules::chain_depth::ReentryBudget;
use crate::rules::hook_registry::HookTrigger;
use crate::rules::permissions::{self, DeliveryPlan};
use crate::rules::transcoding;
use crate::rules::{addresses, attachment};

/// The cancellation reason of a set whose index another process's lane
/// skipped before it ran (RFC §13.5), Python's `delivery_stale`.
const DELIVERY_STALE: &str = "delivery_stale";

/// Which event a delivery set broadcasts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SetKind {
    /// The call's own event, a message or a direct injection: its
    /// deliveries are reported (RFC §8.2) and written into its result.
    Root,
    /// The event of the entry the operation adopted (gap G-070): its
    /// deliveries are reported, and its caller, answered at the commit,
    /// reads them through its completion handle (RFC §10.1 step 18).
    Adopted(EntryId),
    /// A response that re-entered.
    Response,
    /// An event a hook injected. It asks no agent (RFC §9.4); its transport
    /// targets' tasks and observations are collected, as at any delivery,
    /// and the events they return do not re-enter (gap G-033).
    Injected,
    /// A stored event handed to the room's agents again (RFC §11.7.6): its
    /// answers are the call's, its deliveries not reported again.
    Rerun,
    /// An agent's greeting (RFC §19.1), the call's own or an adopted entry's:
    /// it is no message nor injection, so its deliveries are not reported and
    /// no `event_processed` follows it (RFC §8.2).
    Greeting,
}

/// The responses one target returned.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct Response {
    pub(super) responder: ChannelBinding,
    pub(super) events: Vec<RoomEvent>,
}

/// A turn whose response is a stream, to be read (RFC §6.1
/// `response_stream`).
#[derive(Clone, Debug, PartialEq)]
pub(super) struct StreamedTurn {
    pub(super) responder: ChannelBinding,
    /// The turn's `OnEvent` effect, which the driver keeps the stream by.
    pub(super) turn: EffectId,
}

/// What a finished delivery set produced.
#[derive(Clone, Debug, Default, PartialEq)]
pub(super) struct SetOutcome {
    /// The responses, in the order their channels answered.
    pub(super) responses: Vec<Response>,
    /// The turns whose response is a stream, in the order they answered.
    pub(super) streams: Vec<StreamedTurn>,
    /// The intelligence channels not asked at the chain-depth limit.
    pub(super) not_asked: Vec<ChannelBinding>,
    pub(super) tasks: Vec<Task>,
    pub(super) observations: Vec<Observation>,
    /// The transports delivered to, with their results.
    pub(super) delivery_results: BTreeMap<ChannelId, DeliveryResult>,
    /// The transports delivered to without failure, in plan order.
    pub(super) delivered: Vec<ChannelId>,
    /// The targets that failed, with what failed.
    pub(super) failures: BTreeMap<ChannelId, String>,
    /// The agents whose channel was not registered when their turn came:
    /// the turn they were owed was never taken (gap G-048).
    pub(super) unreached: Vec<ChannelId>,
    /// The merged response metadata of the intelligence targets (RFC §10.1).
    pub(super) response_metadata: Metadata,
    /// The room metadata updates the targets returned, merged in the order
    /// they answered, without the keys the implementation reserves.
    pub(super) metadata_updates: Metadata,
    /// The first error an intelligence target's turn met.
    pub(super) error: Option<ErrorRecord>,
    /// Why the set delivered nothing: [`KIT_CLOSED`] when the room's lane
    /// took no place for it, the kit closing.
    pub(super) cancelled: Option<&'static str>,
    /// A rerun's set the room refused when its turn came, its status
    /// refusing new events: it asked no agent (RFC §5.1, §11.7.6).
    pub(super) refused: bool,
}

#[derive(Debug)]
struct Target {
    binding: ChannelBinding,
    role: Role,
    event: RoomEvent,
    context: Box<RoomContext>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Role {
    Transport { delivers: bool },
    Agent,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Call {
    OnEvent,
    Deliver,
}

/// Where a set stands in the room's delivery lane (RFC §10.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Place {
    /// The event took a place: the set waits for its turn first.
    Taken,
    /// The room's lane took no place, the kit closing: the set delivers
    /// nothing, its cascade cancelled [`KIT_CLOSED`].
    Closed,
    /// The event holds no place, its `EnterLane` having failed: the set
    /// runs at once.
    Missing,
    /// The set waits for its place's turn.
    Awaiting(EffectId),
    /// Its turn came: it delivers, then leaves its place.
    Turn,
    /// Its turn came, a rerun's set: it reads its room first, whose status
    /// may have come to refuse new events while it waited (RFC §5.1).
    Checking(EffectId),
    /// It leaves its place.
    Leaving(EffectId),
    /// It left its place.
    Left,
}

/// What became of an event's place in its room's delivery lane, taken
/// under the lock at its commit (RFC §10.1 step 12).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Placement {
    /// The place is taken: the set waits for its turn.
    Taken,
    /// Taking it failed: the set delivers at once, out of the lane's order
    /// (RFC §13.6: an event stored is delivered).
    Missing,
    /// The lane takes no more place, the kit closing (RFC §10.1 step 18):
    /// the event is not delivered, its cascade cancelled [`KIT_CLOSED`].
    Closed,
}

/// The delivery set of one committed event.
#[derive(Debug)]
pub(super) struct DeliverySet {
    kind: SetKind,
    event: RoomEvent,
    context: Box<RoomContext>,
    targets: Vec<Target>,
    /// The agents the room's discussion asks later (RFC §19.7.5).
    deferred: Vec<ChannelId>,
    /// A discussion's own chain-depth limit, for its turn.
    depth_limit: Option<u32>,
    place: Place,
    /// The reentry budget of the cascade that delivers, which the calls its
    /// targets make into a room draw on (gap G-070).
    budget: ReentryBudget,
    calls: BTreeMap<EffectId, (usize, Call)>,
    outcome: SetOutcome,
    /// Whose cascade the set belongs to.
    lineage: Lineage,
    /// Whether its turns are cut: it asks no agent, and the turns it asked
    /// are given up on (RFC §10.1 "Explicit delivery cancellation").
    turns_cut: bool,
    /// The record of its deliveries under way, once they are over and any
    /// failed (RFC §14.3).
    record: Option<DeliveryRecord>,
    /// Whom the event reaches, asked or not, for the set's span.
    reached: usize,
    /// The targets of an injected set that failed, which its outcome does
    /// not report, for the set's span.
    unreported: usize,
}

impl DeliverySet {
    /// The set of `event`, planned as `plan`, with the room's whole context;
    /// `placement`, what became of the event's place in the room's lane. The calls
    /// its targets make into a room draw on `budget`, the cascade's.
    pub(super) fn new(
        kind: SetKind,
        event: RoomEvent,
        plan: DeliveryPlan,
        context: Box<RoomContext>,
        placement: Placement,
        budget: ReentryBudget,
        lineage: Lineage,
    ) -> Self {
        let targets = targets_of(&plan, &event, &context);
        let deferred = plan.deferred.iter().map(|d| d.channel_id.clone()).collect();
        let reached = plan.reached;
        let outcome = SetOutcome {
            not_asked: plan.not_asked,
            ..SetOutcome::default()
        };
        Self {
            kind,
            event,
            context,
            targets,
            deferred,
            depth_limit: None,
            place: match placement {
                Placement::Taken => Place::Taken,
                Placement::Missing => Place::Missing,
                Placement::Closed => Place::Closed,
            },
            budget,
            calls: BTreeMap::new(),
            outcome,
            lineage,
            turns_cut: false,
            record: None,
            reached,
            unreported: 0,
        }
    }

    /// Whose cascade the set belongs to.
    pub(super) fn lineage(&self) -> Lineage {
        self.lineage
    }

    /// Whether the set asks `channel_id` for a turn: an agent its plan
    /// holds.
    pub(super) fn asks(&self, channel_id: &ChannelId) -> bool {
        self.targets
            .iter()
            .any(|target| target.role == Role::Agent && &target.binding.channel_id == channel_id)
    }

    /// Whether `channel_id` is an agent the room's discussion asks later, one
    /// turn at a time (RFC §19.7.5): no agent unavailable.
    pub(super) fn defers(&self, channel_id: &ChannelId) -> bool {
        self.deferred.contains(channel_id)
    }

    /// The chain-depth limit the set's agents are held to: a discussion's
    /// own for its turn, else none, the kit's.
    pub(super) fn depth_limit(&self) -> Option<u32> {
        self.depth_limit
    }

    /// Holds the set's agents to `limit`, a discussion's for its turn.
    pub(super) fn held_to(&mut self, limit: u32) {
        self.depth_limit = Some(limit);
    }

    /// Cuts the set's turns: it asks no agent from now on, and the turns it
    /// asked are no longer waited for; returns them. A transport target is
    /// delivered to all the same.
    pub(super) fn cut(&mut self) -> Vec<EffectId> {
        self.turns_cut = true;
        let turns: Vec<EffectId> = self
            .calls
            .iter()
            .filter(|(_, (index, call))| {
                *call == Call::OnEvent
                    && self
                        .targets
                        .get(*index)
                        .is_some_and(|target| target.role == Role::Agent)
            })
            .map(|(effect, _)| *effect)
            .collect();
        turns
    }

    /// Whether the set waits for `effect`.
    pub(super) fn awaits(&self, effect: EffectId) -> bool {
        if let Some(record) = &self.record {
            return record.awaits(effect);
        }
        match self.place {
            Place::Awaiting(awaited) | Place::Leaving(awaited) | Place::Checking(awaited)
                if awaited == effect =>
            {
                true
            }
            Place::Taken
            | Place::Closed
            | Place::Missing
            | Place::Awaiting(_)
            | Place::Turn
            | Place::Checking(_)
            | Place::Leaving(_)
            | Place::Left => self.calls.contains_key(&effect),
        }
    }

    /// The event the set broadcasts.
    pub(super) fn event(&self) -> &RoomEvent {
        &self.event
    }

    /// The room's whole context at the commit.
    pub(super) fn context(&self) -> &RoomContext {
        &self.context
    }

    /// The kind of event the set broadcasts.
    /// The adopted entry whose caller's completion handle waits for this set,
    /// when one does (RFC §10.1 step 18).
    pub(super) fn entry_waiting(&self) -> Option<EntryId> {
        match (self.kind, self.lineage) {
            (SetKind::Adopted(entry), _) | (SetKind::Greeting, Lineage::Entry(entry)) => {
                Some(entry)
            }
            _ => None,
        }
    }

    pub(super) fn kind(&self) -> SetKind {
        self.kind
    }

    /// Where the answers to its event may go: the event's
    /// `response_visibility` as committed (RFC §5.2, §8.5, gap G-073);
    /// `None` for anywhere.
    pub(super) fn answers_within(&self) -> Option<&Visibility> {
        self.event.response_visibility.as_ref()
    }

    /// Waits for the set's turn in the room's lane, with no deadline: what
    /// waits is committed already (RFC §13.6). A set with no place delivers
    /// at once.
    pub(super) fn start(&mut self, cx: &mut Cx<'_>) -> Progress<SetOutcome> {
        match self.place {
            Place::Taken => {}
            Place::Closed => {
                self.outcome.cancelled = Some(KIT_CLOSED);
                return Progress::Done(std::mem::take(&mut self.outcome));
            }
            Place::Missing
            | Place::Awaiting(_)
            | Place::Turn
            | Place::Checking(_)
            | Place::Leaving(_)
            | Place::Left => {
                return self.deliver(cx);
            }
        }
        let request = HoldRequest::AwaitLane {
            room_id: self.event.room_id.clone(),
            event_id: self.event.id.clone(),
        };
        self.place = Place::Awaiting(cx.io.issue(request));
        Progress::Waiting
    }

    /// Calls every registered target's `on_event()` at once; a target
    /// whose channel is not registered is not called (gap G-048).
    fn deliver(&mut self, cx: &mut Cx<'_>) -> Progress<SetOutcome> {
        for index in 0..self.targets.len() {
            let Some(target) = self.targets.get(index) else {
                continue;
            };
            if cx.config.channel(&target.binding.channel_id).is_none() {
                self.not_registered(index);
                continue;
            }
            if self.turns_cut && target.role == Role::Agent {
                continue;
            }
            let request = ChannelRequest::OnEvent {
                binding: target.binding.clone(),
                event: target.event.clone(),
                context: target.context.clone(),
            };
            let effect = cx.io.issue_drawing_on(request, &self.budget);
            self.calls.insert(effect, (index, Call::OnEvent));
        }
        self.progress(cx)
    }

    /// Takes the driver's answer to one of the set's requests.
    pub(super) fn accept(
        &mut self,
        cx: &mut Cx<'_>,
        effect: EffectId,
        outcome: Outcome,
    ) -> Progress<SetOutcome> {
        if self.record.is_some() {
            return self.on_record(cx, outcome);
        }
        match self.place {
            Place::Awaiting(awaited) if awaited == effect => return self.on_turn(cx, outcome),
            Place::Checking(awaited) if awaited == effect => return self.on_checked(cx, outcome),
            Place::Leaving(awaited) if awaited == effect => return self.on_left(cx, outcome),
            Place::Taken
            | Place::Closed
            | Place::Missing
            | Place::Awaiting(_)
            | Place::Turn
            | Place::Checking(_)
            | Place::Leaving(_)
            | Place::Left => {}
        }
        let Some((index, call)) = self.calls.remove(&effect) else {
            debug_assert!(
                false,
                "delivery set received {effect:?}, which it did not issue"
            );
            return self.progress(cx);
        };
        match (call, outcome) {
            (Call::OnEvent, Outcome::Channel(ChannelOutcome::ProviderDelivery(result))) => {
                self.on_event_refused(cx, index, *result);
            }
            (Call::OnEvent, Outcome::Channel(ChannelOutcome::Raised(raised))) => {
                self.on_event_raised(cx, index, *raised);
            }
            (Call::Deliver, Outcome::Channel(ChannelOutcome::ProviderDelivery(result))) => {
                self.on_delivery_refused(index, *result);
            }
            (Call::OnEvent, outcome) => match outcome.into_turn_output() {
                Ok((output, streamed)) => {
                    self.on_output(cx, index, *output, streamed.then_some(effect));
                }
                Err(error) => self.on_event_failed(cx, index, &error),
            },
            (Call::Deliver, outcome) => match outcome.into_output() {
                Ok(output) => self.on_delivered(index, *output),
                Err(error) => self.on_delivery_failed(index, &error),
            },
        }
        self.progress(cx)
    }

    /// The set's turn came; a turn that could not be awaited is logged, and
    /// the event delivered all the same (RFC §13.6). A lane that takes no
    /// more turn, the kit closing, drops the set before it delivers: it
    /// delivers nothing, its cascade cancelled [`KIT_CLOSED`], and leaves
    /// its place (RFC §12.10.4); so does a set whose index another
    /// process's lane skipped, its cascade cancelled [`DELIVERY_STALE`]
    /// (RFC §13.5).
    fn on_turn(&mut self, cx: &mut Cx<'_>, outcome: Outcome) -> Progress<SetOutcome> {
        if outcome == Outcome::Hold(HoldOutcome::LaneClosed) {
            self.outcome.cancelled = Some(KIT_CLOSED);
            self.place = Place::Turn;
            return self.progress(cx);
        }
        if outcome == Outcome::Hold(HoldOutcome::LaneSkipped) {
            self.outcome.cancelled = Some(DELIVERY_STALE);
            self.place = Place::Turn;
            return self.progress(cx);
        }
        if let Err(error) = outcome.into_lane_turn() {
            tracing::warn!(
                target: "roomkit::core::delivery",
                room_id = %self.event.room_id,
                event_id = %self.event.id,
                error_type = %error.error_type,
                message = %error.message,
                "waiting for the set's turn in the room's delivery lane failed; the event is \
                 delivered all the same"
            );
        }
        if self.kind == SetKind::Rerun {
            return self.check_room(cx);
        }
        self.place = Place::Turn;
        self.deliver(cx)
    }

    /// Reads the room of a rerun's set, its turn come: the room may have
    /// closed while the set waited in its lane, which refuses the rerun as
    /// at its start (RFC §5.1, §13.6).
    fn check_room(&mut self, cx: &mut Cx<'_>) -> Progress<SetOutcome> {
        let request = StoreRequest::LoadContext {
            room_id: self.event.room_id.clone(),
            recent_events: cx.config.recent_events(),
        };
        self.place = Place::Checking(cx.io.issue(request));
        Progress::Waiting
    }

    /// The room of a rerun's set, read once its turn came: a status that
    /// refuses new events, or a room gone, refuses it before any agent runs,
    /// `room_refused_event` naming the `regenerate` operation, with no
    /// status for a room gone; a read that failed is recorded as the set's
    /// error, and no agent runs, as Python's lane records a plan that
    /// raised (`_refuse_closed_rerun`, `_execute`).
    fn on_checked(&mut self, cx: &mut Cx<'_>, outcome: Outcome) -> Progress<SetOutcome> {
        self.place = Place::Turn;
        let status = match outcome.into_context() {
            Ok(context) => context.map(|context| context.room.status),
            Err(error) => {
                tracing::warn!(
                    target: "roomkit::core::delivery",
                    room_id = %self.event.room_id,
                    error_type = %error.error_type,
                    message = %error.message,
                    "reading the room of a rerun once its turn came failed; no agent runs"
                );
                self.outcome.error.get_or_insert(error);
                return self.progress(cx);
            }
        };
        if status.is_some_and(crate::model::RoomStatus::accepts_events) {
            return self.deliver(cx);
        }
        report::room_refused_as(cx, &self.event, RefusedOperation::Regenerate, status);
        self.outcome.refused = true;
        self.progress(cx)
    }

    /// The set left its place; a place that could not be left changes
    /// nothing the set did.
    fn on_left(&mut self, cx: &mut Cx<'_>, outcome: Outcome) -> Progress<SetOutcome> {
        if let Err(error) = outcome.into_left() {
            tracing::warn!(
                target: "roomkit::core::delivery",
                room_id = %self.event.room_id,
                event_id = %self.event.id,
                error_type = %error.error_type,
                message = %error.message,
                "leaving the set's place in the room's delivery lane failed; what the set \
                 delivered stands"
            );
        }
        self.place = Place::Left;
        self.over(cx)
    }

    /// Hands the record of its deliveries `outcome`: the set is over once
    /// the record ended.
    fn on_record(&mut self, cx: &mut Cx<'_>, outcome: Outcome) -> Progress<SetOutcome> {
        let progress = self
            .record
            .as_mut()
            .map_or(Progress::Done(()), |record| record.accept(cx, outcome));
        match progress {
            Progress::Waiting => Progress::Waiting,
            Progress::Done(()) => {
                self.record = None;
                Progress::Done(std::mem::take(&mut self.outcome))
            }
        }
    }

    /// Off its place in the room's lane, the set reports its deliveries when
    /// they are a message's or a sent event's, the call's own or an entry's,
    /// then records them on its event when any failed (RFC §5.13, §8.2,
    /// §14.3), as Python's `_report_root_pass`, before its `AFTER_BROADCAST`
    /// hooks; a set the lane dropped does neither.
    fn over(&mut self, cx: &mut Cx<'_>) -> Progress<SetOutcome> {
        let root = matches!(self.kind, SetKind::Root | SetKind::Adopted(_));
        if root && self.outcome.cancelled.is_none() {
            report::root_deliveries(cx, &self.event, &self.outcome);
            self.record = DeliveryRecord::start(cx, &self.event, &self.outcome.delivery_results);
            if self.record.is_some() {
                return Progress::Waiting;
            }
        }
        Progress::Done(std::mem::take(&mut self.outcome))
    }

    /// Once every target answered, the set leaves its place in the room's
    /// lane, if it holds one, and is over.
    fn progress(&mut self, cx: &mut Cx<'_>) -> Progress<SetOutcome> {
        if !self.calls.is_empty() {
            return Progress::Waiting;
        }
        self.tell_its_span(cx);
        if self.place != Place::Turn {
            return self.over(cx);
        }
        let request = HoldRequest::LeaveLane {
            room_id: self.event.room_id.clone(),
            event_id: self.event.id.clone(),
        };
        self.place = Place::Leaving(cx.io.issue(request));
        Progress::Waiting
    }

    /// Tells the set's span what its targets came to, unless the room's lane
    /// or the room refused the set, which then delivered nothing.
    fn tell_its_span(&self, cx: &mut Cx<'_>) {
        if self.outcome.cancelled.is_some() || self.outcome.refused {
            return;
        }
        cx.io.notify(Notice::Span(SpanNote::SetOver {
            event_id: self.event.id.clone(),
            targets: self.reached,
            delivered: self.outcome.delivered.len(),
            failed: self.outcome.failures.len() + self.unreported,
            streams: self.outcome.streams.len(),
            own: self.lineage == Lineage::Call,
        }));
    }

    /// A target's `on_event()` returned (RFC §10.2 step 3c); an agent's
    /// turn whose response is a stream, `streamed`, the effect it answered,
    /// has its stream read once the set is over.
    fn on_output(
        &mut self,
        cx: &mut Cx<'_>,
        index: usize,
        output: ChannelOutput,
        streamed: Option<EffectId>,
    ) {
        let Some(target) = self.targets.get(index) else {
            return;
        };
        let responder = target.binding.clone();
        let is_agent = target.role == Role::Agent;
        self.collect_metadata(&responder.channel_id, output.metadata_updates);
        if self.kind == SetKind::Injected {
            self.outcome.tasks.extend(output.tasks);
            self.outcome.observations.extend(output.observations);
            self.deliver_if_transport(cx, index);
            return;
        }
        self.outcome.tasks.extend(output.tasks);
        self.outcome.observations.extend(output.observations);
        // Every target's turn that writes a record replies, a transport's
        // too; a stream's stays live until it is read, and is merged there:
        // copying it here would freeze what its tools write later (Python's
        // `_settle_buffered_replies`, RMK-1248).
        if streamed.is_none() {
            reply_record::record_reply(
                &mut self.outcome.response_metadata,
                &responder.channel_id,
                &output.response_metadata.to_metadata(),
                &output.response_events,
            );
        }
        if let Some(mut error) = output.error.filter(|_| is_agent) {
            // A failure its own turn reported already (Python's
            // `mark_reported`): handed on to the caller, never reported
            // again (RFC §19.7.4).
            let reported = error.take_reported();
            let failed = DeliveryResult::raised(responder.channel_id.clone(), &error);
            if reported {
                self.handed_on(index, &error, failed);
            } else {
                self.turn_failed(cx, index, &error, failed);
            }
        }
        match streamed.filter(|_| is_agent) {
            Some(turn) => self.outcome.streams.push(StreamedTurn { responder, turn }),
            None if output.responded && !output.response_events.is_empty() => {
                self.outcome.responses.push(Response {
                    responder,
                    events: output.response_events,
                });
            }
            None => {}
        }
        self.deliver_if_transport(cx, index);
    }

    /// A transport that sends is delivered to once told, when its binding
    /// has a recipient (RFC §10.2 step 3d).
    fn deliver_if_transport(&mut self, cx: &mut Cx<'_>, index: usize) {
        let Some(target) = self.targets.get(index) else {
            return;
        };
        if target.role != (Role::Transport { delivers: true }) {
            return;
        }
        if let Some(key) = missing_recipient(cx, &target.binding) {
            return self.refuse_without_recipient(index, &key);
        }
        let request = ChannelRequest::Deliver {
            binding: target.binding.clone(),
            event: target.event.clone(),
            context: target.context.clone(),
        };
        let effect = cx.io.issue_drawing_on(request, &self.budget);
        self.calls.insert(effect, (index, Call::Deliver));
    }

    /// A target's `on_event()` raised: a turn that failed, or a transport
    /// that is not delivered to; or the driver holds the channel no more.
    fn on_event_failed(&mut self, cx: &mut Cx<'_>, index: usize, error: &ErrorRecord) {
        if error.error_type == CHANNEL_NOT_REGISTERED {
            return self.not_registered(index);
        }
        // A turn its cascade cut is no failure: nothing of it is reported.
        if error.error_type == TURN_CUT {
            return;
        }
        let Some(target) = self.targets.get(index) else {
            return;
        };
        if self.kind == SetKind::Injected {
            self.unreported += 1;
            return;
        }
        let failed = DeliveryResult::raised(target.binding.channel_id.clone(), error);
        if target.role == Role::Agent {
            self.turn_failed(cx, index, error, failed);
        } else {
            self.report_failure(index, failed);
        }
    }

    /// A target's `on_event()` raised Python's `ProviderDeliveryError`, the
    /// provider's refusal `result`: a failure as any raised, its delivery
    /// result reading what the refusal declares.
    fn on_event_refused(&mut self, cx: &mut Cx<'_>, index: usize, result: ProviderResult) {
        let Some(target) = self.targets.get(index) else {
            return;
        };
        if self.kind == SetKind::Injected {
            self.unreported += 1;
            return;
        }
        let failed = DeliveryResult::refused(target.binding.channel_id.clone(), result);
        if target.role == Role::Agent {
            let message = failed.error.as_ref().map(|error| error.message.clone());
            let error = ErrorRecord::new("ProviderDeliveryError", message.unwrap_or_default());
            self.turn_failed(cx, index, &error, failed);
        } else {
            self.report_failure(index, failed);
        }
    }

    /// A target's `on_event()` raised an error declaring its delivery
    /// error, its code and retryability (RMK-1085): a failure as any
    /// raised, its delivery result reading what the error declares, with no
    /// provider message id or answer, as Python reads an agent's error
    /// rebuilt across its turn backend.
    fn on_event_raised(&mut self, cx: &mut Cx<'_>, index: usize, raised: Raised) {
        let Some(target) = self.targets.get(index) else {
            return;
        };
        if self.kind == SetKind::Injected {
            self.unreported += 1;
            return;
        }
        let failed = DeliveryResult {
            error: Some(raised.delivery),
            ..DeliveryResult::raised(target.binding.channel_id.clone(), &raised.error)
        };
        if target.role == Role::Agent {
            self.turn_failed(cx, index, &raised.error, failed);
        } else {
            self.report_failure(index, failed);
        }
    }

    /// A transport's `deliver()` raised Python's `ProviderDeliveryError`,
    /// the provider's refusal `result`.
    fn on_delivery_refused(&mut self, index: usize, result: ProviderResult) {
        let Some(target) = self.targets.get(index) else {
            return;
        };
        let failed = DeliveryResult::refused(target.binding.channel_id.clone(), result);
        self.report_failure(index, failed);
    }

    /// An intelligence target's turn failed (RFC §9.2 `ON_ERROR`, category
    /// `generation`): the event stays delivered, the error is reported, and
    /// so is the target's delivery, `failed`, as the Python implementation
    /// reports a target its broadcast's errors name. A turn whose failure
    /// comes in its stream, as an AI channel's, is no failed delivery.
    fn turn_failed(
        &mut self,
        cx: &mut Cx<'_>,
        index: usize,
        error: &ErrorRecord,
        failed: DeliveryResult,
    ) {
        let Some(target) = self.targets.get(index) else {
            return;
        };
        let id = cx.env.new_event_id();
        let now = cx.env.now();
        let within = self.event.response_visibility.as_ref();
        let payload = stamp::error_event(
            id,
            &target.binding,
            &self.event,
            within,
            error,
            "generation",
            now,
        );
        cx.fire_observers(HookTrigger::OnError, &payload, &target.context);
        if self.outcome.error.is_none() {
            self.outcome.error = Some(error.clone());
        }
        self.report_failure(index, failed);
    }

    /// A target's failure its own turn reported to `ON_ERROR` already: the
    /// set's error and its delivery results read it, no observer is told
    /// again.
    fn handed_on(&mut self, index: usize, error: &ErrorRecord, failed: DeliveryResult) {
        if self.outcome.error.is_none() {
            self.outcome.error = Some(error.clone());
        }
        self.report_failure(index, failed);
    }

    /// Reports a target's failed delivery, `failed`, among the set's
    /// delivery results and its failures, its error's message, as the
    /// Python implementation's `_delivery_results` reports each target its
    /// broadcast's errors name (RFC §5.13).
    fn report_failure(&mut self, index: usize, failed: DeliveryResult) {
        let message = failed
            .error
            .as_ref()
            .map_or_else(String::new, |error| error.message.clone());
        self.outcome
            .delivery_results
            .insert(failed.channel_id.clone(), failed);
        self.fail(index, message);
    }

    /// A transport's `deliver()` returned: sent, unless its provider refused.
    /// Of its output the room keeps the metadata updates alone: a
    /// transport's tasks and observations are its `on_event()`'s, as
    /// Python's router merges them (RFC §10.1 step 15).
    fn on_delivered(&mut self, index: usize, output: ChannelOutput) {
        let Some(target) = self.targets.get(index) else {
            return;
        };
        let channel_id = target.binding.channel_id.clone();
        self.collect_metadata(&channel_id, output.metadata_updates);
        match output.provider_result {
            // A provider's refusal is the Python implementation's
            // `ProviderDeliveryError`, which its transport raises.
            Some(result) if !result.success => {
                self.report_failure(index, DeliveryResult::refused(channel_id, result));
            }
            provider_result => {
                let sent = DeliveryResult {
                    provider_message_id: provider_result
                        .as_ref()
                        .and_then(|result| result.provider_message_id.clone()),
                    provider_result,
                    ..DeliveryResult::new(channel_id.clone(), DeliveryResultStatus::Sent)
                };
                self.outcome
                    .delivery_results
                    .insert(channel_id.clone(), sent);
                self.outcome.delivered.push(channel_id);
            }
        }
    }

    /// A transport whose binding has no recipient under `key` is refused
    /// before any send: `no_recipient`, not retried (RFC §10.2 step 3d).
    fn refuse_without_recipient(&mut self, index: usize, key: &str) {
        let Some(target) = self.targets.get(index) else {
            return;
        };
        let channel_id = target.binding.channel_id.clone();
        tracing::warn!(
            target: "roomkit::core::delivery",
            room_id = %self.event.room_id,
            event_id = %self.event.id,
            channel_id = %channel_id,
            recipient_key = %key,
            "the binding has no recipient; the delivery is refused before any send"
        );
        // Python's `NoRecipientError(channel_id, recipient_key)`, a
        // `ProviderDeliveryError` of the refusal it builds.
        let mut metadata = Metadata::new();
        metadata.insert("code".to_owned(), NO_RECIPIENT.into());
        metadata.insert("retryable".to_owned(), false.into());
        metadata.insert("channel_id".to_owned(), channel_id.as_str().into());
        metadata.insert("recipient_key".to_owned(), key.into());
        let refusal = ProviderResult {
            error: Some(NO_RECIPIENT.to_owned()),
            metadata,
            ..ProviderResult::new(false)
        };
        self.report_failure(index, DeliveryResult::refused(channel_id, refusal));
    }

    /// A transport's `deliver()` raised; or the driver holds the channel no
    /// more.
    fn on_delivery_failed(&mut self, index: usize, error: &ErrorRecord) {
        if error.error_type == CHANNEL_NOT_REGISTERED {
            return self.not_registered(index);
        }
        let Some(target) = self.targets.get(index) else {
            return;
        };
        let failed = DeliveryResult::raised(target.binding.channel_id.clone(), error);
        self.report_failure(index, failed);
    }

    /// Keeps a target's room metadata updates, a side effect collected
    /// whatever the target's permissions (RFC §7.5 rule 3). A key only the
    /// framework writes, of the namespace the implementation reserves or of
    /// the room's private state, is refused (RFC §5.1, §10.6.5, §14.1, gap
    /// G-049).
    fn collect_metadata(&mut self, channel_id: &ChannelId, updates: Metadata) {
        for (key, value) in updates {
            if attachment::the_framework_s(&key) {
                tracing::warn!(
                    target: "roomkit::core::inbound",
                    room_id = %self.event.room_id,
                    channel_id = %channel_id,
                    key = %key,
                    "a channel's room metadata update names a key only the framework writes; \
                     the key is refused"
                );
                continue;
            }
            self.outcome.metadata_updates.insert(key, value);
        }
    }

    /// A target whose channel is not registered, its binding left in the
    /// room (gap G-048): a transport is skipped, and counts as no target;
    /// an intelligence channel is reported `delivery_failed`, and its
    /// delivery result failed `channel_not_registered`, not retryable, as
    /// the Python implementation's `_delivery_error` reports it.
    fn not_registered(&mut self, index: usize) {
        let Some(target) = self.targets.get(index) else {
            return;
        };
        let channel_id = &target.binding.channel_id;
        match target.role {
            Role::Transport { .. } => {
                tracing::debug!(
                    target: "roomkit::core::delivery",
                    room_id = %self.event.room_id,
                    event_id = %self.event.id,
                    channel_id = %channel_id,
                    "the transport is not registered; it is skipped"
                );
            }
            Role::Agent => {
                tracing::warn!(
                    target: "roomkit::core::delivery",
                    room_id = %self.event.room_id,
                    event_id = %self.event.id,
                    channel_id = %channel_id,
                    "the intelligence channel is not registered; its turn is not taken"
                );
                let channel_id = channel_id.clone();
                self.outcome.delivery_results.insert(
                    channel_id.clone(),
                    DeliveryResult {
                        channel_id: channel_id.clone(),
                        status: DeliveryResultStatus::Failed,
                        provider_message_id: None,
                        error: Some(DeliveryError {
                            code: CHANNEL_NOT_REGISTERED.to_owned(),
                            message: CHANNEL_NOT_REGISTERED.to_owned(),
                            retryable: false,
                        }),
                        retry_after: None,
                        provider_result: None,
                    },
                );
                self.outcome.unreached.push(channel_id);
                self.fail(index, CHANNEL_NOT_REGISTERED.to_owned());
            }
        }
    }

    fn fail(&mut self, index: usize, message: String) {
        if let Some(target) = self.targets.get(index) {
            self.outcome
                .failures
                .insert(target.binding.channel_id.clone(), message);
        }
    }
}

/// The error of a delivery refused for want of a recipient (RFC §10.2 step
/// 3d).
const NO_RECIPIENT: &str = "no_recipient";

/// The recipient key the channel of `binding` delivers to, when the binding
/// holds no recipient under it (RFC §10.2 step 3d, gap G-051). A channel that
/// delivers to a destination of its own declares no key; one unregistered
/// since the set started is answered as not registered (gap G-048).
fn missing_recipient(cx: &Cx<'_>, binding: &ChannelBinding) -> Option<String> {
    let key = cx
        .config
        .channel(&binding.channel_id)?
        .recipient_key
        .as_deref()?;
    (!addresses::has_recipient(binding, key)).then(|| key.to_owned())
}

/// The set's targets, transports first then agents, each with the event
/// transcoded for it and the context it may see. A standalone
/// instruction's targets are handed it too: reading none of the room is
/// the turn's duty (RFC §10.1.1 rule 7, `instructions::turn_context`).
fn targets_of(plan: &DeliveryPlan, event: &RoomEvent, context: &RoomContext) -> Vec<Target> {
    let transports = plan.transports.iter().map(|target| {
        let role = Role::Transport {
            delivers: target.delivers,
        };
        (&target.binding, role)
    });
    let agents = plan.agents.iter().map(|binding| (binding, Role::Agent));
    transports
        .chain(agents)
        .map(|(binding, role)| Target {
            event: stamp::for_target(
                event,
                transcoding::transcode(&event.content, &binding.capabilities),
            ),
            context: Box::new(permissions::context_for(binding, context)),
            binding: binding.clone(),
            role,
        })
        .collect()
}

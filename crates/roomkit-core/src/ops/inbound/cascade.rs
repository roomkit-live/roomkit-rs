//! What each finished pass and set adds to the cascade (RFC §10.1 steps 10
//! to 17): a pass's delivery set and the events its hooks injected, a
//! delivery set's responses and the records of the channels it could not
//! ask, the side effects every one collected, and what the call's result
//! reports.
//!
//! The queues carry every cascade of the operation, the call's and its
//! adopted entries' (gap G-070). An event takes its place in the room's
//! delivery lane at its commit, and commits follow one another under the
//! room lock, so sets keep index order: one event's set completes before
//! the next one's begins (RFC §10.2).

use super::delivery::{DeliverySet, Response, SetKind, SetOutcome, StreamedTurn};
use super::holds::AfterTicket;
use super::job::{Finished, Job, RecordNote, RowNote};
use super::pass::{CommitPass, PassOrigin, PassReport, PassResult};
use super::queues::{Delivery, Lineage};
use super::record::{RecordPass, RecordReport, RecordResult};
use super::stream::{self, StreamJob};
use super::{InboundError, InboundResult, Machine, Stage, instructions, report, stamp};
use crate::model::{
    ChannelCategory, ChannelId, ErrorRecord, EventStatus, InjectedEvent, RoomEvent, RoomId,
};
use crate::ops::protocol::EntryId;
use crate::ops::{Abort, Cx};
use crate::rules::chain_depth;
use crate::rules::permissions::DeliveryPlan;

/// What the call's trigger decided, and what the call holds for it.
#[derive(Debug, Default)]
pub(super) struct Trigger {
    /// The room whose admission ticket the call holds, from its off-lock
    /// check to its trigger's pass's end (RFC §9.5.1).
    pub(super) ticket: Option<RoomId>,
    /// Whether the trigger is decided: committed or, for an instruction,
    /// which is never committed, delivered or blocked by its pass. A failure
    /// after it no longer voids the result (RFC §13.6, gap G-065).
    pub(super) decided: bool,
    /// The call's result: the trigger's event, its deliveries, and the
    /// first error met once it is decided.
    pub(super) result: InboundResult,
}

impl Trigger {
    /// Writes what the trigger's pass or record `settled` into the result
    /// (RFC §10.1): its event, whether it was blocked and why; `decided`
    /// once it is.
    fn settle(&mut self, settled: InboundResult, decided: bool) {
        self.decided = decided;
        self.result.event = settled.event;
        self.result.duplicate = settled.duplicate;
        self.result.unavailable_targets = settled.unavailable_targets;
        self.result.blocked = settled.blocked;
        self.result.reason = settled.reason;
    }

    /// Keeps the first error met once the trigger was decided: the result
    /// reports one (RFC §13.6).
    pub(super) fn note_error(&mut self, error: Option<ErrorRecord>) {
        if self.result.error.is_none() {
            self.result.error = error;
        }
    }
}

impl Machine {
    /// Settles a finished job. The trigger's pass releases the call's ticket
    /// (RFC §9.5.1): a pass that is over publishes its decision and queues
    /// its delivery set first, since the release waits while an entry the
    /// call adopted works under the room's lock, and the set's place comes
    /// before that entry's; a pass that failed is settled once the ticket
    /// is released.
    pub(super) fn settle(&mut self, cx: &mut Cx<'_>, finished: Finished) {
        if self.trigger.ticket.is_some() {
            match finished {
                Finished::Pass(Ok(report), PassOrigin::Trigger) => {
                    self.on_pass(cx, report, &PassOrigin::Trigger);
                    return self.release_ticket(cx, AfterTicket::Resume);
                }
                finished @ Finished::Pass(Err(_), PassOrigin::Trigger) => {
                    return self.release_ticket(cx, AfterTicket::Settle(Box::new(finished)));
                }
                Finished::Pass(..) | Finished::Record(..) | Finished::Row(..) => {
                    return self.settle_released(cx, finished);
                }
            }
        }
        self.settle_released(cx, finished);
    }

    /// Settles a finished job the call's ticket no longer holds back.
    fn settle_released(&mut self, cx: &mut Cx<'_>, finished: Finished) {
        match finished {
            Finished::Pass(Ok(report), origin) => self.on_pass(cx, report, &origin),
            Finished::Record(Ok(report), note) => self.on_record(cx, report, note),
            Finished::Row(Ok(report), note) => self.on_row(cx, report, &note),
            Finished::Row(Err(abort), note) => {
                cx.io
                    .settle(note.entry, Ok(stream::row_refused("store_failed")));
                self.abort(cx, abort);
            }
            Finished::Pass(Err(abort), _) | Finished::Record(Err(abort), _) => {
                self.abort(cx, abort);
            }
        }
    }

    /// A pass is over: its side effects are kept, the call's result written
    /// and its decision published when it is the call's own, and its
    /// delivery set and injected events are queued (RFC §10.1 steps 10 to
    /// 12).
    pub(super) fn on_pass(&mut self, cx: &mut Cx<'_>, report: PassReport, origin: &PassOrigin) {
        let trigger = *origin == PassOrigin::Trigger;
        if trigger {
            // The committed event, or the uncommitted instruction (RFC
            // §10.1.1 rule 5).
            let decided = matches!(
                report.result,
                PassResult::Delivered { .. } | PassResult::Blocked { .. }
            );
            self.trigger.settle(entry_result(&report.result), decided);
        }
        self.add_to_cascade(cx, report, origin);
        if trigger {
            let decided = Ok(self.trigger.result.clone());
            self.publish(cx, &decided);
        }
    }

    /// A row's pass is over: its reader is handed what it stored, at its
    /// commit, and it joins the cascade as a response, its delivery skipping
    /// the stream's target when that target read it in the stream (RFC
    /// §12.2 step 14s). A row the target may not read is settled with the
    /// reason why, and its reader never hands it to the target
    /// ([`StreamRead::keeps_from_target`](super::stream::StreamRead::keeps_from_target)).
    fn on_row(&mut self, cx: &mut Cx<'_>, mut report: PassReport, note: &RowNote) {
        let mut result = entry_result(&report.result);
        let kept = self
            .cascade
            .stream()
            .and_then(|read| read.keeps_from_target(cx, &mut report.result, note.skips.as_ref()));
        if let Some(reason) = kept {
            result.reason = Some(reason.to_owned());
        }
        cx.io.settle(note.entry, Ok(result));
        let origin = PassOrigin::Response {
            answers_within: None,
        };
        self.add_to_cascade(cx, report, &origin);
    }

    /// What every pass adds to the cascade, whichever cascade it belongs
    /// to: its tasks and observations, its delivery set, and the events its
    /// hooks injected, which follow the set or, for a record stored blocked,
    /// which has none, are queued at once. The answers to the event may go
    /// where its `response_visibility` says, which binds every pass of its
    /// cascade, the responses to its responses included (RFC §8.5, gap
    /// G-073).
    pub(super) fn add_to_cascade(
        &mut self,
        cx: &mut Cx<'_>,
        report: PassReport,
        origin: &PassOrigin,
    ) {
        let context = match &report.result {
            PassResult::Delivered { context, .. } | PassResult::Blocked { context, .. } => {
                Some(&**context)
            }
            PassResult::Refused { .. }
            | PassResult::Duplicate { .. }
            | PassResult::EditRefused { .. } => None,
        };
        self.side_effects
            .collect(report.tasks, report.observations, context);
        self.trigger.note_error(report.release_error);
        let injected = match &report.result {
            PassResult::Delivered { event, .. } | PassResult::Blocked { event, .. } => {
                self.injected(cx, report.injected_events, event, report.lineage)
            }
            PassResult::Refused { .. }
            | PassResult::Duplicate { .. }
            | PassResult::EditRefused { .. } => Vec::new(),
        };
        let PassResult::Delivered {
            event,
            plan,
            context,
            changed,
        } = report.result
        else {
            for job in injected {
                self.cascade.queue(job);
            }
            return;
        };
        // A response delivered is one of the answers of the call whose
        // cascade it belongs to, the call's own or an adopted entry's
        // (Python's `InboundResult.response_events`).
        if matches!(origin, PassOrigin::Response { .. }) {
            self.answered(report.lineage, &event);
        }
        if let Some(changed) = &changed {
            report::target_changed(
                cx,
                &event,
                &changed.target,
                &changed.context,
                &self.cascade.budget,
            );
        }
        let kind = match origin {
            PassOrigin::Trigger => SetKind::Root,
            PassOrigin::Adopted(entry) => SetKind::Adopted(*entry),
            PassOrigin::Response { .. } => SetKind::Response,
            PassOrigin::Injected { .. } => SetKind::Injected,
        };
        let budget = self.cascade.budget.clone();
        let lineage = report.lineage;
        let mut set = DeliverySet::new(
            kind,
            *event,
            plan,
            context,
            report.placement,
            budget,
            lineage,
        );
        if self.cascade.turns_cut && lineage == Lineage::Call {
            set.cut();
        }
        self.cascade.queue_set(Delivery {
            set: Box::new(set),
            then: injected,
        });
    }

    /// The passes of the events a pass's hooks injected, each within the
    /// cascade's reentry budget (gap G-021), but an instruction (gap G-042).
    fn injected(
        &mut self,
        cx: &mut Cx<'_>,
        injected: Vec<InjectedEvent>,
        trigger: &RoomEvent,
        lineage: Lineage,
    ) -> Vec<Job> {
        let mut jobs = Vec::new();
        for injected in instructions::without_instructions(injected, trigger) {
            let targets = injected.target_channel_ids.clone();
            let event = stamp::stamp_injected(injected, trigger, cx.env.new_event_id());
            let job = if self.cascade.budget.take_pass() {
                Job::Pass(
                    Box::new(
                        CommitPass::new(
                            PassOrigin::Injected {
                                targets: targets.clone(),
                            },
                            event,
                        )
                        .within(lineage),
                    ),
                    PassOrigin::Injected { targets },
                )
            } else {
                let mut event = event;
                event.blocked_by = Some(chain_depth::REENTRY_LOOP_CAP.to_owned());
                Job::Record(
                    Box::new(RecordPass::injected(event, targets)),
                    RecordNote::Refused,
                )
            };
            jobs.push(job);
        }
        jobs
    }

    /// A record is over: the call's own, past the reentry budget it draws
    /// on, decides the call; a channel's not asked at the chain-depth limit
    /// gets its observation and its framework event (RFC §8.3).
    pub(super) fn on_record(&mut self, cx: &mut Cx<'_>, report: RecordReport, note: RecordNote) {
        let RecordNote::NotAsked { agent, trigger } = note else {
            if matches!(note, RecordNote::Trigger) {
                return self.settle_capped_trigger(cx, report);
            }
            return self.trigger.note_error(report.release_error);
        };
        self.trigger.note_error(report.release_error);
        let RecordResult::Committed(record) = report.result else {
            return;
        };
        let id = cx.env.new_observation_id();
        let now = cx.env.now();
        let max_chain_depth = cx.config.max_chain_depth;
        self.side_effects
            .observe(chain_depth::not_asked_observation(
                id,
                &trigger,
                &agent,
                max_chain_depth,
                now,
            ));
        report::chain_depth_exceeded(cx, &record, &agent);
    }

    /// The call's own record, stored past the reentry budget it draws on
    /// or refused by the room's status, is its result, published as its
    /// decision (gap G-070).
    fn settle_capped_trigger(&mut self, cx: &mut Cx<'_>, report: RecordReport) {
        let mut result = record_result(report);
        let error = result.error.take();
        let committed = result.event.is_some();
        self.trigger.settle(result, committed);
        self.trigger.note_error(error);
        let decided = Ok(self.trigger.result.clone());
        self.publish(cx, &decided);
    }

    /// A delivery set finished: its observers run, its responses re-enter,
    /// and the records of the channels not asked are queued (RFC §10.1 steps
    /// 14 to 17). The deliveries of an entry's event are reported, the
    /// call's own written into its result too.
    pub(super) fn on_set(&mut self, cx: &mut Cx<'_>, outcome: SetOutcome, set: &DeliverySet) {
        if let Some(reason) = outcome.cancelled {
            return self.dropped(cx, reason, set);
        }
        let event = set.event();
        report::after_broadcast(cx, event, set.context(), &self.cascade.budget);
        match set.kind() {
            SetKind::Root => self.settle_root(cx, set, &outcome),
            SetKind::Adopted(entry) => {
                report::root_deliveries(cx, event, &outcome);
                self.adopted_deliveries
                    .insert(entry, delivered(set, &outcome));
            }
            SetKind::Response | SetKind::Injected => {}
        }
        self.side_effects
            .collect(outcome.tasks, outcome.observations, Some(set.context()));
        self.side_effects.update_metadata(outcome.metadata_updates);
        for agent in outcome.not_asked {
            let id = cx.env.new_event_id();
            let record = chain_depth::not_asked_record(id, event, &agent, cx.env.now());
            self.cascade.queue(Job::Record(
                Box::new(RecordPass::new(record)),
                RecordNote::NotAsked {
                    agent: Box::new(agent),
                    trigger: Box::new(event.clone()),
                },
            ));
        }
        for response in outcome.responses {
            self.queue_response(cx, response, set);
        }
        self.queue_streams(outcome.streams, set);
    }

    /// A set that delivered nothing, the room's lane taking no place for it
    /// as the kit closes (RFC §10.1 step 18): no hook runs for it, and the
    /// cascade it belongs to reports `reason`, the call's own or an adopted
    /// entry's.
    fn dropped(&mut self, cx: &mut Cx<'_>, reason: &'static str, set: &DeliverySet) {
        let cancelled = InboundResult {
            cancellation_reason: Some(reason.to_owned()),
            ..InboundResult::default()
        };
        if let SetKind::Adopted(entry) = set.kind() {
            return cx.io.delivered(entry, cancelled);
        }
        if set.lineage() == Lineage::Call {
            self.trigger
                .result
                .cancellation_reason
                .get_or_insert_with(|| reason.to_owned());
        }
    }

    /// Queues the streams of the turns `set` asked, to be read once the
    /// cascade's passes and sets are over (RFC §10.1 step 14); a cascade
    /// whose turns are cut reads none (RFC §10.1 "Explicit delivery
    /// cancellation").
    fn queue_streams(&mut self, streams: Vec<StreamedTurn>, set: &DeliverySet) {
        if self.cascade.turns_cut && set.lineage() == Lineage::Call {
            return;
        }
        for StreamedTurn { responder, turn } in streams {
            let job = StreamJob::new(
                turn,
                responder,
                set.event().clone(),
                set.kind(),
                set.lineage(),
                set.context().bindings.clone(),
            );
            self.cascade.queue_stream(job);
        }
    }

    /// Reports the inbound event's deliveries and writes them into the
    /// result (RFC §10.1 step 18), the addresses no turn reached among
    /// them, as Python's `_report_root_pass` refreshes them.
    fn settle_root(&mut self, cx: &mut Cx<'_>, set: &DeliverySet, outcome: &SetOutcome) {
        report::root_deliveries(cx, set.event(), outcome);
        self.trigger.result.unavailable_targets = Some(unavailable_after(set, outcome));
        self.trigger
            .result
            .delivery_results
            .clone_from(&outcome.delivery_results);
        self.trigger
            .result
            .response_metadata
            .clone_from(&outcome.response_metadata);
        self.trigger.note_error(outcome.error.clone());
    }

    /// Queues one channel's responses to the event `set` delivered: each
    /// re-enters as a pass, within where the answers to that event may go,
    /// which the response carries as its own `response_visibility`, so that
    /// it binds what answers it too (RFC §8.5, gap G-073), unless the room
    /// refuses it first (RFC §8.3). An instruction does not re-enter (gap
    /// G-066).
    ///
    /// A buffered response draws one pass of the cascade's reentry budget
    /// per event (RFC §8.3 "Reentry cap"); a turn whose response is a stream
    /// draws its pass, when it is a chained stream, as it is read.
    fn queue_response(&mut self, cx: &mut Cx<'_>, response: Response, set: &DeliverySet) {
        // A cascade whose turns are cut takes no reentry (RFC §10.1
        // "Explicit delivery cancellation").
        if self.cascade.turns_cut && set.lineage() == Lineage::Call {
            return;
        }
        let Response { responder, events } = response;
        let trigger = set.event();
        let at_limit = chain_depth::reaches_limit(trigger.chain_depth, cx.config.max_chain_depth);
        let reentering = events
            .into_iter()
            .filter(|event| instructions::reenters(event, &responder.channel_id, trigger));
        for event in reentering {
            let event = stamp::stamp_response(event, &responder, trigger, cx.env.new_event_id());
            let event = stamp::answer_within(event, set.answers_within(), &set.context().bindings);
            if at_limit && responder.category == ChannelCategory::Transport {
                // Only a transport is asked at the limit; what it answers is
                // stored refused (RFC §8.3).
                self.queue_refused(event, chain_depth::CHAIN_DEPTH_LIMIT);
            } else if self.cascade.budget.take_pass() {
                let origin = PassOrigin::Response {
                    answers_within: set.answers_within().cloned(),
                };
                let pass = CommitPass::new(origin.clone(), event).within(set.lineage());
                self.cascade.queue(Job::Pass(Box::new(pass), origin));
            } else {
                self.queue_refused(event, chain_depth::REENTRY_LOOP_CAP);
            }
        }
    }

    pub(super) fn queue_refused(&mut self, mut event: RoomEvent, blocked_by: &str) {
        event.blocked_by = Some(blocked_by.to_owned());
        self.cascade.queue(Job::Record(
            Box::new(RecordPass::new(event)),
            RecordNote::Refused,
        ));
    }

    /// A pass or a record failed. Before the trigger was decided nothing of
    /// it was written and the message fails, or is refused once past its
    /// deadline; after it, the event stands and the result carries the
    /// error (RFC §13.6). Either way the cascade stops: no pass runs any
    /// more, and the events committed already are still delivered.
    pub(super) fn abort(&mut self, cx: &mut Cx<'_>, abort: Abort) {
        self.cascade.stop();
        if let Abort::DeadlinePassed(locked_event) = abort {
            // Only the trigger's pass carries a deadline, and it is undecided.
            return self.expire(cx, locked_event);
        }
        if self.trigger.decided {
            return self.trigger.note_error(Some(abort.into_record()));
        }
        let error = failure(abort, self.call.room_id.clone());
        self.end(cx, Err(error));
    }
}

impl Machine {
    /// Keeps `response`, delivered in the cascade of `lineage`, among the
    /// answers of the call that cascade belongs to: the call's own, or an
    /// adopted entry's, whose delivery set is over by then.
    fn answered(&mut self, lineage: Lineage, response: &RoomEvent) {
        let answers = match lineage {
            Lineage::Call => &mut self.trigger.result.response_events,
            Lineage::Entry(entry) => match self.adopted_deliveries.get_mut(&entry) {
                Some(delivered) => &mut delivered.response_events,
                None => return,
            },
        };
        answers.push(response.clone());
    }

    /// Tells the caller of each adopted entry whose cascade is over what
    /// its delivery set reported, with the answers its cascade delivered
    /// (RFC §10.1 step 18). An entry adopted from the call's admission work
    /// is told once its delivery set is over: its answers' passes wait for
    /// the call's own pass, which may wait for this report, so they are not
    /// among them.
    pub(super) fn report_adopted_deliveries(&mut self, cx: &mut Cx<'_>) {
        let passing = matches!(self.stage.current(), Stage::Cascade | Stage::Ending(_));
        let over: Vec<EntryId> = self
            .adopted_deliveries
            .keys()
            .copied()
            .filter(|entry| !self.cascade.holds(Lineage::Entry(*entry), passing))
            .collect();
        for entry in over {
            if let Some(delivered) = self.adopted_deliveries.remove(&entry) {
                cx.io.delivered(entry, delivered);
            }
        }
    }
}

/// What an adopted entry's delivery set reports to its caller's completion
/// handle (RFC §10.1 step 18): its deliveries, and the addresses no turn
/// reached among them.
fn delivered(set: &DeliverySet, outcome: &SetOutcome) -> InboundResult {
    InboundResult {
        unavailable_targets: Some(unavailable_after(set, outcome)),
        delivery_results: outcome.delivery_results.clone(),
        response_metadata: outcome.response_metadata.clone(),
        error: outcome.error.clone(),
        ..InboundResult::default()
    }
}

/// What a record stored in place of an entry, past its reentry budget, or
/// refused by the room's status gives the entry's caller (RFC §8.3).
pub(super) fn record_result(report: RecordReport) -> InboundResult {
    match report.result {
        RecordResult::Committed(record) => InboundResult {
            error: report.release_error,
            ..stored_record(*record)
        },
        RecordResult::Replayed(original) => replay_result(&original),
        RecordResult::Refused => InboundResult {
            blocked: true,
            reason: Some("room_closed".to_owned()),
            ..InboundResult::default()
        },
    }
}

/// A record stored `BLOCKED`, as its caller's result reads it: its
/// `blocked_by` as its reason.
pub(super) fn stored_record(record: RoomEvent) -> InboundResult {
    InboundResult {
        blocked: true,
        reason: record.blocked_by.clone(),
        event: Some(record),
        ..InboundResult::default()
    }
}

/// What an entry's pass decided, as its caller's result reads it (RFC
/// §10.1): the committed event, the record stored `BLOCKED` with its reason
/// (gap G-026), or the uncommitted instruction (RFC §10.1.1 rule 5); no
/// event when the room refused it or step 8a refused an edit or a delete;
/// a replay's original, its record's `blocked_by` as its reason (RFC
/// §13.4).
pub(super) fn entry_result(result: &PassResult) -> InboundResult {
    match result {
        PassResult::Delivered { event, plan, .. } => InboundResult {
            unavailable_targets: Some(unavailable_targets(event, plan)),
            event: Some((**event).clone()),
            ..InboundResult::default()
        },
        PassResult::Blocked { event, reason, .. } => InboundResult {
            event: Some((**event).clone()),
            blocked: true,
            reason: reason.clone().or_else(|| event.blocked_by.clone()),
            ..InboundResult::default()
        },
        PassResult::Refused { .. } => InboundResult {
            blocked: true,
            reason: Some("room_closed".to_owned()),
            ..InboundResult::default()
        },
        PassResult::EditRefused { refusal } => InboundResult {
            blocked: true,
            reason: Some(refusal.reason().to_owned()),
            ..InboundResult::default()
        },
        PassResult::Duplicate { event } => replay_result(event),
    }
}

/// The result of a replay, a call whose key its channel holds in the room
/// (RFC §13.4): the event the key committed, its record's `blocked_by` as
/// its reason when it was stored `BLOCKED`.
pub(super) fn replay_result(original: &RoomEvent) -> InboundResult {
    let blocked = original.status == EventStatus::Blocked;
    InboundResult {
        event: Some(original.clone()),
        duplicate: true,
        blocked,
        reason: if blocked {
            original.blocked_by.clone()
        } else {
            None
        },
        ..InboundResult::default()
    }
}

/// The channels `event` addressed that `plan` asks no turn of, in the order
/// the event names them, as the Python implementation's commit reports
/// them: an address the room does not bind, an unregistered channel, or a
/// transport (RFC §10.1 step 18).
fn unavailable_targets(event: &RoomEvent, plan: &DeliveryPlan) -> Vec<ChannelId> {
    event
        .addressed_to
        .iter()
        .flatten()
        .filter(|target| !plan.agents.iter().any(|agent| &agent.channel_id == *target))
        .cloned()
        .collect()
}

/// The channels the event of `set` addressed that no turn of the set
/// reached, in the order the event names them, as the Python
/// implementation's `_unavailable_targets` reports them once the set is
/// over: one the set asks no turn of, and an agent whose channel was not
/// registered when its turn came (RFC §10.1 step 18).
fn unavailable_after(set: &DeliverySet, outcome: &SetOutcome) -> Vec<ChannelId> {
    set.event()
        .addressed_to
        .iter()
        .flatten()
        .filter(|target| !set.asks(target) || outcome.unreached.contains(target))
        .cloned()
        .collect()
}

/// What a pass or a record that failed before the call was decided fails
/// it with.
pub(super) fn failure(abort: Abort, room_id: Option<RoomId>) -> InboundError {
    match (abort, room_id) {
        (Abort::RoomNotFound, Some(room_id)) => InboundError::RoomNotFound(room_id),
        (Abort::Store(error), _) => InboundError::Store(error),
        (Abort::Lock(error), _) => InboundError::Lock(error),
        (Abort::Refused(error), _) => error,
        (abort @ (Abort::RoomNotFound | Abort::DeadlinePassed(_)), _) => {
            InboundError::Store(abort.into_record())
        }
    }
}

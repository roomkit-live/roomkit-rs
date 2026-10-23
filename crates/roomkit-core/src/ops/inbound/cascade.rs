//! What each finished job of the lane adds to the cascade (RFC §10.1 steps
//! 10 to 17): a pass's delivery set and the events its hooks injected, a
//! delivery set's responses and the records of the channels it could not
//! ask, the side effects every job collected, and what the call's result
//! reports.
//!
//! The lane carries every cascade of the operation, the call's and its
//! adopted entries' (gap G-070). A delivery set joins the lane when its
//! event commits, and commits follow one another under the room lock, so
//! sets keep index order: one event's set completes before the next one's
//! begins (RFC §10.2).

use super::delivery::{DeliverySet, Response, SetKind, SetOutcome};
use super::job::{Finished, Job, RecordNote};
use super::pass::{CommitPass, PassOrigin, PassReport, PassResult};
use super::record::{RecordPass, RecordReport, RecordResult};
use super::{InboundError, InboundResult, Machine, instructions, report, stamp};
use crate::model::{ChannelCategory, ErrorRecord, EventStatus, InjectedEvent, RoomEvent};
use crate::ops::{Abort, Cx};
use crate::rules::chain_depth;

impl Machine {
    /// Settles a finished job; the trigger's pass once the call's ticket is
    /// released (RFC §9.5.1).
    pub(super) fn settle(&mut self, cx: &mut Cx<'_>, finished: Finished) {
        if self.ticket.is_some() && matches!(finished, Finished::Pass(_, PassOrigin::Trigger)) {
            return self.release_ticket(cx, super::AfterTicket::Settle(Box::new(finished)));
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

    /// A pass is over: its side effects are kept, the call's result written
    /// when it is the call's own, and its delivery set and injected events
    /// join the lane (RFC §10.1 steps 10 to 12).
    pub(super) fn on_pass(&mut self, cx: &mut Cx<'_>, report: PassReport, origin: &PassOrigin) {
        if *origin == PassOrigin::Trigger {
            self.settle_trigger(&report.result);
        }
        self.add_to_cascade(cx, report, origin);
    }

    /// What every pass adds to the cascade, whichever cascade it belongs
    /// to: its tasks and observations, its delivery set at the back of the
    /// lane, and the events its hooks injected behind it.
    pub(super) fn add_to_cascade(
        &mut self,
        cx: &mut Cx<'_>,
        report: PassReport,
        origin: &PassOrigin,
    ) {
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
                PassOrigin::Adopted(_) => SetKind::Adopted,
                PassOrigin::Response => SetKind::Response,
                PassOrigin::Injected { .. } => SetKind::Injected,
            };
            self.queue(Job::Set(Box::new(DeliverySet::new(
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
        self.decided = matches!(
            result,
            PassResult::Delivered { .. } | PassResult::Blocked { .. }
        );
        let entry = entry_result(result);
        self.result.event = entry.event;
        self.result.blocked = entry.blocked;
        self.result.reason = entry.reason;
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
                self.queue(Job::Pass(
                    Box::new(CommitPass::new(
                        PassOrigin::Injected {
                            targets: targets.clone(),
                        },
                        event,
                    )),
                    PassOrigin::Injected { targets },
                ));
            } else {
                let mut event = event;
                event.blocked_by = Some(chain_depth::REENTRY_LOOP_CAP.to_owned());
                self.queue(Job::Record(
                    Box::new(RecordPass::injected(event, targets)),
                    RecordNote::Refused,
                ));
            }
        }
    }

    pub(super) fn on_record(&mut self, cx: &mut Cx<'_>, report: RecordReport, note: RecordNote) {
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
    /// 14 to 17). The deliveries of an entry's event are reported, the
    /// call's own written into its result too.
    fn on_set(&mut self, cx: &mut Cx<'_>, outcome: SetOutcome, set: &DeliverySet) {
        let event = set.event();
        report::after_broadcast(cx, event, set.context());
        match set.kind() {
            SetKind::Root => self.settle_root(cx, event, &outcome),
            SetKind::Adopted => report::root_deliveries(cx, event, &outcome),
            SetKind::Response | SetKind::Injected => {}
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
            self.queue(Job::Record(
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
                self.queue(Job::Pass(
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
        self.queue(Job::Record(
            Box::new(RecordPass::new(event)),
            RecordNote::Refused,
        ));
    }

    /// Keeps the first error met once the trigger was decided: the result
    /// reports one (RFC §13.6).
    pub(super) fn note_error(&mut self, error: Option<ErrorRecord>) {
        if self.result.error.is_none() {
            self.result.error = error;
        }
    }

    /// A pass or a record failed. Before the trigger was decided nothing of
    /// it was written and the message fails, or is refused once past its
    /// deadline; after it, the event stands and the result carries the
    /// error (RFC §13.6). Either way the cascade stops: no pass runs any
    /// more, and the events committed already are still delivered.
    pub(super) fn abort(&mut self, cx: &mut Cx<'_>, abort: Abort) {
        self.stop_cascade();
        if let Abort::DeadlinePassed(locked_event) = abort {
            // Only the trigger's pass carries a deadline, and it is undecided.
            return self.expire(cx, locked_event);
        }
        if self.decided {
            return self.note_error(Some(abort.into_record()));
        }
        let error = failure(abort, self.room_id.clone());
        self.end(cx, Err(error));
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
        PassResult::Delivered { event, .. } => InboundResult {
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
        PassResult::Duplicate { event } => {
            let blocked = event.status == EventStatus::Blocked;
            InboundResult {
                event: Some((**event).clone()),
                blocked,
                reason: if blocked {
                    event.blocked_by.clone()
                } else {
                    None
                },
                ..InboundResult::default()
            }
        }
    }
}

/// What a pass or a record that failed before the call was decided fails
/// it with.
pub(super) fn failure(abort: Abort, room_id: Option<crate::model::RoomId>) -> InboundError {
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

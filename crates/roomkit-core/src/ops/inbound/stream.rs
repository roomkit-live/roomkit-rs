//! The streamed responses of a cascade's turns (RFC §6.1
//! `response_stream`, §8.3, §10.1 step 14, §12.2 steps 13s and 14s).
//!
//! A turn whose output says a stream follows is read once the cascade's
//! passes and delivery sets are over, after the trigger's own work, one
//! stream at a time in the order the turns answered: a stream another
//! answer's delivery asked for (a chained stream) draws one pass of the
//! cascade's reentry budget as it is read, and past the budget it is
//! closed unread, so that nothing is generated, its agent's answer stored
//! as a record refused `reentry_loop_cap` (RFC §8.3). The trigger's own
//! stream draws none.
//!
//! The room is read again for the stream: its first transport that the
//! response's rows reach and that streams in the room
//! (`supports_streaming_delivery_for`, asked of every such transport at
//! each stream, whatever it declared) reads it as it is written, the one
//! streaming target of the response; no target when the agent cannot write
//! (RFC §7.5 rule 2). The driver hands
//! each row the stream ends back as an [`Entry::Row`](crate::ops::Entry::Row),
//! which commits as a response, through `BEFORE_BROADCAST` once (RFC
//! §9.5), and is delivered to the others while the turn goes on; its
//! reader is handed it once the store holds it. A row the target read in
//! the stream is not delivered to it again (RFC §12.2 step 14s). At each
//! row the room is asked again for the target: once it no longer lets the
//! target read the response (the agent muted, the target detached, or out
//! of the agent's visibility), the target reads nothing more in the stream.

use super::delivery::SetKind;
use super::pass::{CommitPass, PassOrigin, PassResult};
use super::queues::Lineage;
use super::{InboundResult, stamp};
use crate::model::{
    ChannelBinding, ChannelId, ChannelOutput, ErrorRecord, EventDirection, EventSource, EventType,
    RoomContext, RoomEvent, TextContent, Visibility,
};
use crate::ops::Cx;
use crate::ops::protocol::{
    ChannelRequest, EffectId, EntryId, Outcome, ROW_HIDDEN, ROW_WITHHELD, StoreRequest,
    StreamTarget, TURN_CUT,
};
use crate::rules::chain_depth::{self, ReentryBudget};
use crate::rules::hook_registry::HookTrigger;
use crate::rules::permissions::{self, DeliveryPlan};

/// A turn's response whose stream is to be read.
#[derive(Clone, Debug)]
pub(super) struct StreamJob {
    /// The turn's `OnEvent` effect, which the driver keeps the stream by.
    pub(super) turn: EffectId,
    /// The agent whose response it is.
    pub(super) responder: ChannelBinding,
    /// The event it answers, as committed.
    pub(super) answers: RoomEvent,
    /// Where the answers to that event may go (RFC §8.5, gap G-073).
    pub(super) answers_within: Option<Visibility>,
    /// Whose cascade it belongs to.
    pub(super) lineage: Lineage,
    /// Whether another answer's delivery asked for it (RFC §8.3).
    pub(super) chained: bool,
    /// The room's bindings when its turn was asked, which a record stored
    /// in its place is scoped with.
    pub(super) bindings: Vec<ChannelBinding>,
    /// The response's own id, which each of its rows and its failure's
    /// `ON_ERROR` payload carry, once its read is asked (RFC §9.2).
    pub(super) correlation_id: Option<String>,
}

impl StreamJob {
    /// The stream of the turn `turn` of `responder`, asked by the delivery
    /// set of `answers`, of `kind`.
    pub(super) fn new(
        turn: EffectId,
        responder: ChannelBinding,
        answers: RoomEvent,
        kind: SetKind,
        lineage: Lineage,
        bindings: Vec<ChannelBinding>,
    ) -> Self {
        Self {
            turn,
            responder,
            answers_within: answers.response_visibility.clone(),
            answers,
            lineage,
            chained: matches!(kind, SetKind::Response | SetKind::Greeting),
            bindings,
            correlation_id: None,
        }
    }

    /// Whether the stream is read, past the cascade's `budget` for a
    /// chained one (RFC §8.3); otherwise the record stored in its place.
    pub(super) fn admit(&self, cx: &Cx<'_>, budget: &ReentryBudget) -> Result<(), Box<RoomEvent>> {
        if !self.chained || budget.take_pass() {
            return Ok(());
        }
        tracing::warn!(
            target: "roomkit::core::delivery",
            room_id = %self.answers.room_id,
            event_id = %self.answers.id,
            channel_id = %self.responder.channel_id,
            "the cascade's reentry budget is spent: the chained stream is closed unread"
        );
        let record = chain_depth::not_asked_record(
            cx.env.new_event_id(),
            &self.answers,
            &self.responder,
            cx.env.now(),
        );
        Err(Box::new(record))
    }
}

/// Where a stream's read stands.
#[derive(Debug)]
enum ReadStage {
    /// The room is read for the stream's target.
    Loading(EffectId),
    /// The transports the rows reach are asked whether they stream in the
    /// room (RFC §6.1
    /// `supports_streaming_delivery_for`), the room as the read found it.
    Asking(EffectId),
    /// The stream is read, its rows committed as they come.
    Reading(EffectId),
}

/// The read of one stream.
#[derive(Debug)]
pub(super) struct StreamRead {
    job: StreamJob,
    state: ReadStage,
    /// The room as the read found it.
    context: Option<Box<RoomContext>>,
    /// The streaming target's channel, when one reads the stream.
    target: Option<ChannelId>,
}

/// A read after an answer: still reading, or over.
#[derive(Debug)]
pub(super) enum Read {
    Reading(Box<StreamRead>),
    Over(Box<StreamEnd>),
}

/// What a finished read leaves.
#[derive(Debug)]
pub(super) struct StreamEnd {
    pub(super) job: StreamJob,
    /// The room as the read found it.
    pub(super) context: Option<Box<RoomContext>>,
    /// The target's output, the stream's failure in its `error`.
    pub(super) output: Option<ChannelOutput>,
}

impl StreamRead {
    /// Starts the read of `job`: the room is read first.
    pub(super) fn start(cx: &mut Cx<'_>, job: StreamJob) -> Self {
        let request = StoreRequest::LoadContext {
            room_id: job.answers.room_id.clone(),
            recent_events: cx.config.recent_events(),
        };
        Self {
            state: ReadStage::Loading(cx.io.issue(request)),
            job,
            context: None,
            target: None,
        }
    }

    /// Whether the read waits for `effect`.
    pub(super) fn awaits(&self, effect: EffectId) -> bool {
        match self.state {
            ReadStage::Loading(awaited)
            | ReadStage::Asking(awaited)
            | ReadStage::Reading(awaited) => awaited == effect,
        }
    }

    /// Whether `effect` is the read of the stream, whose rows it takes.
    pub(super) fn reads(&self, effect: EffectId) -> bool {
        matches!(self.state, ReadStage::Reading(reading) if reading == effect)
    }

    /// Whose cascade the stream belongs to.
    pub(super) fn lineage(&self) -> Lineage {
        self.job.lineage
    }

    /// The effect reading the stream, once it is read.
    pub(super) fn reading(&self) -> Option<EffectId> {
        match self.state {
            ReadStage::Reading(effect) => Some(effect),
            ReadStage::Loading(_) | ReadStage::Asking(_) => None,
        }
    }

    /// Takes the driver's answer to one of the read's requests; a stream
    /// whose turns are `cut` before it is read is never read.
    pub(super) fn accept(
        mut self,
        cx: &mut Cx<'_>,
        budget: &ReentryBudget,
        outcome: Outcome,
        cut: bool,
    ) -> Read {
        match self.state {
            ReadStage::Loading(_) | ReadStage::Asking(_) if cut => {
                self.unread("its cascade's turns are cut")
            }
            ReadStage::Loading(_) => match outcome.into_context() {
                Ok(Some(context)) => {
                    self.ask(cx, budget, context);
                    Read::Reading(Box::new(self))
                }
                Ok(None) => self.unread("the room is gone"),
                Err(error) => self.unread(&error.message),
            },
            ReadStage::Asking(_) => {
                let streaming = outcome.into_streams_in().unwrap_or_else(|error| {
                    tracing::warn!(
                        target: "roomkit::core::delivery",
                        room_id = %self.job.answers.room_id,
                        error = %error.message,
                        "the channels could not say whether they stream in the room: \
                         none reads the stream"
                    );
                    Vec::new()
                });
                match self.context.take() {
                    Some(context) => {
                        self.read(cx, budget, context, &streaming);
                        Read::Reading(Box::new(self))
                    }
                    None => self.unread("the room is gone"),
                }
            }
            ReadStage::Reading(_) => {
                let output = match outcome.into_output() {
                    Ok(output) => Some(*output),
                    // A read its cascade cut reports nothing.
                    Err(error) if error.error_type == TURN_CUT => None,
                    Err(error) => Some(ChannelOutput {
                        error: Some(error),
                        ..ChannelOutput::default()
                    }),
                };
                Read::Over(Box::new(StreamEnd {
                    job: self.job,
                    context: self.context,
                    output,
                }))
            }
        }
    }

    /// Asks the transports the response's rows reach whether they stream
    /// in the room as `context` holds it (RFC
    /// §6.1 `supports_streaming_delivery_for`); with none, reads the stream
    /// at once, with no target.
    fn ask(&mut self, cx: &mut Cx<'_>, budget: &ReentryBudget, context: Box<RoomContext>) {
        let channel_ids = candidates(cx, &self.job, &context);
        if channel_ids.is_empty() {
            return self.read(cx, budget, context, &[]);
        }
        let request = ChannelRequest::StreamsIn {
            room_id: self.job.answers.room_id.clone(),
            channel_ids,
        };
        self.state = ReadStage::Asking(cx.io.issue(request));
        self.context = Some(context);
    }

    /// Asks the driver to read the stream, with its target among the
    /// `streaming` channels, the room as `context` holds it; the calls the
    /// turn makes into a room draw on `budget`, the cascade's, as its
    /// delivery set's do (gap G-070).
    fn read(
        &mut self,
        cx: &mut Cx<'_>,
        budget: &ReentryBudget,
        context: Box<RoomContext>,
        streaming: &[ChannelId],
    ) {
        let target = target(cx, &self.job, &context, streaming);
        self.target = target
            .as_ref()
            .map(|target| target.binding.channel_id.clone());
        // One id per read, from the environment as every id the core gives.
        let correlation_id = cx.env.new_event_id().to_string();
        self.job.correlation_id = Some(correlation_id.clone());
        let request = ChannelRequest::ReadStream {
            turn: self.job.turn,
            responder: self.job.responder.clone(),
            answers: Box::new(self.job.answers.clone()),
            target,
            correlation_id,
            as_trace: false,
        };
        self.state = ReadStage::Reading(cx.io.issue_drawing_on(request, budget));
        self.context = Some(context);
    }

    /// The read ends before the stream is read, the room unread: nothing
    /// of it is generated.
    fn unread(self, why: &str) -> Read {
        tracing::warn!(
            target: "roomkit::core::delivery",
            room_id = %self.job.answers.room_id,
            channel_id = %self.job.responder.channel_id,
            why,
            "the room could not be read for a turn's stream: it is closed unread"
        );
        Read::Over(Box::new(StreamEnd {
            job: self.job,
            context: None,
            output: None,
        }))
    }

    /// Whether the stream is the call's own, neither an answer's nor an
    /// adopted entry's: its failure is its caller's.
    pub(super) fn is_own(&self) -> bool {
        !self.job.chained && self.job.lineage == Lineage::Call
    }

    /// The pass of `event`, a row the stream ended, handed back as the
    /// entry `entry`: a response to the event the stream answers, within
    /// where its answers may go (RFC §8.5), and the channel its delivery
    /// skips, the target, when it read the row in the stream.
    pub(super) fn row_pass(
        &self,
        cx: &Cx<'_>,
        event: RoomEvent,
        streamed: bool,
    ) -> (CommitPass, Option<ChannelId>) {
        let job = &self.job;
        let row = stamp::stamp_response(event, &job.responder, &job.answers, cx.env.new_event_id());
        let bindings = self
            .context
            .as_ref()
            .map(|context| context.bindings.as_slice())
            .unwrap_or_default();
        let row = stamp::answer_within(row, job.answers_within.as_ref(), bindings);
        let origin = PassOrigin::Response {
            answers_within: job.answers_within.clone(),
        };
        let pass = CommitPass::new(origin, row).within(job.lineage);
        let skips = streamed.then(|| self.target.clone()).flatten();
        (pass, skips)
    }

    /// Why the target may not be handed the row whose pass ended with
    /// `result`, read in the stream when `skips` names the target; `None`
    /// when it may, the row's delivery then skipping a target that reads it
    /// in the stream (RFC §12.2 step 14s).
    ///
    /// Whatever the row's type, the room as its pass read it is asked again
    /// for the target: once the agent may no longer write (RFC §7.2), the
    /// target is no longer bound (§7.5), or the agent's visibility no
    /// longer reaches it, the response is withdrawn from the target
    /// ([`ROW_WITHHELD`]), a row stored `BLOCKED` or refused included. A
    /// message its hooks keep from the target withholds the rest too; a
    /// tool-call row so kept is only hidden ([`ROW_HIDDEN`]), the stream
    /// going on (review of step 4 milestone 6).
    pub(super) fn keeps_from_target(
        &self,
        cx: &Cx<'_>,
        result: &mut PassResult,
        skips: Option<&ChannelId>,
    ) -> Option<&'static str> {
        if let Some(target) = &self.target {
            let still = result
                .context()
                .and_then(|context| answer_plan(cx, &self.job, context))
                .is_some_and(|plan| delivers_to(&plan, target));
            if !still {
                return Some(ROW_WITHHELD);
            }
        }
        let (PassResult::Delivered { event, plan, .. }, Some(skips)) = (result, skips) else {
            return None;
        };
        if !delivers_to(plan, skips) {
            let message = event.r#type == EventType::Message;
            return Some(if message { ROW_WITHHELD } else { ROW_HIDDEN });
        }
        plan.transports
            .retain(|target| &target.binding.channel_id != skips);
        None
    }
}

/// The transports `job`'s response's rows reach in the room as `context`
/// holds it, as a text answer from the agent plans (RFC §10.2 steps 2 and
/// 3d), in plan order, each asked whether it streams in the room whatever
/// it declared at registration, as Python asks every one (a WebSocket
/// channel streams once a client connects); none when the agent cannot
/// write (RFC §7.5 rule 2) or is no longer bound.
fn candidates(cx: &Cx<'_>, job: &StreamJob, context: &RoomContext) -> Vec<ChannelId> {
    let Some(plan) = answer_plan(cx, job, context) else {
        return Vec::new();
    };
    plan.transports
        .into_iter()
        .filter(|transport| transport.delivers)
        .map(|transport| transport.binding.channel_id)
        .collect()
}

/// The streaming target of `job`'s response in the room as `context` holds
/// it (RFC §6.1, §12.2 step 11s): the first transport its rows reach, as a
/// text answer from the agent plans (RFC §10.2 steps 2 and 3d), among the
/// `streaming` channels, those that stream in the room; none when the agent
/// cannot write (RFC §7.5 rule 2), when it is no longer bound, or when no
/// such transport is bound.
fn target(
    cx: &Cx<'_>,
    job: &StreamJob,
    context: &RoomContext,
    streaming: &[ChannelId],
) -> Option<StreamTarget> {
    let plan = answer_plan(cx, job, context)?;
    plan.transports.into_iter().find_map(|transport| {
        (transport.delivers && streaming.contains(&transport.binding.channel_id)).then(|| {
            StreamTarget {
                context: Box::new(permissions::context_for(&transport.binding, context)),
                binding: transport.binding,
            }
        })
    })
}

/// Whom a text answer of `job`'s agent reaches in the room as `context`
/// holds it (RFC §10.2 steps 2 and 3d); none when the agent cannot write
/// (RFC §7.5 rule 2) or is no longer bound.
fn answer_plan(cx: &Cx<'_>, job: &StreamJob, context: &RoomContext) -> Option<DeliveryPlan> {
    let bindings = &context.bindings;
    let responder = context.get_binding(&job.responder.channel_id)?;
    if permissions::write_refusal(responder).is_some() {
        return None;
    }
    let answer = RoomEvent::at(
        job.answers.id.clone(),
        job.answers.room_id.clone(),
        EventType::Message,
        EventSource {
            direction: EventDirection::Outbound,
            ..EventSource::new(responder.channel_id.clone(), responder.channel_type.clone())
        },
        TextContent::new(""),
        job.answers.created_at,
    );
    let answer = stamp::stamp_response(answer, responder, &job.answers, job.answers.id.clone());
    let mut answer = stamp::answer_within(answer, job.answers_within.as_ref(), bindings);
    answer.visibility = permissions::effective_visibility(&answer.visibility, responder, bindings);
    Some(permissions::plan_delivery(
        &answer,
        bindings,
        context.room.agent_response_policy,
        cx.config.max_chain_depth,
    ))
}

/// Whether `plan` delivers to `channel_id`.
fn delivers_to(plan: &DeliveryPlan, channel_id: &ChannelId) -> bool {
    plan.transports
        .iter()
        .any(|target| &target.binding.channel_id == channel_id && target.delivers)
}

/// Fires `ON_ERROR` for the failure of `job`'s stream, `error` (RFC §9.2,
/// §12.2 step 13s), its payload one hop deeper than the event the stream
/// answers, as a failed turn's, carrying the stream's `correlation_id`.
pub(super) fn report_failure(
    cx: &mut Cx<'_>,
    job: &StreamJob,
    context: &RoomContext,
    error: &ErrorRecord,
) {
    let id = cx.env.new_event_id();
    let now = cx.env.now();
    let within = job.answers_within.as_ref();
    let mut payload = stamp::error_event(
        id,
        &job.responder,
        &job.answers,
        within,
        error,
        "streaming",
        now,
    );
    payload.correlation_id.clone_from(&job.correlation_id);
    let seen = permissions::context_for(&job.responder, context);
    cx.fire_observers(HookTrigger::OnError, &payload, &seen);
}

/// The result a row's entry is settled with when it is not committed: its
/// reader is handed nothing.
pub(super) fn row_refused(reason: &str) -> InboundResult {
    InboundResult {
        blocked: true,
        reason: Some(reason.to_owned()),
        ..InboundResult::default()
    }
}

/// A row handed back by a stream's reader, waiting for its pass.
#[derive(Debug)]
pub(super) struct PendingRow {
    pub(super) entry: EntryId,
    pub(super) origin: EffectId,
    pub(super) event: Box<RoomEvent>,
    pub(super) streamed: bool,
}

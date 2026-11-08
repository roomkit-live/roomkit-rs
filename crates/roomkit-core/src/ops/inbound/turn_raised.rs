//! A turn that raised once its stream was read: what follows a strategy's
//! first pass, the supervisor's workers, raised (RFC §19.7.3).
//!
//! The Python implementation reads that first pass and runs those workers
//! within the agent's `on_event()`, so a worker that raises fails the
//! agent's turn as any `on_event()` that raises does: `ON_ERROR`
//! `generation`, with no correlation id (RFC §9.2), the agent's delivery
//! failed among the set's results, reported and recorded on the event (RFC
//! §5.13), and the error the caller's. This implementation reads that pass
//! as the turn's stream, once the set is over; the read's answer
//! [`ChannelOutcome::Raised`](crate::ops::ChannelOutcome::Raised) says the
//! turn raised, and the operation reports it so once the read is over.

use std::collections::BTreeMap;

use super::delivery_record::DeliveryRecord;
use super::queues::Lineage;
use super::stream::StreamJob;
use super::{Machine, stamp};
use crate::model::{
    ChannelId, DeliveryError, DeliveryResult, DeliveryResultStatus, ErrorRecord, RoomContext,
    RoomEvent,
};
use crate::ops::failure_log::Log;
use crate::ops::protocol::{Notice, SpanNote};
use crate::ops::{Cx, FrameworkEvent};
use crate::rules::hook_registry::HookTrigger;
use crate::rules::permissions;

impl Machine {
    /// `job`'s turn raised `error` once its stream was read, the room as the
    /// read found it `context`, declaring the failed delivery `delivery`:
    /// `ON_ERROR` once, `generation`, and the line Python's router logs;
    /// for a stream of the call's own set or of an adopted entry's, never
    /// an answer's, the agent's delivery failed among the set's results,
    /// reported and recorded on its event, and the error the caller's.
    pub(super) fn turn_raised(
        &mut self,
        cx: &mut Cx<'_>,
        job: &StreamJob,
        context: &RoomContext,
        error: ErrorRecord,
        delivery: DeliveryError,
    ) {
        report_raised(cx, job, context, &error);
        if job.chained {
            return;
        }
        let channel_id = job.responder.channel_id.clone();
        let failed = DeliveryResult {
            error: Some(delivery),
            ..DeliveryResult::raised(channel_id.clone(), &error)
        };
        let Some(results) = self.fail_delivery(cx, job.lineage, failed, error) else {
            return;
        };
        report_failed_delivery(cx, &job.answers, &results, channel_id);
        if let Some(record) = DeliveryRecord::start(cx, &job.answers, &results) {
            self.cascade.keep_record(job.lineage, record);
        }
    }

    /// Puts `failed` among the delivery results of `lineage`'s caller, and
    /// `error` as its error, the first it meets; the results it holds now,
    /// or `None` for an entry whose caller was told already.
    fn fail_delivery(
        &mut self,
        cx: &mut Cx<'_>,
        lineage: Lineage,
        failed: DeliveryResult,
        error: ErrorRecord,
    ) -> Option<BTreeMap<ChannelId, DeliveryResult>> {
        if let Lineage::Entry(entry) = lineage {
            let result = self.adopted_deliveries.get_mut(&entry)?;
            result
                .delivery_results
                .insert(failed.channel_id.clone(), failed);
            result.error = result.error.take().or(Some(error));
            return Some(result.delivery_results.clone());
        }
        let result = &mut self.trigger.result;
        result
            .delivery_results
            .insert(failed.channel_id.clone(), failed);
        let results = result.delivery_results.clone();
        let told = error.clone();
        cx.io
            .notify(Notice::Span(SpanNote::CascadeFailed { error: told }));
        self.trigger.note_error(Some(error));
        Some(results)
    }
}

/// Reports that `job`'s turn raised `error`, the room as the read found it
/// `context`: logged once, as Python's router logs a target that raised
/// ("Processing target <channel> failed"), and `ON_ERROR` once,
/// `generation`, with no correlation id (RFC §9.2, §15.2).
fn report_raised(cx: &mut Cx<'_>, job: &StreamJob, context: &RoomContext, error: &ErrorRecord) {
    cx.io.notify(Notice::Failure {
        log: Log::Framework,
        what: format!("Processing target {}", job.responder.channel_id),
        error: error.clone(),
        caller_logs: false,
    });
    let payload = stamp::error_event(
        cx.env.new_event_id(),
        &job.responder,
        &job.answers,
        job.answers_within.as_ref(),
        error,
        "generation",
        cx.env.now(),
    );
    let observed = permissions::context_for(&job.responder, context);
    cx.fire_observers(HookTrigger::OnError, &payload, &observed);
}

/// Reports the failed delivery of `event` to `channel_id` among the set's
/// `results`, as the set reports its failures (RFC §8.2, §5.13):
/// `broadcast_partial_failure` over the set's results, then
/// `delivery_failed`.
fn report_failed_delivery(
    cx: &mut Cx<'_>,
    event: &RoomEvent,
    results: &BTreeMap<ChannelId, DeliveryResult>,
    channel_id: ChannelId,
) {
    let errors: BTreeMap<ChannelId, String> = results
        .iter()
        .filter(|(_, result)| result.status == DeliveryResultStatus::Failed)
        .map(|(id, result)| {
            let message = result.error.as_ref().map(|error| error.message.clone());
            (id.clone(), message.unwrap_or_default())
        })
        .collect();
    let error = errors.get(&channel_id).cloned().unwrap_or_default();
    let partial = FrameworkEvent::BroadcastPartialFailure {
        room_id: event.room_id.clone(),
        event_id: event.id.clone(),
        failed: errors.len(),
        total: results.len(),
        errors,
    };
    cx.io.notify(Notice::FrameworkEvent(partial));
    let failed = FrameworkEvent::DeliveryFailed {
        room_id: event.room_id.clone(),
        event_id: event.id.clone(),
        channel_id,
        error,
    };
    cx.io.notify(Notice::FrameworkEvent(failed));
}

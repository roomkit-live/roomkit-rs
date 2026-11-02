//! What a delegated turn's agents returned once they all answered: their
//! side effects, their answers, buffered or streamed, and their failures.

use crate::model::{
    ChannelBinding, ChannelId, ChannelOutput, ErrorRecord, Metadata, Observation, RoomEvent, Task,
};
use crate::ops::protocol::{CHANNEL_NOT_REGISTERED, ChannelOutcome, EffectId, Outcome, TURN_CUT};
use crate::rules::attachment;

use super::DelegatedAnswer;

/// A buffered turn's answer: its slot, its responder, and its response
/// events.
pub(super) type Buffered = (usize, ChannelBinding, Vec<RoomEvent>);

/// A streamed turn: its `OnEvent` effect, its slot, and its responder.
pub(super) type Streamed = (EffectId, usize, ChannelBinding);

/// A turn's failure, to report once (RFC §9.2): its responder, its error,
/// its category, and its stream's `correlation_id` for a stream's.
#[derive(Debug)]
pub(super) struct Failure {
    pub(super) responder: ChannelBinding,
    pub(super) error: ErrorRecord,
    pub(super) category: &'static str,
    pub(super) correlation_id: Option<String>,
}

/// What the turns returned once they all answered: their side effects,
/// and their answers, buffered or streamed.
#[derive(Default)]
pub(super) struct Collected {
    pub(super) tasks: Vec<Task>,
    pub(super) observations: Vec<Observation>,
    pub(super) metadata: Metadata,
    pub(super) buffered: Vec<Buffered>,
    pub(super) streamed: Vec<Streamed>,
    /// The turns that failed, to report under `generation`.
    pub(super) failures: Vec<Failure>,
}

impl Collected {
    /// The answer of the turn `turn` of `binding`, at `slot`, as its
    /// `outcome` says; its side effects and its rows or its stream kept.
    pub(super) fn take(
        &mut self,
        turn: EffectId,
        binding: ChannelBinding,
        outcome: Option<Outcome>,
        slot: usize,
    ) -> DelegatedAnswer {
        let mut answer = DelegatedAnswer {
            channel_id: binding.channel_id.clone(),
            responded: false,
            rows: Vec::new(),
            record: Metadata::new(),
            error: None,
        };
        match outcome {
            Some(Outcome::Channel(ChannelOutcome::Output(output))) => {
                let ChannelOutput {
                    responded,
                    response_events,
                    response_metadata,
                    tasks,
                    observations,
                    metadata_updates,
                    error,
                    ..
                } = *output;
                answer.responded = responded;
                if let Some(failed) = &error {
                    self.failed(&binding, failed.clone());
                }
                answer.error = error;
                self.tasks.extend(tasks);
                self.observations.extend(observations);
                self.keep_metadata(&binding.channel_id, metadata_updates);
                // Its record names its turn under the task's `turns`
                // whatever ended it (Python's `responder_turn_entries`); an
                // output that did not answer stores no rows, and its end
                // and text are not the task's, as Python's
                // `_buffered_outcome` keeps nothing of it (RFC §23.3 step 6).
                answer.record = response_metadata;
                if responded {
                    self.buffered.push((slot, binding, response_events));
                }
            }
            Some(Outcome::Channel(ChannelOutcome::Streamed(output))) => {
                answer.responded = true;
                answer.record.clone_from(&output.response_metadata);
                self.tasks.extend(output.tasks);
                self.observations.extend(output.observations);
                self.keep_metadata(&binding.channel_id, output.metadata_updates);
                self.streamed.push((turn, slot, binding));
            }
            // A turn that raised answers nothing, and carries no error to
            // the task: it is logged and reported, as Python's router and
            // `_report_intelligence_errors` do, and no output of it kept.
            Some(Outcome::Channel(ChannelOutcome::Raised(raised))) => {
                log_raised(&binding.channel_id, &raised.error);
                self.failed(&binding, raised.error);
            }
            Some(Outcome::Channel(ChannelOutcome::ProviderDelivery(result))) => {
                // Python's `ProviderDeliveryError` says so of a refusal that
                // names no error.
                let message = result
                    .error
                    .unwrap_or_else(|| "provider_send_failed".to_owned());
                let error = ErrorRecord::new("ProviderDeliveryError", message);
                log_raised(&binding.channel_id, &error);
                self.failed(&binding, error);
            }
            Some(Outcome::Failed(error)) => {
                log_raised(&binding.channel_id, &error);
                // An agent not registered ran no turn, and a turn its
                // cascade cut is no failure: neither is reported.
                if error.error_type != CHANNEL_NOT_REGISTERED && error.error_type != TURN_CUT {
                    self.failed(&binding, error);
                }
            }
            Some(_) | None => {}
        }
        answer
    }

    /// Keeps the room metadata updates `channel_id`'s turn returned, as a
    /// room's delivery set keeps them: a key only the framework writes is
    /// refused (RFC §5.1, §10.1 step 15, gap G-049; Python's
    /// `without_framework_keys`).
    fn keep_metadata(&mut self, channel_id: &ChannelId, updates: Metadata) {
        for (key, value) in updates {
            if attachment::the_framework_s(&key) {
                tracing::warn!(
                    target: "roomkit::tasks",
                    channel_id = %channel_id,
                    key = %key,
                    "a delegated turn's room metadata update names a key only the framework writes; \
                     the key is refused"
                );
                continue;
            }
            self.metadata.insert(key, value);
        }
    }

    /// Keeps `binding`'s turn failure, to report under `generation`.
    fn failed(&mut self, binding: &ChannelBinding, error: ErrorRecord) {
        self.failures.push(Failure {
            responder: binding.clone(),
            error,
            category: "generation",
            correlation_id: None,
        });
    }
}

/// Logs the raise of `channel_id`'s turn, which the task does not carry.
fn log_raised(channel_id: &ChannelId, error: &ErrorRecord) {
    tracing::warn!(target: "roomkit::tasks", channel = %channel_id, error = %error.message, "a delegated turn raised");
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::model::{
        ChannelCategory, ChannelDirection, ChannelType, ProviderResult, RoomId, Timestamp,
    };

    // RMK-1188: Python's `ProviderDeliveryError(result)` reads
    // `result.error or "provider_send_failed"` (core/exceptions.py).
    #[test]
    fn a_refusal_naming_no_error_is_reported_as_python_names_it() {
        let worker = ChannelBinding::at(
            ChannelId::new("worker").unwrap(),
            RoomId::new("r::task-1").unwrap(),
            ChannelType::AI,
            ChannelCategory::Intelligence,
            ChannelDirection::Bidirectional,
            Timestamp::UNIX_EPOCH,
        );
        let refused = ChannelOutcome::ProviderDelivery(Box::new(ProviderResult::new(false)));
        let mut collected = Collected::default();

        collected.take(EffectId::first(), worker, Some(refused.into()), 0);

        let reported: Vec<&str> = collected
            .failures
            .iter()
            .map(|failure| failure.error.message.as_str())
            .collect();
        assert_eq!(reported, ["provider_send_failed"]);
    }
}

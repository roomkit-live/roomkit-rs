//! What a delegated turn's agents returned once they all answered: their
//! side effects and their answers, buffered or streamed.

use crate::model::{
    ChannelBinding, ChannelId, ChannelOutput, ErrorRecord, Metadata, Observation, RoomEvent, Task,
};
use crate::ops::protocol::{ChannelOutcome, EffectId, Outcome};

use super::DelegatedAnswer;

/// A buffered turn's answer: its slot, its responder, and its response
/// events.
pub(super) type Buffered = (usize, ChannelBinding, Vec<RoomEvent>);

/// A streamed turn: its `OnEvent` effect, its slot, and its responder.
pub(super) type Streamed = (EffectId, usize, ChannelBinding);

/// What the turns returned once they all answered: their side effects,
/// and their answers, buffered or streamed.
#[derive(Default)]
pub(super) struct Collected {
    pub(super) tasks: Vec<Task>,
    pub(super) observations: Vec<Observation>,
    pub(super) metadata: Metadata,
    pub(super) buffered: Vec<Buffered>,
    pub(super) streamed: Vec<Streamed>,
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
                answer.record = response_metadata;
                answer.error = error;
                self.tasks.extend(tasks);
                self.observations.extend(observations);
                self.metadata.extend(metadata_updates);
                self.buffered.push((slot, binding, response_events));
            }
            Some(Outcome::Channel(ChannelOutcome::Streamed(output))) => {
                answer.responded = true;
                answer.record.clone_from(&output.response_metadata);
                self.tasks.extend(output.tasks);
                self.observations.extend(output.observations);
                self.metadata.extend(output.metadata_updates);
                self.streamed.push((turn, slot, binding));
            }
            // A turn that raised answers nothing, and carries no error to
            // the task: its delivery set logged it, as Python's router
            // does, and kept no output of it.
            Some(Outcome::Channel(ChannelOutcome::Raised(raised))) => {
                log_raised(&binding.channel_id, &raised.error);
            }
            Some(Outcome::Channel(ChannelOutcome::ProviderDelivery(result))) => {
                let message = result.error.unwrap_or_default();
                log_raised(
                    &binding.channel_id,
                    &ErrorRecord::new("ProviderDeliveryError", message),
                );
            }
            Some(Outcome::Failed(error)) => log_raised(&binding.channel_id, &error),
            Some(_) | None => {}
        }
        answer
    }
}

/// Logs the raise of `channel_id`'s turn, which the task does not carry.
fn log_raised(channel_id: &ChannelId, error: &ErrorRecord) {
    tracing::warn!(target: "roomkit::tasks", channel = %channel_id, error = %error.message, "a delegated turn raised");
}

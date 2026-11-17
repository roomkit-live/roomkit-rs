//! A no-call round can spend a shared retry or end unfinished (RFC §6.4).

use roomkit::model::LoopEndReason;
use roomkit_core::rules::transcript::Message;

use super::generation::Generation;
use super::rules::{Limits, Retry, final_round_reason};
use super::{Loop, TurnItem};
use crate::ProviderError;
use crate::steering::Steering;
use crate::stream::Yielder;

impl Loop {
    /// `None` means another generation; `Some` is the reason to finish.
    pub(super) async fn retry_round(
        &self,
        limits: &mut Limits,
        (had_tool_round, generation, has_tools): (bool, &Generation, bool),
        messages: &mut Vec<Message>,
        (steering, out): (&Steering, &Yielder<Result<TurnItem, ProviderError>>),
    ) -> Result<Option<LoopEndReason>, ProviderError> {
        let finish = generation.done.finish_reason.as_deref();
        let policy = self.continuation.as_ref().filter(|_| has_tools);
        match limits.try_again(had_tool_round, &generation.text, finish, policy, || {
            steering.cancelled()
        })? {
            Retry::Again(said) => {
                out.give(Ok(TurnItem::SegmentBreak)).await;
                messages.extend(said);
                Ok(None)
            }
            Retry::Unfinished => Ok(Some(limits.passed().unwrap_or(LoopEndReason::Unfinished))),
            Retry::Done => Ok(Some(final_round_reason(
                had_tool_round,
                &generation.text,
                finish,
                limits.passed(),
            ))),
        }
    }
}

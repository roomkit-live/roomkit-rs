//! The rules the tool loop applies between its rounds (Appendix A.9, RFC
//! §6.4): its deadline, the warning it logs, the tries a round with no call
//! to run gets, and why a round with no call ends the loop, as the
//! reference implementation's `_ai_loop_rules` applies them.

use std::time::Duration;

use roomkit::model::LoopEndReason;
use roomkit_core::rules::transcript::{Message, Role, is_blank};
use tokio::time::Instant;

use super::Bounds;
use crate::error::AiError;
use crate::provider::{
    ProviderError, ResponseSchemaError, ResponseSchemaErrorOptions, ResponseSchemaFailure,
    is_malformed_call, ran_out_of_room,
};
use crate::turn_budget::TurnBudget;

/// What a model whose round of calls was followed by an empty answer is told
/// (roomkit's `_EMPTY_RETRY_NUDGE`): the results are in its context, it only
/// failed to write the answer.
const EMPTY_RETRY_NUDGE: &str = "You called tools and already have their results above. Now \
write your final answer to the user in plain text. Do not call any more tools.";

/// What a model is told when its provider could not parse its call
/// (roomkit's `MALFORMED_CALL_NUDGE`).
const MALFORMED_CALL_NUDGE: &str = "Your last tool call could not be parsed, so it did not run. \
Call the tool again with arguments that are valid JSON matching its parameters, or answer in \
plain text.";

/// What a model is told when it called a tool its request did not enable
/// (roomkit's `UNEXPECTED_CALL_NUDGE`).
const UNEXPECTED_CALL_NUDGE: &str = "The tool you called is not available in this request, so \
it did not run. Use one of the declared tools, or answer in plain text.";

/// The ending of a response whose call its provider did not hand over:
/// one to a tool the request did not enable.
const UNEXPECTED_CALL: &str = "unexpected_tool_call";

/// The limits one turn's loop runs under, and the tries it has spent.
#[derive(Debug)]
pub(super) struct Limits {
    /// When the loop's deadline passes, if it has one.
    deadline: Option<Instant>,
    /// The deadline's length, which the loop's end states.
    pub(super) timeout: Option<Duration>,
    warn_after: u32,
    max_empty_retries: u32,
    empty_retries: u32,
    /// What the turn may spend, if it has a budget (RFC §6.4).
    budget: Option<TurnBudget>,
    /// The tokens its generations billed so far.
    billed: u64,
    /// What its generations cost so far, at the model's price.
    spent: f64,
}

impl Limits {
    /// The limits of a loop starting now under `bounds`, spending at most
    /// `budget`.
    pub(super) fn starting(bounds: &Bounds, budget: Option<TurnBudget>) -> Self {
        Self {
            deadline: bounds.loop_timeout.map(|timeout| Instant::now() + timeout),
            timeout: bounds.loop_timeout,
            warn_after: bounds.warn_after,
            max_empty_retries: bounds.max_empty_retries,
            empty_retries: 0,
            budget,
            billed: 0,
            spent: 0.0,
        }
    }

    /// Counts what one generation's `usage` billed and cost against the
    /// turn's budget (Python's `_ToolLoopState.count`).
    ///
    /// # Errors
    ///
    /// [`AiError::UsageCounter`] for a billed counter the price cannot
    /// read, as Python's `ModelPricing.cost_for` raises it.
    pub(super) fn count(&mut self, usage: &roomkit::model::Metadata) -> Result<(), AiError> {
        let Some(budget) = &self.budget else {
            return Ok(());
        };
        self.billed = self.billed.saturating_add(TurnBudget::tokens_of(usage));
        self.spent += budget.cost_of(usage)?;
        Ok(())
    }

    /// Whether a done event's `usage` is one [`Self::count`] raises for: a
    /// billed counter the turn's price cannot read.
    pub(super) fn cannot_price(&self, usage: &roomkit::model::Metadata) -> bool {
        self.budget
            .as_ref()
            .is_some_and(|budget| budget.cost_of(usage).is_err())
    }

    /// The limit of the loop's own the turn has passed: its deadline, then
    /// its budget (RFC §6.4, Python's `limit_passed`).
    pub(super) fn passed(&self) -> Option<LoopEndReason> {
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Some(LoopEndReason::Timeout);
        }
        self.budget
            .as_ref()
            .filter(|budget| budget.reached(self.billed, self.spent))
            .map(|_| LoopEndReason::BudgetExceeded)
    }

    /// The limit passed at a round boundary, after `rounds` rounds of calls
    /// ran, logged: the round's calls do not run.
    pub(super) fn reached(&self, rounds: u32) -> Option<LoopEndReason> {
        let limit = self.passed();
        match limit {
            Some(LoopEndReason::Timeout) => tracing::warn!(
                target: "roomkit::ai",
                rounds,
                timeout = ?self.timeout,
                "the tool loop passed its deadline"
            ),
            Some(_) => tracing::warn!(
                target: "roomkit::ai",
                rounds,
                billed = self.billed,
                spent = self.spent,
                "the tool loop reached the turn's budget"
            ),
            None => {}
        }
        limit
    }

    /// Logs, once `rounds` rounds of calls ran, that the loop still runs.
    pub(super) fn warn_if_due(&self, rounds: u32) {
        if rounds == self.warn_after {
            tracing::warn!(target: "roomkit::ai", rounds, "the tool loop still runs");
        }
    }

    /// Whether a round that ended with `text` for `finish_reason` and no
    /// call to run is tried again: when it is, the messages the next
    /// generation reads after it, what the model said, then the word to it.
    /// A try is spent only while one is left and the deadline holds.
    pub(super) fn try_again(
        &mut self,
        had_tool_round: bool,
        text: &str,
        finish_reason: Option<&str>,
    ) -> Option<Vec<Message>> {
        let nudge = empty_round_nudge(had_tool_round, text, finish_reason)?;
        if self.empty_retries >= self.max_empty_retries || self.passed().is_some() {
            return None;
        }
        self.empty_retries += 1;
        tracing::warn!(
            target: "roomkit::ai",
            finish_reason,
            retry = self.empty_retries,
            of = self.max_empty_retries,
            "the round left no answer: the model is asked again"
        );
        let mut said = Vec::with_capacity(2);
        if !is_blank(text) {
            said.push(Message::new(Role::Assistant, text));
        }
        said.push(Message::new(Role::User, nudge));
        Some(said)
    }
}

/// What a model whose round ended with no call to run is told, or `None`
/// to end the loop there (roomkit's `_empty_round_nudge`): a call its
/// provider did not hand over, on any round; an empty answer after a round
/// of calls, unless the response ran out of room, which another try would
/// as well.
fn empty_round_nudge(
    had_tool_round: bool,
    text: &str,
    finish_reason: Option<&str>,
) -> Option<&'static str> {
    if is_malformed_call(finish_reason) {
        let unexpected =
            finish_reason.is_some_and(|reason| reason.eq_ignore_ascii_case(UNEXPECTED_CALL));
        return Some(if unexpected {
            UNEXPECTED_CALL_NUDGE
        } else {
            MALFORMED_CALL_NUDGE
        });
    }
    if !is_blank(text) || !had_tool_round {
        return None;
    }
    if ran_out_of_room(finish_reason) {
        tracing::warn!(
            target: "roomkit::ai",
            finish_reason,
            "the response ran out of room before any answer"
        );
        return None;
    }
    Some(EMPTY_RETRY_NUDGE)
}

/// The failure of a turn constrained to a response schema whose loop ended
/// for `reason` short of its answer (RFC §6.7, Appendix A.9): Python's
/// `require_schema_answer`. Every end but `completed` and `cancelled`,
/// which someone chose, leaves the turn no checked document to deliver, its
/// last text the narration of a round. `None` for a turn under no schema,
/// or one that answered.
pub(super) fn unanswered(constrained: bool, reason: LoopEndReason) -> Option<ProviderError> {
    if !constrained || matches!(reason, LoopEndReason::Completed | LoopEndReason::Cancelled) {
        return None;
    }
    let message =
        format!("the tool loop stopped ({reason}) before a final answer in the response schema");
    let options = ResponseSchemaErrorOptions::default();
    Some(ResponseSchemaError::new(message, ResponseSchemaFailure::Truncated, options).into())
}

/// Why a loop whose round made no call to run ends there (roomkit's
/// `final_round_reason`): a call its provider did not hand over is no
/// answer; text, or a turn that ran no call, completes; after a round of
/// calls, no text reads as the response running out of room, the limit the
/// turn passed, or an empty answer.
pub(super) fn final_round_reason(
    had_tool_round: bool,
    text: &str,
    finish_reason: Option<&str>,
    limit: Option<LoopEndReason>,
) -> LoopEndReason {
    if is_malformed_call(finish_reason) {
        return limit.unwrap_or(LoopEndReason::EmptyResponse);
    }
    if !is_blank(text) || !had_tool_round {
        return LoopEndReason::Completed;
    }
    if ran_out_of_room(finish_reason) {
        return LoopEndReason::Truncated;
    }
    limit.unwrap_or(LoopEndReason::EmptyResponse)
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    /// Python's `final_round_reason`, case by case
    /// (`tasks/probes/step5-lot5/py_loop_bounds.out`).
    #[test]
    fn a_round_with_no_call_ends_for_python_s_reason() {
        let timeout = Some(LoopEndReason::Timeout);
        let cases = [
            (
                true,
                "",
                Some("MALFORMED_FUNCTION_CALL"),
                None,
                LoopEndReason::EmptyResponse,
            ),
            (
                true,
                "",
                Some("unexpected_tool_call"),
                timeout,
                LoopEndReason::Timeout,
            ),
            (true, "Answer", None, timeout, LoopEndReason::Completed),
            (false, "", None, None, LoopEndReason::Completed),
            (true, "", Some("length"), timeout, LoopEndReason::Truncated),
            (true, " ", Some("stop"), timeout, LoopEndReason::Timeout),
            (true, "", Some("stop"), None, LoopEndReason::EmptyResponse),
        ];
        for (had_tool_round, text, finish_reason, limit, expected) in cases {
            assert_eq!(
                final_round_reason(had_tool_round, text, finish_reason, limit),
                expected,
                "{text:?} {finish_reason:?}"
            );
        }
    }
}

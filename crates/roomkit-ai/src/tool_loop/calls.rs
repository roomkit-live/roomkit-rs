//! One call of a round: its gate, its tool, its hooks (RFC §9.3, §21.1,
//! §21.6).

use std::time::Duration;

use roomkit::model::{ToolCall, ToolCallOutcome};
use roomkit::{ToolCallReport, ToolGate};
use serde_json::Value;
use tokio::time::Instant;

use super::generation::{Called, Partial};
use super::{Loop, ToolEnd};
use crate::provider::AiProvider;
use crate::texts;
use crate::tools::{ToolError, ToolOutput, schema};

/// The kind a call past its bound reads as (RFC §21.6).
const TIMED_OUT: &str = "ToolTimeoutError";

/// What a call's gate decided.
enum Gated {
    /// It runs, as this call.
    Runs(ToolCall),
    /// It was refused, and ends so.
    Refused(ToolCallReport),
}

impl<P: AiProvider> Loop<P> {
    /// Serves `called`: its gate, then its tool, then the kit's hooks on a
    /// served result; reports it once to the kit's observers (RFC §9.3:
    /// every call fires `ON_TOOL_CALL` once), and ends it as the model
    /// reads it.
    pub(super) async fn serve(&self, called: &Called) -> ToolEnd {
        let started = Instant::now();
        let ended = match self.gate(called).await {
            Gated::Runs(call) => self.execute(call).await,
            Gated::Refused(refused) => refused,
        };
        let ended = match &self.turn {
            Some(turn) => turn.on_tool_call(ended).await,
            None => ended,
        };
        let result = model_text(&ended.result);
        let reported = ended.clone().ended(
            ended.outcome.unwrap_or(ToolCallOutcome::Failed),
            Value::String(result.clone()),
        );
        log_failure(&reported);
        if let Some(turn) = &self.turn {
            turn.report_tool_call(&reported);
        }
        ToolEnd {
            outcome: reported.outcome.unwrap_or(ToolCallOutcome::Failed),
            call: reported.call,
            result,
            duration: started.elapsed(),
        }
    }

    /// The call `called` may run as, or its refusal, in the order of RFC
    /// §21.1 "One refusal per cause": a call that never runs, one the turn
    /// does not carry, one the policy denies, arguments that do not fit the
    /// tool's schema, then the kit's `BEFORE_TOOL_USE` hooks, whose
    /// arguments are checked again.
    async fn gate(&self, called: &Called) -> Gated {
        let call = &called.call;
        let refuse =
            |text: String| Gated::Refused(ended(call.clone(), ToolCallOutcome::Refused, text));
        if let Some(partial) = called.partial {
            return refuse(match partial {
                Partial::Cut => texts::cut(&call.name),
                Partial::Unreadable => texts::unreadable(&call.name),
            });
        }
        let Some((tool, _)) = self.toolbox.find(&call.name) else {
            return refuse(texts::not_declared(&call.name));
        };
        if !self.toolbox.permits(&self.policy, &call.name) {
            return refuse(texts::not_permitted(&call.name));
        }
        if let Some(why) = schema::mismatch(&tool.parameters, &call.arguments) {
            return refuse(texts::invalid_arguments(&call.name, &why));
        }
        let Some(turn) = &self.turn else {
            return Gated::Runs(call.clone());
        };
        let arguments = match turn.before_tool_use(call).await {
            ToolGate::Allowed(arguments) => arguments,
            ToolGate::Refused { text, error_detail } => {
                let refused = ended(call.clone(), ToolCallOutcome::Refused, text);
                return Gated::Refused(match error_detail {
                    Some(detail) => refused.with_error_detail(detail),
                    None => refused,
                });
            }
            // A gate this loop does not know refuses: it fails closed.
            _ => return refuse(texts::error_envelope("The call was not allowed.")),
        };
        let mut gated = call.clone();
        if arguments != call.arguments {
            gated.arguments = arguments;
            if let Some(why) = schema::mismatch(&tool.parameters, &gated.arguments) {
                let text = texts::invalid_rewritten_arguments(&call.name, &why);
                return Gated::Refused(ended(gated, ToolCallOutcome::Refused, text));
            }
        }
        Gated::Runs(gated)
    }

    /// Runs `call` on the tool serving it, within the agent's bound per
    /// call (RFC §21.6), and reads its answer (RFC §9.3).
    async fn execute(&self, call: ToolCall) -> ToolCallReport {
        let Some((_, toolset)) = self.toolbox.find(&call.name) else {
            let text = texts::not_declared(&call.name);
            return ended(call, ToolCallOutcome::Refused, text);
        };
        let context = self.context.clone();
        // The tool gets its own call: its intent is the person's, as a
        // handler of the reference implementation never reads it (RFC §6.4).
        let mut handed = call.clone();
        handed.intent = None;
        let answer = toolset.call(handed, context);
        let answered = match self.bounds.tool_timeout {
            Some(bound) => tokio::time::timeout(bound, answer)
                .await
                .map_err(|_elapsed| bound),
            None => Ok(answer.await),
        };
        match answered {
            Ok(Ok(output)) => served(call, output),
            Ok(Err(error)) => failure(call, error),
            Err(bound) => timed_out(call, bound),
        }
    }
}

/// `call`, served with `output`.
fn served(call: ToolCall, output: ToolOutput) -> ToolCallReport {
    ToolCallReport::new(call).ended(ToolCallOutcome::Served, output.into_value())
}

/// A call whose tool answered `error`: a refusal's and a failure's words
/// as they are, a raised error by its kind, its message for the
/// observers (RFC §9.3).
fn failure(call: ToolCall, error: ToolError) -> ToolCallReport {
    match error {
        ToolError::Refused(words) => ended(call, ToolCallOutcome::Refused, words),
        ToolError::Unserved(message) => {
            tracing::debug!(target: "roomkit::ai", tool = %call.name, %message, "a tool declined a call");
            let text = texts::unserved(&call.name);
            ended(call, ToolCallOutcome::Unserved, text)
        }
        ToolError::Failed(words) => {
            ended(call, ToolCallOutcome::Failed, words.clone()).with_error_detail(words)
        }
        ToolError::Raised { kind, message } => {
            let text = texts::failed(&call.name, &kind);
            ended(call, ToolCallOutcome::Failed, text)
                .with_error_detail(format!("{kind}: {message}"))
        }
    }
}

/// A call whose tool did not answer within `bound`, cut: it fails as a
/// tool that raised (RFC §21.6).
fn timed_out(call: ToolCall, bound: Duration) -> ToolCallReport {
    let detail = format!(
        "{TIMED_OUT}: tool '{}' did not answer within {} s",
        call.name,
        bound.as_secs_f64()
    );
    let text = texts::failed(&call.name, TIMED_OUT);
    ended(call, ToolCallOutcome::Failed, text).with_error_detail(detail)
}

/// `call`, ended with `outcome`, the model reading `text`.
fn ended(call: ToolCall, outcome: ToolCallOutcome, text: String) -> ToolCallReport {
    ToolCallReport::new(call).ended(outcome, Value::String(text))
}

/// `result` as the model reads it: a text as it is, any other value as its
/// JSON (RFC §21.4).
fn model_text(result: &Value) -> String {
    match result {
        Value::String(text) => text.clone(),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::Array(_) | Value::Object(_) => {
            result.to_string()
        }
    }
}

/// Logs a call that did not serve, with what failed (RFC §9.3: the
/// message goes to the log and the observers).
fn log_failure(report: &ToolCallReport) {
    if report.outcome == Some(ToolCallOutcome::Served) {
        return;
    }
    tracing::info!(
        target: "roomkit::ai",
        tool = %report.call.name,
        call = %report.call.id,
        outcome = ?report.outcome,
        detail = report.error_detail.as_deref().unwrap_or_default(),
        "a tool call did not serve"
    );
}

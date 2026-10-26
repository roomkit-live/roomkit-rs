//! One call of a round: its gate, its tool, its hooks (RFC §9.3, §21.1,
//! §21.6).

use std::time::Duration;

use roomkit::model::{ToolCall, ToolCallOutcome};
use roomkit::{ToolCallEvent, ToolGate};
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
    Refused(ToolCallEvent),
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
            |text: String| Gated::Refused(self.event(call.clone(), ToolCallOutcome::Refused, text));
        if let Some(partial) = called.partial {
            return refuse(match partial {
                Partial::Cut => texts::cut(&call.name),
                Partial::Unreadable => texts::unreadable(&call.name),
            });
        }
        let Some((tool, _)) = self.toolbox.find(&call.name) else {
            return refuse(texts::not_declared(&call.name));
        };
        if !self.policy.permits(&call.name) {
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
                let refused = self.event(call.clone(), ToolCallOutcome::Refused, text);
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
                return Gated::Refused(self.event(gated, ToolCallOutcome::Refused, text));
            }
        }
        Gated::Runs(gated)
    }

    /// Runs `call` on the tool serving it, within the agent's bound per
    /// call (RFC §21.6), and reads its answer (RFC §9.3).
    async fn execute(&self, call: ToolCall) -> ToolCallEvent {
        let Some((_, toolset)) = self.toolbox.find(&call.name) else {
            let text = texts::not_declared(&call.name);
            return self.event(call, ToolCallOutcome::Refused, text);
        };
        let context = self.context.clone();
        let answer = toolset.call(call.clone(), context);
        let answered = match self.bounds.tool_timeout {
            Some(bound) => tokio::time::timeout(bound, answer)
                .await
                .map_err(|_elapsed| bound),
            None => Ok(answer.await),
        };
        match answered {
            Ok(Ok(output)) => self.served(call, output),
            Ok(Err(error)) => self.failure(call, error),
            Err(bound) => self.timed_out(call, bound),
        }
    }

    fn served(&self, call: ToolCall, output: ToolOutput) -> ToolCallEvent {
        self.event_with(call, ToolCallOutcome::Served, output.into_value())
    }

    /// A call whose tool answered `error`: a refusal's and a failure's words
    /// as they are, a raised error by its kind, its message for the
    /// observers (RFC §9.3).
    fn failure(&self, call: ToolCall, error: ToolError) -> ToolCallEvent {
        match error {
            ToolError::Refused(words) => self.event(call, ToolCallOutcome::Refused, words),
            ToolError::Failed(words) => self
                .event(call, ToolCallOutcome::Failed, words.clone())
                .with_error_detail(words),
            ToolError::Raised { kind, message } => {
                let text = texts::failed(&call.name, &kind);
                self.event(call, ToolCallOutcome::Failed, text)
                    .with_error_detail(format!("{kind}: {message}"))
            }
        }
    }

    /// A call whose tool did not answer within `bound`, cut: it fails as a
    /// tool that raised (RFC §21.6).
    fn timed_out(&self, call: ToolCall, bound: Duration) -> ToolCallEvent {
        let detail = format!(
            "{TIMED_OUT}: tool '{}' did not answer within {} s",
            call.name,
            bound.as_secs_f64()
        );
        let text = texts::failed(&call.name, TIMED_OUT);
        self.event(call, ToolCallOutcome::Failed, text)
            .with_error_detail(detail)
    }

    /// `call`, ended with `outcome`, the model reading `text`.
    fn event(&self, call: ToolCall, outcome: ToolCallOutcome, text: String) -> ToolCallEvent {
        self.event_with(call, outcome, Value::String(text))
    }

    fn event_with(&self, call: ToolCall, outcome: ToolCallOutcome, result: Value) -> ToolCallEvent {
        ToolCallEvent::new(
            self.context.channel_id.clone(),
            self.context.room_id.clone(),
            call,
        )
        .ended(outcome, result)
    }
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
fn log_failure(event: &ToolCallEvent) {
    if !event.is_error() {
        return;
    }
    tracing::info!(
        target: "roomkit::ai",
        tool = %event.call.name,
        call = %event.call.id,
        outcome = ?event.outcome,
        detail = event.error_detail.as_deref().unwrap_or_default(),
        "a tool call did not serve"
    );
}

//! A pending external call: channel refusals precede the handler's decision.

use roomkit::ToolCallReport;
use roomkit::model::{ToolCall, ToolCallOutcome};
use serde_json::Value;

use super::ProviderCalls;
use crate::provider::StreamToolCall;
use crate::texts;
use crate::tools::{ExternalToolHandler, Occurrence, ProcessToolCallOptions, ToolError};

pub(super) struct Decided {
    pub(super) report: ToolCallReport,
    pub(super) by_channel: bool,
}

impl Decided {
    fn new(call: ToolCall, outcome: ToolCallOutcome, text: String, by_channel: bool) -> Self {
        Self {
            report: ToolCallReport::new(call).ended(outcome, Value::String(text)),
            by_channel,
        }
    }
}

impl ProviderCalls {
    pub(super) async fn settle(
        &self,
        call: &StreamToolCall,
        entry: &Occurrence,
        steering: &crate::steering::Steering,
    ) -> Option<Decided> {
        // The duplicate verdict precedes channel close, as Python's _settle does.
        if entry.duplicate() {
            let ran = ToolCall::new(call.id.clone(), call.name.clone(), call.arguments.clone());
            return Some(Decided::new(
                ran,
                ToolCallOutcome::Refused,
                texts::call_id_in_flight(&call.id),
                true,
            ));
        }
        let handler = self.handler.as_deref()?;
        steering
            .lifetime()
            .call(Box::pin(self.decide(call, handler)))
            .await
    }

    async fn decide(&self, call: &StreamToolCall, handler: &dyn ExternalToolHandler) -> Decided {
        let mut ran = ToolCall::new(call.id.clone(), call.name.clone(), call.arguments.clone());
        if call.partial {
            let text = if call.garbled {
                texts::unreadable(&call.name)
            } else {
                texts::cut(&call.name)
            };
            return Decided::new(ran, ToolCallOutcome::Refused, text, true);
        }
        let decision = handler
            .process_tool_call(
                &call.name,
                &call.arguments,
                ProcessToolCallOptions {
                    tool_call_id: call.id.clone(),
                    room_id: Some(self.room_id.clone()),
                    ..ProcessToolCallOptions::default()
                },
            )
            .await;
        let decision = match decision {
            Ok(decision) => decision,
            Err(error) => return decision_failed(ran, error),
        };
        if !decision.approved {
            let reason = if decision.reason.is_empty() {
                format!("Tool '{}' was denied", call.name)
            } else {
                decision.reason
            };
            let mut decided = Decided::new(
                ran,
                ToolCallOutcome::Refused,
                texts::error_envelope(&reason),
                false,
            );
            decided.report.error_detail = decision.detail;
            return decided;
        }
        if let Some(arguments) = decision.modified_input {
            ran.arguments = arguments;
        }
        Decided::new(
            ran,
            ToolCallOutcome::Served,
            decision.result.unwrap_or_default(),
            false,
        )
    }
}

/// Exceptions from deciding never become the model's private error message.
fn decision_failed(call: ToolCall, error: ToolError) -> Decided {
    let (kind, message) = match error {
        ToolError::Raised { kind, message } => (kind, message),
        ToolError::Refused(message) => ("ToolRefusedError".to_owned(), message),
        ToolError::UnservedToolCall(message) => ("UnservedToolCallError".to_owned(), message),
        ToolError::Failed(message) => ("RuntimeError".to_owned(), message),
    };
    let detail = format!("{kind}: {message}");
    tracing::warn!(target: "roomkit::ai", tool = %call.name, %detail, "External tool handler failed deciding a call");
    let text = texts::failed(&call.name, &kind);
    let mut decided = Decided::new(call, ToolCallOutcome::Failed, text, true);
    decided.report.error_detail = Some(detail);
    decided
}

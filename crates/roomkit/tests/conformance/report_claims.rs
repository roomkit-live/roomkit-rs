//! Scripted handlers call the public report callback, never simulated state.

use std::sync::{Arc, Mutex, Weak};

use roomkit_ai::{ToolResult, current_tool_call, turn_report_claim};
use serde_json::{Value, json};

use crate::run::Run;

type Claim = Arc<dyn Fn() -> bool + Send + Sync>;

#[derive(Clone, Default)]
pub(super) struct ReportClaims(Arc<Mutex<Option<Claim>>>);

impl ReportClaims {
    pub(super) fn answer(&self, mode: &str) -> ToolResult {
        let call = current_tool_call().unwrap();
        let channel = call.channel_id.as_ref().unwrap().as_str();
        let claim = turn_report_claim(&call.tool_call_id, channel).unwrap();
        let result = match mode {
            "matching" => {
                let second = turn_report_claim(&call.tool_call_id, channel).unwrap();
                json!([
                    turn_report_claim("missing", channel).is_some(),
                    turn_report_claim(&call.tool_call_id, "other").is_some(),
                    claim(),
                    second(),
                    claim(),
                ])
            }
            "previous_turn" => {
                let mut first = self.0.lock().unwrap();
                let previous = first.as_ref().map(|claim| claim());
                first.get_or_insert(claim);
                json!([previous])
            }
            _ => {
                assert_eq!(mode, "reused_id", "unknown report claim probe");
                let mut first = self.0.lock().unwrap();
                let first = first.get_or_insert_with(|| Arc::clone(&claim));
                json!([first(), claim()])
            }
        };
        ToolResult::Json(result)
    }
}

/// `claim_then_gate`: claims the report of the call running now, then holds
/// the call at its `gate`, so that a cut there falls between the call's
/// report and its end row (RFC §9.3); the claim's result once the gate
/// opens.
pub(super) async fn claim_then_gate(behaviour: &Value, run: &Weak<Run>) -> ToolResult {
    let call = current_tool_call().unwrap();
    let channel = call.channel_id.as_ref().unwrap().as_str();
    let claimed = turn_report_claim(&call.tool_call_id, channel).unwrap()();
    if let (Some(gate), Some(run)) = (behaviour["gate"].as_str(), run.upgrade()) {
        run.gates.hold(gate).await;
    }
    ToolResult::Json(json!([claimed]))
}

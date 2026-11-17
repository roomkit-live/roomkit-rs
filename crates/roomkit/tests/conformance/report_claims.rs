//! Scripted handlers call the public report callback, never simulated state.

use std::sync::{Arc, Mutex};

use roomkit_ai::{ToolResult, current_tool_call, turn_report_claim};
use serde_json::json;

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

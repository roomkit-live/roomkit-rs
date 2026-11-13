//! One ACP call's merged state and the three views of its result.

use roomkit::ToolCallReport;
use roomkit::model::{Metadata, ToolCall, ToolCallContent, ToolCallOutcome, ToolCallStatus};
use serde_json::{Value, json};
use tokio::time::Instant;

use super::encoding::{display_content, result_text};

pub(super) const TURN_ENDED: &str = "The ACP turn ended before the tool reported a result";

pub(super) struct Call {
    pub(super) request: ToolCall,
    pub(super) started: bool,
    pub(super) finished: bool,
    since: Instant,
    result: Value,
    display: Option<Value>,
}

impl Call {
    pub(super) fn new(id: &str) -> Self {
        Self {
            request: ToolCall::new(id, "tool", Metadata::new()),
            started: false,
            finished: false,
            since: Instant::now(),
            result: Value::Null,
            display: None,
        }
    }

    pub(super) fn merge(&mut self, update: &Value) {
        if let Some(title) = update
            .get("title")
            .and_then(Value::as_str)
            .filter(|title| !title.is_empty())
        {
            title.clone_into(&mut self.request.name);
        } else if self.request.name == "tool"
            && let Some(kind) = update.get("kind").and_then(Value::as_str)
        {
            kind.clone_into(&mut self.request.name);
        }
        if let Some(input) = update.get("rawInput").filter(|v| !v.is_null()) {
            self.request.arguments = if let Value::Object(fields) = input {
                fields.clone()
            } else {
                Metadata::from_iter([("value".to_owned(), input.clone())])
            };
        }
        if let Some(result) = update.get("rawOutput").filter(|v| !v.is_null()) {
            self.result = result.clone();
        }
        if let Some(display) = update.get("content").filter(|v| !v.is_null()) {
            self.display = Some(display_content(display.clone()));
        }
    }

    pub(super) fn start(&self) -> ToolCallContent {
        let mut row = ToolCallContent::new(&self.request.name, &self.request.id);
        row.arguments.clone_from(&self.request.arguments);
        row
    }

    pub(super) fn end(&mut self, failed: bool, interrupted: bool) -> End {
        self.finished = true;
        let outcome = match (failed, interrupted) {
            (_, true) => ToolCallOutcome::Cancelled,
            (true, false) => ToolCallOutcome::Failed,
            (false, false) => ToolCallOutcome::Served,
        };
        let row = self.end_row(outcome);
        let body = if interrupted {
            // Cancellation's envelope uses Python's default ASCII escaping.
            roomkit_ai::dumps(
                &json!({"error": "Tool call cancelled", "tool": self.request.name,
                "hint": "The turn ended before its result."}),
            )
        } else if failed {
            row.error.clone().unwrap_or_default()
        } else {
            result_text(&row.result)
        };
        let report = ToolCallReport::new(self.request.clone()).ended(outcome, Value::String(body));
        let preview = row
            .error
            .clone()
            .filter(|text| !text.is_empty())
            .unwrap_or_else(|| result_text(&row.result))
            .chars()
            .take(500)
            .collect();
        End {
            row,
            report,
            preview,
        }
    }

    fn end_row(&self, outcome: ToolCallOutcome) -> ToolCallContent {
        let failed = outcome != ToolCallOutcome::Served;
        let interrupted = outcome == ToolCallOutcome::Cancelled;
        let result = if self.result.is_null() {
            self.display.clone().unwrap_or(Value::Null)
        } else {
            self.result.clone()
        };
        let error = if interrupted {
            Some(TURN_ENDED.to_owned())
        } else {
            (failed && !result.is_null()).then(|| result_text(&result))
        };
        let mut row = self.start();
        row.result = result;
        row.error = error;
        row.status = if failed {
            ToolCallStatus::Failed
        } else {
            ToolCallStatus::Completed
        };
        row.outcome = Some(outcome);
        row.duration_ms = Some(u64::try_from(self.since.elapsed().as_millis()).unwrap_or(u64::MAX));
        row.structured_content = self
            .display
            .as_ref()
            .map(|display| Metadata::from_iter([("acp_content".to_owned(), display.clone())]));
        row
    }
}

pub(super) struct End {
    pub(super) row: ToolCallContent,
    pub(super) report: ToolCallReport,
    pub(super) preview: String,
}

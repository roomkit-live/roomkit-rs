//! Share the event payload bound with channels whose tools run externally.

use serde_json::Value;

use super::ToolResult;

impl ToolResult {
    /// The external tool's stored payload, under the same shared binary
    /// budget as the local loop's rows (RFC §21.5), display copy first.
    /// Technical bridge for ACP; it does not change the agent's own result.
    #[doc(hidden)]
    #[must_use]
    pub fn into_external_event_payload(self, structured: Option<Value>) -> (Value, Option<Value>) {
        self.into_row_payload(structured)
    }
}

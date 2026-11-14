//! MCP's optional copy of a successful result (Python's `_publish_structured_content`).

use rmcp::model::CallToolResult;
use roomkit::model::ResponseMetadata;
use roomkit_ai::{current_tool_call, dumps};
use serde_json::Value;

/// The copy is bounded independently of the model's text. Python counts its
/// default JSON rendering: spaces and ASCII escapes, including surrogate pairs.
const MAX_CHARS: usize = 512 * 1024;

pub(crate) fn publish(result: &CallToolResult) {
    if result.is_error == Some(true) {
        return;
    }
    let Some(Value::Object(copy)) = &result.structured_content else {
        return;
    };
    let Some(call) = current_tool_call() else {
        return;
    };
    if dumps(&Value::Object(copy.clone())).len() > MAX_CHARS {
        return;
    }
    call.structured_content
        .replace(Some(ResponseMetadata::from(copy.clone())));
}

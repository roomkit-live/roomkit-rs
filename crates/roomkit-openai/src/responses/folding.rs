//! Responses events: text arrives immediately, calls wait for a terminal snapshot.

use super::wire;
use crate::error::OpenAiError;
use roomkit_ai::StreamEvent;

use crate::request::{push_end, push_text, push_thinking};
use serde_json::Value;
use std::collections::VecDeque;

#[derive(Default)]
pub(crate) struct Folding {
    text: String,
    /// Whether the reasoning came as it was written: the terminal response
    /// then has nothing to add of it.
    reasoned: bool,
    items: super::items::Items,
    pub(crate) namespace: Option<String>,
}

impl Folding {
    pub(crate) fn take(
        &mut self,
        data: &str,
        secret: &str,
        ready: &mut VecDeque<StreamEvent>,
    ) -> Result<bool, OpenAiError> {
        if data == "[DONE]" {
            return Err(wire::malformed(
                "Responses stream ended without a terminal response",
            ));
        }
        let value: Value = serde_json::from_str(data)
            .map_err(|_| wire::malformed("invalid Responses event JSON"))?;
        match value.get("type").and_then(Value::as_str) {
            Some("response.output_item.added") => self.items.record(&value, false)?,
            Some("response.output_item.done") => self.items.record(&value, true)?,
            Some("response.output_text.delta" | "response.refusal.delta") => {
                let delta = value
                    .get("delta")
                    .and_then(Value::as_str)
                    .ok_or_else(|| wire::malformed("Responses text delta is not text"))?;
                self.text.push_str(delta);
                push_text(ready, delta.to_owned());
            }
            Some("response.reasoning_summary_text.delta" | "response.reasoning_text.delta") => {
                let delta = value
                    .get("delta")
                    .and_then(Value::as_str)
                    .ok_or_else(|| wire::malformed("Responses reasoning delta is not text"))?;
                self.reasoned = true;
                push_thinking(ready, delta.to_owned());
            }
            Some("response.completed" | "response.incomplete") => {
                let mut response = value
                    .get("response")
                    .ok_or_else(|| wire::malformed("terminal Responses event has no response"))?
                    .clone();
                let expected = value
                    .get("type")
                    .and_then(Value::as_str)
                    .and_then(|kind| kind.strip_prefix("response."));
                if response.get("status").and_then(Value::as_str) != expected {
                    return Err(wire::malformed(
                        "Responses terminal event and status disagree",
                    ));
                }
                self.items.finish(&mut response)?;
                wire::validate_namespace(&response, self.namespace.as_deref())?;
                let whole = wire::response(&response, secret)?;
                let suffix = whole.text.strip_prefix(self.text.as_str()).ok_or_else(|| {
                    wire::malformed("Responses terminal text differs from its streamed text")
                })?;
                if !self.reasoned {
                    push_thinking(ready, whole.thinking.clone().unwrap_or_default());
                }
                if !suffix.is_empty() {
                    push_text(ready, suffix.to_owned());
                }
                push_end(ready, whole.calls, whole.done);
                return Ok(true);
            }
            Some("response.failed" | "response.cancelled") => {
                return Err(wire::failed(
                    value.get("response").unwrap_or(&value),
                    secret,
                ));
            }
            Some("error") => return Err(wire::failed(&value, secret)),
            Some(_) => {} // Lifecycle/composition events; the terminal snapshot owns calls.
            None => return Err(wire::malformed("Responses event has no type")),
        }
        Ok(false)
    }
}

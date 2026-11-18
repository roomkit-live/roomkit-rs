//! Project the synchronous chain to the verdict a channel applies and the event its observers read.
use super::Notification;
use crate::model::tool_verdict::{VerdictList, VerdictObject, VerdictResult, VerdictValue};
use crate::turn::ToolCallChain;
use crate::{ToolCallEvent, ToolCallVerdict};
use serde_json::Value;

impl Notification {
    pub(super) async fn decide(&self, event: ToolCallEvent) -> (ToolCallVerdict, ToolCallEvent) {
        let mut chain = ToolCallChain::new(event.clone());
        if let Some(context) = &self.context
            && let Some(reason) = chain
                .run(&self.hooks, context, |spec, ran| self.timeout(spec, ran))
                .await
        {
            return withheld(event, &reason);
        }
        let detail = chain
            .current
            .result
            .is_none()
            .then(|| chain.errors_detail())
            .flatten();
        let mut observed = chain.current;
        let replacement = chain.replaced.then(|| {
            let (result, read) = model_result(observed.result.take().unwrap_or(Value::Null));
            observed.result = Some(read);
            result
        });
        let replaces_copy = chain.replaced_copy
            && (event.structured_content.is_some() || observed.structured_content.is_some());
        let copy = observed
            .structured_content
            .as_ref()
            .and_then(Value::as_object)
            .map(|fields| {
                let object = VerdictObject::default();
                for (key, value) in fields {
                    object.insert(key, VerdictValue::Json(value.clone()));
                }
                object
            });
        (
            ToolCallVerdict::new(replacement, false, replaces_copy, copy, detail),
            observed,
        )
    }
}

pub(super) fn withheld(mut event: ToolCallEvent, reason: &str) -> (ToolCallVerdict, ToolCallEvent) {
    let reason = crate::tool_calls::error_envelope(reason);
    event.result = Some(Value::String(reason.clone()));
    event.structured_content = None;
    event.is_error = true;
    (
        ToolCallVerdict::new(Some(VerdictResult::Text(reason)), true, false, None, None),
        event,
    )
}

fn model_result(value: Value) -> (VerdictResult, Value) {
    if let Value::Array(items) = &value
        && let Some(parts) = crate::turns::wire::WireResult::strict_parts(items)
    {
        let result = VerdictList::new(parts.iter().cloned().map(VerdictValue::Json).collect());
        return (VerdictResult::Parts(result), Value::Array(parts));
    }
    let text = if let Value::String(text) = value {
        text
    } else {
        let mut text = String::new();
        crate::tasks::json::json(&mut text, &value, false);
        text
    };
    (VerdictResult::Text(text.clone()), Value::String(text))
}

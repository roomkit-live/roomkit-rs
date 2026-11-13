//! Scripted decisions through the public external-handler trait.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, Weak};

use roomkit::model::Metadata;
use roomkit_ai::{
    ExternalToolFuture, ExternalToolHandler, ExternalToolHandlerState, OnToolCancelledOptions,
    OnToolRefusedOptions, OnToolResultOptions, PolicyExternalToolHandler, ProcessToolCallOptions,
    ToolDecision, ToolError, ToolPolicy,
};
use serde::Deserialize;
use serde_json::{Value, json};

use super::Wire;
use crate::calls::Unreadable;
use crate::run::{Run, lock};

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct Policy {
    #[serde(default)]
    allow: Vec<String>,
    #[serde(default)]
    deny: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Decided {
    approved: bool,
    #[serde(default)]
    modified_input: Option<Metadata>,
    #[serde(default)]
    result: Option<String>,
    #[serde(default)]
    reason: String,
    #[serde(default)]
    detail: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Raised {
    raise: String,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Reply {
    Decided(Decided),
    Raised(Raised),
}

#[derive(Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Reports {
    #[default]
    Delegate,
    RaiseBefore,
    RaiseAfter,
    Skip,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Spec {
    #[serde(default)]
    policy: Option<Policy>,
    #[serde(default)]
    decisions: Option<VecDeque<Reply>>,
    #[serde(default)]
    reports: Reports,
}

fn parse(value: &Value) -> Result<Spec, String> {
    if !value.is_object()
        || value
            .get("decisions")
            .and_then(Value::as_array)
            .is_some_and(|ds| ds.iter().any(|d| !d.is_object()))
        || value.get("policy").is_some_and(|p| !p.is_object())
        || value.get("decisions").is_some_and(|d| !d.is_array())
    {
        return Err("expected a policy object or decisions array".to_owned());
    }
    serde_json::from_value(value.clone()).map_err(|e| e.to_string())
}

pub(crate) fn validate(acp: &Value, path: &str, problems: &mut Vec<String>) {
    if let Some(value) = acp.get("external_tool_handler")
        && let Err(error) = parse(value)
    {
        problems.push(format!("{path}.external_tool_handler: {error}"));
    }
}

pub(crate) fn build(
    acp: &Value,
    channel: &str,
    run: &Arc<Run>,
) -> Result<Option<Arc<dyn ExternalToolHandler>>, Unreadable> {
    let Some(value) = acp.get("external_tool_handler") else {
        return Ok(None);
    };
    let spec = parse(value).map_err(|e| format!("acp.external_tool_handler: {e}"))?;
    let calls = Wire::default();
    lock(&run.acp_handlers).insert(channel.to_owned(), Arc::clone(&calls));
    Ok(Some(Arc::new(Scripted {
        policy: PolicyExternalToolHandler::new(roomkit_ai::PolicyExternalToolHandlerOptions {
            policy: spec.policy.map(|p| ToolPolicy {
                allow: p.allow,
                deny: p.deny,
            }),
        }),
        replies: Mutex::new(spec.decisions),
        reports: spec.reports,
        calls,
        run: Arc::downgrade(run),
    })))
}

struct Scripted {
    policy: PolicyExternalToolHandler,
    replies: Mutex<Option<VecDeque<Reply>>>,
    reports: Reports,
    calls: Wire,
    run: Weak<Run>,
}

impl Scripted {
    fn record(&self, method: &str, mut data: Value) {
        data["method"] = json!(method);
        data["channel_id"] = json!(self.channel_id());
        lock(&self.calls).push(data);
    }

    fn next(&self) -> Option<Result<ToolDecision, ToolError>> {
        let mut replies = lock(&self.replies);
        let replies = replies.as_mut()?;
        Some(match replies.pop_front() {
            Some(Reply::Decided(d)) => Ok(ToolDecision {
                approved: d.approved,
                modified_input: d.modified_input,
                result: d.result,
                reason: d.reason,
                detail: d.detail,
            }),
            Some(Reply::Raised(d)) => Err(ToolError::raised_as("RuntimeError", d.raise)),
            None => {
                if let Some(run) = self.run.upgrade() {
                    run.problem("external_tool_handler.decisions exhausted".to_owned());
                }
                Err(ToolError::raised_as(
                    "RuntimeError",
                    "external handler fixture exhausted",
                ))
            }
        })
    }

    async fn report(
        &self,
        delegate: impl Future<Output = Result<(), ToolError>>,
    ) -> Result<(), ToolError> {
        if self.reports == Reports::RaiseBefore {
            return Err(ToolError::raised_as("RuntimeError", "audit sink down"));
        }
        if self.reports != Reports::Skip {
            delegate.await?;
        }
        if self.reports == Reports::RaiseAfter {
            return Err(ToolError::raised_as("RuntimeError", "audit sink down"));
        }
        Ok(())
    }
}

impl ExternalToolHandler for Scripted {
    fn state(&self) -> &ExternalToolHandlerState {
        self.policy.state()
    }

    fn start(&self) -> ExternalToolFuture<'_, ()> {
        Box::pin(async {
            self.record("start", json!({}));
            Ok(())
        })
    }
    fn stop(&self) -> ExternalToolFuture<'_, ()> {
        Box::pin(async {
            self.record("stop", json!({}));
            Ok(())
        })
    }

    fn process_tool_call<'a>(
        &'a self,
        name: &'a str,
        arguments: &'a Metadata,
        options: ProcessToolCallOptions,
    ) -> ExternalToolFuture<'a, ToolDecision> {
        Box::pin(async move {
            self.record("process_tool_call", json!({"name": name, "arguments": arguments,
                "tool_call_id": options.tool_call_id, "room_id": options.room_id, "session_id": options.session_id}));
            match self.next() {
                Some(decision) => decision,
                None => {
                    self.policy
                        .process_tool_call(name, arguments, options)
                        .await
                }
            }
        })
    }

    fn on_tool_result<'a>(
        &'a self,
        name: &'a str,
        arguments: &'a Metadata,
        result: &'a str,
        options: OnToolResultOptions,
    ) -> ExternalToolFuture<'a, ()> {
        Box::pin(async move {
            self.record("on_tool_result", json!({"name": name, "arguments": arguments, "result": result,
                "tool_call_id": options.tool_call_id, "room_id": options.room_id,
                "is_error": options.is_error, "refused_but_ran": options.refused_but_ran, "error_detail": options.error_detail}));
            self.report(self.policy.on_tool_result(name, arguments, result, options))
                .await
        })
    }

    fn on_tool_refused<'a>(
        &'a self,
        name: &'a str,
        arguments: &'a Metadata,
        reason: &'a str,
        options: OnToolRefusedOptions,
    ) -> ExternalToolFuture<'a, ()> {
        Box::pin(async move {
            self.record("on_tool_refused", json!({"name": name, "arguments": arguments, "reason": reason,
                "tool_call_id": options.tool_call_id, "room_id": options.room_id, "detail": options.detail}));
            self.report(
                self.policy
                    .on_tool_refused(name, arguments, reason, options),
            )
            .await
        })
    }

    fn on_tool_cancelled<'a>(
        &'a self,
        name: &'a str,
        arguments: &'a Metadata,
        options: OnToolCancelledOptions,
    ) -> ExternalToolFuture<'a, ()> {
        Box::pin(async move {
            self.record(
                "on_tool_cancelled",
                json!({"name": name, "arguments": arguments,
                "tool_call_id": options.tool_call_id, "room_id": options.room_id}),
            );
            self.report(self.policy.on_tool_cancelled(name, arguments, options))
                .await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handler_fixture_rejects_malformed_controls() {
        for spec in [
            json!(null),
            json!([]),
            json!({"decision":{}}),
            json!({"decisions":null}),
            json!({"decisions":[{}]}),
            json!({"decisions":[{"approved":"false"}]}),
            json!({"decisions":[{"approved":true,"result":1}]}),
            json!({"decisions":[{"raise":"broken","approved":true}]}),
            json!({"policy":[]}),
            json!({"policy":{"deny":"Read"}}),
            json!({"reports":"ignore"}),
        ] {
            assert!(parse(&spec).is_err(), "{spec}");
        }
    }
}

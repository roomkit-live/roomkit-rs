//! Await real ACP permission replies before the scripted agent continues.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::{Value, json};
use tokio::sync::oneshot;

use super::{Wire, Writer, send};
use crate::run::lock;

#[derive(Default)]
pub(crate) struct Exchanges {
    pub(crate) received: Wire,
    pending: Mutex<HashMap<String, Pending>>,
    next_id: AtomicU64,
}

struct Pending {
    session: Value,
    tool_id: Value,
    done: oneshot::Sender<()>,
}

impl Exchanges {
    pub(super) fn accept(&self, message: &Value) {
        let Some(id) = message["id"].as_str() else {
            return;
        };
        let Some(pending) = lock(&self.pending).remove(id) else {
            return;
        };
        if pending.done.is_closed() {
            return;
        }
        let mut record = json!({"session_id": pending.session, "tool_call_id": pending.tool_id});
        for key in ["result", "error"] {
            if let Some(value) = message.get(key) {
                record[key] = value.clone();
            }
        }
        lock(&self.received).push(record);
        let _gone = pending.done.send(());
    }

    pub(super) async fn exchange(&self, session: &Value, request: &Value, writer: &Writer) {
        let id = format!(
            "awaited-permission-{}",
            self.next_id.fetch_add(1, Ordering::Relaxed)
        );
        let (done, waiting) = oneshot::channel();
        lock(&self.pending).insert(
            id.clone(),
            Pending {
                session: session.clone(),
                tool_id: request["toolCall"]["toolCallId"].clone(),
                done,
            },
        );
        let _registration = Registration(self, id.clone());
        let mut params = request.clone();
        params["sessionId"] = session.clone();
        send(
            writer,
            &json!({"jsonrpc":"2.0", "id":id,
            "method":"session/request_permission", "params":params}),
        )
        .await;
        let _gone = waiting.await;
    }
}

struct Registration<'a>(&'a Exchanges, String);

impl Drop for Registration<'_> {
    fn drop(&mut self) {
        lock(&self.0.pending).remove(&self.1);
    }
}

pub(crate) fn validate(prompt: &Value, path: &str, found: &mut Vec<String>) {
    let Some(steps) = prompt.get("permission_steps") else {
        return;
    };
    let Some(steps) = steps.as_array() else {
        found.push(format!("{path}.permission_steps: expected a list"));
        return;
    };
    for (index, step) in steps.iter().enumerate() {
        let at = format!("{path}.permission_steps[{index}]");
        if !valid_step(step) {
            found.push(format!(
                "{at}: expected an ACP request and optional updates"
            ));
        }
    }
}

fn valid_step(step: &Value) -> bool {
    let Some(fields) = step.as_object() else {
        return false;
    };
    if fields
        .keys()
        .any(|key| !["request", "updates"].contains(&key.as_str()))
    {
        return false;
    }
    let request = &step["request"];
    request.as_object().is_some_and(|fields| {
        fields
            .keys()
            .all(|key| ["toolCall", "options"].contains(&key.as_str()))
    }) && request["toolCall"]["toolCallId"].is_string()
        && request["options"]
            .as_array()
            .is_some_and(|options| options.iter().all(valid_option))
        && step.get("updates").is_none_or(|updates| {
            updates
                .as_array()
                .is_some_and(|values| values.iter().all(Value::is_object))
        })
}

fn valid_option(option: &Value) -> bool {
    option["optionId"].is_string()
        && option["name"].is_string()
        && option["kind"].as_str().is_some_and(|kind| {
            ["allow_once", "allow_always", "reject_once", "reject_always"].contains(&kind)
        })
}

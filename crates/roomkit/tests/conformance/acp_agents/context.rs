//! Host context from a script, never a replacement prompt composer.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, Weak};

use roomkit::model::{RoomContext, RoomEvent};
use roomkit_acp::{AcpContextContributor, AcpError};
use serde_json::{Value, json};

use super::Wire;
use crate::calls::Unreadable;
use crate::run::{Run, lock};

#[derive(Clone)]
struct Reply {
    blocks: Result<Vec<String>, String>,
    gate: Option<String>,
}

fn parse(value: &Value) -> Result<Vec<Reply>, String> {
    let object = value
        .as_object()
        .filter(|object| object.len() == 1 && object.contains_key("responses"))
        .ok_or("expected a responses script")?;
    let responses = object["responses"]
        .as_array()
        .ok_or("responses must be an array")?;
    responses
        .iter()
        .map(|reply| {
            let members = reply
                .as_object()
                .filter(|object| {
                    object
                        .keys()
                        .all(|key| matches!(key.as_str(), "blocks" | "error" | "gate"))
                })
                .ok_or("invalid context response")?;
            if members.contains_key("blocks") == members.contains_key("error") {
                return Err("expected blocks or error".into());
            }
            let gate = reply
                .get("gate")
                .map(|gate| {
                    gate.as_str()
                        .map(str::to_owned)
                        .ok_or("gate must be a string")
                })
                .transpose()?;
            let blocks = if let Some(blocks) = reply.get("blocks") {
                Ok(blocks
                    .as_array()
                    .ok_or("blocks must be an array")?
                    .iter()
                    .map(|block| {
                        block
                            .as_str()
                            .map(str::to_owned)
                            .ok_or("blocks must be strings")
                    })
                    .collect::<Result<Vec<_>, _>>()?)
            } else {
                Err(reply["error"]
                    .as_str()
                    .ok_or("error must be a string")?
                    .to_owned())
            };
            Ok(Reply { blocks, gate })
        })
        .collect()
}

pub(crate) fn history(acp: &Value) -> Result<i64, String> {
    acp.get("room_history").map_or(Ok(20), |value| {
        value
            .as_i64()
            .filter(|value| *value >= 0)
            .ok_or_else(|| "room_history must be a nonnegative integer".to_owned())
    })
}

pub(crate) fn validate(acp: &Value, path: &str, found: &mut Vec<String>) {
    if let Err(error) = history(acp) {
        found.push(format!("{path}.{error}"));
    }
    if let Some(value) = acp.get("context_contributor")
        && let Err(error) = parse(value)
    {
        found.push(format!("{path}.context_contributor: {error}"));
    }
}

struct Callback {
    replies: Mutex<VecDeque<Reply>>,
    requests: Wire,
    run: Weak<Run>,
}

#[cfg(test)]
mod tests {
    use super::*;

    // Python: tests/conformance/test_acp_context_fixture.py
    #[test]
    fn malformed_context_and_history_are_fixture_failures() {
        for spec in [
            json!(null),
            json!({}),
            json!({"responses":null}),
            json!({"responses":[null]}),
            json!({"responses":[{"blocks":"not a list"}]}),
            json!({"responses":[{"blocks":[12]}]}),
            json!({"responses":[{"blocks":[],"error":"both"}]}),
            json!({"responses":[{"error":12}]}),
            json!({"responses":[{"blocks":[],"gate":null}]}),
            json!({"responses":[{"blocks":[],"unknown":true}]}),
        ] {
            assert!(parse(&spec).is_err(), "{spec}");
        }
        for value in [json!(true), json!(-1), json!("20"), json!(0.5), json!(null)] {
            assert!(history(&json!({"room_history":value})).is_err(), "{value}");
        }
    }
}

impl Callback {
    async fn resolve(
        &self,
        context: RoomContext,
        event: RoomEvent,
    ) -> Result<Vec<String>, AcpError> {
        let reply = {
            let mut requests = lock(&self.requests);
            requests.push(json!({
                "room_id": context.room.id,
                "event": event,
                "recent_events": context.recent_events,
                "history_start_index": context.history_start_index,
            }));
            lock(&self.replies).pop_front().ok_or_else(|| {
                let error = format!(
                    "acp.context_contributor: callback script exhausted at call {}",
                    requests.len()
                );
                if let Some(run) = self.run.upgrade() {
                    run.problem(error.clone());
                }
                AcpError::Runtime(error)
            })?
        };
        if let Some(gate) = reply.gate {
            let run = self
                .run
                .upgrade()
                .ok_or_else(|| AcpError::Runtime("context fixture run ended".into()))?;
            run.gates.hold(&gate).await;
        }
        reply.blocks.map_err(AcpError::Runtime)
    }
}

pub(crate) fn build(
    acp: &Value,
    channel: &str,
    run: &Arc<Run>,
) -> Result<Option<AcpContextContributor>, Unreadable> {
    let Some(value) = acp.get("context_contributor") else {
        return Ok(None);
    };
    let replies = parse(value).map_err(|error| format!("acp.context_contributor: {error}"))?;
    let requests = Wire::default();
    lock(&run.acp_context_requests).insert(channel.to_owned(), Arc::clone(&requests));
    let callback = Arc::new(Callback {
        replies: Mutex::new(replies.into()),
        requests,
        run: Arc::downgrade(run),
    });
    Ok(Some(AcpContextContributor::new(move |context, event| {
        let callback = Arc::clone(&callback);
        async move { callback.resolve(context, event).await }
    })))
}

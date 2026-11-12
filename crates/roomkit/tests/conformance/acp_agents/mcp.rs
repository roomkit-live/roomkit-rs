//! Strict scripted MCP suppliers, independent of the remote agent's wire.

#[cfg(test)]
#[path = "mcp_tests.rs"]
mod tests;

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, Weak};

use agent_client_protocol_schema::v1::McpServer;
use roomkit::model::RoomId;
use roomkit_acp::{AcpError, McpServers, McpServersForRoom};
use serde_json::{Value, json};

use super::Wire;
use crate::calls::Unreadable;
use crate::run::{Run, lock};

struct Reply {
    result: Result<Vec<McpServer>, String>,
    gate: Option<String>,
}

enum Source {
    Fixed(Vec<McpServer>),
    Callback {
        asynchronous: bool,
        replies: Vec<Reply>,
    },
}

fn members(value: &Value, required: &[&str], optional: &[&str]) -> bool {
    value.as_object().is_some_and(|object| {
        required.iter().all(|key| object.contains_key(*key))
            && object
                .keys()
                .all(|key| required.contains(&key.as_str()) || optional.contains(&key.as_str()))
    })
}

fn descriptors(value: &Value) -> Result<Vec<McpServer>, String> {
    let values = value
        .as_array()
        .ok_or("expected an array of MCP descriptors")?;
    for server in values {
        let pairs = match server.get("type") {
            Some(kind) if kind == "http" || kind == "sse" => {
                if !members(server, &["type", "name", "url", "headers"], &["_meta"]) {
                    return Err("invalid URL MCP descriptor members".into());
                }
                &server["headers"]
            }
            None => {
                if !members(server, &["name", "command", "args", "env"], &["_meta"]) {
                    return Err("invalid stdio MCP descriptor members".into());
                }
                &server["env"]
            }
            _ => return Err("unknown MCP descriptor type".into()),
        };
        if !pairs.as_array().is_some_and(|pairs| {
            pairs
                .iter()
                .all(|pair| members(pair, &["name", "value"], &["_meta"]))
        }) {
            return Err("invalid MCP header or environment entries".into());
        }
    }
    serde_json::from_value(value.clone()).map_err(|error| error.to_string())
}

fn reply(value: &Value, asynchronous: bool) -> Result<Reply, String> {
    let result = if members(value, &["servers"], &["gate"]) {
        Ok(descriptors(&value["servers"])?)
    } else if members(value, &["error"], &["gate"]) {
        Err(value["error"]
            .as_str()
            .ok_or("expected a callback error string")?
            .to_owned())
    } else {
        return Err("expected exactly servers or error, with an optional gate".into());
    };
    let gate = value
        .get("gate")
        .map(|gate| {
            if !asynchronous {
                return Err("a gate requires async mode".to_owned());
            }
            gate.as_str()
                .map(str::to_owned)
                .ok_or_else(|| "expected a gate name".to_owned())
        })
        .transpose()?;
    Ok(Reply { result, gate })
}

fn parse(value: &Value) -> Result<Source, String> {
    if value.is_array() {
        return descriptors(value).map(Source::Fixed);
    }
    if !members(value, &["mode", "responses"], &[]) {
        return Err("expected an MCP list or a mode/responses callback script".into());
    }
    let asynchronous = match value["mode"].as_str() {
        Some("sync") => false,
        Some("async") => true,
        _ => return Err("invalid callback mode".into()),
    };
    let replies = value["responses"]
        .as_array()
        .ok_or("expected callback responses")?
        .iter()
        .map(|value| reply(value, asynchronous))
        .collect::<Result<_, _>>()?;
    Ok(Source::Callback {
        asynchronous,
        replies,
    })
}

pub(crate) fn validate(acp: &Value, path: &str, found: &mut Vec<String>) {
    if let Some(value) = acp.get("mcp_servers")
        && let Err(error) = parse(value)
    {
        found.push(format!("{path}.mcp_servers: {error}"));
    }
    if let Some(value) = acp["agent"]["initialize"].get("mcp_capabilities")
        && (!members(value, &[], &["http", "sse"])
            || !value
                .as_object()
                .is_some_and(|object| object.values().all(Value::is_boolean)))
    {
        found.push(format!(
            "{path}.agent.initialize.mcp_capabilities: expected http/sse booleans"
        ));
    }
}

struct Callback {
    replies: Mutex<VecDeque<Reply>>,
    requests: Wire,
    run: Weak<Run>,
}

impl Callback {
    fn next(&self, room: &RoomId) -> Result<Reply, AcpError> {
        let mut requests = lock(&self.requests);
        requests.push(json!(room.as_str()));
        if let Some(reply) = lock(&self.replies).pop_front() {
            return Ok(reply);
        }
        let error = format!(
            "acp.mcp_servers: callback script exhausted at call {}",
            requests.len()
        );
        if let Some(run) = self.run.upgrade() {
            run.problem(error.clone());
        }
        Err(AcpError::Runtime(error))
    }

    async fn resolve(&self, room: RoomId) -> Result<Vec<McpServer>, AcpError> {
        let reply = self.next(&room)?;
        if let Some(gate) = reply.gate {
            let run = self
                .run
                .upgrade()
                .ok_or_else(|| AcpError::Runtime("MCP fixture run ended".into()))?;
            run.gates.hold(&gate).await;
        }
        reply.result.map_err(AcpError::Runtime)
    }
}

pub(crate) fn servers(
    acp: &Value,
    channel: &str,
    run: &Arc<Run>,
) -> Result<Option<McpServers>, Unreadable> {
    let requests = Wire::default();
    lock(&run.acp_mcp_requests).insert(channel.to_owned(), Arc::clone(&requests));
    let Some(value) = acp.get("mcp_servers") else {
        return Ok(None);
    };
    let source = parse(value).map_err(|error| format!("acp.mcp_servers: {error}"))?;
    let (asynchronous, replies) = match source {
        Source::Fixed(servers) => return Ok(Some(servers.into())),
        Source::Callback {
            asynchronous,
            replies,
        } => (asynchronous, replies),
    };
    let source = Arc::new(Callback {
        replies: Mutex::new(replies.into()),
        requests,
        run: Arc::downgrade(run),
    });
    let callback = if asynchronous {
        McpServersForRoom::new(move |room| {
            let source = Arc::clone(&source);
            async move { source.resolve(room).await }
        })
    } else {
        McpServersForRoom::new(move |room| {
            std::future::ready(
                source
                    .next(&room)
                    .and_then(|reply| reply.result.map_err(AcpError::Runtime)),
            )
        })
    };
    Ok(Some(McpServers::ForRoom(callback)))
}

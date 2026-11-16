//! A scripted JSON/SSE peer, never a replacement client implementation.
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::{JoinHandle, JoinSet};

use crate::calls::Unreadable;

struct State {
    spec: Value,
    position: usize,
    initialized: usize,
    discovery: bool,
    requests: Vec<Value>,
    problems: Vec<String>,
}

pub(super) struct Peer {
    pub(super) url: String,
    state: Arc<Mutex<State>>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl Peer {
    pub(super) async fn start(spec: Value) -> Result<Self, Unreadable> {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .map_err(|e| e.to_string())?;
        let url = format!(
            "http://{}/mcp",
            listener.local_addr().map_err(|e| e.to_string())?
        );
        let state = Arc::new(Mutex::new(State {
            spec,
            position: 0,
            initialized: 0,
            discovery: false,
            requests: vec![],
            problems: vec![],
        }));
        let shared = Arc::clone(&state);
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut clients = JoinSet::new();
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    client = listener.accept() => match client {
                        Ok((socket, _)) => {
                            let shared = Arc::clone(&shared);
                            clients.spawn(async move {
                                if let Err(error) = exchange(socket, &shared).await {
                                    shared.lock().unwrap().problems.push(error);
                                }
                            });
                        }
                        Err(error) => { shared.lock().unwrap().problems.push(error.to_string()); break; }
                    },
                    Some(result) = clients.join_next(), if !clients.is_empty() => {
                        if let Err(error) = result { shared.lock().unwrap().problems.push(error.to_string()); }
                    }
                }
            }
            clients.shutdown().await;
        });
        Ok(Self {
            url,
            state,
            stop: Some(stop),
            task: Some(task),
        })
    }

    pub(super) fn discovery(&self, active: bool) {
        self.state.lock().unwrap().discovery = active;
    }

    pub(super) async fn finish(mut self) -> Result<Value, Unreadable> {
        if let Some(stop) = self.stop.take() {
            let _sent = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.await.map_err(|e| e.to_string())?;
        }
        let mut state = self.state.lock().unwrap();
        if state.position != state.spec["replies"].as_array().unwrap().len() {
            state
                .problems
                .push("MCP peer has unconsumed replies".into());
        }
        if let Some(items) = state.spec["initializations"].as_array()
            && state.initialized != items.len()
        {
            state
                .problems
                .push("MCP peer has unconsumed initializations".into());
        }
        if !state.problems.is_empty() {
            return Err(state.problems.join("; "));
        }
        Ok(json!(state.requests))
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        // Sending stop (including dropping its sender) drains connection tasks.
        self.stop.take();
    }
}

impl State {
    fn reply(&mut self, message: &Value) -> Option<Value> {
        let method = message["method"].as_str().unwrap_or_default();
        if method == "initialize" {
            self.requests.push(json!({"method":method}));
            return Some(
                json!({"jsonrpc":"2.0","id":message["id"],"result":self.initialization()}),
            );
        }
        let mut observed = json!({"method":method});
        if let Some(params) = message.get("params") {
            observed["params"] = params.clone();
        } else if method == "tools/list" && self.discovery {
            observed["params"] = json!({});
        }
        if method == "tools/list"
            && self.discovery
            && let Some(params) = observed["params"].as_object_mut()
            && let Some(meta) = params.get_mut("_meta").and_then(Value::as_object_mut)
            && meta.get("progressToken").is_some_and(Value::is_u64)
        {
            meta.shift_remove("progressToken");
            if meta.is_empty() {
                params.shift_remove("_meta");
            }
        }
        self.requests.push(observed.clone());
        let Some(reply) = self.spec["replies"]
            .as_array()
            .and_then(|v| v.get(self.position))
            .cloned()
        else {
            self.problems
                .push(format!("Unexpected MCP request: {observed}"));
            return Some(
                json!({"jsonrpc":"2.0","id":message["id"],"error":{"code":-32603,"message":"fixture exhausted"}}),
            );
        };
        self.position += 1;
        let expected = json!({"method":reply["method"],"params":reply.get("params").cloned().unwrap_or_else(||json!({}))});
        if observed != expected {
            self.problems
                .push(format!("MCP request {observed} differs from {expected}"));
        }
        if reply["hold"] == true {
            return None;
        }
        let field = if reply.get("error").is_some() {
            "error"
        } else {
            "result"
        };
        Some(json!({"jsonrpc":"2.0","id":message["id"],field:reply[field]}))
    }

    fn initialization(&mut self) -> Value {
        let spec = if let Some(items) = self.spec["initializations"].as_array() {
            if self.initialized >= items.len() {
                self.problems
                    .push("MCP initialization script exhausted".into());
            }
            items.get(self.initialized).unwrap_or(&self.spec)
        } else {
            &self.spec
        };
        self.initialized += 1;
        let mut capabilities = json!({"resources":{}});
        if let Some(extensions) = spec.get("extensions") {
            capabilities["extensions"] = extensions.clone();
        }
        let mut result = json!({
            "protocolVersion":"2025-11-25", "capabilities":capabilities,
            "serverInfo":spec.get("server_info").cloned().unwrap_or_else(||json!({"name":"host-test","version":"1"})),
        });
        if let Some(instructions) = spec.get("instructions") {
            result["instructions"] = instructions.clone();
        }
        result
    }
}

async fn exchange(mut socket: TcpStream, shared: &Arc<Mutex<State>>) -> Result<(), String> {
    let Some((head, message)) = request(&mut socket).await? else {
        return Ok(());
    };
    if !head.starts_with("POST ") {
        return send(
            &mut socket,
            if head.starts_with("DELETE ") {
                200
            } else {
                405
            },
            "",
            false,
        )
        .await;
    }
    if message.get("id").is_none() {
        return send(&mut socket, 202, "", false).await;
    }
    let (reply, sse) = {
        let mut state = shared.lock().unwrap();
        (state.reply(&message), state.spec["framing"] == "sse")
    };
    if let Some(reply) = reply {
        let body = if sse {
            format!("event: message\ndata: {reply}\n\n")
        } else {
            reply.to_string()
        };
        send(&mut socket, 200, &body, sse).await
    } else {
        let mut byte = [0];
        let _closed = socket.read(&mut byte).await;
        Ok(())
    }
}

async fn request(socket: &mut TcpStream) -> Result<Option<(String, Value)>, String> {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        if bytes.len() == 65536 {
            return Err("MCP fixture header too large".into());
        }
        match socket.read_u8().await {
            Ok(byte) => bytes.push(byte),
            Err(_) => return Ok(None), // cancelled request, no scripted interaction
        }
    }
    let head = String::from_utf8(bytes).map_err(|e| e.to_string())?;
    if !head.starts_with("POST ") {
        return Ok(Some((head, Value::Null)));
    }
    let length = head
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(key, _)| key.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.trim().parse::<usize>().ok())
        .filter(|&n| n > 0 && n <= 4 * 1024 * 1024)
        .ok_or("MCP fixture needs bounded JSON")?;
    let mut body = vec![0; length];
    socket
        .read_exact(&mut body)
        .await
        .map_err(|e| e.to_string())?;
    Ok(Some((
        head,
        serde_json::from_slice(&body).map_err(|e| e.to_string())?,
    )))
}

async fn send(socket: &mut TcpStream, status: u16, body: &str, sse: bool) -> Result<(), String> {
    let kind = if sse {
        "text/event-stream"
    } else {
        "application/json"
    };
    let message = format!(
        "HTTP/1.1 {status} Response\r\nContent-Type: {kind}\r\nContent-Length: {}\r\nConnection: close\r\nMcp-Session-Id: conformance-session\r\n\r\n{body}",
        body.len()
    );
    socket
        .write_all(message.as_bytes())
        .await
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(method: &str) -> State {
        State {
            spec: json!({"replies":[{"method":method,"params":{},"result":{}}]}),
            position: 0,
            initialized: 0,
            discovery: false,
            requests: vec![],
            problems: vec![],
        }
    }

    // Python: test_mcp_host_fixture.py::test_host_request_parameters_are_not_normalized_into_an_empty_object
    #[test]
    fn host_parameters_are_not_normalized_into_an_empty_object() {
        for method in ["vendor/read", "tools/list"] {
            for params in [None, Some(Value::Null), Some(json!([])), Some(json!(false))] {
                let mut state = state(method);
                let mut request = json!({"jsonrpc":"2.0","id":1,"method":method});
                if let Some(params) = params {
                    request["params"] = params;
                }
                let _reply = state.reply(&request);
                assert!(!state.problems.is_empty(), "{request}");
            }
        }
    }

    #[test]
    fn only_discovery_ignores_generated_progress() {
        for discovery in [false, true] {
            let mut state = state("tools/list");
            state.discovery = discovery;
            let _reply = state.reply(&json!({
                "jsonrpc":"2.0","id":1,"method":"tools/list",
                "params":{"_meta":{"progressToken":0}}
            }));
            assert_eq!(state.problems.is_empty(), discovery);
        }
    }
}

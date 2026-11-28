//! A scripted JSON/SSE peer, never a replacement client implementation.
#[path = "peer/legacy.rs"]
mod legacy;
#[path = "peer/modern.rs"]
mod modern;
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::{JoinHandle, JoinSet};

use crate::calls::Unreadable;

struct State {
    legacy: Option<legacy::State>,
    spec: Value,
    position: usize,
    initialized: usize,
    discovery: bool,
    tool_call: bool,
    requests: Vec<Value>,
    problems: Vec<String>,
    /// The arguments each listed tool mirrors into a header (2026-07-28).
    mirrored: modern::Mirrored,
}

pub(super) struct Peer {
    pub(super) url: String,
    state: Arc<Mutex<State>>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl Peer {
    pub(super) async fn start(spec: Value, legacy: bool) -> Result<Self, Unreadable> {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .map_err(|e| e.to_string())?;
        let url = format!(
            "http://{}/mcp",
            listener.local_addr().map_err(|e| e.to_string())?
        );
        let state = Arc::new(Mutex::new(State {
            legacy: legacy.then(legacy::State::default),
            spec,
            position: 0,
            initialized: 0,
            discovery: false,
            tool_call: false,
            requests: vec![],
            problems: vec![],
            mirrored: modern::Mirrored::default(),
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

    pub(super) async fn settle_closed(&self) -> Result<(), Unreadable> {
        legacy::settled(&self.state).await
    }

    pub(super) fn http(&self, spec: &Value) -> Option<Value> {
        legacy::observed(&self.state.lock().unwrap(), spec)
    }

    /// A legacy peer's messages posted, as sent, notifications included.
    pub(super) fn posted(&self) -> Option<Value> {
        legacy::posted(&self.state.lock().unwrap())
    }

    pub(super) fn operation(&self, discovery: bool, tool_call: bool) {
        let mut state = self.state.lock().unwrap();
        state.discovery = discovery;
        state.tool_call = tool_call;
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
        legacy::check(&mut state);
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
    /// Whether the peer is a server of MCP 2026-07-28 (`era`).
    fn modern(&self) -> bool {
        self.spec["era"] == modern::MODERN
    }

    fn reply(&mut self, message: &Value) -> Option<Value> {
        let method = message["method"].as_str().unwrap_or_default();
        if matches!(method, "initialize" | "server/discover") {
            return Some(self.negotiation(method, message));
        }
        let stripped;
        let message = if self.modern() {
            stripped = modern::without_protocol_meta(message, &mut self.problems);
            &stripped
        } else {
            message
        };
        let mut observed = json!({"method":method});
        if let Some(params) = message.get("params") {
            observed["params"] = params.clone();
        }
        if ((method == "tools/list" && self.discovery)
            || (method == "tools/call" && self.tool_call))
            && let Some(params) = observed.get_mut("params").and_then(Value::as_object_mut)
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
        // A script without params is a request without them: an empty set
        // goes out as no params member (RK-21.2-046).
        let mut expected = json!({"method":reply["method"]});
        if let Some(params) = reply.get("params") {
            expected["params"] = params.clone();
        }
        if observed != expected {
            self.problems
                .push(format!("MCP request {observed} differs from {expected}"));
            return Some(json!({
                "jsonrpc":"2.0","id":message["id"],
                "error":{"code":-32603,"message":"fixture request mismatch"}
            }));
        }
        if reply["hold"] == true {
            return None;
        }
        let field = if reply.get("error").is_some() {
            "error"
        } else {
            "result"
        };
        if method == "tools/list" && field == "result" {
            self.mirrored.remember(&reply["result"]);
        }
        Some(json!({"jsonrpc":"2.0","id":message["id"],field:reply[field]}))
    }

    /// The answer to a request that negotiates the protocol, recorded by its
    /// method only: a server of an earlier revision knows no discovery and
    /// the client falls back to the handshake; a 2026-07-28 server answers
    /// discovery and takes no handshake.
    fn negotiation(&mut self, method: &str, message: &Value) -> Value {
        self.requests.push(json!({"method":method}));
        let id = &message["id"];
        if method == "server/discover" {
            let result = modern::discovery(self.initialization());
            return json!({"jsonrpc":"2.0","id":id,"result":result});
        }
        if !self.modern() {
            return json!({"jsonrpc":"2.0","id":id,"result":self.initialization()});
        }
        self.problems
            .push("MCP 2026-07-28 peer received an initialize".into());
        json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"Method not found"}})
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
    if shared.lock().unwrap().legacy.is_some() {
        return legacy::exchange(socket, &head, &message, shared).await;
    }
    if !head.starts_with("POST ") {
        let status = if head.starts_with("DELETE ") {
            200
        } else {
            405
        };
        return send(&mut socket, status, "", false, true).await;
    }
    let modern = {
        let mut state = shared.lock().unwrap();
        let modern = state.modern();
        if modern {
            let State {
                mirrored, problems, ..
            } = &mut *state;
            modern::check_headers(&modern::headers(&head), &message, mirrored, problems);
        }
        modern
    };
    // A 2026-07-28 server keeps no session.
    let session = !modern;
    if message.get("id").is_none() {
        return send(&mut socket, 202, "", false, session).await;
    }
    if message["method"] == "server/discover" && !modern {
        let (status, body) = {
            let mut state = shared.lock().unwrap();
            state.requests.push(json!({"method":"server/discover"}));
            modern::refusal(&state.spec, &message["id"])
        };
        return send(&mut socket, status, &body.to_string(), false, false).await;
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
        send(&mut socket, 200, &body, sse, session).await
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

async fn send(
    socket: &mut TcpStream,
    status: u16,
    body: &str,
    sse: bool,
    session: bool,
) -> Result<(), String> {
    let kind = if sse {
        "text/event-stream"
    } else {
        "application/json"
    };
    let held = if session {
        "Mcp-Session-Id: conformance-session\r\n"
    } else {
        ""
    };
    let message = format!(
        "HTTP/1.1 {status} Response\r\nContent-Type: {kind}\r\nContent-Length: {}\r\nConnection: close\r\n{held}\r\n{body}",
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
            legacy: None,
            spec: json!({"replies":[{"method":method,"params":{},"result":{}}]}),
            position: 0,
            initialized: 0,
            discovery: false,
            tool_call: false,
            requests: vec![],
            problems: vec![],
            mirrored: modern::Mirrored::default(),
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

    // roomkit-specs conformance/README.md, `mcp_session`: omitted script
    // params mean a request sent with no params member (RK-21.2-046).
    #[test]
    fn a_script_without_params_matches_only_a_request_without_them() {
        for method in ["vendor/read", "tools/list"] {
            for discovery in [false, true] {
                for params in [None, Some(json!({})), Some(Value::Null)] {
                    let mut state = state(method);
                    state.spec = json!({"replies":[{"method":method,"result":{}}]});
                    state.discovery = discovery;
                    let mut request = json!({"jsonrpc":"2.0","id":1,"method":method});
                    if let Some(params) = &params {
                        request["params"] = params.clone();
                    }
                    let _reply = state.reply(&request);
                    assert_eq!(state.problems.is_empty(), params.is_none(), "{request}");
                    let mut observed = json!({"method":method});
                    if let Some(params) = params {
                        observed["params"] = params;
                    }
                    assert_eq!(state.requests, [observed]);
                }
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

    // Python: test_only_public_call_tool_ignores_the_sdks_numeric_progress_token.
    #[test]
    fn only_public_call_tool_ignores_numeric_progress_without_changing_user_parameters() {
        for public_call in [false, true] {
            for meta in [
                json!({"progressToken":0}),
                json!({"progressToken":"caller"}),
                json!({"progressToken":0,"caller":true}),
            ] {
                let mut state = state("tools/call");
                state.tool_call = public_call;
                state.spec["replies"][0]["params"] = json!({"name":"echo","arguments":{}});
                let _reply = state.reply(&json!({"id":1,"method":"tools/call", "params":{"name":"echo","arguments":{},"_meta":meta}}));
                assert_eq!(
                    state.problems.is_empty(),
                    public_call && meta == json!({"progressToken":0})
                );
            }
        }
    }

    // Python: test_mcp_host_fixture.py::test_a_wrong_held_request_is_refused_instead_of_hanging
    #[test]
    fn a_wrong_held_request_is_refused_instead_of_hanging() {
        let mut state = state("vendor/read");
        state.spec = json!({"replies":[{"method":"vendor/read","hold":true}]});
        let reply = state.reply(&json!({
            "jsonrpc":"2.0","id":2,"method":"vendor/next","params":{}
        }));
        assert_eq!(
            reply,
            Some(json!({
                "jsonrpc":"2.0","id":2,
                "error":{"code":-32603,"message":"fixture request mismatch"}
            }))
        );
        assert_eq!(state.problems.len(), 1);
        assert!(state.problems[0].contains("vendor/next"));
    }
}

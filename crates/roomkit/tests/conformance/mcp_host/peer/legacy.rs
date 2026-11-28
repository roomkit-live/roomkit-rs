//! Actual GET/POST framing and stream-close observations for legacy SSE.

use std::fmt::Write as _;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Notify, mpsc};

#[derive(Default)]
pub(super) struct State {
    connections: usize,
    closed: usize,
    messages: Option<mpsc::Sender<Value>>,
    requests: Vec<Value>,
    /// Each JSON-RPC message posted, as sent, notifications included.
    posted: Vec<Value>,
    changed: Arc<Notify>,
}

pub(super) async fn exchange(
    mut socket: TcpStream,
    head: &str,
    message: &Value,
    shared: &Arc<Mutex<super::State>>,
) -> Result<(), String> {
    let parts: Vec<_> = head
        .lines()
        .next()
        .ok_or("missing HTTP request line")?
        .split(' ')
        .collect();
    let method = parts.first().ok_or("missing HTTP method")?;
    let target = parts.get(1).ok_or("missing HTTP target")?;
    let headers: Vec<_> = head
        .lines()
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| json!([name.to_ascii_lowercase(), value.trim()]))
        .collect();
    {
        let mut state = shared.lock().unwrap();
        state
            .legacy
            .as_mut()
            .unwrap()
            .requests
            .push(json!({"method":method,"target":target,"headers":headers}));
    }
    match *method {
        "GET" => get(socket, shared).await,
        "POST" => {
            let (sender, reply) = {
                let mut state = shared.lock().unwrap();
                state.legacy.as_mut().unwrap().posted.push(message.clone());
                let legacy = state.legacy.as_ref().unwrap();
                if *target != format!("/messages?session={}", legacy.connections) {
                    return Err("MCP POST did not use its announced endpoint".into());
                }
                let sender = legacy
                    .messages
                    .clone()
                    .ok_or("MCP POST preceded the GET stream")?;
                let reply = message.get("id").and_then(|_| state.reply(message));
                (sender, reply)
            };
            super::send(&mut socket, 202, "", false, true).await?;
            if let Some(reply) = reply {
                sender
                    .send(reply)
                    .await
                    .map_err(|_| "MCP GET closed before its reply")?;
            }
            Ok(())
        }
        _ => Err(format!("unexpected legacy SSE method {method}")),
    }
}

struct Closed(Arc<Mutex<super::State>>);
impl Drop for Closed {
    fn drop(&mut self) {
        let mut state = self.0.lock().unwrap();
        let legacy = state.legacy.as_mut().unwrap();
        legacy.closed += 1;
        legacy.changed.notify_one();
    }
}

async fn get(mut socket: TcpStream, shared: &Arc<Mutex<super::State>>) -> Result<(), String> {
    let (sender, mut received) = mpsc::channel(16);
    let (connection, announce) = {
        let mut state = shared.lock().unwrap();
        let legacy = state.legacy.as_mut().unwrap();
        legacy.connections += 1;
        legacy.messages = Some(sender);
        let connection = legacy.connections;
        let announce = match state.spec.get("endpoint_on_connections") {
            Some(items) => items
                .as_array()
                .unwrap()
                .get(connection - 1)
                .and_then(Value::as_bool)
                .ok_or("MCP endpoint script exhausted")?,
            None => true,
        };
        (connection, announce)
    };
    let _closed = Closed(Arc::clone(shared));
    let mut response =
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\n\r\n"
            .to_owned();
    if announce {
        let _written = write!(
            response,
            "event: endpoint\ndata: /messages?session={connection}\n\n"
        );
    }
    socket
        .write_all(response.as_bytes())
        .await
        .map_err(|e| e.to_string())?;
    let mut byte = [0];
    loop {
        tokio::select! {
            _ = socket.read(&mut byte) => return Ok(()),
            message = received.recv() => {
                let Some(message) = message else { return Ok(()); };
                socket.write_all(format!("event: message\ndata: {message}\n\n").as_bytes())
                    .await.map_err(|e| e.to_string())?;
            }
        }
    }
}

pub(super) async fn settled(shared: &Arc<Mutex<super::State>>) -> Result<(), String> {
    let changed = {
        let state = shared.lock().unwrap();
        let Some(legacy) = &state.legacy else {
            return Ok(());
        };
        Arc::clone(&legacy.changed)
    };
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let waiting = changed.notified();
            {
                let state = shared.lock().unwrap();
                let legacy = state.legacy.as_ref().unwrap();
                if legacy.closed == legacy.connections {
                    return;
                }
            }
            waiting.await;
        }
    })
    .await
    .map_err(|_| "MCP GET stream did not close".to_owned())
}

pub(super) fn observed(state: &super::State, spec: &Value) -> Option<Value> {
    let legacy = state.legacy.as_ref()?;
    let names: Vec<_> = ["headers", "headers_after_construction"]
        .into_iter()
        .filter_map(|key| spec[key].as_object())
        .flat_map(|map| map.keys().map(|key| key.to_ascii_lowercase()))
        .collect();
    let mut requests = legacy.requests.clone();
    for request in &mut requests {
        request["headers"]
            .as_array_mut()
            .unwrap()
            .retain(|entry| names.iter().any(|name| entry[0] == *name));
    }
    Some(
        json!({"connections":legacy.connections,"closed_connections":legacy.closed,"requests":requests}),
    )
}

pub(super) fn posted(state: &super::State) -> Option<Value> {
    Some(Value::Array(state.legacy.as_ref()?.posted.clone()))
}

pub(super) fn check(state: &mut super::State) {
    if let Some(legacy) = &state.legacy
        && let Some(endpoints) = state.spec["endpoint_on_connections"].as_array()
        && endpoints.len() != legacy.connections
    {
        state
            .problems
            .push("MCP peer has unconsumed endpoint announcements".into());
    }
}

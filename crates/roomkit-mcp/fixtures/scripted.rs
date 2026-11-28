//! A server answering each method as its plan says, every line it reads
//! appended to its log: Python's scripted server of
//! `tests/test_mcp_discovery.py`. A plan maps a method to its answer's
//! members (`{"result": ...}` or `{"error": ...}`), `"silent"` for none, or
//! `{"late": {...}}` for an answer sent only after the next request's; a
//! method it does not plan is never answered.

use std::fs::OpenOptions;
use std::io::{self, BufRead, Write};

use serde_json::{Map, Value, json};

pub(super) fn serve(log: &str, plan: &str) {
    let Ok(plan) = serde_json::from_str::<Map<String, Value>>(plan) else {
        return;
    };
    let Ok(mut log) = OpenOptions::new().create(true).append(true).open(log) else {
        return;
    };
    let mut held: Vec<Value> = Vec::new();
    for line in io::stdin().lock().lines().map_while(Result::ok) {
        if writeln!(log, "{line}").and_then(|()| log.flush()).is_err() {
            return;
        }
        let Ok(message) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let Some(id) = message.get("id") else {
            continue;
        };
        let method = message["method"].as_str().unwrap_or_default();
        let Some(answer) = plan.get(method).and_then(Value::as_object) else {
            continue;
        };
        let reply = |members: &Map<String, Value>| {
            let mut reply = json!({"jsonrpc": "2.0", "id": id});
            if let Some(reply) = reply.as_object_mut() {
                reply.extend(members.clone());
            }
            reply
        };
        if let Some(late) = answer.get("late").and_then(Value::as_object) {
            held.push(reply(late));
            continue;
        }
        let mut stdout = io::stdout().lock();
        for reply in std::iter::once(reply(answer)).chain(held.drain(..)) {
            if writeln!(stdout, "{reply}").is_err() {
                return;
            }
        }
        if stdout.flush().is_err() {
            return;
        }
    }
}

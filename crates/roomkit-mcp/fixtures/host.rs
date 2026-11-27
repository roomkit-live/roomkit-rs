//! Stdio host-access peer: explicit state and release, never fixed sleeps.
use serde_json::{Value, json};
use std::io::{self, BufRead, Write};

pub(super) fn serve(modern: bool) {
    let mut state = State {
        modern,
        ..State::default()
    };
    for line in io::stdin().lock().lines().map_while(Result::ok) {
        let Ok(message) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        if message.get("method").is_none() {
            continue;
        }
        let Some(id) = message.get("id") else {
            continue;
        };
        let Some(result) = state.answer(&message) else {
            continue;
        };
        let reply = match result {
            Ok(result) => json!({"jsonrpc":"2.0","id":id,"result":result}),
            Err(message) => json!({"jsonrpc":"2.0","id":id,
                "error":{"code":-32602,"message":message}}),
        };
        if !write(&reply) {
            return;
        }
    }
}

#[derive(Default)]
struct State {
    listings: usize,
    /// The `params` member of each `tools/list` received, as [`received`]
    /// gives it.
    listing_params: Vec<Value>,
    echoes: usize,
    held: Vec<Value>,
    reads: usize,
    modern: bool,
}

impl State {
    fn answer(&mut self, message: &Value) -> Option<Result<Value, &'static str>> {
        let id = &message["id"];
        let params = &message["params"];
        let value = match message["method"].as_str().unwrap_or_default() {
            "initialize" => handshake(self.modern),
            "tools/list" => {
                self.listings += 1;
                self.listing_params.push(received(message));
                catalogue()
            }
            "host/state" => json!({
                "listings":self.listings,"listing_params":self.listing_params,
                "echoes":self.echoes,"held":self.held.len(),"reads":self.reads,
            }),
            "host/hold" => {
                self.held.push(id.clone());
                return None;
            }
            "host/echo" => {
                self.echoes += 1;
                received(message)
            }
            "host/ping" => {
                if !write(&json!({"jsonrpc":"2.0","id":id,"method":"ping"})) {
                    return None;
                }
                json!({"after_ping":true})
            }
            "host/release" => self.release(),
            "vendor/read" => {
                json!({"contents":[],"vendor":{"keep":7},"optional":null,"inner":{"optional":null}})
            }
            "tools/call" => json!({"content":[{"type":"text","text":params["arguments"]["text"]}]}),
            "resources/read" if params["uri"] == "resource://hold" => {
                self.held.push(id.clone());
                return None;
            }
            "resources/read" if params["uri"] == "resource://body" => {
                self.reads += 1;
                resource()
            }
            _ => return Some(Err("unknown resource")),
        };
        Some(Ok(value))
    }

    fn release(&mut self) -> Value {
        for old in self.held.drain(..) {
            if !write(&json!({"jsonrpc":"2.0","id":old,"result":{"old":true}})) {
                break;
            }
        }
        json!({"released":true})
    }
}

/// The `params` member of `message` as it came: `{"params": ...}`, or
/// `{"no_params": true}` for a request without one.
fn received(message: &Value) -> Value {
    message.get("params").map_or_else(
        || json!({"no_params":true}),
        |params| json!({"params":params}),
    )
}

fn handshake(modern: bool) -> Value {
    json!({
        "protocolVersion":if modern {"2026-07-28"} else {"2025-11-25"},
        "capabilities":{"resources":{},"tools":{},"extensions":{"vendor/good":{"enabled":true},"vendor/bad":false}},
        "serverInfo":{"name":"host","version":"1","vendor":{"keep":7},"optional":null},
        "instructions":"Use the host.",
    })
}

fn catalogue() -> Value {
    json!({"tools":[{"name":"board","inputSchema":{"type":"object"},"_meta":{"ui":{"resourceUri":"ui://board/view.html"}}}]})
}

fn resource() -> Value {
    json!({"contents":[{"uri":"resource://body","text":"Résumé","vendor":7,"optional":null}],
        "vendor":8,"optional":null,"ttlMs":60000})
}

fn write(value: &Value) -> bool {
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "{value}")
        .and_then(|()| stdout.flush())
        .is_ok()
}

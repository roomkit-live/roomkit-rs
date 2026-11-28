//! The conformance runner's fixture process (roomkit-specs
//! conformance/README.md, "Scripted agents", `mcp_servers` `stdio`, and
//! "Scripted ACP agents", `acp.process`): a harmless process a provider or
//! a channel starts from its command, which speaks on its stdin and stdout,
//! JSON-RPC one message per line, and reports to the run. It calls no
//! vendor.
//!
//! ```text
//! roomkit-conformance-fixture <report path> <fixture spec as JSON> [args...]
//! ```
//!
//! The spec's `mode` says what it serves: `mcp`, the MCP server its
//! `server` describes (the runner's `mcp_servers`); `acp`, an ACP agent that
//! answers `initialize` with the client's protocol version,
//! `authenticate`, `session/new` (`session-1`, `session-2`...) and
//! `session/close`, and each `session/prompt` with one
//! `agent_message_chunk` of the spec's `says`, then `end_turn`, takes
//! `session/cancel`, and answers any other request `method_not_found`.
//!
//! It appends to its report, one JSON line each: its start, with the
//! arguments it received after its own and the variables of its
//! environment the spec's `env` names; each `tools/call` it was sent, with
//! its `_meta` (`mcp`), or each message it received (`acp`); and its exit,
//! once its input ended.

#![expect(
    clippy::indexing_slicing,
    reason = "a test fixture reads its JSON by key, an absent key reading null"
)]

mod mcp_wire;

use std::fs::OpenOptions;
use std::io::{self, BufRead, Write};
use std::process::ExitCode;

use serde_json::{Map, Value, json};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let (Some(report), Some(spec)) = (args.get(1), args.get(2)) else {
        complain("usage: roomkit-conformance-fixture <report> <spec> [args...]");
        return ExitCode::FAILURE;
    };
    let Ok(spec) = serde_json::from_str::<Value>(spec) else {
        complain("the spec is no JSON");
        return ExitCode::FAILURE;
    };
    let report = Report(report.clone());
    let env: Map<String, Value> = spec["env"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(|name| {
            (
                name.to_owned(),
                std::env::var(name).map_or(Value::Null, Value::String),
            )
        })
        .collect();
    let given = args.get(3..).unwrap_or_default();
    report.write(&json!({ "event": "start", "argv": given, "env": env }));
    let served = serve(&spec, &report);
    report.write(&json!({ "event": "exit" }));
    match served {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            complain(&error.to_string());
            ExitCode::FAILURE
        }
    }
}

/// Says what went wrong on the standard error, which the run does not read.
fn complain(what: &str) {
    let _written = writeln!(io::stderr(), "roomkit-conformance-fixture: {what}");
}

/// The lines the run reads of this process.
struct Report(String);

impl Report {
    fn write(&self, line: &Value) {
        let opened = OpenOptions::new().create(true).append(true).open(&self.0);
        if let Ok(mut file) = opened {
            let _written = writeln!(file, "{line}");
        }
    }
}

/// Answers each message of the input until it ends.
fn serve(spec: &Value, report: &Report) -> io::Result<()> {
    let mut sessions = 0_u64;
    let mut stdout = io::stdout().lock();
    for line in io::stdin().lock().lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let Ok(message) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let Some(method) = message["method"].as_str() else {
            continue;
        };
        let answers = if spec["mode"] == "acp" {
            report.write(&json!({
                "event": "received", "method": method, "params": message.get("params"),
            }));
            acp(spec, &message, &mut sessions)
        } else {
            if method == "tools/call" {
                let params = &message["params"];
                report.write(&json!({
                    "event": "call",
                    "name": params["name"],
                    "arguments": params.get("arguments").cloned().unwrap_or_else(|| json!({})),
                    "_meta": params.get("_meta").cloned().unwrap_or_else(|| json!({})),
                }));
            }
            mcp_wire::answer(&spec["server"], &message)
                .into_iter()
                .collect()
        };
        for answer in answers {
            writeln!(stdout, "{answer}")?;
        }
        stdout.flush()?;
    }
    Ok(())
}

/// What the ACP agent sends for `message`, in order.
fn acp(spec: &Value, message: &Value, sessions: &mut u64) -> Vec<Value> {
    let Some(id) = message.get("id") else {
        return Vec::new();
    };
    let params = &message["params"];
    let mut sent = Vec::new();
    let result = match message["method"].as_str().unwrap_or_default() {
        "initialize" => Some(json!({
            "protocolVersion": params["protocolVersion"],
            "agentCapabilities": { "sessionCapabilities": { "close": {} } },
        })),
        "authenticate" | "session/close" => Some(json!({})),
        "session/new" => {
            *sessions += 1;
            Some(json!({ "sessionId": format!("session-{sessions}") }))
        }
        "session/prompt" => {
            sent.push(json!({
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": params["sessionId"],
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "content": { "type": "text", "text": spec["says"] },
                    },
                },
            }));
            Some(json!({ "stopReason": "end_turn" }))
        }
        _ => None,
    };
    sent.push(match result {
        Some(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
        None => json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {
                "code": -32601,
                "message": "Method not found",
                "data": { "method": message["method"] },
            },
        }),
    });
    sent
}

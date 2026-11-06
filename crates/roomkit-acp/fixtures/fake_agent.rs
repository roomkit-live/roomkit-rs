//! A stand-in ACP agent over stdio for roomkit-acp's tests: one JSON-RPC
//! message per line, one behaviour per mode. It calls no vendor.
//!
//! ```text
//! fake-acp-agent [<mode>]
//! ```
//!
//! Modes:
//! - `echo` (the default): `initialize` answers the client's version, with
//!   `session/close` announced, `agentInfo` `fake-acp-agent` and, in its
//!   `_meta`, the agent's `cwd` and `env`; `session/new` answers
//!   `session-1`, `session-2`...; `session/prompt` answers `end_turn` after
//!   one `agent_message_chunk` per text block of the prompt, its text
//!   echoed, or, for the text `ask the client`, after asking the client
//!   each of the methods of [`ASKED`] and one `agent_message_chunk` per
//!   answer (`{"method": ..., "answer": ...}`); a permission is asked with
//!   a `waiting` update sent before its answer is read. `session/close` and
//!   `authenticate` answer `{}`; `session/cancel` is taken silently; any
//!   other request is answered `method_not_found`.
//! - `exit`: `echo`, but it exits once it answered `initialize`.
//! - `version:<n>`: `echo`, its `initialize` answering version `<n>`.
//! - `junk`: `echo`, each answer preceded by a blank line, a line that is
//!   no JSON and a JSON line that is no object.

#![expect(
    clippy::indexing_slicing,
    reason = "a test fixture reads its JSON by key, an absent key reading null"
)]

use std::collections::BTreeMap;
use std::io::{self, BufRead, Write};
use std::process::ExitCode;

use serde_json::{Value, json};

/// The client's methods `ask the client` asks, in order, with their
/// parameters.
const ASKED: &[&str] = &[
    "fs/read_text_file",
    "fs/write_text_file",
    "terminal/create",
    "elicitation/create",
    "_vendor/extension",
    "session/request_permission",
];

struct Agent {
    mode: String,
    input: io::Lines<io::StdinLock<'static>>,
    output: io::StdoutLock<'static>,
    next_id: u64,
    sessions: u64,
}

fn main() -> ExitCode {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "echo".to_owned());
    let mut agent = Agent {
        mode,
        input: io::stdin().lock().lines(),
        output: io::stdout().lock(),
        next_id: 0,
        sessions: 0,
    };
    while let Some(Ok(line)) = agent.input.next() {
        let Ok(message) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let Some(method) = message["method"].as_str().map(str::to_owned) else {
            continue;
        };
        if message.get("id").is_none() {
            // A notification: `session/cancel` is taken silently.
            continue;
        }
        let answer = agent.answer(&method, &message["params"]);
        agent.respond(&message["id"], answer);
        if method == "initialize" && agent.mode == "exit" {
            return ExitCode::SUCCESS;
        }
    }
    ExitCode::SUCCESS
}

impl Agent {
    /// The answer to the client's request `method`: its result, or its
    /// error.
    fn answer(&mut self, method: &str, params: &Value) -> Result<Value, Value> {
        match method {
            "initialize" => Ok(self.initialized(params)),
            "session/new" => {
                self.sessions += 1;
                Ok(json!({ "sessionId": format!("session-{}", self.sessions) }))
            }
            "session/prompt" => {
                self.prompted(params);
                Ok(json!({ "stopReason": "end_turn" }))
            }
            "session/close" | "authenticate" => Ok(json!({})),
            other => Err(json!({
                "code": -32601,
                "message": "Method not found",
                "data": { "method": other },
            })),
        }
    }

    fn initialized(&self, params: &Value) -> Value {
        let version = match self.mode.strip_prefix("version:") {
            Some(version) => json!(version.parse::<u64>().unwrap_or(0)),
            None => params["protocolVersion"].clone(),
        };
        let env: BTreeMap<String, String> = std::env::vars().collect();
        let cwd = std::env::current_dir()
            .map(|cwd| cwd.display().to_string())
            .unwrap_or_default();
        json!({
            "protocolVersion": version,
            "agentCapabilities": { "sessionCapabilities": { "close": {} } },
            "agentInfo": { "name": "fake-acp-agent", "title": "Fake ACP Agent", "version": "1.0.0" },
            "_meta": { "cwd": cwd, "env": env, "initialize": params },
        })
    }

    /// The updates of a prompt, sent before its answer.
    fn prompted(&mut self, params: &Value) {
        let session = params["sessionId"].clone();
        let texts: Vec<String> = params["prompt"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|block| block["text"].as_str().map(str::to_owned))
            .collect();
        if texts.first().map(String::as_str) == Some("ask the client") {
            for method in ASKED {
                let answer = self.ask(method, &session);
                self.said(
                    &session,
                    &json!({ "method": method, "answer": answer }).to_string(),
                );
            }
            return;
        }
        for text in texts {
            self.said(&session, &text);
        }
    }

    /// Asks the client `method`, and returns its answer, the response
    /// without `jsonrpc` and `id`.
    fn ask(&mut self, method: &str, session: &Value) -> Value {
        let params = match method {
            "session/request_permission" => json!({
                "sessionId": session,
                "toolCall": { "toolCallId": "tool-1", "title": "Read file" },
                "options": [
                    { "optionId": "allow-once", "name": "Allow once", "kind": "allow_once" },
                    { "optionId": "reject-once", "name": "Reject once", "kind": "reject_once" },
                ],
            }),
            "fs/read_text_file" => json!({ "sessionId": session, "path": "/etc/hostname" }),
            "fs/write_text_file" => {
                json!({ "sessionId": session, "path": "/tmp/x", "content": "x" })
            }
            "terminal/create" => json!({ "sessionId": session, "command": "ls" }),
            _ => json!({ "sessionId": session }),
        };
        let id = self.next_id;
        self.next_id += 1;
        self.send(&json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }));
        if method == "session/request_permission" {
            // The client reads this while it answers the permission.
            self.said(session, "waiting");
        }
        while let Some(Ok(line)) = self.input.next() {
            let Ok(message) = serde_json::from_str::<Value>(&line) else {
                continue;
            };
            if message.get("method").is_none() && message["id"] == json!(id) {
                let mut answer = message;
                if let Some(answer) = answer.as_object_mut() {
                    answer.shift_remove("jsonrpc");
                    answer.shift_remove("id");
                }
                return answer;
            }
        }
        Value::Null
    }

    /// One `agent_message_chunk` of `text` in `session`.
    fn said(&mut self, session: &Value, text: &str) {
        self.send(&json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": session,
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": { "type": "text", "text": text },
                },
            },
        }));
    }

    fn respond(&mut self, id: &Value, answer: Result<Value, Value>) {
        let message = match answer {
            Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
            Err(error) => json!({ "jsonrpc": "2.0", "id": id, "error": error }),
        };
        self.send(&message);
    }

    fn send(&mut self, message: &Value) {
        if self.mode == "junk" {
            let _ignored = writeln!(self.output, "\nnot json\n[1, 2]");
        }
        let _ignored = writeln!(self.output, "{message}");
        let _ignored = self.output.flush();
    }
}

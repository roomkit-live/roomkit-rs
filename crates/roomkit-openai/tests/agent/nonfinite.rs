//! Shared native Chat Completions cases for argument text and the following request.

use std::process::Command;
use std::sync::{Arc, Mutex};

use roomkit_ai::{AiTool, ToolResult, Tools};
use serde::Deserialize;
use serde_json::{Value, json};

use super::{Reply, Room, chunk, ending, messages};

const CASES: &str = include_str!("nonfinite_tool_turns.json");

#[derive(Deserialize)]
struct Case {
    id: String,
    arguments: String,
    handler_arguments: Vec<Value>,
    model_result: serde_json::Map<String, Value>,
}

/// The Python provider test owns the oracle. The ordinary Rust test also runs
/// from a standalone checkout. Compare the copy when a Python checkout is supplied.
#[test]
fn nonfinite_turn_cases_match_the_python_oracle() {
    let Ok(python) = std::env::var("ROOMKIT_PYTHON") else {
        return;
    };
    let script = r"from pathlib import Path
from importlib.util import find_spec
import sys
root = Path(find_spec('roomkit').origin).resolve().parents[2]
sys.stdout.buffer.write((root / 'tests/test_providers/nonfinite_tool_turns.json').read_bytes())";
    let output = Command::new(python).args(["-c", script]).output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(String::from_utf8(output.stdout).unwrap(), CASES);
}

#[tokio::test]
async fn argument_text_controls_execution_and_the_next_model_request() {
    let cases: Vec<Case> = serde_json::from_str(CASES).unwrap();
    for case in cases {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&seen);
        let tools = Tools::new().tool(AiTool::new("search", "Search").unwrap(), move |call, _| {
            captured.lock().unwrap().push(json!(call.arguments));
            async { Ok(ToolResult::Json(json!({"handled": true}))) }
        });
        let fragment = json!({"tool_calls": [{
            "index": 0, "id": "tool1", "function": {"name": "search", "arguments": case.arguments}
        }]});
        let mut first = vec![chunk(&fragment)];
        first.extend(ending("tool_calls", 4, 1));
        let mut second = vec![chunk(&json!({"content": "Done."}))];
        second.extend(ending("stop", 8, 1));
        let room = Room::serving(vec![Reply::events(&first), Reply::events(&second)], tools).await;
        assert_eq!(
            room.says(&room.sms, "alice", "Alice", "Go.").await.error,
            None,
            "{}",
            case.id
        );
        assert_eq!(*seen.lock().unwrap(), case.handler_arguments, "{}", case.id);
        assert_eq!(room.fake.requests().len(), 2, "{}", case.id);
        let second = messages(&room.fake, 1);
        let replies: Vec<_> = second
            .iter()
            .filter(|message| message["role"] == "tool")
            .collect();
        let [reply] = replies.as_slice() else {
            panic!("{}: one tool result expected", case.id)
        };
        assert_eq!(reply["tool_call_id"], "tool1", "{}", case.id);
        let mut content: serde_json::Map<String, Value> =
            serde_json::from_str(reply["content"].as_str().unwrap()).unwrap();
        content.shift_remove("hint");
        assert_eq!(content, case.model_result, "{}", case.id);
    }
}

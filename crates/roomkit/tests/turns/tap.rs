//! RFC §10.6.3, §10.6.4: the probe's turn, a streamed answer whose tool
//! sends an event through its turn's host, carries the messages the Python
//! implementation's in-memory backend carries for it, in the same order,
//! leaving aside those of features 0.1 lacks (the readers of owner-process
//! state a Python turn asks first, `room_tasks` and `room_vision`, its
//! ephemerals, its framework events).
//!
//! The messages this crate carries for the turn are kept in
//! `tests/fixtures/turns/rust-case1.jsonl`, the ids the run draws at
//! random named in order: the test checks the file is what the turn
//! carries, and `ROOMKIT_WRITE_TURNS_FIXTURE=1` writes it again. Python
//! parses it with its own models (`read_rust.py`) in `make turns-fixture`
//! only, which runs this test first: a change to the file is read by
//! Python once that target runs, not by `make all`.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use pretty_assertions::assert_eq;
use roomkit::model::{DirectEvent, EventContent, EventType};
use roomkit::testing::{TappedMessage, TurnTap};
use roomkit::{ConversationStore, current_turn};
use roomkit_ai::{ToolError, ToolOutput};
use serde_json::Value;

use crate::support::{asks, channel, kit, received, room};

fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/turns")
}

/// One message as the comparison reads it: its family, its type, and
/// what tells it apart (an item's kind and text, a verb, a pull's grant).
fn step(family: &str, message: &Value) -> String {
    if family == "request" {
        return "request".to_owned();
    }
    let message = &message["message"];
    let kind = message["type"].as_str().unwrap_or_default();
    let detail = match kind {
        "item" => {
            let item = &message["item"];
            let text = item["text"]
                .as_str()
                .map(|text| format!(" {text:?}"))
                .unwrap_or_default();
            let status = item["status"]
                .as_str()
                .map(|status| format!(" {status}"))
                .unwrap_or_default();
            let end = match item["kind"].as_str() {
                Some("loop_end") => format!(
                    " {} rounds={} usage={}",
                    item["reason"], item["rounds"], item["usage"]
                ),
                _ => String::new(),
            };
            format!(
                " {}{text}{status}{end}",
                item["kind"].as_str().unwrap_or_default()
            )
        }
        "pull" => format!(" count={} read={}", message["count"], message["read"]),
        "host_call" => format!(" {}", message["verb"].as_str().unwrap_or_default()),
        "output" => format!(" streams={}", message["streams"]),
        "tool_args" => format!(" {}", message["arguments"]),
        _ => String::new(),
    };
    format!("{family} {kind}{detail}")
}

/// Python's messages for the turn, those of features 0.1 lacks left aside.
fn python_steps() -> Vec<String> {
    let text = std::fs::read_to_string(fixtures().join("python-case1.jsonl")).unwrap();
    let lines: Vec<(String, Value)> = text
        .lines()
        .map(|line| {
            let line: Value = serde_json::from_str(line).unwrap();
            (
                line["direction"].as_str().unwrap().to_owned(),
                line["message"].clone(),
            )
        })
        .collect();
    let readers: BTreeSet<String> = lines
        .iter()
        .filter(|(_, message)| {
            let verb = message["message"]["verb"].as_str();
            matches!(verb, Some("room_tasks" | "room_vision"))
        })
        .map(|(_, message)| message["message"]["call_id"].as_str().unwrap().to_owned())
        .collect();
    lines
        .iter()
        .filter(|(_, message)| {
            let kind = message["message"]["type"].as_str();
            let call = message["message"]["call_id"].as_str().unwrap_or_default();
            !matches!(kind, Some("ephemeral" | "framework_event")) && !readers.contains(call)
        })
        .map(|(family, message)| step(family, message))
        .collect()
}

/// The turn's messages, its families of a turn's link only.
fn turn_messages(tap: &TurnTap) -> Vec<(String, Value)> {
    tap.messages()
        .into_iter()
        .filter(|message: &TappedMessage| matches!(message.family, "request" | "frame" | "control"))
        .map(|message| {
            (
                message.family.to_owned(),
                serde_json::from_str(&message.json).unwrap(),
            )
        })
        .collect()
}

/// The messages written as the fixture's lines, the ids the run drew at
/// random (the turn's, the executor's, each call's) named in order.
fn fixture_lines(messages: &[(String, Value)]) -> String {
    let mut drawn: BTreeMap<String, String> = BTreeMap::new();
    let mut name = |id: &str, prefix: &str| {
        let count = drawn
            .values()
            .filter(|named| named.starts_with(prefix))
            .count()
            + 1;
        drawn
            .entry(id.to_owned())
            .or_insert_with(|| format!("{prefix}{count}"));
    };
    for (_, message) in messages {
        if let Some(turn) = message["turn_id"].as_str() {
            name(turn, "turn-");
        }
        if let Some(executor) = message["message"]["executor_id"].as_str() {
            name(executor, "executor-");
        }
        if let Some(call) = message["message"]["call_id"].as_str() {
            name(call, "verb-");
        }
    }
    messages
        .iter()
        .map(|(family, message)| {
            let mut line =
                serde_json::json!({ "direction": family, "message": message }).to_string();
            for (id, named) in &drawn {
                line = line.replace(id.as_str(), named);
            }
            line + "\n"
        })
        .collect()
}

/// RFC §10.6.3, §10.6.4: the probe's turn crosses as Python's does:
/// `accepted`, `output` with its stream following, the window granted,
/// the round's text and the call's start, the window granted past the
/// start, the tool's `send_event` as a `host_call` answered by its
/// `host_result`, the arguments the call ran with, its end, the answer and
/// the loop's end, the stream's end, then the turn's end.
#[tokio::test(start_paused = true)]
async fn the_probe_s_turn_carries_python_s_messages() {
    let kit = kit(|_call, _context| async {
        let turn = current_turn().unwrap();
        let note = DirectEvent::new(channel("ai"), EventContent::text("(looking it up)"));
        turn.send_event(note)
            .await
            .map_err(|error| ToolError::raised(&error))?;
        Ok(ToolOutput::Text("42".to_owned()))
    })
    .await;
    let tap = TurnTap::on(&kit);
    let result = asks(&kit, "What is it?").await;
    roomkit::testing::idle(&kit).await;
    let messages = turn_messages(&tap);
    let rust: Vec<String> = messages
        .iter()
        .map(|(family, message)| step(family, message))
        .collect();
    let timeline: Vec<(EventType, String, u32)> = kit
        .store()
        .list_events(&room(), &received())
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.index > 1)
        .map(|event| {
            (
                event.event_type,
                roomkit::text_of(&event.content),
                event.chain_depth,
            )
        })
        .collect();
    let mut expected = python_steps();
    // The extended usage record uses the protocol's existing record frame.
    let end = expected
        .iter()
        .position(|step| step.starts_with("frame item loop_end"))
        .unwrap();
    expected.insert(end, "frame record".to_owned());
    assert_eq!((result.error, rust), (None, expected));
    assert_eq!(
        timeline,
        vec![
            (EventType::Message, "What is it?".to_owned(), 0),
            (EventType::Message, "Let me check.".to_owned(), 1),
            (
                EventType::ToolCallStart,
                "[Tool call: lookup]".to_owned(),
                1
            ),
            (EventType::Message, "(looking it up)".to_owned(), 1),
            (EventType::ToolCallEnd, "[Tool call: lookup]".to_owned(), 1),
            (EventType::Message, "It is 42.".to_owned(), 1),
        ]
    );
    let written = fixture_lines(&messages);
    let path = fixtures().join("rust-case1.jsonl");
    if std::env::var_os("ROOMKIT_WRITE_TURNS_FIXTURE").is_some() {
        std::fs::write(&path, &written).unwrap();
    }
    assert_eq!(
        std::fs::read_to_string(&path).unwrap_or_default(),
        written,
        "rust-case1.jsonl is not what the turn carries: write it again with \
         ROOMKIT_WRITE_TURNS_FIXTURE=1, then run `make turns-fixture`"
    );
}

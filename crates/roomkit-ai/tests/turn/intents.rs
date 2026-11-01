//! A tool call's intent through the kit (RFC §6.4): the sentence an agent's
//! model writes with a call for the person, shown in each tool's schema as
//! `_roomkit_intent`, taken out of the call's arguments before its gate and
//! carried as the call's intent, on its hooks' event, its rows and the
//! conversation the model reads back.
use std::sync::{Arc, Mutex};

use pretty_assertions::assert_eq;
use roomkit::model::HookResult;
use roomkit::model::hook::{AllowOptions, BlockOptions};
use roomkit::model::{EventContent, EventType, ToolCallContent, ToolCallOutcome};
use roomkit::{HookExecution, HookOptions, HookTrigger, ToolCallEvent};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiChannelOptions, AiTool, ToolResult, Tools};
use serde_json::{Value, json};

use super::support::Room;

const INTENT: &str = "Je recherche les fichiers texte du projet.";

/// `shell`, its schema closed, recording the arguments it receives; a call
/// handed to it never carries an intent, as a Python handler never reads one.
fn shell(received: &Arc<Mutex<Vec<Value>>>, parameters: Value) -> Tools {
    let received = Arc::clone(received);
    Tools::new().tool(
        AiTool {
            parameters,
            ..AiTool::new("shell", "Run a command").unwrap()
        },
        move |call, _context| {
            assert_eq!(call.intent, None, "the tool never receives the intent");
            received
                .lock()
                .unwrap()
                .push(Value::Object(call.arguments.clone()));
            async { Ok(ToolResult::Json(json!("a.txt"))) }
        },
    )
}

fn closed() -> Value {
    json!({
        "type": "object",
        "properties": { "command": { "type": "string" } },
        "required": ["command"],
        "additionalProperties": false,
    })
}

fn calling(arguments: &Value) -> ScriptedRound {
    ScriptedRound::calling("", [("c1", "shell", arguments.to_string().as_str())])
}

/// A row's type, arguments, intent and outcome.
type Row = (EventType, Value, Option<String>, Option<ToolCallOutcome>);

/// What a hook saw of a call: its arguments and its intent.
type Seen = Arc<Mutex<Vec<(Value, Option<String>)>>>;

/// What the kit's observers read of each call: its tool, its outcome, its
/// intent.
type Reported = Arc<Mutex<Vec<(String, bool, Option<String>)>>>;

/// Each tool row of the room.
async fn rows(room: &Room) -> Vec<Row> {
    room.timeline()
        .await
        .into_iter()
        .filter_map(|event| {
            let EventContent::ToolCall(ToolCallContent {
                arguments,
                intent,
                outcome,
                ..
            }) = event.content
            else {
                return None;
            };
            Some((event.r#type, Value::Object(arguments), intent, outcome))
        })
        .collect()
}

/// Each tool row's intent.
async fn intents(room: &Room) -> Vec<Option<String>> {
    rows(room).await.into_iter().map(|row| row.2).collect()
}

/// Off, nothing is taken: the schema is the tool's, the reserved key is an
/// argument like any other, which the tool's closed schema refuses, as
/// Python's (RMK-1042, `tasks/probes/RMK-1040/python-today.txt`), and no row
/// carries an intent.
#[tokio::test]
async fn off_nothing_is_taken_from_a_call() {
    let call = json!({ "command": "ls", "_roomkit_intent": INTENT });
    let model = ScriptedProvider::new([calling(&call), ScriptedRound::answering("Done.")]);
    let received = Arc::default();
    let room = Room::equipped(model.clone(), shell(&received, closed()), |options| {
        AiChannelOptions {
            tool_call_intents_required: true,
            ..options
        }
    })
    .await;
    room.says("Files?").await;
    let intents = intents(&room).await;
    assert_eq!(
        (
            model.requests()[0].tools[0].parameters.clone(),
            received.lock().unwrap().clone(),
            intents,
        ),
        (closed(), Vec::new(), vec![None, None])
    );
}

/// On, the tool receives its own arguments, a closed schema included; the
/// intent rides the hooks' event and both rows, and the next request reads
/// the call with its intent among its arguments.
#[tokio::test]
async fn a_call_s_intent_rides_apart_from_its_arguments() {
    let call = json!({ "command": "ls", "_roomkit_intent": format!("  {INTENT}\n") });
    let first_usage = json!({ "input_tokens": 10, "output_tokens": 2 });
    let last_usage = json!({ "input_tokens": 20, "output_tokens": 4 });
    let model = ScriptedProvider::new([
        calling(&call).with_usage(first_usage.as_object().unwrap().clone()),
        ScriptedRound::answering("Done.").with_usage(last_usage.as_object().unwrap().clone()),
    ]);
    let received = Arc::default();
    let room = Room::equipped(model.clone(), shell(&received, closed()), |options| {
        AiChannelOptions {
            tool_call_intents: true,
            ..options
        }
    })
    .await;
    let seen: Seen = Arc::default();
    let before = Arc::clone(&seen);
    room.kit
        .hook(
            HookTrigger::BeforeToolUse,
            move |event: ToolCallEvent, _context| {
                before
                    .lock()
                    .unwrap()
                    .push((Value::Object(event.arguments.clone()), event.intent.clone()));
                async { Ok(HookResult::allow(AllowOptions::default())) }
            },
            HookOptions {
                name: "watch".to_owned(),
                ..Default::default()
            },
        )
        .unwrap();
    room.says("Files?").await;
    let command = json!({ "command": "ls" });
    let shown = &model.requests()[0].tools[0].parameters;
    assert_eq!(
        (
            shown.pointer("/properties/_roomkit_intent/type"),
            shown.get("required"),
            received.lock().unwrap().clone(),
            seen.lock().unwrap().clone(),
        ),
        (
            Some(&json!("string")),
            Some(&json!(["command"])),
            vec![command.clone()],
            vec![(command.clone(), Some(INTENT.to_owned()))],
        )
    );
    assert_eq!(
        rows(&room).await,
        [
            (
                EventType::ToolCallStart,
                command.clone(),
                Some(INTENT.to_owned()),
                None
            ),
            (
                EventType::ToolCallEnd,
                command,
                Some(INTENT.to_owned()),
                Some(ToolCallOutcome::Served)
            ),
        ]
    );
    let replayed = &model.requests()[1].messages;
    let written: Vec<Value> = replayed
        .iter()
        .flat_map(|message| message.tool_calls())
        .map(|call| Value::Object(call.arguments.clone()))
        .collect();
    assert_eq!(
        written,
        [json!({ "command": "ls", "_roomkit_intent": INTENT })]
    );
    // The same loop extracts intents and keeps the last generation apart
    // from cumulative usage (RMK-1040 and RMK-1029).
    let record = room
        .timeline()
        .await
        .into_iter()
        .find(|row| row.metadata.contains_key("ai_last_usage"))
        .unwrap()
        .metadata;
    assert_eq!(record["ai_last_usage"], last_usage);
    assert_eq!(
        record["ai_usage"],
        json!({ "input_tokens": 30, "output_tokens": 6 })
    );
    assert_eq!(record["ai_usage_complete"], true);
}

/// A later turn reads the earlier call as the turn that made it did.
#[tokio::test]
async fn a_later_turn_reads_the_call_with_its_intent() {
    let call = json!({ "command": "ls", "_roomkit_intent": INTENT });
    let model = ScriptedProvider::new([
        calling(&call),
        ScriptedRound::answering("Done."),
        ScriptedRound::answering("Again."),
    ]);
    let received = Arc::default();
    let room = Room::equipped(model.clone(), shell(&received, closed()), |options| {
        AiChannelOptions {
            tool_call_intents: true,
            ..options
        }
    })
    .await;
    room.says("Files?").await;
    room.says("And now?").await;
    let calls = |request: usize| -> Vec<Value> {
        model.requests()[request]
            .messages
            .iter()
            .flat_map(|message| message.tool_calls())
            .map(|call| Value::Object(call.arguments.clone()))
            .collect()
    };
    assert_eq!((calls(2), calls(1)), (vec![call.clone()], vec![call]));
}

/// A tool whose schema declares the property keeps it as its argument.
#[tokio::test]
async fn a_tool_that_declares_the_property_keeps_it() {
    let owner = json!({
        "type": "object",
        "properties": { "_roomkit_intent": { "type": "string" } },
    });
    let call = json!({ "_roomkit_intent": "keep" });
    let model = ScriptedProvider::new([calling(&call), ScriptedRound::answering("Done.")]);
    let received = Arc::default();
    let room = Room::equipped(model.clone(), shell(&received, owner.clone()), |options| {
        AiChannelOptions {
            tool_call_intents: true,
            tool_call_intents_required: true,
            ..options
        }
    })
    .await;
    room.says("Label?").await;
    let intents = intents(&room).await;
    assert_eq!(
        (
            model.requests()[0].tools[0].parameters.clone(),
            received.lock().unwrap().clone(),
            intents,
        ),
        (owner, vec![call], vec![None, None])
    );
}

/// The requirement changes the model's schemas, never the handler contract or
/// execution of calls from a provider that ignores that requirement.
#[tokio::test]
async fn required_intents_are_presented_and_extracted_without_rejecting_missing_ones() {
    let model = ScriptedProvider::new([
        calling(&json!({"command":"ls", "_roomkit_intent": INTENT})),
        calling(&json!({"command":"pwd"})),
        ScriptedRound::answering("Done."),
    ]);
    let received = Arc::default();
    let room = Room::equipped(model.clone(), shell(&received, closed()), |options| {
        AiChannelOptions {
            tool_call_intents: true,
            tool_call_intents_required: true,
            ..options
        }
    })
    .await;
    room.says("Inspect.").await;
    assert_eq!(
        *received.lock().unwrap(),
        [json!({"command":"ls"}), json!({"command":"pwd"})]
    );
    assert_eq!(
        intents(&room).await,
        [Some(INTENT.to_owned()), Some(INTENT.to_owned()), None, None]
    );
    for request in model.requests() {
        assert_eq!(
            request.tools[0].parameters["required"],
            json!(["command", "_roomkit_intent"])
        );
    }
}

/// An intent that is no text is none, and the call is served all the same;
/// a refused call keeps its own.
#[tokio::test]
async fn an_unusable_intent_never_fails_and_a_refusal_keeps_its_intent() {
    let unusable = json!({ "command": "ls", "_roomkit_intent": { "nested": true } });
    let invalid = json!({ "command": 3, "_roomkit_intent": INTENT });
    let model = ScriptedProvider::new([
        calling(&unusable),
        calling(&invalid),
        ScriptedRound::answering("Done."),
    ]);
    let received = Arc::default();
    let room = Room::equipped(model, shell(&received, closed()), |options| {
        AiChannelOptions {
            tool_call_intents: true,
            ..options
        }
    })
    .await;
    room.says("Files?").await;
    let ends: Vec<_> = rows(&room)
        .await
        .into_iter()
        .filter(|row| row.0 == EventType::ToolCallEnd)
        .map(|row| (row.2, row.3))
        .collect();
    assert_eq!(
        ends,
        [
            (None, Some(ToolCallOutcome::Served)),
            (Some(INTENT.to_owned()), Some(ToolCallOutcome::Refused)),
        ]
    );
    assert_eq!(*received.lock().unwrap(), [json!({ "command": "ls" })]);
}

/// A call the approval refuses and one whose tool fails keep their intent on
/// the report the kit's observers read.
#[tokio::test]
async fn a_refused_or_failed_call_is_reported_with_its_intent() {
    let blocked = json!({ "command": "rm -rf build", "_roomkit_intent": "Je nettoie." });
    let failing = json!({ "_roomkit_intent": "J'échoue." });
    let model = ScriptedProvider::new([
        ScriptedRound::calling(
            "",
            [
                ("c1", "shell", blocked.to_string().as_str()),
                ("c2", "fail", failing.to_string().as_str()),
            ],
        ),
        ScriptedRound::answering("Done."),
    ]);
    let received = Arc::default();
    let tools = shell(&received, closed()).tool(
        AiTool {
            parameters: json!({ "type": "object" }),
            ..AiTool::new("fail", "Fails").unwrap()
        },
        |_call, _context| async { Err(roomkit_ai::ToolError::raised_as("RuntimeError", "broken")) },
    );
    let room = Room::equipped(model, tools, |options| AiChannelOptions {
        tool_call_intents: true,
        ..options
    })
    .await;
    room.kit
        .hook(
            HookTrigger::BeforeToolUse,
            |event: ToolCallEvent, _context| {
                let blocks = event.name == "shell";
                async move {
                    Ok(if blocks {
                        HookResult::block("not now", BlockOptions::default())
                    } else {
                        HookResult::allow(AllowOptions::default())
                    })
                }
            },
            HookOptions {
                name: "approve".to_owned(),
                ..Default::default()
            },
        )
        .unwrap();
    let reported: Reported = Arc::default();
    let observed = Arc::clone(&reported);
    room.kit
        .hook(
            HookTrigger::OnToolCall,
            move |event: ToolCallEvent, _context| {
                observed.lock().unwrap().push((
                    event.name.clone(),
                    event.refused,
                    event.intent.clone(),
                ));
                async {}
            },
            HookOptions {
                execution: HookExecution::Async,
                name: "audit".to_owned(),
                ..Default::default()
            },
        )
        .unwrap();
    room.says("Clean?").await;
    roomkit::testing::idle(&room.kit).await;
    let mut reports = reported.lock().unwrap().clone();
    reports.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        reports,
        [
            ("fail".to_owned(), false, Some("J'échoue.".to_owned())),
            ("shell".to_owned(), true, Some("Je nettoie.".to_owned())),
        ]
    );
    assert!(received.lock().unwrap().is_empty());
}

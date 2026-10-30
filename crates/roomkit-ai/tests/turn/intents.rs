//! A tool call's intent through the kit (RFC §6.4): the sentence an agent's
//! model writes with a call for the person, shown in each tool's schema as
//! `_roomkit_intent`, taken out of the call's arguments before its gate and
//! carried as the call's intent, on its hooks' event, its rows and the
//! conversation the model reads back.
use std::sync::{Arc, Mutex};

use pretty_assertions::assert_eq;
use roomkit::ToolUse;
use roomkit::model::{EventContent, EventType, ToolCallContent, ToolCallOutcome};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{ToolDefinition, ToolOutput, Tools};
use serde_json::{Value, json};

use super::support::Room;

const INTENT: &str = "Je recherche les fichiers texte du projet.";

/// `shell`, its schema closed, recording the arguments it receives.
fn shell(received: &Arc<Mutex<Vec<Value>>>, parameters: Value) -> Tools {
    let received = Arc::clone(received);
    Tools::new().tool(
        ToolDefinition::new("shell", "Run a command", parameters),
        move |call, _context| {
            received
                .lock()
                .unwrap()
                .push(Value::Object(call.arguments.clone()));
            async { Ok(ToolOutput::Json(json!("a.txt"))) }
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
/// argument like any other, and no row carries an intent.
#[tokio::test]
async fn off_the_model_sees_and_the_tool_gets_what_they_did_before() {
    let call = json!({ "command": "ls", "_roomkit_intent": INTENT });
    let model = ScriptedProvider::new([calling(&call), ScriptedRound::answering("Done.")]);
    let received = Arc::default();
    let room = Room::new(model.clone(), shell(&received, closed())).await;
    room.says("Files?").await;
    let intents = intents(&room).await;
    assert_eq!(
        (
            model.requests()[0].tools[0].parameters.clone(),
            received.lock().unwrap().clone(),
            intents,
        ),
        (closed(), vec![call], vec![None, None])
    );
}

/// On, the tool receives its own arguments, a closed schema included; the
/// intent rides the hooks' event and both rows, and the next request reads
/// the call as the model wrote it.
#[tokio::test]
async fn a_call_s_intent_rides_apart_from_its_arguments() {
    let call = json!({ "command": "ls", "_roomkit_intent": format!("  {INTENT}\n") });
    let model = ScriptedProvider::new([calling(&call), ScriptedRound::answering("Done.")]);
    let received = Arc::default();
    let room = Room::equipped(model.clone(), shell(&received, closed()), |agent| {
        agent.with_tool_call_intents(true)
    })
    .await;
    let seen: Seen = Arc::default();
    let before = Arc::clone(&seen);
    room.kit
        .before_tool_use("watch", move |event, _context| {
            before.lock().unwrap().push((
                Value::Object(event.call.arguments.clone()),
                event.call.intent.clone(),
            ));
            async { Ok(ToolUse::Allow) }
        })
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
        .flat_map(|message| &message.tool_calls)
        .map(|call| Value::Object(call.arguments.clone()))
        .collect();
    assert_eq!(
        written,
        [json!({ "command": "ls", "_roomkit_intent": INTENT })]
    );
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
    let room = Room::equipped(model.clone(), shell(&received, closed()), |agent| {
        agent.with_tool_call_intents(true)
    })
    .await;
    room.says("Files?").await;
    room.says("And now?").await;
    let calls = |request: usize| -> Vec<Value> {
        model.requests()[request]
            .messages
            .iter()
            .flat_map(|message| &message.tool_calls)
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
    let room = Room::equipped(model.clone(), shell(&received, owner.clone()), |agent| {
        agent.with_tool_call_intents(true)
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
    let room = Room::equipped(model, shell(&received, closed()), |agent| {
        agent.with_tool_call_intents(true)
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

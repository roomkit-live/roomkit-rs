//! A tool call's intent (RFC §6.4): the sentence the model writes with a
//! call, for the person. With `with_tool_call_intents(true)`, the agent
//! shows its model each tool's schema with one optional property more,
//! `_roomkit_intent`; the model fills it ("Je recherche les fichiers texte
//! du projet."), and the loop takes it out of the call's arguments before
//! the call's gate and carries it apart, as the call's `intent`:
//!
//! - the request shows the tool's schema with the property; the tool's own
//!   schema, which the call is checked against, stays closed;
//! - the tool receives its own arguments only;
//! - `BEFORE_TOOL_USE` and the rows read the intent beside the arguments;
//! - the next request reads the call with its intent among its arguments.
//!
//! It runs against a local fake of the OpenAI wire, which scripts the
//! model's call, so it needs no network and no key.
//!
//! ```sh
//! cargo run --example tool_call_intent
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

use std::error::Error;
use std::process::ExitCode;

use roomkit::model::{
    ChannelId, ChannelType, EventContent, InboundMessage, RoomEvent, RoomId, TextContent,
    ToolCallContent,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{AttachChannelOptions, CreateRoomOptions, ProcessInboundOptions, ToolUse};
use roomkit_ai::{ToolDefinition, ToolOutput, Tools};
use roomkit_openai::{OpenAiAgent, OpenAiConfig};
use serde_json::{Value, json};

use shared::{Checks, FakeOpenAi, Log, OpenAiEndpoint, Reply};

const INTENT: &str = "Je recherche les fichiers texte du projet.";

/// The model's script: one call with its intent, then the answer.
fn script() -> Vec<Reply> {
    let arguments = json!({ "command": "find . -name '*.txt'", "_roomkit_intent": INTENT });
    vec![
        Reply::calling("", [("call_1", "shell", arguments.to_string().as_str())]),
        Reply::from("Deux fichiers texte : notes.txt et docs/todo.txt."),
    ]
}

/// `shell`, its schema closed, logging the arguments it receives.
fn shell(received: &Log<Value>) -> Tools {
    let received = received.clone();
    let parameters = json!({
        "type": "object",
        "properties": { "command": { "type": "string" } },
        "required": ["command"],
        "additionalProperties": false,
    });
    Tools::new().tool(
        ToolDefinition::new("shell", "Run a shell command in the project.", parameters),
        move |call, _context| {
            received.push(Value::Object(call.arguments.clone()));
            async { Ok(ToolOutput::Json(json!("notes.txt\ndocs/todo.txt"))) }
        },
    )
}

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let endpoint = OpenAiEndpoint::Fake(FakeOpenAi::start(&script()).await?);
    let received = Log::new();
    let (kit, sms, room) = project_room(&endpoint, &received).await?;
    let approvals = Log::new();
    let asked = approvals.clone();
    kit.before_tool_use("approve", move |event, _context| {
        asked.push(event.call.intent.clone());
        async { Ok(ToolUse::Allow) }
    })?;

    let message = InboundMessage::new(
        sms.channel_id().clone(),
        "+15551234567",
        TextContent::new("Trouve les fichiers texte du projet."),
    );
    let options = ProcessInboundOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    };
    kit.process_inbound(message, options).await?;
    kit.close().await?;

    let rows = tool_rows(shared::timeline(&kit, &room).await?);
    let requests = endpoint
        .fake()
        .map(FakeOpenAi::requests)
        .unwrap_or_default();
    let (shown, replayed) = (shown_schema(&requests), replayed_call(&requests));
    println!("The schema the model is shown: {shown}");
    println!("The tool receives: {:?}", received.entries());
    for row in &rows {
        let arguments = Value::Object(row.arguments.clone());
        println!("Row: arguments={arguments} intent={:?}", row.intent);
    }
    println!("The next request reads the call as: {replayed}");

    let command = json!({ "command": "find . -name '*.txt'" });
    let mut checks = Checks::default();
    checks.expect(
        "the model is shown the intent property, the schema still closed",
        shown.pointer("/properties/_roomkit_intent/type") == Some(&json!("string"))
            && shown.get("additionalProperties") == Some(&json!(false)),
    );
    checks.expect(
        "the tool receives its own arguments only",
        received.entries() == [command.clone()],
    );
    checks.expect(
        "BEFORE_TOOL_USE reads the intent",
        approvals.entries() == [Some(INTENT.to_owned())],
    );
    checks.expect(
        "both rows carry the tool's arguments and the intent apart",
        rows.len() == 2
            && rows.iter().all(|row| {
                Value::Object(row.arguments.clone()) == command
                    && row.intent.as_deref() == Some(INTENT)
            }),
    );
    checks.expect(
        "the next request reads the call with its intent among its arguments",
        replayed == json!({ "command": "find . -name '*.txt'", "_roomkit_intent": INTENT }),
    );
    Ok(checks.finish())
}

/// The room a person writes to by SMS, with an agent whose model writes
/// each call's intent.
async fn project_room(
    endpoint: &OpenAiEndpoint,
    received: &Log<Value>,
) -> Result<(roomkit::RoomKit, RecordingTransport, RoomId), Box<dyn Error>> {
    let config = OpenAiConfig::new(endpoint.base_url(), endpoint.api_key(), endpoint.model());
    let agent = OpenAiAgent::new(ChannelId::new("assistant")?, config)?
        .with_tools(shell(received))?
        .with_tool_call_intents(true);
    let kit = shared::kit();
    let sms = RecordingTransport::new(
        ChannelId::new("sms")?,
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(sms.clone())?;
    kit.register_channel(agent)?;
    let room = RoomId::new("project")?;
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await?;
    for channel in ["sms", "assistant"] {
        kit.attach_channel(
            &room,
            &ChannelId::new(channel)?,
            AttachChannelOptions::default(),
        )
        .await?;
    }
    Ok((kit, sms, room))
}

/// The room's tool rows.
fn tool_rows(timeline: Vec<RoomEvent>) -> Vec<ToolCallContent> {
    timeline
        .into_iter()
        .filter_map(|event| {
            let EventContent::ToolCall(row) = event.content else {
                return None;
            };
            Some(row)
        })
        .collect()
}

/// The schema of the tool the first request shows.
fn shown_schema(requests: &[Value]) -> Value {
    requests
        .first()
        .and_then(|request| request.pointer("/tools/0/function/parameters"))
        .cloned()
        .unwrap_or_default()
}

/// The arguments of the call the second request replays.
fn replayed_call(requests: &[Value]) -> Value {
    requests
        .get(1)
        .and_then(|request| request.get("messages"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .find_map(|message| message.pointer("/tool_calls/0/function/arguments"))
        .and_then(Value::as_str)
        .and_then(|text| serde_json::from_str::<Value>(text).ok())
        .unwrap_or_default()
}

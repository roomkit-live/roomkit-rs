//! An AI agent's earlier tool calls, read back on its next turn (RFC §6.4
//! "The tools of earlier turns"): Alice books a meeting room by SMS, and
//! the agent's answer leaves out the booking number. Her next message asks
//! for it, and the model reads it from the earlier call's result, which
//! the history replays at its place: the first question, the assistant's
//! call, the tool's result, the first answer, then the new question, the
//! way the first turn sent them to the model.
//!
//! It runs against a local fake of the OpenAI wire, which scripts the
//! model's calls and keeps every request, so it needs no network and no
//! key.
//!
//! ```sh
//! cargo run --example agent_tool_history
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

use std::error::Error;
use std::process::ExitCode;

use roomkit::model::{ChannelId, ChannelType, InboundMessage, RoomId, TextContent};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{AttachChannelOptions, CreateRoomOptions, ProcessInboundOptions, RoomKit};
use roomkit_ai::{AiChannel, AiChannelOptions, AiError, AiTool, ToolHandler, ToolResult};
use serde_json::{Value, json};

use shared::{Checks, FakeOpenAi, OpenAiEndpoint, Reply};

/// What Alice writes, turn after turn.
const BOOK: &str = "Book the Quebec room on 2026-11-03 at 14:00 for 90 minutes.";
const ASK: &str = "What is my booking number, and when does it end?";

/// The booking's number, which only the tool's result holds.
const BOOKING: &str = "BK-4821";

/// The model's script: the booking's call, an answer without its number,
/// then the answer to the follow-up, read from the replayed result.
fn script() -> Vec<Reply> {
    let booking = r#"{"room": "Quebec", "date": "2026-11-03", "start": "14:00", "minutes": 90}"#;
    vec![
        Reply::calling("", [("call_1", "book_meeting_room", booking)]),
        Reply::from("The Quebec room is booked for November 3 from 14:00."),
        Reply::from("Your booking number is BK-4821; it ends at 15:30."),
    ]
}

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let endpoint = OpenAiEndpoint::Fake(FakeOpenAi::start(&script()).await?);
    let (kit, sms, room) = office(&endpoint).await?;
    for text in [BOOK, ASK] {
        println!("Alice: {text}");
        let message = InboundMessage::new(
            sms.channel_id().clone(),
            "+15551234567",
            TextContent::new(text),
        );
        let options = ProcessInboundOptions {
            room_id: Some(room.clone()),
            ..Default::default()
        };
        kit.process_inbound(message, options).await?;
        if let Some(answer) = sms.texts().last() {
            println!("Assistant: {answer}");
        }
    }
    kit.close().await?;

    let requests = endpoint
        .fake()
        .map(FakeOpenAi::requests)
        .unwrap_or_default();
    let messages = |request: Option<&Value>| -> Vec<Value> {
        request
            .and_then(|request| request.pointer("/messages")?.as_array().cloned())
            .unwrap_or_default()
    };
    let (first_turn, second_turn) = (messages(requests.get(1)), messages(requests.get(2)));
    println!("\nWhat the model read on the second turn:");
    for message in &second_turn {
        println!("  {message}");
    }
    let mut checks = Checks::default();
    check_the_history(&mut checks, &first_turn, &second_turn);
    checks.expect(
        "the first answer left the booking number out, and the second reads it",
        sms.texts()
            .first()
            .is_some_and(|answer| !answer.contains(BOOKING))
            && sms
                .texts()
                .last()
                .is_some_and(|answer| answer.contains(BOOKING)),
    );
    Ok(checks.finish())
}

/// The checks on the second turn's request: the first turn's round at its
/// place, as the first turn sent it.
fn check_the_history(checks: &mut Checks, first_turn: &[Value], second_turn: &[Value]) {
    let at = |position: usize, pointer: &str| {
        second_turn
            .get(position)
            .and_then(|message| message.pointer(pointer))
            .cloned()
            .unwrap_or_default()
    };
    let roles: Vec<Value> = (0..second_turn.len()).map(|n| at(n, "/role")).collect();
    checks.expect(
        "the second turn reads the question, the call, its result, the answer, then the new question",
        roles == ["user", "assistant", "tool", "assistant", "user"],
    );
    let arguments = at(1, "/tool_calls/0/function/arguments");
    let arguments: Value =
        serde_json::from_str(arguments.as_str().unwrap_or("{}")).unwrap_or_default();
    checks.expect(
        "the call is the assistant's, as the model made it, with no text",
        at(1, "/content").is_null()
            && at(1, "/tool_calls/0/id") == "call_1"
            && at(1, "/tool_calls/0/function/name") == "book_meeting_room"
            && arguments.pointer("/room") == Some(&json!("Quebec"))
            && arguments.pointer("/minutes") == Some(&json!(90)),
    );
    checks.expect(
        "the result answers the call by its id and holds the booking number",
        at(2, "/tool_call_id") == "call_1"
            && at(2, "/content")
                .as_str()
                .is_some_and(|content| content.contains(BOOKING)),
    );
    checks.expect(
        "the call and its result read as the first turn sent them to the model",
        first_turn.len() == 3 && second_turn.get(..3) == Some(first_turn),
    );
    checks.expect(
        "the first answer follows the round, then the new question",
        at(3, "/content") == "The Quebec room is booked for November 3 from 14:00."
            && at(4, "/content") == ASK,
    );
}

/// The office Alice writes to by SMS, with an agent that books its rooms.
async fn office(
    endpoint: &OpenAiEndpoint,
) -> Result<(RoomKit, RecordingTransport, RoomId), Box<dyn Error>> {
    let options = AiChannelOptions {
        tools: vec![booking()?.into()],
        tool_handler: Some(ToolHandler::new(|_name, arguments| async move {
            let mut booked = json!({ "booked": true, "booking_id": BOOKING, "end": "15:30" });
            if let Value::Object(fields) = &mut booked {
                fields.extend(arguments);
            }
            Ok(ToolResult::Json(booked))
        })),
        ..AiChannelOptions::default()
    };
    let agent = AiChannel::new(ChannelId::new("assistant")?, endpoint.provider()?, options)?;
    let kit = shared::kit();
    let sms = RecordingTransport::new(
        ChannelId::new("sms")?,
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(sms.clone())?;
    kit.register_channel(agent)?;
    let room = RoomId::new("office")?;
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

/// `book_meeting_room`, which the agent's handler serves: the booking's
/// number is only in its result.
fn booking() -> Result<AiTool, AiError> {
    let parameters = json!({
        "type": "object",
        "properties": {
            "room": { "type": "string" },
            "date": { "type": "string", "description": "YYYY-MM-DD" },
            "start": { "type": "string", "description": "HH:MM" },
            "minutes": { "type": "integer" },
        },
        "required": ["room", "date", "start", "minutes"],
    });
    Ok(AiTool {
        parameters,
        ..AiTool::new("book_meeting_room", "Book a meeting room")?
    })
}

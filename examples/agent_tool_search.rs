//! Tool Search (RFC §6.4, §21.1): an agent with a large catalogue declares a
//! few tools and hides the rest behind `find_tools` and `list_tools`, which it
//! serves itself.
//!
//! Alice asks by SMS for a text to be sent. The agent has 30 tools, and one
//! pinned. The model sees the pinned tool and the two discovery tools; it
//! searches, the next round declares the match, the call runs, and it answers.
//! Her next message's turn still declares the tool it used: a tool found or
//! used stays callable for the room, the rest stays hidden.
//!
//! Past the thresholds the catalogue hides itself (`tool_search: None`, the
//! default): more than 8,000 tokens of schemas, more than 10% of the model's
//! window, or, the window unknown, more than 20 tools. This example forces it,
//! so that its small catalogue is hidden too. `ON_AI_RESPONSE` reports what
//! each round declared and why, which the first round's hook cannot.
//!
//! It runs against a local fake of the OpenAI wire, which scripts the model's
//! calls, so it needs no network and no key.
//!
//! ```sh
//! cargo run --example agent_tool_search
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

use std::error::Error;
use std::process::ExitCode;

use roomkit::model::hook::AllowOptions;
use roomkit::model::{ChannelId, ChannelType, HookResult, InboundMessage, RoomId, TextContent};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, HookExecution, HookOptions, HookTrigger,
    ProcessInboundOptions, RoomKit,
};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiError, AiResponseEvent, AiTool, ToolDeclarationOrigin,
    ToolEntry, ToolHandler, ToolResult,
};
use serde_json::{Value, json};

use shared::{Checks, FakeOpenAi, Log, OpenAiEndpoint, Reply};

/// The model's script: search, call what it found, answer; then, on Alice's
/// next message, answer.
fn script() -> Vec<Reply> {
    vec![
        Reply::calling(
            "",
            [(
                "call_1",
                "find_tools",
                r#"{"query": "send a text message to a contact"}"#,
            )],
        ),
        Reply::calling(
            "",
            [(
                "call_2",
                "send_sms",
                r#"{"to": "+15145550123", "body": "On my way!"}"#,
            )],
        ),
        Reply::from("Done, your text is sent."),
        Reply::from("Yes, it went out."),
    ]
}

/// The catalogue: a tool to send a text among 28 others, and one to ask for
/// help, which every round declares.
fn catalogue() -> Result<Vec<ToolEntry>, AiError> {
    let mut tools = vec![
        AiTool {
            parameters: json!({
                "type": "object",
                "properties": { "to": { "type": "string" }, "body": { "type": "string" } },
                "required": ["to", "body"],
            }),
            // Keywords find_tools searches beside the name and the description.
            tags: vec!["sms".to_owned(), "phone".to_owned()],
            ..AiTool::new("send_sms", "Send a text message to a phone number.")?
        },
        AiTool::new("get_help", "Explain what this assistant can do.")?,
    ];
    for at in 0..28 {
        let (name, does) = (
            format!("crm_action_{at}"),
            format!("Update CRM record {at}."),
        );
        tools.push(AiTool::new(name, does)?);
    }
    Ok(tools.into_iter().map(ToolEntry::Definition).collect())
}

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let endpoint = OpenAiEndpoint::Fake(FakeOpenAi::start(&script()).await?);
    let options = AiChannelOptions {
        system_prompt: Some("You are a helpful assistant.".to_owned()),
        tools: catalogue()?,
        tool_handler: Some(ToolHandler::new(|name, _arguments| async move {
            Ok(ToolResult::Text(format!("{name} ran")))
        })),
        // Forced for the example; leave it `None` to let the thresholds decide.
        tool_search: Some(true),
        tool_search_pinned: vec!["get_help".to_owned()],
        tool_search_miss_hint: Some("get_help says what this assistant does.".to_owned()),
        ..AiChannelOptions::default()
    };
    let agent = AiChannel::new(ChannelId::new("assistant")?, endpoint.provider()?, options)?;
    let kit = shared::kit();
    let reports = report_declarations(&kit)?;
    let (sms, room) = room_of(&kit, agent).await?;
    for said in ["Text Sam that I am on my way.", "Did it go out?"] {
        let message = InboundMessage::new(sms.clone(), "+15551234567", TextContent::new(said));
        kit.process_inbound(
            message,
            ProcessInboundOptions {
                room_id: Some(room.clone()),
                ..Default::default()
            },
        )
        .await?;
    }
    // ON_AI_RESPONSE's observers run in the background: the close lets them finish.
    kit.close().await?;

    let rounds: Vec<Vec<String>> = endpoint
        .fake()
        .map(FakeOpenAi::requests)
        .unwrap_or_default()
        .iter()
        .map(declared)
        .collect();
    for (at, tools) in rounds.iter().enumerate() {
        println!("Request {at} declares: {}", tools.join(", "));
    }
    let reports = reports.entries();
    println!("ON_AI_RESPONSE reports: {reports:?}");
    let holds = |request: usize, name: &str| {
        rounds
            .get(request)
            .is_some_and(|tools| tools.iter().any(|tool| tool == name))
    };
    let mut checks = Checks::default();
    checks.expect(
        "the first round declares the pinned tool and the discovery tools alone, then \
         read_stored_result",
        rounds.first().is_some_and(|tools| tools.len() == 4)
            && holds(0, "get_help")
            && holds(0, "find_tools")
            && holds(0, "list_tools")
            && holds(0, "read_stored_result"),
    );
    checks.expect(
        "the round after the search declares what it found, no other tool of the catalogue",
        holds(1, "send_sms") && !holds(1, "crm_action_0"),
    );
    checks.expect(
        "the next turn's first round still declares the tool it used",
        holds(3, "send_sms") && !holds(3, "crm_action_0"),
    );
    checks.expect(
        "ON_AI_RESPONSE reports the tool as revealed on its turn, then as sticky",
        reports
            == [
                ("send_sms".to_owned(), ToolDeclarationOrigin::Revealed),
                ("send_sms".to_owned(), ToolDeclarationOrigin::Sticky),
            ],
    );
    Ok(checks.finish())
}

/// The names of the tools a request to the wire declares.
fn declared(request: &Value) -> Vec<String> {
    request
        .get("tools")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|tool| tool.pointer("/function/name").and_then(Value::as_str))
        .map(str::to_owned)
        .collect()
}

/// Keeps why each turn declared `send_sms`, from its `ON_AI_RESPONSE`.
fn report_declarations(
    kit: &RoomKit,
) -> Result<Log<(String, ToolDeclarationOrigin)>, Box<dyn Error>> {
    let reports = Log::new();
    let kept = reports.clone();
    kit.hook(
        HookTrigger::OnAiResponse,
        move |event: AiResponseEvent, _context| {
            let sent = event.declared_tools.iter().find(|t| t.name == "send_sms");
            if let Some(tool) = sent {
                kept.push((tool.name.clone(), tool.origin));
            }
            async move { Ok(HookResult::allow(AllowOptions::default())) }
        },
        HookOptions {
            name: "report".to_owned(),
            execution: HookExecution::Async,
            ..Default::default()
        },
    )?;
    Ok(reports)
}

/// The room Alice writes to by SMS, with the agent.
async fn room_of(
    kit: &RoomKit,
    agent: AiChannel<roomkit_openai::OpenAiAiProvider>,
) -> Result<(ChannelId, RoomId), Box<dyn Error>> {
    let sms = RecordingTransport::new(
        ChannelId::new("sms")?,
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    let sms_id = sms.channel_id().clone();
    kit.register_channel(sms)?;
    kit.register_channel(agent)?;
    let room = RoomId::new("room_1")?;
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
    Ok((sms_id, room))
}

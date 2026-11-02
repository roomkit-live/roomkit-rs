//! Write to an agent while it works: its running turn reads the message
//! (RFC §6.4 "A message for the running turn", §21.3).
//!
//! A person asks an agent to list the files; while its tool runs, they add
//! "Only the .py files." With `steer_room_turns: true` that second message
//! is stored in the room like any message, and handed to the turn already
//! running instead of starting a second turn beside it: the model reads it
//! before its next round and answers once, knowing both. A message that
//! comes while the model writes its answer is read too: the same turn asks
//! the model again. The Python implementation's
//! `examples/agent_steered_turn.py`.
//!
//! It runs on a stand-in model, so it needs no network and no key.
//!
//! ```sh
//! cargo run --example agent_steered_turn
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

use std::error::Error;
use std::future::Future;
use std::process::ExitCode;

use roomkit::model::{
    ChannelId, ChannelType, EventType, InboundMessage, RoomEvent, RoomId, TextContent,
};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, ProcessInboundOptions, RoomKit, RoomKitOptions,
};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiContext, AiProvider, AiResponse, AiRole, AiTool, AiToolCall,
    ProviderError, ToolHandler, ToolResult,
};
use tokio::sync::watch;

use shared::{Checks, Log, recording, text_of};

const STEERED: &str = "Only the .py files.";

/// A stand-in model: it lists the files first, then answers from what the
/// person said last; it logs what it reads of the person each time.
struct Coder {
    read: Log<Vec<String>>,
}

impl AiProvider for Coder {
    fn model_name(&self) -> &'static str {
        "coder"
    }

    fn generate<'a>(
        &'a self,
        context: &'a AiContext,
    ) -> impl Future<Output = Result<AiResponse, ProviderError>> + Send + 'a {
        std::future::ready(Ok(self.answer(context)))
    }
}

impl Coder {
    /// The answer to `context`: a call to `list_files` before any result,
    /// then the files the person asked for last.
    fn answer(&self, context: &AiContext) -> AiResponse {
        let said: Vec<String> = context
            .messages
            .iter()
            .filter(|message| message.role == AiRole::User)
            .map(roomkit_ai::AiMessage::text)
            .collect();
        println!("The model reads: {said:?}");
        self.read.push(said.clone());
        if !context.messages.iter().any(|m| m.tool_result().is_some()) {
            return AiResponse {
                tool_calls: vec![AiToolCall::new("c1", "list_files")],
                ..AiResponse::new("")
            };
        }
        let last = said.last().map(String::as_str).unwrap_or_default();
        AiResponse::new(if last.contains(STEERED) {
            "a.py, main.py"
        } else {
            "a.py, main.py, notes.txt"
        })
    }
}

/// `list_files`, slow: it says it started, then waits until released.
fn list_files(listing: watch::Sender<bool>, go_on: watch::Receiver<bool>) -> ToolHandler {
    ToolHandler::new(move |_name, _arguments| {
        let (listing, mut go_on) = (listing.clone(), go_on.clone());
        async move {
            listing.send_replace(true);
            drop(go_on.wait_for(|go| *go).await);
            Ok(ToolResult::Text("a.py main.py notes.txt".to_owned()))
        }
    })
}

/// The person's message on the WebSocket.
fn said(body: &str) -> Result<(InboundMessage, ProcessInboundOptions), Box<dyn Error>> {
    let message = InboundMessage::new(ChannelId::new("ws")?, "dev", TextContent::new(body));
    let options = ProcessInboundOptions {
        room_id: Some(RoomId::new("session")?),
        ..Default::default()
    };
    Ok((message, options))
}

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let (listing, mut listed) = watch::channel(false);
    let (go_on, waiting) = watch::channel(false);
    let read = Log::new();
    let options = AiChannelOptions {
        tools: vec![AiTool::new("list_files", "List the project's files.")?.into()],
        tool_handler: Some(list_files(listing, waiting)),
        steer_room_turns: true,
        ..AiChannelOptions::default()
    };
    let coder = AiChannel::new(
        ChannelId::new("coder")?,
        Coder { read: read.clone() },
        options,
    )?;
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(recording("ws", ChannelType::WEBSOCKET)?)?;
    kit.register_channel(coder)?;
    let room = RoomId::new("session")?;
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await?;
    for channel in ["ws", "coder"] {
        let channel = ChannelId::new(channel)?;
        kit.attach_channel(&room, &channel, AttachChannelOptions::default())
            .await?;
    }

    let (first, options) = said("List the files.")?;
    let asking = kit.clone();
    let turn = tokio::spawn(async move { asking.process_inbound(first, options).await });
    listed.wait_for(|listed| *listed).await?;
    println!("While the tool runs, the person writes: {STEERED:?}");
    let (steered, options) = said(STEERED)?;
    kit.process_inbound(steered, options).await?;
    go_on.send_replace(true);
    turn.await??;

    let timeline = shared::timeline(&kit, &room).await?;
    for event in timeline.iter().filter(|event| is_message(event)) {
        println!("{:<6} {}", event.source.channel_id, text_of(&event.content));
    }
    kit.close().await?;

    let by = |channel: &str| -> Vec<String> {
        timeline
            .iter()
            .filter(|event| is_message(event) && event.source.channel_id.as_str() == channel)
            .map(|event| text_of(&event.content))
            .collect()
    };
    let reads = read.entries();
    let mut checks = Checks::default();
    checks.expect(
        "the person's two messages are in the room, once each",
        by("ws") == ["List the files.", STEERED],
    );
    checks.expect(
        "one turn: the model was asked twice, its second round reading the steered message",
        reads.len() == 2
            && reads
                .get(1)
                .is_some_and(|said| said.iter().any(|s| s == STEERED)),
    );
    checks.expect(
        "the agent answers once, knowing both",
        by("coder") == ["a.py, main.py"],
    );
    Ok(checks.finish())
}

/// Whether `event` is a message a person reads.
fn is_message(event: &RoomEvent) -> bool {
    event.r#type == EventType::Message
}

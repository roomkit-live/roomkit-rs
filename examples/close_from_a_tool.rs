//! The kit's one shutdown, closed from a tool (RFC §12.10.4): an operator
//! tells a service's agent by SMS to shut the service down for maintenance,
//! and the model calls the agent's `shut_down` tool, which closes the kit.
//!
//! The tool's `close` runs inside the agent's turn, the kit's own work: the
//! shutdown neither waits for that call nor cuts it. It cuts the turn, as
//! every turn, the tool's call left to run on, its end row stored, closes
//! every channel, the operator's pager among them, and the tool's `close`
//! returns then, at once, where waiting for its own turn would have held it
//! for the close's 5 s grace before cutting it. The service's own `close`,
//! once the message's call returned, waits for the same shutdown, which
//! ended already, and returns at once: the channels are closed once. A call
//! made then is refused. It runs against a local fake of the OpenAI wire,
//! which scripts the model's call, so it needs no network and no key.
//!
//! ```sh
//! cargo run --example close_from_a_tool
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

use std::error::Error;
use std::future::Future;
use std::process::ExitCode;
use std::time::Duration;

use roomkit::model::{
    ChannelCategory, ChannelDirection, ChannelId, ChannelType, InboundMessage, RoomId, TextContent,
};
use roomkit::testing::RecordingTransport;
use roomkit::{
    AttachChannelOptions, Channel, ChannelDeclaration, CreateRoomOptions, ProcessInboundOptions,
    RoomKit, RoomKitError,
};
use roomkit_ai::{ToolDefinition, ToolError, ToolOutput, Tools};
use roomkit_openai::{OpenAiAgent, OpenAiConfig};
use serde_json::json;
use tokio::time::Instant;

use shared::{Checks, FakeOpenAi, Log, OpenAiEndpoint, Reply};

/// Far less than the close's grace, 5 s, which a close that waited for its
/// own turn would take.
const AT_ONCE: Duration = Duration::from_secs(1);

/// The model's script: the call to `shut_down`, then the answer.
fn script() -> Vec<Reply> {
    vec![
        Reply::calling("Shutting down.", [("call_1", "shut_down", "{}")]),
        Reply::from("The service is closed for maintenance."),
    ]
}

/// The operator's pager, a channel that notes its close.
#[derive(Clone)]
struct Pager {
    declaration: ChannelDeclaration,
    closes: Log<&'static str>,
}

impl Pager {
    fn new() -> Result<Self, Box<dyn Error>> {
        let declaration = ChannelDeclaration::new(
            ChannelId::new("pager")?,
            ChannelType::WEBHOOK,
            ChannelCategory::Transport,
            ChannelDirection::Outbound,
        );
        Ok(Self {
            declaration,
            closes: Log::new(),
        })
    }
}

impl Channel for Pager {
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
    }

    fn close(&self) -> impl Future<Output = ()> + Send {
        let closes = self.closes.clone();
        async move { closes.push("pager closed") }
    }
}

/// What the tool saw of its `close`: how long it took, and whether the
/// pager was closed when it returned.
type Closed = (Duration, bool);

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let endpoint = OpenAiEndpoint::Fake(FakeOpenAi::start(&script()).await?);
    let kit = shared::kit();
    let pager = Pager::new()?;
    let closed = Log::new();
    let room = service_room(&kit, &endpoint, &pager, &closed).await?;

    println!("The operator asks the service's agent to shut the service down.");
    let message = InboundMessage::new(
        ChannelId::new("sms")?,
        "+15551234567",
        TextContent::new("Shut the service down for maintenance."),
    );
    let options = ProcessInboundOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    };
    let result = kit.process_inbound(message, options).await?;
    let started = Instant::now();
    kit.close().await?;
    let second = started.elapsed();

    let refused = kit
        .create_room(CreateRoomOptions::default())
        .await
        .is_err_and(|error| matches!(error, RoomKitError::KitClosed));
    let closed = closed.entries();
    println!("The tool's close: {closed:?}; the service's own close took {second:?}.");
    println!("The pager: {:?}", pager.closes.entries());
    let mut checks = Checks::default();
    checks.expect(
        "the tool's close returns at once, every channel closed",
        closed
            .first()
            .is_some_and(|(took, pager_closed)| *took < AT_ONCE && *pager_closed),
    );
    checks.expect(
        "the message's call returns, its delivery dropped by the close",
        result.cancellation_reason.as_deref() == Some("kit_closed"),
    );
    checks.expect(
        "the service's own close returns at once, the channels closed once",
        second < AT_ONCE && pager.closes.entries() == ["pager closed"],
    );
    checks.expect("a call made once the kit is closed is refused", refused);
    Ok(checks.finish())
}

/// The room the operator writes to by SMS, with the service's agent, its
/// `shut_down` tool closing the kit, and the operator's pager.
async fn service_room(
    kit: &RoomKit,
    endpoint: &OpenAiEndpoint,
    pager: &Pager,
    closed: &Log<Closed>,
) -> Result<RoomId, Box<dyn Error>> {
    let config = OpenAiConfig::new(endpoint.base_url(), endpoint.api_key(), endpoint.model())
        .with_system_prompt("You run a service for its operator.");
    let agent = OpenAiAgent::new(ChannelId::new("assistant")?, config)?
        .with_tools(shut_down(kit, pager, closed))?;
    kit.register_channel(RecordingTransport::new(
        ChannelId::new("sms")?,
        ChannelType::SMS,
    ))?;
    kit.register_channel(agent)?;
    kit.register_channel(pager.clone())?;
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
    Ok(room)
}

/// The `shut_down` tool: it closes the kit, held weakly since the kit holds
/// the agent, and notes what its close took.
fn shut_down(kit: &RoomKit, pager: &Pager, closed: &Log<Closed>) -> Tools {
    let (kit, pager, closed) = (kit.downgrade(), pager.clone(), closed.clone());
    let definition = ToolDefinition::new(
        "shut_down",
        "Shut the service down",
        json!({ "type": "object" }),
    );
    Tools::new().tool(definition, move |_call, _context| {
        let (kit, pager, closed) = (kit.upgrade(), pager.clone(), closed.clone());
        async move {
            let started = Instant::now();
            if let Some(kit) = kit {
                kit.close()
                    .await
                    .map_err(|error| ToolError::raised(&error))?;
            }
            closed.push((started.elapsed(), !pager.closes.entries().is_empty()));
            Ok(ToolOutput::from("closed"))
        }
    })
}

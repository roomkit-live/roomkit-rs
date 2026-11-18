//! Route text turns to workers by room organization (RFC §10.6.7).
//! Run `cargo run --example turns_executor_organizations`.

use std::error::Error;
use std::future::ready;
use std::process::ExitCode;
use std::sync::Arc;

use roomkit::model::{
    ChannelCategory, ChannelId, ChannelType, InboundMessage, RoomId, TextContent,
};
use roomkit::store::ListEventsOptions;
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::turns::{InMemoryTurnBackend, TurnBackend};
use roomkit::{
    AgentIdentity, AgentPresenceOptions, AttachChannelOptions, ChannelDeclaration,
    ConversationStore, CreateRoomOptions, ProcessInboundOptions, RoomKit, RoomKitError,
    RoomKitOptions, TurnExecutor, TurnExecutorOptions,
};
use shared::{Checks, Script, ScriptedAgent, text_of};

fn worker(
    id: &str,
    organization: Option<&str>,
    answers: Script,
) -> Result<Arc<TurnExecutor>, RoomKitError> {
    TurnExecutor::new(
        move |channel| ready(Ok(Some(ScriptedAgent::new(channel, answers)))),
        TurnExecutorOptions {
            executor_id: Some(id.into()),
            hosts: Some(["support".into()].into()),
            organizations: organization.map(|org| [org.to_owned()].into()),
            ..Default::default()
        },
    )
    .map(Arc::new)
}

async fn run() -> Result<ExitCode, Box<dyn Error + Send + Sync>> {
    let backend = Arc::new(InMemoryTurnBackend::new());
    let tenant = worker("acme-pool", Some("acme"), &[("Hi.", "Hello from acme.")])?;
    let platform = worker("platform-pool", None, &[("Hi.", "Hello from platform.")])?;
    for worker in [&tenant, &platform] {
        backend.serve(Arc::<TurnExecutor>::clone(worker)).await?;
    }
    let kit = RoomKit::new(RoomKitOptions {
        turn_backend: Some(backend),
        ..Default::default()
    });
    let chat: ChannelId = "chat".parse()?;
    let support: ChannelId = "support".parse()?;
    kit.register_channel(RecordingTransport::new(
        chat.clone(),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    ))?;
    kit.register_channel(ChannelDeclaration {
        identity: Some(AgentIdentity::default()),
        ..ChannelDeclaration::new(support.clone())
    })?;

    let mut checks = Checks::default();
    for (name, org, expected) in [
        ("acme-room", Some("acme"), Some("Hello from acme.")),
        ("platform-room", None, Some("Hello from platform.")),
        ("globex-room", Some("globex"), None),
    ] {
        let room: RoomId = name.parse()?;
        checks.expect(
            name,
            room_answers(&kit, &room, org, &chat, &support, expected).await?,
        );
    }
    kit.close().await?;
    tenant.close().await;
    platform.close().await;
    Ok(checks.finish())
}

async fn room_answers(
    kit: &RoomKit,
    room: &RoomId,
    org: Option<&str>,
    chat: &ChannelId,
    support: &ChannelId,
    expected: Option<&str>,
) -> Result<bool, Box<dyn Error + Send + Sync>> {
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        organization_id: org.map(str::to_owned),
        ..Default::default()
    })
    .await?;
    kit.attach_channel(room, chat, AttachChannelOptions::default())
        .await?;
    kit.attach_channel(
        room,
        support,
        AttachChannelOptions {
            category: Some(ChannelCategory::Intelligence),
            ..Default::default()
        },
    )
    .await?;
    let result = kit
        .process_inbound(
            InboundMessage::new(chat.clone(), "ana", TextContent::new("Hi.")),
            ProcessInboundOptions {
                room_id: Some(room.clone()),
                ..Default::default()
            },
        )
        .await?;
    let timeline = kit
        .store()
        .list_events(room, ListEventsOptions::default())
        .await?;
    let replies: Vec<_> = timeline
        .iter()
        .filter(|event| event.source.channel_id == *support)
        .map(|event| text_of(&event.content))
        .collect();
    let presence = kit
        .agent_presence(room, AgentPresenceOptions::default())
        .await?;
    let online = presence
        .iter()
        .find(|agent| agent.channel_id == support.as_str())
        .map(|agent| agent.online);
    Ok(replies.iter().map(String::as_str).eq(expected)
        && result.error.is_none() == expected.is_some()
        && online == Some(expected.is_some()))
}

fn main() -> Result<ExitCode, Box<dyn Error + Send + Sync>> {
    tokio::runtime::Runtime::new()?.block_on(run())
}

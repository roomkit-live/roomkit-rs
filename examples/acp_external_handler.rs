//! A custom handler approves an ACP agent's read and delegates its report.
//!
//! Run `cargo run --example acp_external_handler`. The agent is this program
//! started with `--acp-agent`; no credentials, vendor or network are needed.

#[path = "acp_external_handler/agent.rs"]
mod agent;

use std::error::Error;
use std::sync::{Arc, Mutex, PoisonError};

use roomkit::model::{
    ChannelId, ChannelType, InboundMessage, Metadata, RoomContext, RoomId, TextContent,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, HookExecution, HookRegistration, HookTrigger,
    ProcessInboundOptions, RoomKit, RoomKitOptions, ToolCallEvent,
};
use roomkit_acp::{AcpChannel, AcpChannelOptions, StdioAcpTransport};
use roomkit_ai::{
    ExternalToolFuture, ExternalToolHandler, ExternalToolHandlerState, OnToolResultOptions,
    PolicyExternalToolHandler, ProcessToolCallOptions, ToolDecision, ToolPolicy,
};

struct AuditedHandler {
    policy: PolicyExternalToolHandler,
    calls: Mutex<Vec<String>>,
}

impl ExternalToolHandler for AuditedHandler {
    fn state(&self) -> &ExternalToolHandlerState {
        self.policy.state()
    }

    fn process_tool_call<'a>(
        &'a self,
        tool_name: &'a str,
        tool_input: &'a Metadata,
        options: ProcessToolCallOptions,
    ) -> ExternalToolFuture<'a, ToolDecision> {
        // A custom handler can await an application's human approval here.
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(format!("approve:{tool_name}:{}", options.tool_call_id));
        self.policy
            .process_tool_call(tool_name, tool_input, options)
    }

    fn on_tool_result<'a>(
        &'a self,
        tool_name: &'a str,
        tool_input: &'a Metadata,
        result: &'a str,
        options: OnToolResultOptions,
    ) -> ExternalToolFuture<'a, ()> {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(format!("report:{tool_name}:{result}"));
        self.policy
            .on_tool_result(tool_name, tool_input, result, options)
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    if std::env::args().any(|arg| arg == "--acp-agent") {
        return agent::serve();
    }
    tokio::runtime::Runtime::new()?.block_on(run())
}

async fn run() -> Result<(), Box<dyn Error>> {
    let handler = Arc::new(AuditedHandler {
        policy: PolicyExternalToolHandler::new(roomkit_ai::PolicyExternalToolHandlerOptions {
            policy: Some(ToolPolicy {
                allow: vec!["Read".to_owned()],
                ..Default::default()
            }),
        }),
        calls: Mutex::new(Vec::new()),
    });
    let channel = AcpChannel::new(
        ChannelId::new("agent")?,
        std::env::current_dir()?,
        AcpChannelOptions::<StdioAcpTransport> {
            command: Some(vec![
                std::env::current_exe()?.to_string_lossy().into_owned(),
                "--acp-agent".into(),
            ]),
            external_tool_handler: Some(Arc::<AuditedHandler>::clone(&handler)),
            ..Default::default()
        },
    )?;
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(channel)?;
    kit.register_channel(RecordingTransport::new(
        ChannelId::new("web")?,
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    ))?;
    let room = RoomId::new("review")?;
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await?;
    for name in ["agent", "web"] {
        kit.attach_channel(
            &room,
            &ChannelId::new(name)?,
            AttachChannelOptions::default(),
        )
        .await?;
    }
    let observed = Arc::new(Mutex::new(Vec::new()));
    let reports = Arc::clone(&observed);
    kit.turn_executor().hooks().register(HookRegistration::new(
        HookTrigger::OnToolCall,
        HookExecution::Async,
        move |event: ToolCallEvent, _: RoomContext| {
            reports
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(event);
            async {}
        },
    ))?;
    let result = kit
        .process_inbound(
            InboundMessage::new(
                ChannelId::new("web")?,
                "alice",
                TextContent::new("Read the guide"),
            ),
            ProcessInboundOptions {
                room_id: Some(room),
                ..Default::default()
            },
        )
        .await?;
    kit.close().await?;
    assert!(result.error.is_none());
    assert_eq!(handler.channel_id(), "agent");
    assert_eq!(
        *handler.calls.lock().unwrap_or_else(PoisonError::into_inner),
        ["approve:Read:read-1", "report:Read:guide contents"]
    );
    let observed = observed.lock().unwrap_or_else(PoisonError::into_inner);
    assert_eq!(observed.len(), 1);
    let report = observed.first().ok_or("no tool report")?;
    assert_eq!(report.name, "Read");
    assert!(!report.is_error && !report.cancelled);
    Ok(())
}

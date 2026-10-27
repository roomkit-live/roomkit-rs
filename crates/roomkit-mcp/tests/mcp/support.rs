//! The fake server's command, the process a test watches, and a room whose
//! agent serves a server's tools on a scripted model.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use std::{env, fs, process};

use roomkit::model::{
    ChannelId, ChannelType, EventContent, EventType, InboundMessage, RoomEvent, RoomId,
    ToolCallContent,
};
use roomkit::testing::RecordingTransport;
use roomkit::{ConversationStore, RoomKit};
use roomkit_ai::AiAgent;
use roomkit_ai::testing::ScriptedProvider;
use roomkit_mcp::{McpCommand, McpServer};

/// The stand-in server (`fixtures/fake_server.rs`).
const FAKE: &str = env!("CARGO_BIN_EXE_fake-mcp-server");

/// The grace a test gives a server to exit at its close.
pub(crate) const GRACE: Duration = Duration::from_millis(100);

/// The fake server in `mode`, connecting within 10 seconds, given
/// [`GRACE`] to exit.
pub(crate) fn fake(mode: &str) -> McpCommand {
    McpCommand::new(FAKE)
        .arg(mode)
        .connect_timeout(Duration::from_secs(10))
        .close_grace(GRACE)
}

/// The fake server in `mode`, connected.
pub(crate) async fn connected(mode: &str) -> McpServer {
    fake(mode).connect().await.unwrap()
}

/// A file the fake server writes its process's id to, removed when
/// dropped.
pub(crate) struct PidFile {
    path: PathBuf,
}

impl PidFile {
    pub(crate) fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let name = format!(
            "roomkit-mcp-{}-{}.pid",
            process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        );
        Self {
            path: env::temp_dir().join(name),
        }
    }

    /// `command`, writing its process's id to this file.
    pub(crate) fn on(&self, command: McpCommand) -> McpCommand {
        command.arg("--pid-file").arg(&self.path)
    }

    /// The id the server wrote.
    pub(crate) fn pid(&self) -> u32 {
        fs::read_to_string(&self.path).unwrap().parse().unwrap()
    }
}

impl Drop for PidFile {
    fn drop(&mut self) {
        let _gone = fs::remove_file(&self.path);
    }
}

/// Whether the process `pid` exists, a zombie included: once it is gone,
/// its parent has waited for it.
pub(crate) fn exists(pid: u32) -> bool {
    PathBuf::from(format!("/proc/{pid}")).exists()
}

/// Whether the process `pid` is gone within `bound`, polled: a process
/// killed on a drop is reaped by the runtime, which nothing awaits.
pub(crate) async fn gone_within(pid: u32, bound: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + bound;
    while exists(pid) {
        if tokio::time::Instant::now() > deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    true
}

pub(crate) fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

pub(crate) fn room_id() -> RoomId {
    RoomId::new("room_1").unwrap()
}

/// A room where Alice writes by SMS to an agent, the kit holding both.
pub(crate) struct Room {
    pub(crate) kit: RoomKit,
}

impl Room {
    /// The room, its agent the one `agent` builds.
    pub(crate) async fn with(agent: AiAgent<ScriptedProvider>) -> Self {
        let kit = RoomKit::new();
        kit.register_channel(RecordingTransport::new(channel("sms"), ChannelType::SMS))
            .unwrap();
        kit.register_channel(agent).unwrap();
        kit.create_room(&room_id()).await.unwrap();
        for id in ["sms", "assistant"] {
            kit.attach(&room_id(), &channel(id)).await.unwrap();
        }
        Self { kit }
    }

    pub(crate) async fn says(&self, text: &str) {
        let message = InboundMessage::new(channel("sms"), "+15550001", EventContent::text(text));
        let result = self
            .kit
            .process_inbound_in(&room_id(), message)
            .await
            .unwrap();
        assert!(result.error.is_none(), "{:?}", result.error);
    }

    /// The end row of each call, in the room's order.
    pub(crate) async fn ends(&self) -> Vec<ToolCallContent> {
        let timeline: Vec<RoomEvent> = self
            .kit
            .store()
            .list_events(&room_id(), true)
            .await
            .unwrap();
        timeline
            .into_iter()
            .filter(|event| event.event_type == EventType::ToolCallEnd)
            .filter_map(|event| {
                let EventContent::ToolCall(row) = event.content else {
                    return None;
                };
                Some(row)
            })
            .collect()
    }
}

/// An agent on `model`.
pub(crate) fn agent(model: &ScriptedProvider) -> AiAgent<ScriptedProvider> {
    AiAgent::new(channel("assistant"), model.clone())
}

//! The fake server's command, the process a test watches, and a room whose
//! agent serves a server's tools on a scripted model.

use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use std::{env, fs, process};

use roomkit::model::{
    ChannelId, ChannelType, EventContent, EventType, InboundMessage, RoomEvent, RoomId,
    TextContent, ToolCallContent,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, ConversationStore, CreateRoomOptions, ProcessInboundOptions, RoomKit,
    RoomKitOptions,
};
use roomkit_ai::testing::ScriptedProvider;
use roomkit_ai::{AiChannel, AiChannelOptions};
use roomkit_mcp::{AsToolHandlerOptions, FromCommandOptions, McpError, McpToolProvider};

/// The stand-in server (`fixtures/fake_server.rs`).
const FAKE: &str = env!("CARGO_BIN_EXE_fake-mcp-server");

/// The grace a test gives a server to exit at its close.
pub(crate) const GRACE: Duration = Duration::from_millis(100);

/// How a test starts the fake server: its arguments and its provider's
/// options.
#[derive(Clone, Debug)]
pub(crate) struct Fake {
    options: FromCommandOptions,
}

impl Fake {
    /// The same server, `arg` after its arguments.
    pub(crate) fn arg(mut self, arg: impl Into<OsString>) -> Self {
        self.options.args.push(arg.into());
        self
    }

    /// The same server, given the variable `key` set to `value`.
    pub(crate) fn env(mut self, key: impl Into<OsString>, value: impl Into<OsString>) -> Self {
        self.options.env.insert(key.into(), value.into());
        self
    }

    /// The same server, connecting within `bound`.
    pub(crate) fn connect_timeout(mut self, bound: Duration) -> Self {
        self.options.connect_timeout = bound;
        self
    }

    /// The same server, labelled `label`.
    pub(crate) fn label(mut self, label: &str) -> Self {
        self.options.label = Some(label.to_owned());
        self
    }

    /// The same server, only the tools `names` kept.
    pub(crate) fn tools(mut self, names: &[&str]) -> Self {
        let names: Vec<String> = names.iter().map(|name| (*name).to_owned()).collect();
        self.options.tool_filter =
            Some(Arc::new(move |name| names.iter().any(|kept| kept == name)));
        self
    }

    /// The provider of this server, not connected, given [`GRACE`] to exit.
    pub(crate) fn provider(self) -> McpToolProvider {
        self.built().unwrap()
    }

    /// The provider of this server, connected.
    pub(crate) async fn connect(self) -> Result<McpToolProvider, McpError> {
        let provider = self.built()?;
        provider.connect().await?;
        Ok(provider)
    }

    /// The provider of this server, or why it is refused.
    fn built(self) -> Result<McpToolProvider, McpError> {
        Ok(McpToolProvider::from_command(FAKE, self.options)?.with_close_grace(GRACE))
    }
}

/// The fake server in `mode`, connecting within 10 seconds, given
/// [`GRACE`] to exit.
pub(crate) fn fake(mode: &str) -> Fake {
    Fake {
        options: FromCommandOptions {
            args: vec![mode.into()],
            connect_timeout: Duration::from_secs(10),
            ..FromCommandOptions::default()
        },
    }
}

/// The fake server in `mode`, connected.
pub(crate) async fn connected(mode: &str) -> McpToolProvider {
    fake(mode).connect().await.unwrap()
}

/// A channel answering through `model`, serving `server`'s tools through
/// its handler, as `adjust` adjusts its options.
pub(crate) fn serving(
    model: &ScriptedProvider,
    server: &McpToolProvider,
    adjust: impl FnOnce(AiChannelOptions) -> AiChannelOptions,
) -> AiChannel<ScriptedProvider> {
    let options = AiChannelOptions {
        tools: server
            .get_tools()
            .unwrap()
            .into_iter()
            .map(Into::into)
            .collect(),
        tool_handler: Some(
            server
                .as_tool_handler(AsToolHandlerOptions::default())
                .unwrap(),
        ),
        ..AiChannelOptions::default()
    };
    AiChannel::new(channel("assistant"), model.clone(), adjust(options)).unwrap()
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
    pub(crate) fn on(&self, command: Fake) -> Fake {
        command.arg("--pid-file").arg(&self.path)
    }

    /// `command`, a wrapper, having the server it starts as its child
    /// write its process's id to this file.
    pub(crate) fn on_child(&self, command: Fake) -> Fake {
        command.arg("--child-pid-file").arg(&self.path)
    }

    /// The file's path.
    pub(crate) fn path(&self) -> &PathBuf {
        &self.path
    }

    /// The id the server wrote.
    pub(crate) fn pid(&self) -> u32 {
        fs::read_to_string(&self.path)
            .unwrap()
            .trim()
            .parse()
            .unwrap()
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
    pub(crate) async fn with(agent: AiChannel<ScriptedProvider>) -> Self {
        let kit = RoomKit::new(RoomKitOptions::default());
        kit.register_channel(RecordingTransport::new(
            channel("sms"),
            ChannelType::SMS,
            RecordingTransportOptions::default(),
        ))
        .unwrap();
        kit.register_channel(agent).unwrap();
        kit.create_room(CreateRoomOptions {
            room_id: Some(room_id()),
            ..Default::default()
        })
        .await
        .unwrap();
        for id in ["sms", "assistant"] {
            kit.attach_channel(&room_id(), &channel(id), AttachChannelOptions::default())
                .await
                .unwrap();
        }
        Self { kit }
    }

    pub(crate) async fn says(&self, text: &str) {
        let message = InboundMessage::new(channel("sms"), "+15550001", TextContent::new(text));
        let result = self
            .kit
            .process_inbound(
                message,
                ProcessInboundOptions {
                    room_id: Some(room_id()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(result.error.is_none(), "{:?}", result.error);
    }

    /// The end row of each call, in the room's order.
    pub(crate) async fn ends(&self) -> Vec<ToolCallContent> {
        let timeline: Vec<RoomEvent> = self
            .kit
            .store()
            .list_events(&room_id(), every_row())
            .await
            .unwrap();
        timeline
            .into_iter()
            .filter(|event| event.r#type == EventType::ToolCallEnd)
            .filter_map(|event| {
                let EventContent::ToolCall(row) = event.content else {
                    return None;
                };
                Some(row)
            })
            .collect()
    }
}

/// Every row of a room's timeline, the refused ones included.
fn every_row() -> roomkit::store::ListEventsOptions {
    roomkit::store::ListEventsOptions {
        limit: u64::MAX,
        event_filter: Some(roomkit::EventFilter {
            include_blocked: true,
            ..roomkit::EventFilter::default()
        }),
        ..roomkit::store::ListEventsOptions::default()
    }
}

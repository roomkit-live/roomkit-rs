//! `AcpChannel` through a kit, its agent the `fake-acp-agent` fixture
//! spawned over stdio: what the shared scenarios cannot say (the spawn,
//! the constructor, a dead transport, a refused handshake).

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelId, ChannelType, EventContent, EventType, InboundMessage, RoomEvent, RoomId, TextContent,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, Channel, ConversationStore, CreateRoomOptions, InboundResult,
    ProcessInboundOptions, RoomKit, RoomKitOptions,
};
use roomkit_acp::{
    AcpChannel, AcpChannelOptions, AcpClient, AcpError, AcpTransport, ClientSideConnection,
    StdioAcpTransport, StdioAcpTransportOptions,
};
use serde_json::{Value, json};

use crate::support::PATIENCE;

/// The stand-in agent (`fixtures/fake_agent.rs`).
const FAKE: &str = env!("CARGO_BIN_EXE_fake-acp-agent");

/// The agent's directory: this crate's, an absolute path.
const CWD: &str = env!("CARGO_MANIFEST_DIR");

fn id(name: &str) -> ChannelId {
    ChannelId::new(name).unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("room-1").unwrap()
}

/// A stdio transport whose liveness a test can turn off, counting its
/// opens and closes, and keeping the id of the agent it last spawned.
struct Watched {
    inner: StdioAcpTransport,
    alive: Arc<AtomicBool>,
    opened: Arc<AtomicUsize>,
    closed: Arc<AtomicUsize>,
    pid: Arc<Mutex<Option<u32>>>,
}

#[derive(Clone, Default)]
struct Watch {
    alive: Arc<AtomicBool>,
    opened: Arc<AtomicUsize>,
    closed: Arc<AtomicUsize>,
    pid: Arc<Mutex<Option<u32>>>,
}

impl Watch {
    fn transport(&self, mode: &str) -> Watched {
        let inner = StdioAcpTransport::new(
            vec![FAKE.into(), mode.into()],
            CWD,
            StdioAcpTransportOptions::default(),
        )
        .unwrap();
        Watched {
            inner,
            alive: Arc::clone(&self.alive),
            opened: Arc::clone(&self.opened),
            closed: Arc::clone(&self.closed),
            pid: Arc::clone(&self.pid),
        }
    }
}

impl AcpTransport for Watched {
    type Connection = ClientSideConnection;

    fn name(&self) -> &'static str {
        "watched"
    }

    async fn open(&self, client: Arc<dyn AcpClient>) -> Result<ClientSideConnection, AcpError> {
        self.opened.fetch_add(1, Ordering::SeqCst);
        self.alive.store(true, Ordering::SeqCst);
        let connection = self.inner.open(client).await?;
        *self.pid.lock().unwrap() = self.inner.process();
        Ok(connection)
    }

    async fn close(&self) {
        self.closed.fetch_add(1, Ordering::SeqCst);
        self.inner.close().await;
    }

    fn is_alive(&self) -> bool {
        self.alive.load(Ordering::SeqCst) && self.inner.is_alive()
    }
}

/// A room where Alice writes over websocket to the channel `claude`.
async fn room<T: AcpTransport>(channel: AcpChannel<T>) -> RoomKit {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(RecordingTransport::new(
        id("ws"),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.register_channel(channel).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    for name in ["ws", "claude"] {
        kit.attach_channel(&room_id(), &id(name), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    kit
}

async fn says(kit: &RoomKit, text: &str) -> InboundResult {
    let message = InboundMessage::new(id("ws"), "alice", TextContent::new(text));
    let processing = kit.process_inbound(
        message,
        ProcessInboundOptions {
            room_id: Some(room_id()),
            ..Default::default()
        },
    );
    tokio::time::timeout(PATIENCE, processing)
        .await
        .expect("no answer")
        .unwrap()
}

/// The texts of the agent's messages in the room, in order.
async fn answers(kit: &RoomKit) -> Vec<String> {
    let events: Vec<RoomEvent> = kit
        .store()
        .list_events(&room_id(), roomkit::store::ListEventsOptions::default())
        .await
        .unwrap();
    events
        .into_iter()
        .filter(|event| {
            event.r#type == EventType::Message && event.source.channel_id == id("claude")
        })
        .filter_map(|event| {
            if let EventContent::Text(text) = event.content {
                Some(text.body)
            } else {
                None
            }
        })
        .collect()
}

fn refused(options: AcpChannelOptions<StdioAcpTransport>, cwd: &str) -> String {
    match AcpChannel::new(id("acp"), cwd, options) {
        Err(AcpError::Value(message)) => message,
        Err(other) => format!("refused otherwise: {other}"),
        Ok(_) => "not refused".to_owned(),
    }
}

// Python: tests/test_channels/test_acp.py::TestACPChannel::test_requires_argument_vector_and_absolute_cwd,
// test_command_and_transport_are_mutually_exclusive, test_spawn_only_options_are_rejected_with_a_transport
#[test]
fn the_channel_takes_a_command_or_a_transport_and_an_absolute_cwd() {
    let command = || Some(vec!["agent".to_owned()]);
    let transport = || {
        Some(
            StdioAcpTransport::new(
                vec!["agent".into()],
                CWD,
                StdioAcpTransportOptions::default(),
            )
            .unwrap(),
        )
    };
    assert_eq!(
        refused(
            AcpChannelOptions {
                command: command(),
                ..Default::default()
            },
            "relative"
        ),
        "cwd must be an absolute path"
    );
    assert!(
        refused(
            AcpChannelOptions {
                command: command(),
                transport: transport(),
                ..Default::default()
            },
            CWD
        )
        .contains("one, not both")
    );
    assert!(refused(AcpChannelOptions::default(), CWD).contains("pass command to spawn"));
    assert!(
        refused(
            AcpChannelOptions {
                transport: transport(),
                env: Some(std::collections::BTreeMap::default()),
                ..Default::default()
            },
            CWD,
        )
        .contains("env and inherit_env")
    );
    assert!(
        refused(
            AcpChannelOptions {
                transport: transport(),
                inherit_env: Some(vec!["PATH".into()]),
                ..Default::default()
            },
            CWD,
        )
        .contains("env and inherit_env")
    );
    assert_eq!(
        refused(
            AcpChannelOptions {
                command: Some(vec![]),
                ..Default::default()
            },
            CWD
        ),
        "command must be a non-empty sequence of arguments"
    );
}

// Python: tests/test_channels/test_acp.py::TestACPChannel::test_info_reports_the_transport_in_use
#[test]
fn info_reports_the_transport_in_use() {
    let spawned: AcpChannel = AcpChannel::new(
        id("acp"),
        CWD,
        AcpChannelOptions {
            command: Some(vec!["agent".into()]),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(spawned.info()["transport"], "stdio");
    assert_eq!(spawned.info()["connected"], false);
    // No SDK speaks for the channel: the key is there, empty.
    assert_eq!(spawned.info().get("sdk_version"), Some(&Value::Null));
    let watched = AcpChannel::new(
        id("acp"),
        CWD,
        AcpChannelOptions {
            transport: Some(Watch::default().transport("echo")),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(watched.info()["transport"], "watched");
}

// Python: tests/test_channels/test_acp.py::TestACPChannel::test_official_sdk_stdio_round_trip
#[tokio::test]
async fn a_message_reaches_the_spawned_agent_and_its_answer_the_room() {
    let watch = Watch::default();
    let channel = AcpChannel::new(
        id("claude"),
        CWD,
        AcpChannelOptions {
            transport: Some(watch.transport("echo")),
            ..Default::default()
        },
    )
    .unwrap();
    let kit = room(channel).await;
    let result = says(&kit, "hello").await;
    assert!(result.error.is_none(), "{:?}", result.error);
    assert_eq!(
        result.response_metadata.get("acp"),
        Some(json!({ "protocol_version": 1, "prompt_returned": true }))
    );
    assert_eq!(answers(&kit).await, ["hello"]);
    let pid = watch.pid.lock().unwrap().unwrap();
    kit.close().await.unwrap();
    assert_eq!(watch.closed.load(Ordering::SeqCst), 1);
    let deadline = tokio::time::Instant::now() + PATIENCE;
    while std::path::Path::new(&format!("/proc/{pid}")).exists() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the agent outlived the kit"
        );
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

// Python: tests/test_channels/test_acp.py::TestACPChannel::test_a_dead_transport_reconnects_and_drops_sessions
#[tokio::test]
async fn a_dead_transport_reconnects_and_drops_the_sessions() {
    let watch = Watch::default();
    let channel = AcpChannel::new(
        id("claude"),
        CWD,
        AcpChannelOptions {
            transport: Some(watch.transport("echo")),
            ..Default::default()
        },
    )
    .unwrap();
    let kit = room(channel).await;
    assert!(says(&kit, "one").await.error.is_none());
    watch.alive.store(false, Ordering::SeqCst);
    // The new agent knows no session of the old one's: the room's is
    // opened again, or the prompt is refused.
    let result = says(&kit, "two").await;
    assert!(result.error.is_none(), "{:?}", result.error);
    assert_eq!(watch.opened.load(Ordering::SeqCst), 2);
    assert_eq!(watch.closed.load(Ordering::SeqCst), 1);
    // The echo agent's replacement receives the conversation it missed.
    assert_eq!(
        answers(&kit).await,
        [
            "one",
            "[Room context — 1 message you did not receive. Context only; the request follows.]\n\
             [1] @ws: “one”\n[End of room context]\n\ntwo",
        ]
    );
    kit.close().await.unwrap();
}

// Python: tests/test_channels/test_acp.py::TestACPChannel::test_a_failed_handshake_closes_the_transport
#[tokio::test]
async fn a_refused_handshake_fails_the_turn_and_closes_the_transport() {
    let watch = Watch::default();
    let channel = AcpChannel::new(
        id("claude"),
        CWD,
        AcpChannelOptions {
            transport: Some(watch.transport("version:2")),
            ..Default::default()
        },
    )
    .unwrap();
    let kit = room(channel).await;
    let result = says(&kit, "hello").await;
    let error = result.error.map(|error| error.message).unwrap_or_default();
    assert!(
        error.contains("ACP protocol negotiation failed: agent selected version 2, expected 1"),
        "{error}"
    );
    assert_eq!(watch.closed.load(Ordering::SeqCst), 1);
    assert_eq!(answers(&kit).await, Vec::<String>::new());
    kit.close().await.unwrap();
}

// Python: tests/test_channels/test_acp.py::TestACPChannel::test_active_turns_counts_a_turn_from_prompt_to_stream_end
#[tokio::test]
async fn no_turn_is_counted_once_the_answer_is_in() {
    let watch = Watch::default();
    let channel = AcpChannel::new(
        id("claude"),
        CWD,
        AcpChannelOptions {
            transport: Some(watch.transport("echo")),
            ..Default::default()
        },
    )
    .unwrap();
    let kit = room(channel.clone()).await;
    says(&kit, "hello").await;
    let info = channel.info();
    assert_eq!(channel.active_turns(), 0);
    assert_eq!(channel.session_id(&room_id()).as_deref(), Some("session-1"));
    assert_eq!(info["active_turns"], Value::from(0));
    assert_eq!(info["session_count"], Value::from(1));
    assert_eq!(info["connected"], Value::Bool(true));
    kit.close().await.unwrap();
}

//! Language forms of Python's fixed iterable and callback inputs.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use agent_client_protocol_schema::v1::McpServer;
use roomkit::Channel;
use roomkit::model::{ChannelId, RoomId};
use serde_json::json;
use tokio::sync::mpsc;

use super::{Agent, PATIENCE, Piped};
use crate::{AcpChannel, AcpChannelOptions, McpServers, McpServersForRoom};

fn descriptor(name: &str) -> McpServer {
    serde_json::from_value(json!({"name":name,"command":"/usr/local/bin/tools",
        "args":["--mode","text"],"env":[{"name":"TOKEN","value":"private-env"}]}))
    .unwrap()
}

// Python: tests/test_channels/test_acp_session_setup.py::TestMCPServersPerRoom::test_any_iterable_of_servers_still_works
#[tokio::test]
async fn test_any_iterable_of_servers_still_works() {
    let consumed = AtomicUsize::new(0);
    let servers: Vec<_> = ["a", "b"]
        .into_iter()
        .map(|name| {
            consumed.fetch_add(1, Ordering::SeqCst);
            descriptor(name)
        })
        .collect();
    let expected = json!(servers);
    let (agents, mut opened) = mpsc::unbounded_channel();
    let channel = AcpChannel::new(
        ChannelId::new("agent").unwrap(),
        "/srv/project",
        AcpChannelOptions {
            transport: Some(Piped {
                agents,
                closed: Arc::default(),
                log: Arc::default(),
            }),
            mcp_servers: Some(servers.into()),
            ..AcpChannelOptions::default()
        },
    )
    .unwrap();
    assert_eq!(consumed.load(Ordering::SeqCst), 2);
    let exercise = async {
        for name in ["room-1", "room-2"] {
            let room = RoomId::new(name).unwrap();
            channel
                .set_config_option(&room, "model", json!("opus"))
                .await
                .unwrap();
        }
    };
    let serve = async {
        let mut agent = Agent::new(opened.recv().await.unwrap());
        let request = agent.read().await;
        assert_eq!(request["method"], "initialize");
        agent
            .answer(
                &request,
                json!({"protocolVersion":1,"agentCapabilities":{}}),
            )
            .await;
        for room in ["room-1", "room-2"] {
            let request = agent.read().await;
            assert_eq!(request["method"], "session/new");
            assert_eq!(request["params"]["mcpServers"], expected);
            assert_eq!(request["params"]["_meta"]["roomkit.live/roomId"], room);
            agent
                .answer(
                    &request,
                    json!({"sessionId":room,"configOptions":[{
                "id":"model","name":"Model","type":"select","currentValue":"opus",
                "options":[{"value":"opus","name":"Opus"}]}]}),
                )
                .await;
        }
        agent
    };
    let (_, mut agent) = tokio::time::timeout(PATIENCE, async { tokio::join!(exercise, serve) })
        .await
        .unwrap();
    assert_eq!(consumed.load(Ordering::SeqCst), 2);
    channel.close().await;
    assert!(
        tokio::time::timeout(PATIENCE, agent.lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .is_none()
    );
}

#[test]
fn mcp_debug_omits_descriptor_credentials_and_callback_captures() {
    let servers = vec![
        descriptor("private-name"),
        serde_json::from_value(json!({
        "type":"http","name":"private-http","url":"https://private-url.example.test",
        "headers":[{"name":"Authorization","value":"private-header"}]}))
        .unwrap(),
    ];
    let options = AcpChannelOptions::<crate::StdioAcpTransport> {
        mcp_servers: Some(servers.into()),
        ..AcpChannelOptions::default()
    };
    assert!(!format!("{options:?}").contains("private-"));
    let secret = "private-callback".to_owned();
    let callback =
        McpServersForRoom::new(move |_| std::future::ready(Ok(vec![descriptor(&secret)])));
    assert!(!format!("{callback:?}").contains("private-"));
    assert!(!format!("{:?}", McpServers::ForRoom(callback)).contains("private-"));
}

//! Public constructor paths and their session declaration (RFC Appendix A.9.1).

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use roomkit::Channel;
use roomkit::model::{ChannelId, RoomId};
use serde_json::json;
use tokio::io::DuplexStream;
use tokio::sync::mpsc;

use super::{Agent, PATIENCE, Piped};
use crate::{AcpChannel, AcpChannelOptions, AcpError};

// Python: tests/test_channels/test_acp_directories.py::test_additional_directories_must_be_absolute
#[test]
fn test_additional_directories_must_be_absolute() {
    for path in ["relative", ""] {
        let options = AcpChannelOptions {
            command: Some(vec!["unused-agent".to_owned()]),
            additional_directories: Some(vec![PathBuf::from("/srv/valid"), PathBuf::from(path)]),
            ..AcpChannelOptions::default()
        };
        let result: Result<AcpChannel, _> =
            AcpChannel::new(ChannelId::new("agent").unwrap(), "/srv/project", options);
        assert!(matches!(result, Err(AcpError::Value(message))
            if message == "additional_directories must be an absolute path"));
    }
}

fn channel_with_paths() -> (AcpChannel<Piped>, mpsc::UnboundedReceiver<DuplexStream>) {
    let (agents, opened) = mpsc::unbounded_channel();
    let options = AcpChannelOptions {
        transport: Some(Piped {
            agents,
            closed: Arc::new(AtomicUsize::new(0)),
            log: Arc::default(),
        }),
        additional_directories: Some(vec![
            PathBuf::from("~/shared"),
            PathBuf::from("/srv/remote-reference"),
        ]),
        ..AcpChannelOptions::default()
    };
    let channel = AcpChannel::new(
        ChannelId::new("agent").unwrap(),
        "/srv/remote-project",
        options,
    )
    .unwrap();
    (channel, opened)
}

async fn declaration(mut opened: mpsc::UnboundedReceiver<DuplexStream>) -> Agent {
    let home = PathBuf::from(std::env::var_os("HOME").expect("HOME for the expansion case"));
    let mut agent = Agent::new(opened.recv().await.unwrap());
    let initialize = agent.read().await;
    assert_eq!(initialize["method"], "initialize");
    agent
        .answer(
            &initialize,
            json!({"protocolVersion":1, "agentCapabilities":{}}),
        )
        .await;
    let request = agent.read().await;
    assert_eq!(request["method"], "session/new");
    assert_eq!(request["params"]["cwd"], "/srv/remote-project");
    assert_eq!(
        request["params"]["additionalDirectories"],
        json!([home.join("shared"), "/srv/remote-reference"])
    );
    agent
        .answer(
            &request,
            json!({"sessionId":"s1","configOptions":[{
                "id":"model", "name":"Model", "type":"select", "currentValue":"opus",
                "options":[{"value":"opus","name":"Opus"}]
            }]}),
        )
        .await;
    agent
}

// Python: tests/test_channels/test_acp_directories.py::test_additional_directories_expand_home_and_accept_paths
#[tokio::test]
async fn test_additional_directories_expand_home_and_accept_paths() {
    let (channel, mut opened) = channel_with_paths();
    assert!(matches!(
        opened.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    let room = RoomId::new("room-1").unwrap();
    let setting = channel.set_config_option(&room, "model", json!("opus"));
    let (answer, mut agent) = tokio::time::timeout(PATIENCE, async {
        tokio::join!(setting, declaration(opened))
    })
    .await
    .unwrap();
    assert_eq!(json!(answer.unwrap()), json!({"model":"opus"}));
    channel.close().await;
    assert!(
        tokio::time::timeout(PATIENCE, agent.lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .is_none()
    );
}

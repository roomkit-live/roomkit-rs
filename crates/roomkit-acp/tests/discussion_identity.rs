//! Live ACP handles are resolved before Discussion is installed (RFC §19.7.5).

use std::collections::BTreeMap;

use roomkit::model::ChannelId;
use roomkit::orchestration::{DiscussionOptions, StrategyAgent};
use roomkit::{Channel, Discussion};
use roomkit_acp::{AcpChannel, AcpChannelOptions, StdioAcpTransport};

fn channel(id: &str, name: Option<&str>) -> AcpChannel<StdioAcpTransport> {
    AcpChannel::new(
        ChannelId::new(id).unwrap(),
        std::env::current_dir().unwrap(),
        AcpChannelOptions {
            command: Some(vec!["agent-not-started".to_owned()]),
            name: name.map(str::to_owned),
            ..Default::default()
        },
    )
    .unwrap()
}

// Python: tests/test_channels/test_acp_discussion_identity.py::test_a_live_acp_agent_is_named_before_discussion_installation
#[test]
fn test_a_live_acp_agent_is_named_before_discussion_installation() {
    for (id, given, expected) in [
        ("agent:7c1e", None, "Developer"),
        ("dev", None, "dev"),
        ("agent:7c1e", Some("coder"), "coder"),
    ] {
        let channel = channel(id, Some("Developer"));
        let id = channel.channel_id();
        let discussion = Discussion::new(
            vec![StrategyAgent::channel(channel.clone())],
            DiscussionOptions {
                handles: given.map(|handle| BTreeMap::from([(id.clone(), handle.to_owned())])),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            discussion.handles(),
            BTreeMap::from([(id, expected.to_owned())])
        );
        assert!(channel.declaration().identity.is_none());
        assert!(!channel.declaration().takes_owner_tools);
    }
}

// Python: tests/test_channels/test_acp_discussion_identity.py::test_a_live_acp_agent_without_a_usable_handle_is_refused
#[test]
fn test_a_live_acp_agent_without_a_usable_handle_is_refused() {
    for name in [None, Some(""), Some("  ")] {
        assert!(
            Discussion::new(
                vec![StrategyAgent::channel(channel("agent:7c1e", name))],
                DiscussionOptions::default(),
            )
            .is_err()
        );
    }
}

//! ACP construction preserves Python's public identity attributes (RFC §6.4).

use roomkit::Channel;
use roomkit::model::ChannelId;
use roomkit_acp::{AcpChannel, AcpChannelOptions, StdioAcpTransport};

// Python: tests/test_channels/test_acp_discussion.py::test_an_acp_agent_carries_an_identity
// Python: tests/test_channels/test_acp_identity.py::test_an_acp_agent_preserves_its_identity
#[test]
fn an_acp_agent_preserves_its_identity() {
    let directory = std::env::current_dir().unwrap();
    for identity in [
        [None, None, None],
        [Some(""), Some(""), Some("")],
        [Some("  "), Some("\t"), Some("\n")],
        [Some("Développeuse"), Some("dev"), Some("reads the code 🔎")],
    ] {
        let channel = AcpChannel::<StdioAcpTransport>::new(
            ChannelId::new("agent:7c1e").unwrap(),
            &directory,
            AcpChannelOptions {
                command: Some(vec!["agent-not-started".to_owned()]),
                name: identity[0].map(str::to_owned),
                role: identity[1].map(str::to_owned),
                description: identity[2].map(str::to_owned),
                ..Default::default()
            },
        )
        .unwrap();

        for observed in [&channel, &channel.clone()] {
            assert_eq!(
                [observed.name(), observed.role(), observed.description()],
                identity
            );
            assert_eq!(observed.channel_id().as_str(), "agent:7c1e");
            assert!(observed.declaration().identity.is_none());
        }
    }
}

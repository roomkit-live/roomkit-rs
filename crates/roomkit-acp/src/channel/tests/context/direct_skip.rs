//! Every direct ACP input kind excluded by the Python channel contract.

use super::{AcpChannelOptions, Channel, ChannelId, EventType, arguments, channel, transport};
use roomkit::model::{EventContent, TextContent, ToolCallContent};

// Python: tests/test_channels/test_acp.py::test_skips_own_tool_activity_and_change_events
#[tokio::test]
async fn test_skips_own_tool_activity_and_change_events() {
    let (transport, mut opened) = transport();
    let channel = channel(AcpChannelOptions {
        transport: Some(transport),
        ..AcpChannelOptions::default()
    });
    let (mut own, binding, context) = arguments();
    own.source.channel_id = ChannelId::new("agent").unwrap();
    assert!(
        !channel
            .on_event(own, binding.clone(), context.clone())
            .await
            .unwrap()
            .responded
    );
    for (kind, body) in [
        (EventType::Edit, "Correction: Why?"),
        (EventType::Delete, "[Message deleted]"),
        (EventType::System, "The order shipped"),
    ] {
        let (mut event, _, _) = arguments();
        event.r#type = kind;
        event.content = EventContent::Text(TextContent::new(body));
        assert!(
            !channel
                .on_event(event, binding.clone(), context.clone())
                .await
                .unwrap()
                .responded
        );
    }
    let (mut tool, _, _) = arguments();
    tool.r#type = EventType::ToolCallStart;
    tool.content = EventContent::ToolCall(ToolCallContent::new("Read", "tool-1"));
    assert!(
        !channel
            .on_event(tool, binding, context)
            .await
            .unwrap()
            .responded
    );
    assert!(opened.try_recv().is_err());
    channel.close().await;
}

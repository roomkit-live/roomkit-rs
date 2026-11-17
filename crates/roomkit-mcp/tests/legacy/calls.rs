//! Constructor label/filter/discovery reach the same public handler on legacy SSE.

use super::peer::{Mode, Peer};
use roomkit_ai::{ToolError, ToolResult};
use roomkit_mcp::{AsToolHandlerOptions, FromUrlOptions, McpToolProvider, McpToolProviderOptions};
use std::sync::Arc;
use std::time::Duration;

// Python: test_legacy_sse_uses_the_announced_endpoint_and_retained_headers.
#[tokio::test]
async fn constructors_preserve_discovery_filter_and_labelled_handler_routing() {
    for direct in [false, true] {
        for discover in [false, true] {
            let peer = Peer::start(Mode::Normal).await;
            let options = McpToolProviderOptions {
                transport: "sse".into(),
                discover,
                label: Some("capture".into()),
                tool_filter: Some(Arc::new(|name| name == "echo")),
                connect_timeout: Some(Duration::from_secs(3)),
                ..Default::default()
            };
            let provider = if direct {
                McpToolProvider::new(Some(&peer.url), options)
            } else {
                McpToolProvider::from_url(
                    &peer.url,
                    FromUrlOptions {
                        transport: options.transport,
                        discover,
                        label: options.label,
                        tool_filter: options.tool_filter,
                        connect_timeout: Duration::from_secs(3),
                        ..Default::default()
                    },
                )
            }
            .unwrap();
            provider.connect().await.unwrap();
            assert_eq!(
                provider.tool_names().unwrap(),
                if discover { vec!["echo"] } else { vec![] }
            );
            let handler = provider
                .as_tool_handler(AsToolHandlerOptions {
                    gate_discovery: false,
                })
                .unwrap();
            let before = peer.shared.seen.lock().unwrap().requests.len();
            let refused = handler
                .call("mcp__another__echo".into(), serde_json::Map::default())
                .await;
            assert!(
                matches!(refused, Err(ToolError::UnservedToolCall(_))),
                "{refused:?}"
            );
            assert_eq!(peer.shared.seen.lock().unwrap().requests.len(), before);
            assert_eq!(
                handler
                    .call("mcp__capture__echo".into(), serde_json::Map::default())
                    .await
                    .unwrap(),
                ToolResult::Text("done".into())
            );
            provider.close().await;
            peer.closed().await;
        }
    }
}

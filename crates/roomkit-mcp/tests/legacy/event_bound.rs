//! Python: `test_mcp_event_bound.py`, the legacy HTTP+SSE case: an event is
//! bounded at Python's SDK's 1 MiB, which its legacy client does not let a
//! host set.

use super::peer::{Mode, Peer};
use roomkit_mcp::{CallToolOptions, McpError, McpToolProvider};
use std::time::Duration;

const MIB: usize = 1024 * 1024;

async fn call_big(provider: &McpToolProvider) -> Result<String, McpError> {
    provider
        .call_tool(
            "big",
            serde_json::Map::default(),
            CallToolOptions {
                timeout: Duration::from_secs(3),
            },
        )
        .await
}

// Python: test_an_event_past_the_bound_fails_the_legacy_connection.
#[tokio::test]
async fn an_event_past_the_bound_fails_the_legacy_connection() {
    let peer = Peer::start(Mode::LargeResult(MIB)).await;
    let provider = peer.provider(None, Duration::from_secs(3));
    provider.connect().await.unwrap();
    let failed = call_big(&provider).await.map(|text| text.len());
    assert!(matches!(failed, Err(McpError::Call { .. })), "{failed:?}");
    // The one stream carries every answer: the connection ends with it.
    peer.closed().await;
    let after = call_big(&provider).await.map(|text| text.len());
    assert!(
        matches!(after, Err(McpError::Call { .. } | McpError::NotConnected)),
        "{after:?}"
    );
    provider.close().await;
}

#[tokio::test]
async fn an_event_within_the_bound_is_read() {
    let peer = Peer::start(Mode::LargeResult(MIB - 1024)).await;
    let provider = peer.provider(None, Duration::from_secs(3));
    provider.connect().await.unwrap();
    assert!(call_big(&provider).await.unwrap().starts_with("xxx"));
    provider.close().await;
    peer.closed().await;
}

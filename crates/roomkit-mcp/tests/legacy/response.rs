//! Python: `test_mcp_legacy_response.py`, against an actual GET stream.

use super::peer::{Mode, Peer};
use roomkit_mcp::McpError;
use std::time::Duration;

#[tokio::test]
async fn get_content_type_uses_the_sdk_substring_before_parameters() {
    for (content_type, accepted) in [
        (Some("text/event-stream"), true),
        (Some("text/event-stream; charset=utf-8"), true),
        (Some("prefix-text/event-stream-suffix"), true),
        (Some("Text/Event-Stream"), false),
        (Some("text/plain; text/event-stream"), false),
        (None, false),
    ] {
        let peer = Peer::start(Mode::Response(content_type)).await;
        let provider = peer.provider(None, Duration::from_secs(1));
        let result = provider.connect().await;
        if accepted {
            result.unwrap();
            assert!(provider.connected());
            provider.close().await;
        } else {
            assert!(
                matches!(
                    result,
                    Err(McpError::Http {
                        reason: "expected an SSE response",
                        ..
                    })
                ),
                "{result:?}"
            );
            assert_eq!(peer.shared.seen.lock().unwrap().requests.len(), 1);
            assert_eq!(
                peer.shared.seen.lock().unwrap().messages,
                Vec::<serde_json::Value>::new()
            );
        }
        assert!(!provider.connected());
        peer.closed().await;
    }
}

#[tokio::test]
async fn eof_before_endpoint_fails_without_waiting_for_open_timeout() {
    let peer = Peer::start(Mode::Eof).await;
    let provider = peer.provider(None, Duration::from_secs(30));
    let result = tokio::time::timeout(Duration::from_secs(1), provider.connect())
        .await
        .unwrap();
    assert!(
        matches!(
            result,
            Err(McpError::Http {
                reason: "MCP event stream closed before its endpoint",
                ..
            })
        ),
        "{result:?}"
    );
    assert!(!provider.connected());
    assert_eq!(peer.shared.seen.lock().unwrap().requests.len(), 1);
    assert_eq!(
        peer.shared.seen.lock().unwrap().messages,
        Vec::<serde_json::Value>::new()
    );
}

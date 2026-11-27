//! Python: `test_mcp_legacy_response.py`, against an actual GET stream.

use super::peer::{Mode, Peer};
use roomkit_mcp::McpError;
use std::time::Duration;

// Python: test_legacy_get_uses_the_sdk_content_type_match.
#[tokio::test]
async fn get_content_type_is_read_as_a_media_type() {
    for (content_type, accepted) in [
        (Some("text/event-stream"), true),
        (Some("text/event-stream; charset=utf-8"), true),
        // The part before ";", trimmed and lowercased, is text/event-stream,
        // as the MCP Python SDK 2 reads it (RMK-2203).
        (Some("prefix-text/event-stream-suffix"), false),
        (Some("Text/Event-Stream"), true),
        (Some("text/event-stream ; charset=utf-8"), true),
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

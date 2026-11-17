//! Python: cancellation at the endpoint/listing barrier cleans both directions.

use super::peer::{Mode, Peer};
use roomkit_mcp::{FromUrlOptions, McpError, McpToolProvider};
use std::time::Duration;

async fn cancellation() {
    for stage in ["endpoint", "listing"] {
        let peer = Peer::start(Mode::HoldOpening(stage)).await;
        let provider = McpToolProvider::from_url(
            &peer.url,
            FromUrlOptions {
                transport: "sse".into(),
                connect_timeout: Duration::from_secs(3),
                ..Default::default()
            },
        )
        .unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            provider.connect_until(peer.opening_ready(stage)),
        )
        .await
        .unwrap();
        assert!(matches!(result, Err(McpError::Cancelled)), "{result:?}");
        assert!(!provider.connected());
        peer.closed().await;
        assert_eq!(
            peer.shared.seen.lock().unwrap().requests.len(),
            if stage == "endpoint" { 1 } else { 4 }
        );
        provider.connect().await.unwrap();
        assert_eq!(provider.tool_names().unwrap(), ["echo", "hidden"]);
        provider.close().await;
        peer.closed().await;
        let seen = peer.shared.seen.lock().unwrap();
        assert_eq!((seen.opened, seen.closed), (2, 2));
    }
}

#[tokio::test]
async fn cancelling_opening_closes_reader_and_writer_before_reentry() {
    cancellation().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_opening_closes_reader_and_writer_on_several_workers() {
    cancellation().await;
}

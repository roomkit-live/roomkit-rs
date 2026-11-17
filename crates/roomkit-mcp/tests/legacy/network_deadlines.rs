//! Public twins of Python's real legacy read/write deadline captures.

use super::network_peer::{Mode, Peer};
use roomkit_mcp::{CallToolOptions, FromUrlOptions, McpError, McpToolProvider, RequestOptions};
use serde_json::{Map, json};
use std::{sync::Arc, time::Duration};

fn provider(peer: &Peer) -> Arc<McpToolProvider> {
    Arc::new(
        McpToolProvider::from_url(
            &peer.url,
            FromUrlOptions {
                transport: "sse".into(),
                discover: false,
                connect_timeout: Duration::from_secs(600),
                ..Default::default()
            },
        )
        .unwrap(),
    )
}

fn manual_clock() -> tokio::task::JoinHandle<()> {
    tokio::spawn(async {
        loop {
            tokio::task::yield_now().await;
        }
    })
}

async fn settle() {
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test(start_paused = true)]
async fn a_post_response_can_take_more_than_the_write_budget() {
    let manual = manual_clock();
    let peer = Peer::start(Mode::SlowPost).await;
    let provider = provider(&peer);
    let caller = Arc::clone(&provider);
    let opening = tokio::spawn(async move { caller.connect().await });
    peer.state.held.notified().await;
    settle().await;
    tokio::time::advance(Duration::from_secs(6)).await;
    assert!(!opening.is_finished());
    peer.state.released.notify_one();
    opening.await.unwrap().unwrap();
    peer.messages(2).await;
    provider.close().await;
    peer.state.closed.notified().await;
    manual.abort();
    assert_eq!(
        peer.state
            .messages
            .lock()
            .unwrap()
            .iter()
            .map(|m| m["method"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["initialize", "notifications/initialized"]
    );
}

#[tokio::test(start_paused = true)]
async fn a_post_read_timeout_stops_the_writer_but_preserves_the_opening_deadline() {
    for mode in [Mode::PostHeaders, Mode::PostBody] {
        let manual = manual_clock();
        let peer = Peer::start(mode).await;
        let provider = provider(&peer);
        let caller = Arc::clone(&provider);
        let opening = tokio::spawn(async move { caller.connect().await });
        peer.state.held.notified().await;
        settle().await;
        tokio::time::advance(Duration::from_secs(200)).await;
        peer.comment();
        settle().await;
        tokio::time::advance(Duration::from_secs(101)).await;
        settle().await;
        assert!(!opening.is_finished());
        assert_eq!(peer.state.requests.lock().unwrap().len(), 2);
        peer.comment();
        settle().await;
        tokio::time::advance(Duration::from_secs(300)).await;
        let result = opening.await.unwrap();
        assert!(
            matches!(
                result,
                Err(McpError::Http {
                    reason: "connection timed out",
                    ..
                })
            ),
            "{result:?}"
        );
        peer.state.closed.notified().await;
        assert!(!provider.connected());
        assert_eq!(peer.state.messages.lock().unwrap().len(), 1);
        provider.connect().await.unwrap();
        peer.messages(3).await;
        provider.close().await;
        peer.state.closed.notified().await;
        manual.abort();
    }
}

#[tokio::test(start_paused = true)]
async fn a_stalled_upload_expires_its_write_and_the_next_request_sees_a_closed_writer() {
    stalled_upload(Mode::Upload).await;
    stalled_upload(Mode::EarlyUpload).await;
}

async fn stalled_upload(mode: Mode) {
    let manual = manual_clock();
    let peer = Peer::start(mode).await;
    let provider = provider(&peer);
    provider.connect().await.unwrap();
    let caller = Arc::clone(&provider);
    let upload = tokio::spawn(async move {
        caller
            .call_tool(
                "upload",
                Map::from_iter([("body".into(), json!("x".repeat(8 * 1024 * 1024)))]),
                CallToolOptions {
                    timeout: Duration::from_secs(10),
                },
            )
            .await
    });
    peer.state.held.notified().await;
    settle().await;
    tokio::time::advance(Duration::from_secs(4)).await;
    let next = provider.request(
        "after/failed-write",
        None,
        RequestOptions {
            timeout: Duration::from_secs(20),
        },
    );
    tokio::pin!(next);
    assert!(futures_util::poll!(&mut next).is_pending());
    tokio::time::advance(Duration::from_secs(2)).await;
    settle().await;
    let next = futures_util::poll!(&mut next);
    assert!(
        matches!(next, std::task::Poll::Ready(Err(McpError::Call { .. }))),
        "{mode:?}: {next:?}"
    );
    assert!(!upload.is_finished()); // The accepted MCP call retains its own bound.
    tokio::time::advance(Duration::from_secs(5)).await;
    let result = upload.await.unwrap();
    assert!(
        matches!(result, Err(McpError::CallTimeout { .. })),
        "{result:?}"
    );
    provider.close().await;
    peer.state.closed.notified().await;
    manual.abort();
    assert_eq!(peer.state.requests.lock().unwrap().len(), 4);
    assert_eq!(peer.state.messages.lock().unwrap().len(), 2);
}

#[tokio::test(start_paused = true)]
async fn progressing_headers_and_endpoint_comments_finish_opening_after_a_read_budget() {
    for mode in [Mode::PacedHeaders, Mode::PacedEndpoint] {
        let manual = manual_clock();
        let peer = Peer::start(mode).await;
        let provider = provider(&peer);
        let caller = Arc::clone(&provider);
        let opening = tokio::spawn(async move { caller.connect().await });
        for _ in 0..2 {
            peer.state.progress.notified().await;
            settle().await;
            tokio::time::advance(Duration::from_secs(200)).await;
            assert!(!opening.is_finished());
            peer.state.proceed.notify_one();
        }
        opening.await.unwrap().unwrap();
        let result = provider
            .request(
                "host/read",
                None,
                RequestOptions {
                    timeout: Duration::from_secs(1),
                },
            )
            .await
            .unwrap();
        assert_eq!(result.get("done"), Some(&json!(true)));
        provider.close().await;
        peer.state.closed.notified().await;
        manual.abort();
    }
}

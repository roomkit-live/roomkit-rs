//! Python: `test_mcp_legacy_deadlines.py`; real sockets under a controlled clock.

use roomkit_mcp::{FromUrlOptions, McpError, McpToolProvider};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::oneshot;

struct Peer {
    url: String,
    entered: oneshot::Receiver<()>,
    task: tokio::task::JoinHandle<()>,
}

impl Peer {
    async fn stalled(tls: bool) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (entered, received) = oneshot::channel();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buffer = [0; 4096];
            let size = stream.read(&mut buffer).await.unwrap();
            assert!(size > 0);
            if tls {
                assert_eq!(buffer.first(), Some(&22)); // Actual TLS ClientHello.
            } else {
                assert!(buffer.starts_with(b"GET /mcp HTTP/1.1\r\n"));
            }
            entered.send(()).unwrap();
            // Do not answer: the socket remains live until the client closes it.
            let _closed = stream.read(&mut buffer).await;
            let _closed = stream.shutdown().await;
        });
        let scheme = if tls { "https" } else { "http" };
        Self {
            url: format!("{scheme}://127.0.0.1:{port}/mcp"),
            entered: received,
            task,
        }
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Prevent Tokio's automatic clock jumps while real loopback I/O is starting.
fn keep_clock_manual() -> tokio::task::JoinHandle<()> {
    tokio::spawn(async {
        loop {
            tokio::task::yield_now().await;
        }
    })
}

async fn stalled(tls: bool, seconds: u64) {
    let manual = keep_clock_manual();
    let mut peer = Peer::stalled(tls).await;
    let provider = Arc::new(
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
    );
    let caller = Arc::clone(&provider);
    let mut opening = tokio::spawn(async move { caller.connect().await });
    (&mut peer.entered).await.unwrap();
    tokio::time::advance(Duration::from_secs(seconds - 1)).await;
    assert!(!opening.is_finished());
    tokio::time::advance(Duration::from_secs(2)).await;
    manual.abort();
    let result = tokio::time::timeout(Duration::from_secs(1), &mut opening).await;
    if result.is_err() {
        opening.abort();
    }
    let result = result.unwrap().unwrap();
    assert!(
        matches!(
            result,
            Err(McpError::Http {
                reason: "HTTP connection failed",
                ..
            })
        ),
        "{result:?}"
    );
    assert!(!provider.connected());
}

#[tokio::test(start_paused = true)]
async fn connect_timeout_covers_the_tls_handshake_at_five_seconds() {
    stalled(true, 5).await;
}

#[tokio::test(start_paused = true)]
async fn get_response_header_read_times_out_at_three_hundred_seconds() {
    stalled(false, 300).await;
}

#[tokio::test(start_paused = true)]
async fn an_endpoint_read_timeout_closes_get_but_keeps_the_opening_bound() {
    use super::peer::{Mode, Peer as HttpPeer};
    let manual = keep_clock_manual();
    let peer = HttpPeer::start(Mode::HoldOpening("endpoint")).await;
    let provider = Arc::new(peer.provider(None, Duration::from_secs(600)));
    let caller = Arc::clone(&provider);
    let mut opening = tokio::spawn(async move { caller.connect().await });
    peer.opening_ready("endpoint").await;
    // Drive the real response through the loopback reactor without advancing time.
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_secs(301)).await;
    manual.abort();
    peer.closed().await;
    assert!(!opening.is_finished());
    let result = tokio::time::timeout(Duration::from_secs(301), &mut opening)
        .await
        .unwrap()
        .unwrap();
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
    assert!(!provider.connected());
    assert_eq!(peer.shared.seen.lock().unwrap().requests.len(), 1);
    // Resume real time for another real socket connection, independent of the old clock.
    tokio::time::resume();
    provider.connect().await.unwrap();
    provider.close().await;
    peer.closed().await;
    assert_eq!(peer.shared.seen.lock().unwrap().opened, 2);
}

#[tokio::test(start_paused = true)]
async fn progress_in_headers_restarts_the_read_budget() {
    let manual = keep_clock_manual();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/mcp", listener.local_addr().unwrap());
    let (written, mut written_rx) = tokio::sync::mpsc::channel(1);
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut head = [0; 4096];
        assert!(socket.read(&mut head).await.unwrap() > 0);
        for fragment in [
            b"HTTP/1.1 200 OK\r\n".as_slice(),
            b"Content-Type: text/event-stream\r\n".as_slice(),
        ] {
            socket.write_all(fragment).await.unwrap();
            written.send(()).await.unwrap();
            tokio::time::sleep(Duration::from_secs(200)).await;
        }
        std::future::pending::<()>().await;
    });
    let provider = Arc::new(
        McpToolProvider::from_url(
            &url,
            FromUrlOptions {
                transport: "sse".into(),
                discover: false,
                connect_timeout: Duration::from_secs(600),
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let (cancel, cancelled) = oneshot::channel();
    let caller = Arc::clone(&provider);
    let mut opening = tokio::spawn(async move {
        caller
            .connect_until(async {
                let _cancelled = cancelled.await;
            })
            .await
    });
    for _ in 0..2 {
        written_rx.recv().await.unwrap();
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_secs(200)).await;
    }
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    let ended_early = opening.is_finished();
    let _cancelled = cancel.send(());
    manual.abort();
    let result = tokio::time::timeout(Duration::from_secs(1), &mut opening)
        .await
        .unwrap()
        .unwrap();
    peer.abort();
    assert!(
        !ended_early,
        "headers made progress every 200 seconds: {result:?}"
    );
    assert!(matches!(result, Err(McpError::Cancelled)), "{result:?}");
}

#[tokio::test(start_paused = true)]
async fn an_idle_connected_reader_fails_its_pending_request_then_can_reenter() {
    use super::peer::{Mode, Peer as HttpPeer};
    let manual = keep_clock_manual();
    let peer = HttpPeer::start(Mode::HeldResponse).await;
    let provider = Arc::new(peer.provider(None, Duration::from_secs(600)));
    provider.connect().await.unwrap();
    let caller = Arc::clone(&provider);
    let call = tokio::spawn(async move {
        caller
            .call_tool(
                "sent",
                serde_json::Map::default(),
                roomkit_mcp::CallToolOptions {
                    timeout: Duration::from_secs(600),
                },
            )
            .await
    });
    peer.messages(3).await;
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_secs(301)).await;
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
    assert!(call.is_finished());
    let result = call.await.unwrap();
    assert!(matches!(result, Err(McpError::Call { .. })), "{result:?}");
    provider.close().await;
    peer.closed().await;
    provider.connect().await.unwrap();
    assert_eq!(
        provider
            .call_tool(
                "kept",
                serde_json::Map::default(),
                roomkit_mcp::CallToolOptions::default()
            )
            .await
            .unwrap(),
        "done"
    );
    provider.close().await;
    peer.closed().await;
    assert_eq!(peer.shared.seen.lock().unwrap().opened, 2);
    manual.abort();
}

#[tokio::test(start_paused = true)]
async fn sse_comments_restart_the_socket_read_budget() {
    use super::peer::{Mode, Peer as HttpPeer};
    let manual = keep_clock_manual();
    let peer = HttpPeer::start(Mode::HeldResponse).await;
    let provider = Arc::new(peer.provider(None, Duration::from_secs(600)));
    provider.connect().await.unwrap();
    let caller = Arc::clone(&provider);
    let call = tokio::spawn(async move {
        caller
            .call_tool(
                "sent",
                serde_json::Map::default(),
                roomkit_mcp::CallToolOptions {
                    timeout: Duration::from_secs(600),
                },
            )
            .await
    });
    peer.messages(3).await;
    for _ in 0..2 {
        tokio::time::advance(Duration::from_secs(200)).await;
        peer.comment();
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
    }
    assert!(!call.is_finished());
    assert!(provider.connected());
    call.abort();
    let _stopped = call.await;
    provider.close().await;
    peer.closed().await;
    manual.abort();
}

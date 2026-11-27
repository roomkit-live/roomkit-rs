//! Python: `test_mcp_legacy_writer.py`, caller ownership at the writer rendezvous.

use super::peer::{Mode, Peer};
use roomkit_mcp::{
    CallToolOptions, McpError, McpToolProvider, ReadResourceOptions, RequestOptions,
};
use serde_json::{Value, json};
use std::time::Duration;

#[derive(Clone, Copy)]
enum Kind {
    Tool,
    Host,
    Resource,
}

impl Kind {
    fn method(self) -> &'static str {
        match self {
            Self::Tool => "tools/call",
            Self::Host => "host/read",
            Self::Resource => "resources/read",
        }
    }
    fn result(self) -> Value {
        match self {
            Self::Tool => json!("done"),
            Self::Host => json!({"done":true}),
            Self::Resource => json!({"contents":[{"uri":"file:///kept", "text":"done"}]}),
        }
    }
}

async fn invoke(
    provider: &McpToolProvider,
    kind: Kind,
    name: &str,
    timeout: Duration,
) -> Result<Value, McpError> {
    match kind {
        Kind::Tool => provider
            .call_tool(
                name,
                serde_json::Map::default(),
                CallToolOptions { timeout },
            )
            .await
            .map(Value::String),
        Kind::Host => provider
            .request(
                "host/read",
                Some(&serde_json::Map::from_iter([("name".into(), json!(name))])),
                RequestOptions { timeout },
            )
            .await
            .map(Value::Object),
        Kind::Resource => provider
            .read_resource(&format!("file:///{name}"), ReadResourceOptions { timeout })
            .await
            .map(Value::Object),
    }
}

async fn queued() {
    for kind in [Kind::Tool, Kind::Host, Kind::Resource] {
        for cancellation in [false, true] {
            let peer = Peer::start(Mode::HeldPost).await;
            let provider = peer.provider(None, Duration::from_secs(3));
            provider.connect().await.unwrap();
            tokio::time::timeout(Duration::from_secs(1), peer.shared.post_held.notified())
                .await
                .unwrap();
            if cancellation {
                let request = invoke(&provider, kind, "abandoned", Duration::from_secs(30));
                tokio::pin!(request);
                assert!(futures_util::poll!(&mut request).is_pending());
            } else {
                let expired = invoke(&provider, kind, "abandoned", Duration::from_millis(30)).await;
                assert!(
                    matches!(expired, Err(McpError::CallTimeout { .. })),
                    "{expired:?}"
                );
            }
            assert_eq!(peer.shared.seen.lock().unwrap().requests.len(), 3);
            peer.shared.release_post.notify_one();
            assert_eq!(
                invoke(&provider, kind, "kept", Duration::from_secs(1))
                    .await
                    .unwrap(),
                kind.result()
            );
            provider.close().await;
            peer.closed().await;
            let seen = peer.shared.seen.lock().unwrap();
            // Python's SDK counts a request its writer took as sent: it tells
            // the server once the writer is free, the request never posted.
            assert_eq!(
                methods(&seen.messages),
                [
                    "initialize",
                    "notifications/initialized",
                    "notifications/cancelled",
                    kind.method()
                ]
            );
            assert_eq!(
                seen.messages.get(2).unwrap().pointer("/params/reason"),
                Some(&json!("caller cancelled"))
            );
            assert_eq!(seen.requests.len(), 5);
        }
    }
}

fn methods(messages: &[Value]) -> Vec<&str> {
    messages
        .iter()
        .map(|message| message["method"].as_str().unwrap())
        .collect()
}

async fn sent() {
    for kind in [Kind::Tool, Kind::Host, Kind::Resource] {
        let peer = Peer::start(Mode::HeldResponse).await;
        let provider = peer.provider(None, Duration::from_secs(3));
        provider.connect().await.unwrap();
        let expired = invoke(&provider, kind, "sent", Duration::from_millis(30)).await;
        assert!(
            matches!(expired, Err(McpError::CallTimeout { .. })),
            "{expired:?}"
        );
        assert_eq!(
            invoke(&provider, kind, "kept", Duration::from_secs(1))
                .await
                .unwrap(),
            kind.result()
        );
        provider.close().await;
        peer.closed().await;
        let seen = peer.shared.seen.lock().unwrap();
        // Python's SDK tells the server a request it gave up on is cancelled.
        assert_eq!(
            methods(&seen.messages),
            [
                "initialize",
                "notifications/initialized",
                kind.method(),
                "notifications/cancelled",
                kind.method()
            ]
        );
        let sent = seen.messages.get(2).unwrap().get("id");
        assert_eq!(
            seen.messages.get(3).unwrap().get("params"),
            Some(&json!({"requestId": sent, "reason": "caller cancelled"}))
        );
        assert_eq!(seen.requests.len(), 6);
    }
}

// Python: test_abandoned_request_waiting_for_the_legacy_writer_is_not_posted.
#[tokio::test]
async fn abandoned_requests_waiting_for_the_writer_are_not_posted() {
    queued().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abandoned_requests_waiting_for_the_writer_on_several_workers() {
    queued().await;
}
// Python: test_expired_request_already_posted_sends_a_cancellation_notification.
#[tokio::test]
async fn expired_requests_already_posted_send_a_cancellation_notification() {
    sent().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_requests_already_posted_on_several_workers() {
    sent().await;
}

#[test]
fn a_queued_future_can_be_dropped_outside_its_runtime() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (peer, provider) = runtime.block_on(async {
        let peer = Peer::start(Mode::HeldPost).await;
        let provider = peer.provider(None, Duration::from_secs(3));
        provider.connect().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), peer.shared.post_held.notified())
            .await
            .unwrap();
        (peer, provider)
    });
    let mut request = Box::pin(invoke(
        &provider,
        Kind::Tool,
        "abandoned",
        Duration::from_secs(30),
    ));
    runtime.block_on(async {
        assert!(futures_util::poll!(&mut request).is_pending());
    });
    drop(request);
    runtime.block_on(async {
        peer.shared.release_post.notify_one();
        assert_eq!(
            invoke(&provider, Kind::Tool, "kept", Duration::from_secs(1))
                .await
                .unwrap(),
            json!("done")
        );
        provider.close().await;
        peer.closed().await;
        let seen = peer.shared.seen.lock().unwrap();
        assert_eq!(
            methods(&seen.messages)[2..],
            ["notifications/cancelled", "tools/call"]
        );
        assert_eq!(seen.requests.len(), 5);
    });
}

// Python: test_mcp_legacy_write_deadline.py::
// test_concurrent_callers_wait_before_http_pool_acquisition.
#[tokio::test]
async fn concurrent_callers_wait_for_the_writer_and_each_is_cancelled() {
    let peer = Peer::start(Mode::HeldPost).await;
    let provider = peer.provider(None, Duration::from_secs(3));
    provider.connect().await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), peer.shared.post_held.notified())
        .await
        .unwrap();
    let mut callers: Vec<_> = (0..64)
        .map(|_| {
            Box::pin(provider.request(
                "never/sent",
                None,
                RequestOptions {
                    timeout: Duration::from_secs(30),
                },
            ))
        })
        .collect();
    for caller in &mut callers {
        assert!(futures_util::poll!(caller).is_pending());
    }
    // GET, the initialize answered, the initialized notification held.
    assert_eq!(peer.shared.seen.lock().unwrap().requests.len(), 3);
    drop(callers);
    peer.shared.release_post.notify_one();
    assert_eq!(
        invoke(&provider, Kind::Host, "kept", Duration::from_secs(1))
            .await
            .unwrap(),
        Kind::Host.result()
    );
    provider.close().await;
    peer.closed().await;
    let seen = peer.shared.seen.lock().unwrap();
    // None of the 64 requests went out; the server is told of each once the
    // writer is free.
    let methods = methods(&seen.messages);
    assert_eq!(methods[..2], ["initialize", "notifications/initialized"]);
    assert_eq!(methods[2..66], ["notifications/cancelled"; 64]);
    assert_eq!(methods[66..], ["host/read"]);
    assert_eq!(seen.requests.len(), 4 + 64);
}

//! Streamable HTTP lifecycle and bounds (RFC §21.2).
#![cfg(feature = "http")]
#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]
use axum::{
    Router,
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
};
use roomkit_mcp::{CallToolOptions, FromUrlOptions, McpError, McpToolProvider};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Default)]
struct Seen {
    posts: Vec<String>,
    authorized: bool,
    compressed: bool,
    session: bool,
    deleted: usize,
    redirected: usize,
}
struct Shared {
    mode: &'static str,
    seen: Mutex<Seen>,
    listing: tokio::sync::Notify,
}
struct Fixture {
    url: String,
    shared: Arc<Shared>,
    task: tokio::task::JoinHandle<()>,
}
impl Fixture {
    async fn start(mode: &'static str) -> Self {
        let shared = Arc::new(Shared {
            mode,
            seen: Mutex::new(Seen::default()),
            listing: tokio::sync::Notify::new(),
        });
        let app = Router::new()
            .route("/mcp", any(handle))
            .route("/redirect-target", any(redirected))
            .with_state(Arc::clone(&shared));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/mcp", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self { url, shared, task }
    }
    fn client(&self) -> McpToolProvider {
        self.labelled(None)
    }
    /// The fixture's provider, its tools named after `label` when given.
    fn labelled(&self, label: Option<&str>) -> McpToolProvider {
        let headers = [
            ("authorization", "Bearer fixture-private-value"),
            ("x-api-key", "fixture-key"),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value.to_owned()))
        .collect();
        let options = FromUrlOptions {
            headers,
            label: label.map(str::to_owned),
            ..FromUrlOptions::default()
        };
        McpToolProvider::from_url(&self.url, options)
            .unwrap()
            .with_connect_timeout(Duration::from_secs(3))
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn redirected(State(shared): State<Arc<Shared>>) -> StatusCode {
    shared.seen.lock().unwrap().redirected += 1;
    StatusCode::OK
}
/// What one POST `name` showed: its credentials, encoding and session.
fn record(shared: &Shared, name: &str, headers: &HeaderMap) {
    let mut seen = shared.seen.lock().unwrap();
    seen.posts.push(name.into());
    seen.authorized = headers.get("authorization").unwrap() == "Bearer fixture-private-value"
        && headers.get("x-api-key").unwrap() == "fixture-key";
    seen.compressed |= headers.contains_key("accept-encoding");
    if name == "tools/list" {
        seen.session = headers
            .get("mcp-session-id")
            .is_some_and(|v| v == "fixture-session");
    }
}

async fn handle(
    State(shared): State<Arc<Shared>>,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if shared.mode == "unauthorized" {
        return (StatusCode::UNAUTHORIZED, "fixture-private-response").into_response();
    }
    if shared.mode == "redirect" {
        return (
            StatusCode::TEMPORARY_REDIRECT,
            [("location", "/redirect-target")],
        )
            .into_response();
    }
    if method == Method::DELETE {
        shared.seen.lock().unwrap().deleted += 1;
        return StatusCode::NO_CONTENT.into_response();
    }
    if method == Method::GET {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    let request: Value = serde_json::from_slice(&body).unwrap();
    let name = request.get("method").unwrap().as_str().unwrap_or("");
    record(&shared, name, &headers);
    if name.starts_with("notifications/") {
        return StatusCode::ACCEPTED.into_response();
    }
    let id = request.get("id").unwrap();
    if name == "server/discover" {
        return (StatusCode::BAD_REQUEST, "Legacy server").into_response();
    }
    if name == "tools/list" {
        shared.listing.notify_one();
        if shared.mode == "hang" {
            return Response::builder()
                .header("content-type", "text/event-stream")
                .body(Body::from_stream(futures_util::stream::pending::<
                    Result<Bytes, std::io::Error>,
                >()))
                .unwrap();
        }
        if shared.mode == "large-json" {
            return (
                [("content-type", "application/json")],
                "x".repeat(4 * 1024 * 1024 + 1),
            )
                .into_response();
        }
        if shared.mode == "large-sse" {
            return (
                [("content-type", "text/event-stream")],
                format!("data: {}\n\n", "x".repeat(4 * 1024 * 1024 + 1)),
            )
                .into_response();
        }
    }
    if name == "tools/call" && shared.mode == "expired" {
        return StatusCode::NOT_FOUND.into_response();
    }
    if name == "tools/call" && shared.mode == "delayed" {
        tokio::time::sleep(Duration::from_millis(10_100)).await;
    }
    let result = match name {
        "initialize" => {
            json!({"protocolVersion":"2025-03-26","capabilities":{"tools":{}},"serverInfo":{"name":"fixture","version":"1"},"instructions":"Fixture instructions"})
        }
        "tools/list" => {
            json!({"tools":[{"name":"echo","description":"Echo","inputSchema":{"type":"object","properties":{"text":{"type":"string"}}}}]})
        }
        "tools/call" => json!({"content":[{"type":"text","text":"fixture answer"}]}),
        _ => json!({}),
    };
    let body = json!({"jsonrpc":"2.0","id":id,"result":result}).to_string();
    let sse = shared.mode == "sse";
    let mut response = Response::builder().header(
        "content-type",
        if sse {
            "text/event-stream; charset=utf-8"
        } else {
            "application/json"
        },
    );
    if name == "initialize" && shared.mode != "stateless" {
        response = response.header("mcp-session-id", "fixture-session");
    }
    response
        .body(Body::from(if sse {
            format!("event: message\ndata: {body}\n\n")
        } else {
            body
        }))
        .unwrap()
}
async fn roundtrip(mode: &'static str) {
    let fixture = Fixture::start(mode).await;
    let server = fixture.labelled(Some("remote"));
    server.connect().await.unwrap();
    assert!(server.connected());
    assert_eq!(server.process_id(), None);
    assert_eq!(
        server.instructions().unwrap().as_deref(),
        Some("Fixture instructions")
    );
    assert_eq!(server.tool_names().unwrap(), ["echo"]);
    let answer = server
        .call_tool("echo", serde_json::Map::new(), CallToolOptions::default())
        .await
        .unwrap();
    assert_eq!(answer, "fixture answer");
    tokio::time::timeout(Duration::from_secs(6), server.close())
        .await
        .unwrap();
    assert!(!server.connected());
    let seen = fixture.shared.seen.lock().unwrap();
    assert!(seen.authorized);
    // Uncompressed even though the test build enables reqwest's gzip.
    assert!(!seen.compressed);
    assert_eq!(seen.session, mode != "stateless");
    assert_eq!(seen.deleted, usize::from(mode != "stateless"));
}
#[tokio::test]
async fn json_replies_and_session_cleanup() {
    roundtrip("json").await;
}
#[tokio::test]
async fn sse_replies_and_session_cleanup() {
    roundtrip("sse").await;
}
#[tokio::test]
async fn stateless_server_needs_no_session() {
    roundtrip("stateless").await;
}
#[tokio::test]
async fn a_long_tool_can_delay_response_headers_past_the_connection_timeout() {
    tokio::time::timeout(Duration::from_secs(15), roundtrip("delayed"))
        .await
        .unwrap();
}
#[tokio::test]
async fn authorization_failure_omits_credentials_and_response_body() {
    let fixture = Fixture::start("unauthorized").await;
    let client = fixture.client();
    assert!(!format!("{client:?}").contains("fixture-private"));
    let error = client.connect().await.unwrap_err();
    assert!(matches!(
        error,
        McpError::Http {
            status: Some(401),
            ..
        }
    ));
    assert!(!format!("{error:?}").contains("fixture-private"));
}
#[tokio::test]
async fn redirects_are_not_followed() {
    let fixture = Fixture::start("redirect").await;
    assert!(fixture.client().connect().await.is_err());
    assert_eq!(fixture.shared.seen.lock().unwrap().redirected, 0);
}
#[tokio::test]
async fn oversized_json_and_sse_fail_before_discovery_completes() {
    for mode in ["large-json", "large-sse"] {
        let fixture = Fixture::start(mode).await;
        let error = fixture.client().connect().await.unwrap_err();
        assert!(matches!(error, McpError::Http { .. }));
        assert_eq!(fixture.shared.seen.lock().unwrap().deleted, 1);
    }
}
#[tokio::test]
async fn cancelled_discovery_deletes_its_session_before_returning() {
    let fixture = Fixture::start("hang").await;
    let client = fixture.client();
    let error = tokio::time::timeout(
        Duration::from_secs(6),
        client.connect_until(fixture.shared.listing.notified()),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(matches!(error, McpError::Cancelled));
    assert_eq!(fixture.shared.seen.lock().unwrap().deleted, 1);
}
#[tokio::test]
async fn discovery_deadline_drains_the_connection() {
    let fixture = Fixture::start("hang").await;
    let client = fixture
        .labelled(None)
        .with_connect_timeout(Duration::from_millis(100));
    let error = tokio::time::timeout(Duration::from_secs(5), client.connect())
        .await
        .unwrap()
        .unwrap_err();
    assert!(matches!(
        error,
        McpError::Http {
            reason: "connection timed out",
            ..
        }
    ));
    assert_eq!(fixture.shared.seen.lock().unwrap().deleted, 1);
}
#[tokio::test]
async fn expired_session_never_replays_a_tool_call() {
    let fixture = Fixture::start("expired").await;
    let server = fixture.client();
    server.connect().await.unwrap();
    assert!(
        server
            .call_tool("echo", serde_json::Map::new(), CallToolOptions::default())
            .await
            .is_err()
    );
    server.close().await;
    let seen = fixture.shared.seen.lock().unwrap();
    assert_eq!(seen.posts.iter().filter(|n| *n == "tools/call").count(), 1);
    assert_eq!(seen.posts.iter().filter(|n| *n == "initialize").count(), 1);
}
#[test]
fn rejects_url_credentials_and_transport_owned_headers() {
    for url in [
        "file:///tmp/server",
        "https://user:secret@example.com/mcp",
        "https://example.com/mcp#fragment",
    ] {
        let refused = McpToolProvider::from_url(url, FromUrlOptions::default());
        assert!(
            matches!(refused, Err(McpError::Configuration { .. })),
            "{url}"
        );
    }
    for header in ["Mcp-Session-Id", "host", "content-length", "accept"] {
        let options = FromUrlOptions {
            headers: [(header.to_owned(), "secret".to_owned())]
                .into_iter()
                .collect(),
            ..FromUrlOptions::default()
        };
        let refused = McpToolProvider::from_url("http://localhost/mcp", options);
        assert!(
            matches!(refused, Err(McpError::Configuration { .. })),
            "{header}"
        );
    }
}

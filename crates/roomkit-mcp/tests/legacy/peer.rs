//! A local HTTP peer records the actual requests and owns its GET response.
#![expect(
    clippy::panic,
    reason = "an unexpected wire request is a broken test fixture"
)]

use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::{
    Router,
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, Method, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::any,
};
use roomkit_mcp::{FromUrlOptions, McpHeaders, McpToolProvider};
use serde_json::{Value, json};
use tokio::sync::{Notify, mpsc};

#[derive(Clone, Default)]
pub(super) enum Mode {
    #[default]
    Normal,
    Endpoint(&'static str),
    Response(Option<&'static str>),
    Eof,
    HeldPost,
    HeldResponse,
    HoldOpening(&'static str),
    Redirect {
        method: Method,
        status: StatusCode,
        outside: Option<String>,
    },
}

#[derive(Default)]
pub(super) struct Seen {
    pub requests: Vec<(Method, String, HeaderMap)>,
    pub messages: Vec<Value>,
    pub opened: usize,
    pub closed: usize,
    sender: Option<mpsc::UnboundedSender<String>>,
}

pub(super) struct Shared {
    pub seen: Mutex<Seen>,
    changed: Notify,
    pub post_held: Notify,
    pub release_post: Notify,
    mode: Mode,
    origin: String,
}

pub(super) struct Peer {
    pub url: String,
    pub shared: Arc<Shared>,
    task: tokio::task::JoinHandle<()>,
}

impl Peer {
    pub(super) async fn start(mode: Mode) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host = if matches!(mode, Mode::Endpoint("host-case")) {
            "localhost"
        } else {
            "127.0.0.1"
        };
        let origin = format!("http://{host}:{}", listener.local_addr().unwrap().port());
        let shared = Arc::new(Shared {
            seen: Mutex::new(Seen::default()),
            changed: Notify::new(),
            post_held: Notify::new(),
            release_post: Notify::new(),
            mode,
            origin: origin.clone(),
        });
        let app = Router::new()
            .fallback(any(handle))
            .with_state(Arc::clone(&shared));
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            url: format!("{origin}/mcp"),
            shared,
            task,
        }
    }

    pub(super) fn provider(
        &self,
        headers: Option<McpHeaders>,
        timeout: Duration,
    ) -> McpToolProvider {
        McpToolProvider::from_url(
            &self.url,
            FromUrlOptions {
                transport: "sse".into(),
                headers,
                discover: false,
                connect_timeout: timeout,
                ..Default::default()
            },
        )
        .unwrap()
    }

    pub(super) async fn opening_ready(&self, stage: &str) {
        loop {
            let changed = self.shared.changed.notified();
            {
                let seen = self.shared.seen.lock().unwrap();
                let ready = if stage == "endpoint" {
                    seen.opened > 0
                } else {
                    seen.messages.iter().any(|message| {
                        message
                            .get("method")
                            .is_some_and(|method| method == "tools/list")
                    })
                };
                if ready {
                    return;
                }
            }
            changed.await;
        }
    }

    pub(super) async fn closed(&self) {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let changed = self.shared.changed.notified();
                {
                    let seen = self.shared.seen.lock().unwrap();
                    if seen.opened == seen.closed {
                        return;
                    }
                }
                changed.await;
            }
        })
        .await
        .unwrap();
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Close(Arc<Shared>);
impl Drop for Close {
    fn drop(&mut self) {
        self.0.seen.lock().unwrap().closed += 1;
        self.0.changed.notify_one();
    }
}

async fn handle(
    State(shared): State<Arc<Shared>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    shared
        .seen
        .lock()
        .unwrap()
        .requests
        .push((method.clone(), uri.to_string(), headers));
    if let Mode::Redirect {
        method: redirected,
        status,
        outside,
    } = &shared.mode
        && *redirected == method
        && uri.path() != "/accepted"
    {
        return (
            *status,
            [("location", outside.as_deref().unwrap_or("/accepted"))],
        )
            .into_response();
    }
    if method == Method::GET {
        return get(shared);
    }
    assert_eq!(method, Method::POST);
    let message: Value = serde_json::from_slice(&body).unwrap();
    let hold = matches!(shared.mode, Mode::HeldPost)
        && message
            .get("method")
            .is_some_and(|value| value == "notifications/initialized");
    record_message(&shared, message);
    if hold {
        shared.post_held.notify_one();
        shared.release_post.notified().await;
    }
    StatusCode::ACCEPTED.into_response()
}

fn record_message(shared: &Shared, message: Value) {
    let mut seen = shared.seen.lock().unwrap();
    let reply = match message.get("method").and_then(Value::as_str) {
        Some("initialize") => Some(
            json!({"protocolVersion":"2025-11-25", "capabilities":{}, "serverInfo":{"name":"legacy", "version":"1"}}),
        ),
        Some("tools/list") => {
            if matches!(shared.mode, Mode::HoldOpening("listing")) && seen.opened == 1 {
                None
            } else {
                Some(
                    json!({"tools":[{"name":"echo", "description":"echo", "inputSchema":{"type":"object"}}, {"name":"hidden", "description":"hidden", "inputSchema":{"type":"object"}}]}),
                )
            }
        }
        Some("tools/call" | "host/read" | "resources/read")
            if matches!(shared.mode, Mode::HeldResponse)
                && (message
                    .pointer("/params/name")
                    .is_some_and(|value| value == "sent")
                    || message
                        .pointer("/params/uri")
                        .is_some_and(|value| value == "file:///sent")) =>
        {
            None
        }
        Some("tools/call") => Some(json!({"content":[{"type":"text", "text":"done"}]})),
        Some("host/read") => Some(json!({"done":true})),
        Some("resources/read") => Some(json!({"contents":[{"uri":"file:///kept", "text":"done"}]})),
        Some("notifications/initialized" | "notifications/cancelled") => None,
        _ => panic!("unexpected request {message}"),
    };
    if let Some(result) = reply {
        let response = json!({"jsonrpc":"2.0", "id":message.get("id").unwrap(), "result":result});
        seen.sender
            .as_ref()
            .unwrap()
            .send(format!("event: message\ndata: {response}\n\n"))
            .unwrap();
    }
    seen.messages.push(message);
    shared.changed.notify_one();
}

fn get(shared: Arc<Shared>) -> Response {
    if matches!(shared.mode, Mode::Eof) {
        return ([("content-type", "text/event-stream")], "").into_response();
    }
    let content_type = match shared.mode {
        Mode::Response(value) => value,
        Mode::Normal
        | Mode::Endpoint(_)
        | Mode::Eof
        | Mode::HeldPost
        | Mode::HeldResponse
        | Mode::HoldOpening(_)
        | Mode::Redirect { .. } => Some("text/event-stream"),
    };
    let (sender, receiver) = mpsc::unbounded_channel();
    let connection = {
        let mut seen = shared.seen.lock().unwrap();
        seen.opened += 1;
        seen.sender = Some(sender.clone());
        seen.opened
    };
    let relative = format!("/messages?session={connection}");
    let target = match shared.mode {
        Mode::Endpoint("relative") => relative.trim_start_matches('/').to_owned(),
        Mode::Endpoint("absolute") => format!("{}{relative}", shared.origin),
        Mode::Endpoint("host-case") => format!(
            "{}{relative}",
            shared.origin.replace("localhost", "LOCALHOST")
        ),
        Mode::Endpoint("userinfo") => format!(
            "{}{relative}",
            shared.origin.replace("http://", "http://user@")
        ),
        Mode::Normal
        | Mode::Endpoint(_)
        | Mode::Redirect { .. }
        | Mode::HoldOpening(_)
        | Mode::Response(_)
        | Mode::Eof
        | Mode::HeldPost
        | Mode::HeldResponse => relative,
    };
    if !(matches!(shared.mode, Mode::HoldOpening("endpoint")) && connection == 1) {
        sender
            .send(format!("event: endpoint\ndata: {target}\n\n"))
            .unwrap();
    }
    shared.changed.notify_one();
    let stream = futures_util::stream::unfold(
        (receiver, Close(shared)),
        |(mut received, guard)| async move {
            received
                .recv()
                .await
                .map(|text| (Ok::<_, Infallible>(text), (received, guard)))
        },
    );
    let mut response = Body::from_stream(stream).into_response();
    if let Some(content_type) = content_type {
        response
            .headers_mut()
            .insert("content-type", content_type.parse().unwrap());
    }
    response
}

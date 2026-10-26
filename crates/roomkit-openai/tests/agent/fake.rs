//! A fake of the OpenAI wire: a local HTTP server, bound to a free port,
//! that answers each request with the next scripted reply, the last one
//! again once they are spent, and keeps what it was sent.

use std::sync::{Arc, Mutex, PoisonError};

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

/// What the fake answers a request with.
#[derive(Clone, Debug)]
pub(crate) struct Reply {
    status: u16,
    body: String,
}

impl Reply {
    /// A chat completion whose one choice answers `text`.
    pub(crate) fn completion(text: &str) -> Self {
        let body = json!({
            "id": "chatcmpl-1",
            "object": "chat.completion",
            "model": "fake-model",
            "choices": [{
                "index": 0,
                "message": { "role": "assistant", "content": text },
                "finish_reason": "stop",
            }],
            "usage": { "prompt_tokens": 12, "completion_tokens": 3, "total_tokens": 15 },
        });
        Self {
            status: 200,
            body: body.to_string(),
        }
    }

    /// An answer of HTTP `status` holding `body`.
    pub(crate) fn status(status: u16, body: &str) -> Self {
        Self {
            status,
            body: body.to_owned(),
        }
    }
}

/// A request the fake received.
#[derive(Clone, Debug)]
pub(crate) struct Request {
    /// The request's path.
    pub(crate) path: String,
    /// Its `Authorization` header, if any.
    pub(crate) authorization: Option<String>,
    /// Its body, as JSON.
    pub(crate) body: Value,
}

/// The fake server, stopped when dropped.
#[derive(Debug)]
pub(crate) struct FakeOpenAi {
    base_url: String,
    requests: Arc<Mutex<Vec<Request>>>,
    server: JoinHandle<()>,
}

impl FakeOpenAi {
    /// A server answering with `replies`, in order.
    pub(crate) async fn start(replies: Vec<Reply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}/v1", listener.local_addr().unwrap());
        let requests = Arc::default();
        let server = tokio::spawn(serve(listener, replies, Arc::clone(&requests)));
        Self {
            base_url,
            requests,
            server,
        }
    }

    /// The API root the agent is configured with.
    pub(crate) fn base_url(&self) -> &str {
        &self.base_url
    }

    /// What the server received so far, in order.
    pub(crate) fn requests(&self) -> Vec<Request> {
        self.requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl Drop for FakeOpenAi {
    fn drop(&mut self) {
        self.server.abort();
    }
}

/// Answers each connection with the next reply.
async fn serve(listener: TcpListener, replies: Vec<Reply>, requests: Arc<Mutex<Vec<Request>>>) {
    let mut next = 0;
    while let Ok((stream, _)) = listener.accept().await {
        let reply = replies
            .get(next)
            .or(replies.last())
            .cloned()
            .unwrap_or_else(|| Reply::status(500, "no reply scripted"));
        next += 1;
        let request = exchange(stream, &reply).await;
        requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(request);
    }
}

/// Reads one HTTP/1.1 request from `stream`, answers it with `reply`, and
/// closes the connection.
async fn exchange(mut stream: TcpStream, reply: &Reply) -> Request {
    let mut received = Vec::new();
    let head_end = loop {
        if let Some(at) = received.windows(4).position(|window| window == b"\r\n\r\n") {
            break at + 4;
        }
        read_more(&mut stream, &mut received).await;
    };
    let head = String::from_utf8_lossy(&received[..head_end]).into_owned();
    let mut lines = head.split("\r\n");
    let path = lines
        .next()
        .and_then(|line| line.split(' ').nth(1))
        .unwrap_or_default()
        .to_owned();
    let headers: Vec<(String, String)> = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_lowercase(), value.trim().to_owned()))
        .collect();
    let header = |name: &str| {
        headers
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.clone())
    };
    let length: usize = header("content-length").map_or(0, |value| value.parse().unwrap());
    while received.len() < head_end + length {
        read_more(&mut stream, &mut received).await;
    }
    let body = serde_json::from_slice(&received[head_end..head_end + length]).unwrap_or_default();
    let answer = format!(
        "HTTP/1.1 {} Fake\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        reply.status,
        reply.body.len(),
        reply.body
    );
    stream.write_all(answer.as_bytes()).await.unwrap();
    stream.shutdown().await.unwrap();
    Request {
        path,
        authorization: header("authorization"),
        body,
    }
}

/// Reads what `stream` holds next into `received`.
async fn read_more(stream: &mut TcpStream, received: &mut Vec<u8>) {
    let mut chunk = [0; 4096];
    let read = stream.read(&mut chunk).await.unwrap();
    assert!(read > 0, "the client closed the connection mid-request");
    received.extend_from_slice(&chunk[..read]);
}

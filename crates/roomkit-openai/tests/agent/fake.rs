//! A fake of the OpenAI wire: a local HTTP server, bound to a free port,
//! that answers each request with the next scripted reply, the last one
//! again once they are spent, and keeps what it was sent.

use std::fmt::Write;
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
    length: Length,
    location: Option<String>,
    silent: bool,
    /// The answer's content type.
    content_type: &'static str,
    /// What a body streamed past its own repeats, until the client stops.
    fill: Vec<u8>,
    /// The wait before each server-sent event after the first, when the
    /// body is sent event by event.
    pace: Option<std::time::Duration>,
    /// More headers of the answer's head.
    headers: Vec<(&'static str, String)>,
    /// The wait after which the connection closes with no answer, if it
    /// does.
    closes_after: Option<std::time::Duration>,
}

/// The length an answer's head declares.
#[derive(Clone, Copy, Debug)]
enum Length {
    /// The body's.
    Body,
    /// This one, whatever the body's.
    Declared(usize),
    /// None: the body runs until the connection closes, `usize` bytes of
    /// it, or fewer when the client stops reading.
    Streamed(usize),
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
            length: Length::Body,
            location: None,
            silent: false,
            content_type: "application/json",
            fill: b" ".to_vec(),
            pace: None,
            headers: Vec::new(),
            closes_after: None,
        }
    }

    /// A streamed completion whose events come `pace` apart, one by one.
    pub(crate) fn paced(chunks: &[Value], pace: std::time::Duration) -> Self {
        Self {
            pace: Some(pace),
            ..Self::events(chunks)
        }
    }

    /// A streamed completion (server-sent events): one event per chunk of
    /// `chunks`, each a JSON chunk, then `[DONE]`.
    pub(crate) fn events(chunks: &[Value]) -> Self {
        let mut body = String::new();
        for chunk in chunks {
            writeln!(body, "data: {chunk}\n").unwrap();
        }
        body.push_str("data: [DONE]\n\n");
        Self {
            body,
            content_type: "text/event-stream",
            ..Self::completion("")
        }
    }

    /// Raw SSE data, including streams that close without a terminal event.
    pub(crate) fn sse(body: &str) -> Self {
        Self {
            body: body.into(),
            content_type: "text/event-stream",
            ..Self::completion("")
        }
    }

    /// A streamed answer that sends `fill` again and again, `bytes` bytes
    /// of it, or until the client stops reading.
    pub(crate) fn endless(fill: &str, bytes: usize) -> Self {
        Self {
            body: String::new(),
            length: Length::Streamed(bytes),
            content_type: "text/event-stream",
            fill: fill.as_bytes().to_vec(),
            ..Self::completion("")
        }
    }

    /// A chat completion answered whole, its body `body`.
    pub(crate) fn whole(body: &Value) -> Self {
        Self {
            body: body.to_string(),
            ..Self::completion("")
        }
    }

    /// A chat completion whose one choice says `text` and calls `name`
    /// with the argument text `arguments`, as the call `id`.
    pub(crate) fn calling(text: &str, id: &str, name: &str, arguments: &str) -> Self {
        let body = json!({
            "id": "chatcmpl-2",
            "object": "chat.completion",
            "model": "fake-model",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": text,
                    "tool_calls": [{
                        "id": id,
                        "type": "function",
                        "function": { "name": name, "arguments": arguments },
                    }],
                },
                "finish_reason": "tool_calls",
            }],
            "usage": { "prompt_tokens": 20, "completion_tokens": 5, "total_tokens": 25 },
        });
        Self {
            body: body.to_string(),
            ..Self::completion("")
        }
    }

    /// An answer of HTTP `status` holding `body`.
    pub(crate) fn status(status: u16, body: &str) -> Self {
        Self {
            status,
            body: body.to_owned(),
            length: Length::Body,
            location: None,
            silent: false,
            content_type: "application/json",
            fill: b" ".to_vec(),
            pace: None,
            headers: Vec::new(),
            closes_after: None,
        }
    }

    /// An answer whose head declares `length` bytes, holding a short body.
    pub(crate) fn declaring(length: usize) -> Self {
        Self {
            length: Length::Declared(length),
            ..Self::completion("short")
        }
    }

    /// An answer that declares no length and streams `bytes` bytes, until
    /// the client stops reading.
    pub(crate) fn streaming(bytes: usize) -> Self {
        Self {
            length: Length::Streamed(bytes),
            ..Self::status(200, "")
        }
    }

    /// A temporary redirect to `location`, which keeps the method and the
    /// body of the request a client follows it with.
    pub(crate) fn redirect(location: &str) -> Self {
        Self {
            location: Some(location.to_owned()),
            ..Self::status(307, "")
        }
    }

    /// The same answer, its head carrying the header `name`.
    pub(crate) fn with_header(mut self, name: &'static str, value: &str) -> Self {
        self.headers.push((name, value.to_owned()));
        self
    }

    /// A stream's head, then the connection closes before its body's first
    /// byte, short of the length it declared.
    pub(crate) fn dropped_before_its_first_event() -> Self {
        Self {
            body: String::new(),
            length: Length::Declared(1024),
            content_type: "text/event-stream",
            ..Self::completion("")
        }
    }

    /// The head of a JSON answer of HTTP `status`, then the connection closes
    /// short of the length it declared.
    pub(crate) fn dropped_in_its_body(status: u16) -> Self {
        Self {
            length: Length::Declared(1024),
            ..Self::status(status, "")
        }
    }

    /// The connection closes once the request is read, with no answer.
    pub(crate) fn closed() -> Self {
        Self::silent_for(std::time::Duration::ZERO)
    }

    /// No answer for `wait`, then the connection closes.
    pub(crate) fn silent_for(wait: std::time::Duration) -> Self {
        Self {
            closes_after: Some(wait),
            ..Self::status(200, "")
        }
    }

    /// A page of models, each of `ids`, as `GET /models` answers.
    pub(crate) fn models(ids: &[&str]) -> Self {
        let data: Vec<Value> = ids
            .iter()
            .map(|id| json!({ "id": id, "object": "model", "owned_by": "fake" }))
            .collect();
        Self::status(200, &json!({ "object": "list", "data": data }).to_string())
    }

    /// No answer at all: the connection stays open, silent.
    pub(crate) fn silent() -> Self {
        Self {
            silent: true,
            ..Self::status(200, "")
        }
    }
}

/// A request the fake received.
#[derive(Clone, Debug)]
pub(crate) struct Request {
    /// The request's method.
    pub(crate) method: String,
    /// The request's path.
    pub(crate) path: String,
    /// Its `Authorization` header, if any.
    pub(crate) authorization: Option<String>,
    /// Its `Accept-Encoding` header, if any.
    pub(crate) accept_encoding: Option<String>,
    /// Each of its `Authorization` headers, in order.
    pub(crate) authorizations: Vec<String>,
    /// The name of each of its headers, in lower case.
    pub(crate) header_names: Vec<String>,
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
        exchange(stream, &reply, &requests).await;
    }
}

/// Reads one HTTP/1.1 request from `stream`, keeps it in `requests`, answers
/// it with `reply`, and closes the connection.
async fn exchange(mut stream: TcpStream, reply: &Reply, requests: &Mutex<Vec<Request>>) {
    let mut received = Vec::new();
    let head_end = loop {
        if let Some(at) = received.windows(4).position(|window| window == b"\r\n\r\n") {
            break at + 4;
        }
        read_more(&mut stream, &mut received).await;
    };
    let head = String::from_utf8_lossy(&received[..head_end]).into_owned();
    let mut lines = head.split("\r\n");
    let mut request_line = lines.next().unwrap_or_default().split(' ');
    let method = request_line.next().unwrap_or_default().to_owned();
    let path = request_line.next().unwrap_or_default().to_owned();
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
    let request = Request {
        method,
        path,
        authorization: header("authorization"),
        accept_encoding: header("accept-encoding"),
        authorizations: headers
            .iter()
            .filter(|(key, _)| key == "authorization")
            .map(|(_, value)| value.clone())
            .collect(),
        header_names: headers.iter().map(|(key, _)| key.clone()).collect(),
        body,
    };
    requests
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .push(request);
    answer(&mut stream, reply).await;
}

/// Writes `reply` on `stream`, then closes it. A client that stops reading
/// ends the answer early, which is no failure of the fake.
async fn answer(stream: &mut TcpStream, reply: &Reply) {
    if reply.silent {
        std::future::pending::<()>().await;
    }
    if let Some(wait) = reply.closes_after {
        tokio::time::sleep(wait).await;
        return;
    }
    let location = reply
        .location
        .as_ref()
        .map(|location| format!("Location: {location}\r\n"))
        .unwrap_or_default();
    let length = match reply.length {
        Length::Body => format!("Content-Length: {}\r\n", reply.body.len()),
        Length::Declared(length) => format!("Content-Length: {length}\r\n"),
        Length::Streamed(_) => String::new(),
    };
    let mut headers = String::new();
    for (name, value) in &reply.headers {
        write!(headers, "{name}: {value}\r\n").unwrap();
    }
    let head = format!(
        "HTTP/1.1 {} Fake\r\nContent-Type: {}\r\n{length}{location}{headers}Connection: close\r\n\r\n",
        reply.status, reply.content_type
    );
    let mut sent = stream.write_all(head.as_bytes()).await.is_ok();
    if let Some(pace) = reply.pace {
        for (at, event) in reply.body.split_inclusive("\n\n").enumerate() {
            if at > 0 {
                tokio::time::sleep(pace).await;
            }
            sent = sent && stream.write_all(event.as_bytes()).await.is_ok();
            stream.flush().await.ok();
        }
    } else {
        sent = sent && stream.write_all(reply.body.as_bytes()).await.is_ok();
    }
    if let Length::Streamed(bytes) = reply.length {
        let whole = (64 * 1024 / reply.fill.len().max(1)).max(1) * reply.fill.len();
        let chunk: Vec<u8> = reply.fill.iter().copied().cycle().take(whole).collect();
        let mut left = bytes;
        while sent && left > 0 {
            let size = left.min(chunk.len());
            sent = stream.write_all(&chunk[..size]).await.is_ok();
            left -= size;
        }
    }
    if sent {
        stream.shutdown().await.ok();
    }
}

/// Reads what `stream` holds next into `received`.
async fn read_more(stream: &mut TcpStream, received: &mut Vec<u8>) {
    let mut chunk = [0; 4096];
    let read = stream.read(&mut chunk).await.unwrap();
    assert!(read > 0, "the client closed the connection mid-request");
    received.extend_from_slice(&chunk[..read]);
}

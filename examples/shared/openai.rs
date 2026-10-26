//! Where an example's OpenAI agent sends its completions: the endpoint the
//! environment names, or a fake of the OpenAI wire, so that the example
//! runs offline and without a key.

use std::error::Error;
use std::time::Duration;
use std::{env, fmt, io};

use roomkit_openai::OpenAiConfig;
use serde_json::{Map, Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use crate::Log;

/// The variables that name a real endpoint, all three set.
const BASE_URL: &str = "OPENAI_BASE_URL";
const API_KEY: &str = "OPENAI_API_KEY";
const MODEL: &str = "OPENAI_MODEL";

/// The largest request the fake reads.
const REQUEST_LIMIT: usize = 1 << 20;

/// The endpoint an example's agent calls. Its `Debug` hides the key and
/// the base URL's credentials, as the crate's own does.
pub enum OpenAiEndpoint {
    /// The endpoint `OPENAI_BASE_URL`, `OPENAI_API_KEY` and `OPENAI_MODEL`
    /// name.
    Real {
        /// The API's root.
        base_url: String,
        /// The key.
        api_key: String,
        /// The model.
        model: String,
    },
    /// A fake of the OpenAI wire on a free local port.
    Fake(FakeOpenAi),
}

impl OpenAiEndpoint {
    /// The endpoint the environment names when its three variables are
    /// set, else a fake answering with `replies`, in order: texts, or
    /// [`Reply::calling`] for a round of calls.
    ///
    /// # Errors
    ///
    /// Returns the error binding the fake's port failed with.
    pub async fn from_env<R: Clone + Into<Reply>>(replies: &[R]) -> Result<Self, Box<dyn Error>> {
        if let (Ok(base_url), Ok(api_key), Ok(model)) =
            (env::var(BASE_URL), env::var(API_KEY), env::var(MODEL))
        {
            return Ok(Self::Real {
                base_url,
                api_key,
                model,
            });
        }
        Ok(Self::Fake(FakeOpenAi::start(replies).await?))
    }

    /// The API's root.
    pub fn base_url(&self) -> &str {
        match self {
            Self::Real { base_url, .. } => base_url,
            Self::Fake(fake) => &fake.base_url,
        }
    }

    /// The key: none for the fake.
    pub fn api_key(&self) -> &str {
        match self {
            Self::Real { api_key, .. } => api_key,
            Self::Fake(_) => "",
        }
    }

    /// The model.
    pub fn model(&self) -> &str {
        match self {
            Self::Real { model, .. } => model,
            Self::Fake(_) => "fake-model",
        }
    }

    /// The API's root as it may be shown: the crate's own redaction
    /// (`OpenAiConfig::shown_base_url`), its scheme, host and port, never
    /// its credentials.
    pub fn shown_base_url(&self) -> String {
        OpenAiConfig::new(self.base_url(), "", self.model()).shown_base_url()
    }

    /// The fake, when the example runs against it.
    pub fn fake(&self) -> Option<&FakeOpenAi> {
        match self {
            Self::Real { .. } => None,
            Self::Fake(fake) => Some(fake),
        }
    }
}

impl fmt::Debug for OpenAiEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Real { model, .. } => f
                .debug_struct("Real")
                .field("base_url", &self.shown_base_url())
                .field("api_key", &"hidden")
                .field("model", model)
                .finish(),
            Self::Fake(fake) => f.debug_tuple("Fake").field(fake).finish(),
        }
    }
}

/// What the fake answers a completion with: a text, or a text and calls.
/// Asked to stream (`stream: true`), it sends the text word by word as
/// server-sent events, each call's arguments in two fragments, then the
/// completion's end and its usage; otherwise the whole completion.
#[derive(Clone, Debug)]
pub struct Reply {
    text: String,
    calls: Vec<(String, String, String)>,
    /// How long it waits between two words of a streamed text.
    pause: Duration,
}

impl Reply {
    /// A completion saying `text` and making `calls`, each an id, a tool's
    /// name and its argument text.
    pub fn calling<'a>(
        text: &str,
        calls: impl IntoIterator<Item = (&'a str, &'a str, &'a str)>,
    ) -> Self {
        Self {
            text: text.to_owned(),
            calls: calls
                .into_iter()
                .map(|(id, name, arguments)| (id.to_owned(), name.to_owned(), arguments.to_owned()))
                .collect(),
            pause: Duration::ZERO,
        }
    }

    /// The same reply, waiting `pause` between two words of its streamed
    /// text, as a model writing slowly.
    #[must_use]
    pub fn pausing(mut self, pause: Duration) -> Self {
        self.pause = pause;
        self
    }

    /// The words its streamed text is written in, each with the space after
    /// it.
    pub fn words(&self) -> Vec<&str> {
        self.text.split_inclusive(' ').collect()
    }

    /// The text it says.
    pub fn text(&self) -> &str {
        &self.text
    }

    /// Whether it makes calls.
    pub fn calls(&self) -> bool {
        !self.calls.is_empty()
    }

    /// The completion's body.
    fn completion(&self, number: usize) -> Value {
        let calls: Vec<Value> = self
            .calls
            .iter()
            .map(|(id, name, arguments)| {
                json!({
                    "id": id,
                    "type": "function",
                    "function": { "name": name, "arguments": arguments },
                })
            })
            .collect();
        let mut message = Map::new();
        message.insert("role".to_owned(), json!("assistant"));
        message.insert("content".to_owned(), json!(self.text));
        if !calls.is_empty() {
            message.insert("tool_calls".to_owned(), Value::Array(calls));
        }
        json!({
            "id": format!("chatcmpl-{number}"),
            "object": "chat.completion",
            "model": "fake-model",
            "choices": [{
                "index": 0,
                "message": message,
                "finish_reason": if self.calls() { "tool_calls" } else { "stop" },
            }],
            "usage": { "prompt_tokens": 40, "completion_tokens": 10, "total_tokens": 50 },
        })
    }
}

impl Reply {
    /// The completion's chunks as a stream sends them (`stream_options.
    /// include_usage`): one per word, each call's start and the two halves of
    /// its arguments, the end, then the usage.
    fn chunks(&self) -> Vec<Value> {
        let delta = |delta: Value| json!({ "choices": [{ "index": 0, "delta": delta }] });
        let mut chunks: Vec<Value> = self
            .words()
            .into_iter()
            .map(|word| delta(json!({ "content": word })))
            .collect();
        for (index, (id, name, arguments)) in self.calls.iter().enumerate() {
            let middle = arguments
                .char_indices()
                .nth(arguments.chars().count() / 2)
                .map_or(arguments.len(), |(at, _)| at);
            let (first, second) = arguments.split_at(middle);
            chunks.push(delta(json!({ "tool_calls": [{ "index": index, "id": id,
                "type": "function", "function": { "name": name, "arguments": first } }] })));
            chunks.push(delta(json!({ "tool_calls": [{ "index": index,
                "function": { "arguments": second } }] })));
        }
        let finish = if self.calls() { "tool_calls" } else { "stop" };
        chunks.push(json!({ "choices": [{ "index": 0, "delta": {}, "finish_reason": finish }] }));
        chunks.push(json!({ "choices": [],
            "usage": { "prompt_tokens": 40, "completion_tokens": 10, "total_tokens": 50 } }));
        chunks
    }
}

impl From<&str> for Reply {
    fn from(text: &str) -> Self {
        Self::calling(text, [])
    }
}

/// A local HTTP server speaking the Chat Completions wire: it answers each
/// request with the next of its replies, the last one again once they are
/// spent, and keeps the body of each. Stopped when dropped.
#[derive(Debug)]
pub struct FakeOpenAi {
    base_url: String,
    requests: Log<Value>,
    server: JoinHandle<()>,
}

impl FakeOpenAi {
    /// A fake on a free local port, answering with `replies`.
    ///
    /// # Errors
    ///
    /// Returns the error binding the port failed with.
    pub async fn start<R: Clone + Into<Reply>>(replies: &[R]) -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let base_url = format!("http://{}/v1", listener.local_addr()?);
        let requests = Log::new();
        let replies = replies.iter().cloned().map(Into::into).collect();
        let server = tokio::spawn(serve(listener, replies, requests.clone()));
        Ok(Self {
            base_url,
            requests,
            server,
        })
    }

    /// The bodies of the requests it received, in order.
    pub fn requests(&self) -> Vec<Value> {
        self.requests.entries()
    }
}

impl Drop for FakeOpenAi {
    fn drop(&mut self) {
        self.server.abort();
    }
}

/// Answers each connection with the next reply, one at a time.
async fn serve(listener: TcpListener, replies: Vec<Reply>, requests: Log<Value>) {
    let mut next = 0;
    while let Ok((mut stream, _)) = listener.accept().await {
        let reply = replies.get(next).or(replies.last()).cloned();
        next += 1;
        let Ok(body) = request_body(&mut stream).await else {
            continue;
        };
        let request: Value = serde_json::from_slice(&body).unwrap_or_default();
        let streams = request.get("stream") == Some(&Value::Bool(true));
        requests.push(request);
        let reply = reply.unwrap_or_else(|| Reply::from(""));
        // A client gone meanwhile reports it itself; the connection closes as
        // the stream is dropped.
        if streams {
            // One at a time, as a model answers: the next request waits.
            let _sent = send_events(&mut stream, &reply).await;
            continue;
        }
        let completion = reply.completion(next).to_string();
        let answer = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
             Connection: close\r\n\r\n{completion}",
            completion.len()
        );
        let _sent = stream.write_all(answer.as_bytes()).await;
    }
}

/// Sends `reply` as a stream of server-sent events, its words `pause`
/// apart, until it is over or the client stops reading.
async fn send_events(stream: &mut TcpStream, reply: &Reply) -> io::Result<()> {
    let head = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n";
    stream.write_all(head.as_bytes()).await?;
    for (position, chunk) in reply.chunks().iter().enumerate() {
        if position > 0 && !reply.pause.is_zero() && position < reply.words().len() {
            tokio::time::sleep(reply.pause).await;
        }
        stream
            .write_all(format!("data: {chunk}\n\n").as_bytes())
            .await?;
        stream.flush().await?;
    }
    stream.write_all(b"data: [DONE]\n\n").await?;
    stream.shutdown().await
}

/// The body of the HTTP/1.1 request `stream` sends.
async fn request_body(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut received = Vec::new();
    let head_end = loop {
        if let Some(at) = received.windows(4).position(|window| window == b"\r\n\r\n") {
            break at + 4;
        }
        read_more(stream, &mut received).await?;
    };
    let head = String::from_utf8_lossy(received.get(..head_end).unwrap_or_default());
    let length = head
        .split("\r\n")
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.trim().eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.trim().parse::<usize>().ok())
        .unwrap_or_default();
    while received.len() < head_end + length {
        read_more(stream, &mut received).await?;
    }
    Ok(received
        .get(head_end..head_end + length)
        .unwrap_or_default()
        .to_vec())
}

/// Reads what `stream` holds next into `received`.
async fn read_more(stream: &mut TcpStream, received: &mut Vec<u8>) -> io::Result<()> {
    let mut chunk = [0; 4096];
    let read = stream.read(&mut chunk).await?;
    if read == 0 || received.len() > REQUEST_LIMIT {
        return Err(io::Error::other("the request ended early or is too long"));
    }
    received.extend(chunk.get(..read).unwrap_or_default());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn neither_debug_nor_the_shown_url_holds_a_secret() {
        let endpoint = OpenAiEndpoint::Real {
            base_url: "https://user:pass@example.com:8443/v1".to_owned(),
            api_key: "sk-secret".to_owned(),
            model: "gpt-6-luna".to_owned(),
        };
        let printed = format!("{endpoint:?} {}", endpoint.shown_base_url());
        for secret in ["sk-secret", "user", "pass"] {
            assert!(!printed.contains(secret), "{secret} in {printed}");
        }
        assert_eq!(endpoint.shown_base_url(), "https://example.com:8443");
    }

    #[test]
    fn a_base_url_with_a_mark_in_its_password_shows_none_of_it() {
        let endpoint = OpenAiEndpoint::Real {
            base_url: "https://user:pa#ss@example.com/v1".to_owned(),
            api_key: "sk-secret".to_owned(),
            model: "gpt-6-luna".to_owned(),
        };
        let shown = endpoint.shown_base_url();
        for secret in ["user", "pa", "ss"] {
            assert!(!shown.contains(secret), "{secret} in {shown}");
        }
    }
}

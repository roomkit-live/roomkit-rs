//! Where an example's OpenAI agent sends its completions: the endpoint the
//! environment names, or a fake of the OpenAI wire, so that the example
//! runs offline and without a key.

use std::error::Error;
use std::{env, fmt, io};

use serde_json::{Value, json};
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
    /// set, else a fake answering with `replies`, in order.
    ///
    /// # Errors
    ///
    /// Returns the error binding the fake's port failed with.
    pub async fn from_env(replies: &[&str]) -> Result<Self, Box<dyn Error>> {
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

    /// The API's root as it may be shown: its scheme, host and port, never
    /// its user information, path or query, which may carry credentials.
    pub fn shown_base_url(&self) -> String {
        let base_url = self.base_url();
        let Some((scheme, rest)) = base_url.split_once("://") else {
            return "(not a URL)".to_owned();
        };
        let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
        let host = authority.rsplit('@').next().unwrap_or_default();
        format!("{scheme}://{host}")
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
    pub async fn start(replies: &[&str]) -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let base_url = format!("http://{}/v1", listener.local_addr()?);
        let requests = Log::new();
        let replies = replies.iter().map(|reply| (*reply).to_owned()).collect();
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
async fn serve(listener: TcpListener, replies: Vec<String>, requests: Log<Value>) {
    let mut next = 0;
    while let Ok((mut stream, _)) = listener.accept().await {
        let reply = replies.get(next).or(replies.last());
        next += 1;
        let Ok(body) = request_body(&mut stream).await else {
            continue;
        };
        requests.push(serde_json::from_slice(&body).unwrap_or_default());
        let completion = json!({
            "id": format!("chatcmpl-{next}"),
            "object": "chat.completion",
            "model": "fake-model",
            "choices": [{
                "index": 0,
                "message": { "role": "assistant", "content": reply },
                "finish_reason": "stop",
            }],
        })
        .to_string();
        let answer = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
             Connection: close\r\n\r\n{completion}",
            completion.len()
        );
        // A client gone meanwhile reports it itself; the connection closes as
        // the stream is dropped.
        let _sent = stream.write_all(answer.as_bytes()).await;
    }
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
}

//! A scripted model's `chat_dialect` (README, "Scripted agents"): each of its
//! requests written through this implementation's Chat Completions rendering,
//! to a local server that keeps what it is sent. The model still answers from
//! its rounds; the rendering is read once the run is over.

use roomkit_ai::{AiContext, AiProvider};
use roomkit_openai::{OpenAiAiProvider, OpenAiConfig};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const ANSWER: &str = r#"{"choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":""}}]}"#;

/// What `context` is on the wire under `dialect`: the request's `messages`.
///
/// # Errors
///
/// A dialect this implementation does not have, or a request that fails.
pub(crate) async fn chat_messages(dialect: &str, context: &AiContext) -> Result<Value, String> {
    if dialect != "openai" {
        return Err(format!(
            "chat_dialect {dialect}: roomkit-rs renders the openai one alone (RMK-1361)"
        ));
    }
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| e.to_string())?;
    let port = listener.local_addr().map_err(|e| e.to_string())?.port();
    let config = OpenAiConfig {
        base_url: Some(format!("http://127.0.0.1:{port}/v1")),
        ..OpenAiConfig::new("scripted", "scripted")
    };
    let provider = OpenAiAiProvider::new(config).map_err(|e| e.to_string())?;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.map_err(|e| e.to_string())?;
        let body = read_request(&mut stream).await?;
        let head = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{ANSWER}",
            ANSWER.len()
        );
        stream
            .write_all(head.as_bytes())
            .await
            .map_err(|e| e.to_string())?;
        serde_json::from_slice::<Value>(&body).map_err(|e| e.to_string())
    });
    provider
        .generate(context)
        .await
        .map_err(|e| e.to_string())?;
    let mut sent = server.await.map_err(|e| e.to_string())??;
    Ok(sent["messages"].take())
}

/// The body of the one request `stream` carries.
async fn read_request(stream: &mut TcpStream) -> Result<Vec<u8>, String> {
    let mut read = Vec::new();
    let mut chunk = [0_u8; 4096];
    loop {
        if let Some(end) = read.windows(4).position(|window| window == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&read[..end]).to_ascii_lowercase();
            let length: usize = head
                .lines()
                .find_map(|line| line.strip_prefix("content-length:"))
                .and_then(|length| length.trim().parse().ok())
                .ok_or("a request with no content-length")?;
            let body = end + 4;
            if read.len() >= body + length {
                return Ok(read[body..body + length].to_vec());
            }
        }
        let got = stream.read(&mut chunk).await.map_err(|e| e.to_string())?;
        if got == 0 {
            return Err("a request cut short".to_owned());
        }
        read.extend_from_slice(&chunk[..got]);
    }
}

//! A scripted model's `chat_dialect` (README, "Scripted agents"): each of its
//! requests written through this implementation's Chat Completions rendering,
//! to a local server that keeps what it is sent. The model still answers from
//! its rounds; the rendering is read once the run is over.

use roomkit_ai::{AiContext, AiProvider};
use roomkit_openai::deepseek::{DeepSeekAiProvider, DeepSeekConfig};
use roomkit_openai::kimi::{KimiAiProvider, KimiConfig};
use roomkit_openai::minimax::{MiniMaxAiProvider, MiniMaxConfig};
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
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| e.to_string())?;
    let port = listener.local_addr().map_err(|e| e.to_string())?.port();
    let base_url = format!("http://127.0.0.1:{port}/v1");
    let (answer, content_type) = if dialect == "anthropic" {
        (
            "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"model\":\"scripted\",\"usage\":{}}}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
            "text/event-stream",
        )
    } else {
        (ANSWER, "application/json")
    };
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.map_err(|e| e.to_string())?;
        let body = read_request(&mut stream).await?;
        let head = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{answer}",
            answer.len()
        );
        stream
            .write_all(head.as_bytes())
            .await
            .map_err(|e| e.to_string())?;
        serde_json::from_slice::<Value>(&body).map_err(|e| e.to_string())
    });
    let failed = |error: roomkit_ai::ProviderError| error.to_string();
    match dialect {
        "anthropic" => {
            let config = roomkit_anthropic::AnthropicConfig {
                base_url: Some(base_url),
                enable_prompt_caching: false,
                ..roomkit_anthropic::AnthropicConfig::new("scripted", "scripted")
            };
            roomkit_anthropic::AnthropicAiProvider::new(config)
                .map_err(failed)?
                .generate(context)
                .await
                .map_err(failed)?;
        }
        "openai" => {
            let config = OpenAiConfig {
                base_url: Some(base_url),
                ..OpenAiConfig::new("scripted", "scripted")
            };
            let provider = OpenAiAiProvider::new(config).map_err(failed)?;
            provider.generate(context).await.map_err(failed)?;
        }
        "openai_thinking_field" => {
            let config = OpenAiConfig {
                base_url: Some(base_url),
                thinking_field: Some("reasoning_content".to_owned()),
                ..OpenAiConfig::new("scripted", "scripted")
            };
            let provider = OpenAiAiProvider::new(config).map_err(failed)?;
            provider.generate(context).await.map_err(failed)?;
        }
        "deepseek" => {
            let config = DeepSeekConfig {
                base_url,
                ..DeepSeekConfig::new("scripted", "scripted")
            };
            let provider = DeepSeekAiProvider::new(config).map_err(failed)?;
            provider.generate(context).await.map_err(failed)?;
        }
        "minimax" => {
            let config = MiniMaxConfig {
                base_url,
                ..MiniMaxConfig::new("scripted", "scripted")
            };
            let provider = MiniMaxAiProvider::new(config).map_err(failed)?;
            provider.generate(context).await.map_err(failed)?;
        }
        "kimi" => {
            let config = KimiConfig {
                base_url,
                ..KimiConfig::new("scripted", "scripted")
            };
            let provider = KimiAiProvider::new(config).map_err(failed)?;
            provider.generate(context).await.map_err(failed)?;
        }
        other => {
            server.abort();
            return Err(format!(
                "chat_dialect {other}: roomkit-rs has no such rendering"
            ));
        }
    }
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

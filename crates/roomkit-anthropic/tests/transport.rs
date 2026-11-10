//! Local fault injection for errors that a live API cannot reproduce on demand.
use roomkit_ai::{AiContext, AiMessage, AiProvider, AiRole};
use roomkit_anthropic::{AnthropicAiProvider, AnthropicConfig};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[expect(
    clippy::unwrap_used,
    reason = "the test server must receive one complete bounded HTTP request"
)]
async fn request(stream: &mut TcpStream) {
    let mut bytes = Vec::new();
    loop {
        let byte = stream.read_u8().await.unwrap();
        bytes.push(byte);
        assert!(bytes.len() < 32 * 1024);
        if bytes.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    let head = String::from_utf8(bytes).unwrap().to_ascii_lowercase();
    let length = head
        .lines()
        .find_map(|line| line.strip_prefix("content-length:"))
        .map_or(0, |value| value.trim().parse::<usize>().unwrap());
    assert!(length < 32 * 1024);
    let mut body = vec![0; length];
    stream.read_exact(&mut body).await.unwrap();
}

// Python: Anthropic SDK _base_client.py: retry before consuming an error body.
#[tokio::test]
async fn retryable_headers_retry_even_when_the_body_is_unreadable() {
    for length in [5_000_000, 10] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            request(&mut first).await;
            first.write_all(format!("HTTP/1.1 503 Service Unavailable\r\nRetry-After: 0.001\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
            drop(first);
            let (mut second, _) = listener.accept().await.unwrap();
            request(&mut second).await;
            second.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"data\":[]}").await.unwrap();
        });
        let provider = AnthropicAiProvider::new(AnthropicConfig {
            base_url: Some(base),
            max_retries: 1,
            ..AnthropicConfig::new("test-key", "model")
        })
        .unwrap();
        let models = tokio::time::timeout(Duration::from_secs(3), provider.list_models())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(models.len(), 0);
        server.await.unwrap();
    }
}

// Python: test_anthropic.py::test_generate_stream cleanup of messages.stream().
#[tokio::test]
async fn cancelling_an_active_stream_closes_its_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let (opened, ready) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut connection, _) = listener.accept().await.unwrap();
        request(&mut connection).await;
        let event = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{}}\n\n";
        connection.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n{:x}\r\n{event}\r\n", event.len()).as_bytes()).await.unwrap();
        opened.send(()).unwrap();
        let mut byte = [0];
        let read = tokio::time::timeout(Duration::from_secs(3), connection.read(&mut byte))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read, 0);
    });
    let provider = AnthropicAiProvider::new(AnthropicConfig {
        base_url: Some(base),
        max_retries: 3,
        ..AnthropicConfig::new("test-key", "model")
    })
    .unwrap();
    let generation = tokio::spawn(async move {
        provider
            .generate(&AiContext {
                messages: vec![AiMessage::new(AiRole::User, "hello")],
                ..AiContext::default()
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(3), ready)
        .await
        .unwrap()
        .unwrap();
    generation.abort();
    assert!(generation.await.unwrap_err().is_cancelled());
    server.await.unwrap();
}

//! Renewable credentials are resolved for each request and errors retain the
//! exact credential used by that response, never a shared mutable token.
use super::{Reply, Room};
use roomkit_openai::{OpenAiConfig, TokenError, TokenFuture, TokenSource};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct Rotating(AtomicUsize);
impl TokenSource for Rotating {
    fn token(&self) -> TokenFuture<'_> {
        let n = self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { Ok(format!("rotating-{n}")) })
    }
}
struct Refused;
impl TokenSource for Refused {
    fn token(&self) -> TokenFuture<'_> {
        Box::pin(async { Err(TokenError::SignInRequired) })
    }
}
#[tokio::test]
async fn a_rotating_source_is_resolved_again_and_its_current_error_is_redacted() {
    let source = Arc::new(Rotating(AtomicUsize::new(0)));
    let room = Room::configured(
        vec![
            Reply::completion("First"),
            Reply::events(&[json!({"error":{"message":"credential rotating-1 refused"}})]),
        ],
        "unused-static",
        |config| {
            let shared = Arc::clone(&source);
            OpenAiConfig {
                token_source: Some(shared),
                ..config
            }
        },
    )
    .await;
    assert_eq!(
        room.says(&room.sms, "alice", "Alice", "First").await.error,
        None
    );
    let failed = room.says(&room.sms, "alice", "Alice", "Second").await;
    let error = failed.error.unwrap().message;
    assert!(!error.contains("rotating-1"), "{error}");
    let requests = room.fake.requests();
    assert_eq!(
        requests[0].authorization.as_deref(),
        Some("Bearer rotating-0")
    );
    assert_eq!(
        requests[1].authorization.as_deref(),
        Some("Bearer rotating-1")
    );
    assert_eq!(source.0.load(Ordering::SeqCst), 2);
}
/// A request tried again under `max_retries` asks its source again, as
/// Python's SDK refreshes a key before each try.
#[tokio::test]
async fn each_try_of_a_request_resolves_its_credential() {
    let source = Arc::new(Rotating(AtomicUsize::new(0)));
    let room = Room::configured(
        vec![
            Reply::status(503, "{}").with_header("retry-after-ms", "1"),
            Reply::completion("Recovered"),
        ],
        "unused-static",
        |config| {
            let shared = Arc::clone(&source);
            OpenAiConfig {
                token_source: Some(shared),
                max_retries: 1,
                ..config
            }
        },
    )
    .await;
    let said = room.says(&room.sms, "alice", "Alice", "Hello").await;
    assert_eq!(said.error, None);
    let requests = room.fake.requests();
    assert_eq!(
        requests
            .iter()
            .map(|request| request.authorization.as_deref())
            .collect::<Vec<_>>(),
        [Some("Bearer rotating-0"), Some("Bearer rotating-1")]
    );
}
#[tokio::test]
async fn a_failed_credential_source_never_falls_back_to_the_static_key() {
    let room = Room::configured(vec![], "unused-static", |config| OpenAiConfig {
        token_source: Some(Arc::new(Refused)),
        ..config
    })
    .await;
    let result = room.says(&room.sms, "alice", "Alice", "Hello").await;
    assert!(result.error.unwrap().message.contains("sign in again"));
    assert!(room.fake.requests().is_empty());
}

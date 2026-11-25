//! What Redis keeps of a presence claim: the key Python reads, and its
//! lifetime.
#![cfg(feature = "redis")]

use roomkit::{RedisTurnBackend, RedisTurnBackendOptions};

#[tokio::test]
async fn a_presence_claim_is_kept_under_the_python_key_for_a_week() {
    let Ok(url) = std::env::var("REDIS_URL") else {
        return;
    };
    let prefix = format!("roomkit:test:agent-presence:{}", uuid::Uuid::new_v4());
    let node = RedisTurnBackend::new(
        Some(&url),
        RedisTurnBackendOptions {
            key_prefix: prefix.clone(),
            ..Default::default()
        },
    )
    .unwrap();
    let agent = "a:b";
    let room = "r/É";
    assert!(node.claim_presence(agent, room, Some(true)).await.unwrap());
    assert!(node.claim_presence(agent, room, Some(false)).await.unwrap());

    let key = format!("{prefix}:presence:r%2F%C3%89:a%3Ab");
    let mut redis = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let (state, ttl): (String, i64) = redis::pipe()
        .cmd("GET")
        .arg(&key)
        .cmd("PTTL")
        .arg(&key)
        .query_async(&mut redis)
        .await
        .unwrap();
    assert_eq!(state, "offline");
    assert!((6 * 24 * 60 * 60 * 1000..=7 * 24 * 60 * 60 * 1000).contains(&ttl));
    node.close().await.unwrap();
}

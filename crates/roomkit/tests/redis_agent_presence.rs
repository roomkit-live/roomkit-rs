//! Cross-owner presence claims and publication using a real Redis registry.
#![cfg(feature = "redis")]
#![expect(clippy::unwrap_used, reason = "a broken test fixture fails the test")]

use roomkit::{RedisTurnBackend, RedisTurnBackendOptions};

fn nodes() -> Option<(String, RedisTurnBackend, RedisTurnBackend)> {
    let url = std::env::var("REDIS_URL").ok()?;
    let prefix = format!("roomkit:test:agent-presence:{}", uuid::Uuid::new_v4());
    let make = || {
        RedisTurnBackend::new(
            Some(&url),
            RedisTurnBackendOptions {
                key_prefix: prefix.clone(),
                ..RedisTurnBackendOptions::default()
            },
        )
        .unwrap()
    };
    Some((prefix.clone(), make(), make()))
}

#[tokio::test]
async fn two_owners_claim_each_change_once_and_share_the_python_key() {
    let Some((prefix, first, second)) = nodes() else {
        return;
    };
    let agent = "a:b";
    let room = "r/É";
    let (one, two) = tokio::join!(
        first.claim_presence(agent, room, Some(true)),
        second.claim_presence(agent, room, Some(true)),
    );
    assert_ne!(one.unwrap(), two.unwrap());
    assert!(
        second
            .claim_presence(agent, room, Some(false))
            .await
            .unwrap()
    );
    assert!(
        !first
            .claim_presence(agent, room, Some(false))
            .await
            .unwrap()
    );
    let key = format!("{prefix}:presence:r%2F%C3%89:a%3Ab");
    let url = std::env::var("REDIS_URL").unwrap();
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
    assert!(!second.claim_presence(agent, room, None).await.unwrap());
    assert!(
        first
            .claim_presence(agent, room, Some(false))
            .await
            .unwrap()
    );
    assert!(!first.claim_presence(agent, room, None).await.unwrap());
    first.close().await.unwrap();
    assert!(first.claim_presence(agent, room, Some(true)).await.is_err());
    second.close().await.unwrap();
}

//! The data pool belongs to the application; the inbox and beat use their own links.
use super::super::{RedisTurnBackend, RedisTurnBackendOptions, TurnTimeouts};
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use std::time::Duration;

async fn connection_id(connection: &mut impl redis::aio::ConnectionLike) -> i64 {
    redis::cmd("CLIENT")
        .arg("ID")
        .query_async(connection)
        .await
        .unwrap()
}

#[tokio::test]
async fn an_injected_pool_keeps_its_response_timeout_ownership_and_connection() {
    let Ok(url) = std::env::var("REDIS_URL") else {
        return;
    };
    let client = redis::Client::open(url.as_str()).unwrap();
    let config = ConnectionManagerConfig::new().set_response_timeout(Some(Duration::from_secs(2)));
    let mut application = ConnectionManager::new_with_config(client, config)
        .await
        .unwrap();
    let options = RedisTurnBackendOptions {
        client: Some(application.clone()),
        key_prefix: format!("roomkit:client-proof:{}", uuid::Uuid::new_v4()),
        timeouts: TurnTimeouts {
            heartbeat_timeout: Duration::from_millis(100),
            ..Default::default()
        },
        ..Default::default()
    };
    let backend = RedisTurnBackend::new(Some(&url), options).unwrap();
    let mut data = backend.connection().await.unwrap();
    let mut blocking = redis::cmd("BLPOP");
    blocking
        .arg(format!("roomkit:client-proof:{}", uuid::Uuid::new_v4()))
        .arg(0.25_f64);
    let read = blocking.query_async::<Option<(String, String)>>(&mut data);
    {
        tokio::pin!(read);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut read)
                .await
                .is_err()
        );
        assert_eq!(read.await.unwrap(), None);
    }
    let app_id = connection_id(&mut application).await;
    assert_eq!(connection_id(&mut data).await, app_id);
    let inbox_id = connection_id(&mut backend.inbox_connection().await.unwrap()).await;
    let beat_id = connection_id(&mut backend.beat_connection().await.unwrap()).await;
    assert_ne!(inbox_id, beat_id);
    assert_ne!(inbox_id, app_id);
    assert_ne!(beat_id, app_id);
    assert!(
        backend
            .claim_presence("agent", "room", Some(true))
            .await
            .unwrap()
    );
    assert!(!backend.claim_presence("agent", "room", None).await.unwrap());
    backend.close().await.unwrap();
    drop(backend);
    assert_eq!(connection_id(&mut application).await, app_id);
}

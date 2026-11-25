//! The broker and the registry of this process keep the ports' contract,
//! and Redis's when `REDIS_URL` names a server: the checks are the ones a
//! service's own tests call (`crate::testing`).
use std::sync::Arc;

use super::{InMemoryTurnBroker, InMemoryTurnRegistry};
use crate::testing::{check_turn_broker, check_turn_registry};

#[cfg(feature = "redis")]
fn redis() -> Option<super::redis_broker::RedisBroker> {
    let url = std::env::var("REDIS_URL").ok()?;
    Some(super::redis_broker::RedisBroker::new(
        redis::Client::open(url.as_str()).unwrap(),
        None,
        format!("roomkit:broker:{}", uuid::Uuid::new_v4()),
        crate::TurnTimeouts::default(),
    ))
}

#[tokio::test]
async fn the_services_of_this_process_keep_the_ports() {
    check_turn_broker(Arc::new(InMemoryTurnBroker::new())).await;
    check_turn_registry(Arc::new(InMemoryTurnRegistry::new())).await;
}

#[cfg(feature = "redis")]
#[tokio::test]
async fn redis_keeps_the_ports() {
    let Some(redis) = redis() else {
        return;
    };
    let redis = Arc::new(redis);
    check_turn_broker(Arc::<super::redis_broker::RedisBroker>::clone(&redis)).await;
    check_turn_registry(redis).await;
}

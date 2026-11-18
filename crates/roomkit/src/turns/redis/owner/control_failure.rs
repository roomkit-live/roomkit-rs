//! An accepted turn reports a Redis control write failure to its owner (§10.6.8).
use super::*;
use crate::turns::pipe::pipe;
use crate::turns::{Carried, Observers, RedisTurnBackendOptions};
use redis::AsyncCommands;
use std::time::Duration;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn a_control_redis_refuses_reports_frame_loss_to_the_owner() {
    let url = std::env::var("REDIS_URL").unwrap();
    let backend = RedisTurnBackend::new(
        Some(&url),
        RedisTurnBackendOptions {
            key_prefix: format!("test:roomkit:control-failure:{}", Uuid::new_v4()),
            ..RedisTurnBackendOptions::default()
        },
    )
    .unwrap();
    let peer = "worker";
    let id: Arc<str> = "running".into();
    let (frames, mut read) = pipe(Arc::clone(&id), Carried::Frame, Observers::default());
    let (controls, outgoing) = pipe(Arc::clone(&id), Carried::Control, Observers::default());
    let link = Arc::new(MemoryTurn::new(
        Arc::clone(&id),
        frames,
        controls,
        backend.timeouts(),
    ));
    let route = backend.route_turn(&id, peer, false).unwrap();
    let mut conn = backend.connection().await.unwrap();
    let key = backend.key("inbox:worker");
    conn.set_ex::<_, _, ()>(&key, "wrong type", 60)
        .await
        .unwrap();

    link.controls.put(&Control::Heartbeat).unwrap();
    tokio::time::timeout(
        Duration::from_secs(3),
        backend.forward_owner(peer.into(), link, outgoing, route),
    )
    .await
    .unwrap();
    let failure = read.get().await.unwrap_err();
    assert_eq!(failure.reason(), "frame_lost");
    conn.del::<_, ()>(&key).await.unwrap();
}

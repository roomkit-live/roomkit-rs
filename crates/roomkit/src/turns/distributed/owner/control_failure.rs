//! An accepted turn reports a Redis control write failure to its owner (§10.6.8).
use super::*;
use crate::turns::pipe::pipe;
use crate::turns::{Carried, Observers, RedisTurnBackend, RedisTurnBackendOptions};
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
        backend.forward_owner(peer.into(), link, outgoing, route, None),
    )
    .await
    .unwrap();
    let failure = read.get().await.unwrap_err();
    assert_eq!(failure.reason(), "frame_lost");
    conn.del::<_, ()>(&key).await.unwrap();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn an_inbound_frame_is_read_while_an_outgoing_write_is_backed_up() {
    let backend = RedisTurnBackend::new(
        Some(&std::env::var("REDIS_URL").unwrap()),
        RedisTurnBackendOptions {
            key_prefix: format!("test:roomkit:frame-during-retry:{}", Uuid::new_v4()),
            ..Default::default()
        },
    )
    .unwrap();
    let peer = "worker";
    let mut conn = backend.connection().await.unwrap();
    let key = backend.key("inbox:worker");
    conn.set_ex::<_, _, ()>(&key, "wrong type", 300)
        .await
        .unwrap();
    backend.listen().await;
    let previous = Message::new(
        "answer",
        backend.node_id(),
        "reply".to_owned(),
        "",
        "earlier",
    );
    let writer = backend.post(peer, &previous);
    tokio::pin!(writer);
    tokio::select! {biased; r=&mut writer => panic!("reply unexpectedly finished {r:?}"), ()=std::future::ready(())=> {}}

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
    link.controls.put(&Control::Heartbeat).unwrap();
    let forwarding = backend.forward_owner(peer.into(), link, outgoing, route, None);
    tokio::pin!(forwarding);
    tokio::select! {biased; ()=&mut forwarding=>panic!("owner bridge exited"), ()=std::future::ready(())=>{}}

    let frame = Message::new(
        "frame",
        peer,
        serde_json::json!({"turn_id":"running","seq":1,"message":{"type":"heartbeat"}}).to_string(),
        &id,
        "",
    );
    backend.post(backend.node_id(), &frame).await.unwrap();
    let received = tokio::select! {
        result=read.get()=>matches!(result,Ok((1,Frame::Heartbeat))),
        ()=&mut forwarding=>false,
        ()=tokio::time::sleep(Duration::from_millis(500))=>false,
    };
    conn.del::<_, ()>(&key).await.unwrap();
    backend.close().await.unwrap();
    assert!(received, "blocked writes must not hold incoming frames");
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn cancelling_a_redis_handover_after_delivery_releases_the_waiting_worker() {
    let url = std::env::var("REDIS_URL").unwrap();
    let backend = RedisTurnBackend::new(
        Some(&url),
        RedisTurnBackendOptions {
            key_prefix: format!("test:roomkit:cancel-handover:{}", Uuid::new_v4()),
            ..Default::default()
        },
    )
    .unwrap();
    let id = Arc::<str>::from("pending");
    let (frames, _frames) = pipe(Arc::clone(&id), Carried::Frame, Observers::default());
    let (controls, _controls) = pipe(Arc::clone(&id), Carried::Control, Observers::default());
    let memory = Arc::new(MemoryTurn::new(id, frames, controls, backend.timeouts()));
    let target = Target {
        node: "worker".into(),
        executor_id: "ai".into(),
    };
    let key = backend.key("inbox:worker");
    let mut conn = backend.connection().await.unwrap();
    let mut hand_over = |_| {};
    let mut waiting = Box::pin(backend.hand_elsewhere(&target, &memory, &mut hand_over, "{}"));
    tokio::time::timeout(Duration::from_secs(2), async {
        tokio::select! {
            _ = &mut waiting => panic!("the worker never answered"),
            () = async { while conn.xlen::<_,u64>(&key).await.unwrap() == 0 { tokio::task::yield_now().await; } } => {}
        }
        std::thread::scope(|scope| scope.spawn(move || drop(waiting)).join().unwrap());
        while conn.xlen::<_,u64>(&key).await.unwrap() < 2 { tokio::task::yield_now().await; }
        let stream: redis::streams::StreamRangeReply = conn.xrange_all(&key).await.unwrap();
        let kinds: Vec<String> = stream.ids.iter().filter_map(|item| item.get("kind")).collect();
        assert_eq!(kinds, ["request","released"]);
    }).await.unwrap();
    backend.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn releasing_a_public_link_tells_its_peer_only_before_its_end_was_read() {
    use crate::turns::{TurnLink, TurnRequest};
    let url = std::env::var("REDIS_URL").unwrap();
    let backend = RedisTurnBackend::new(
        Some(&url),
        RedisTurnBackendOptions {
            key_prefix: format!("test:roomkit:public-release:{}", Uuid::new_v4()),
            ..Default::default()
        },
    )
    .unwrap();
    let fixture = include_str!("../../../../tests/fixtures/turns/python-case1.jsonl");
    let row: serde_json::Value = serde_json::from_str(fixture.lines().next().unwrap()).unwrap();
    let request: TurnRequest = serde_json::from_value(row["message"].clone()).unwrap();
    let mut conn = backend.connection().await.unwrap();
    for (peer, read_end) in [("reader", true), ("abandoned", false)] {
        let (link, controls) =
            super::super::transport::RedisLink::new(request.clone(), backend.timeouts());
        let route = backend.route_turn(&request.turn_id, peer, false).unwrap();
        link.memory.frames.put(&Frame::End).unwrap();
        if read_end {
            assert!(matches!(link.receive().await.unwrap(), (1, Frame::End)));
        }
        link.release();
        backend
            .forward_owner(
                peer.into(),
                Arc::clone(&link.memory),
                controls,
                route,
                Some(Arc::clone(&link.read_end)),
            )
            .await;
        let stream: redis::streams::StreamRangeReply = conn
            .xrange_all(backend.key(&format!("inbox:{peer}")))
            .await
            .unwrap();
        let kinds: Vec<String> = stream
            .ids
            .iter()
            .filter_map(|item| item.get("kind"))
            .collect();
        let expected = if read_end { vec![] } else { vec!["released"] };
        assert_eq!(kinds, expected);
    }
    backend.close().await.unwrap();
}

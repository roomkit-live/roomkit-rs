//! An accepted turn reports a refused control write to its owner (§10.6.8).
use super::*;
use crate::turns::distributed::test_support::Services;
use crate::turns::pipe::pipe;
use crate::turns::{Carried, Observers};
use std::time::Duration;

#[tokio::test]
async fn a_control_the_broker_refuses_reports_frame_loss_to_the_owner() {
    let services = Services::default();
    let backend = services.default_node();
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
    services.broker.refuse(peer);

    link.controls.put(&Control::Heartbeat).unwrap();
    tokio::time::timeout(
        Duration::from_secs(3),
        backend.forward_owner(peer.into(), link, outgoing, route, None),
    )
    .await
    .unwrap();
    let failure = read.get().await.unwrap_err();
    assert_eq!(failure.reason(), "frame_lost");
}

#[tokio::test]
async fn an_inbound_frame_is_read_while_an_outgoing_write_is_backed_up() {
    let services = Services::default();
    let backend = services.default_node();
    let peer = "worker";
    services.broker.refuse(peer);
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
    services.broker.accept(peer);
    backend.close().await.unwrap();
    assert!(received, "blocked writes must not hold incoming frames");
}

#[tokio::test]
async fn cancelling_a_handover_after_delivery_releases_the_waiting_worker() {
    let services = Services::default();
    let backend = services.default_node();
    let id = Arc::<str>::from("pending");
    let (frames, _frames) = pipe(Arc::clone(&id), Carried::Frame, Observers::default());
    let (controls, _controls) = pipe(Arc::clone(&id), Carried::Control, Observers::default());
    let memory = Arc::new(MemoryTurn::new(id, frames, controls, backend.timeouts()));
    let target = Target {
        node: "worker".into(),
        executor_id: "ai".into(),
    };
    let posted = || services.broker.posted("worker");
    let mut hand_over = |_| {};
    let mut waiting = Box::pin(backend.hand_elsewhere(&target, &memory, &mut hand_over, "{}"));
    tokio::time::timeout(Duration::from_secs(2), async {
        tokio::select! {
            _ = &mut waiting => panic!("the worker never answered"),
            () = async { while posted().is_empty() { tokio::task::yield_now().await; } } => {}
        }
        std::thread::scope(|scope| scope.spawn(move || drop(waiting)).join().unwrap());
        while posted().len() < 2 {
            tokio::task::yield_now().await;
        }
        let kinds: Vec<String> = posted().into_iter().map(|message| message.kind).collect();
        assert_eq!(kinds, ["request", "released"]);
    })
    .await
    .unwrap();
    backend.close().await.unwrap();
}

#[tokio::test]
async fn releasing_a_public_link_tells_its_peer_only_before_its_end_was_read() {
    use crate::turns::{TurnLink, TurnRequest};
    let services = Services::default();
    let backend = services.default_node();
    let fixture = include_str!("../../../../tests/fixtures/turns/python-case1.jsonl");
    let row: serde_json::Value = serde_json::from_str(fixture.lines().next().unwrap()).unwrap();
    let request: TurnRequest = serde_json::from_value(row["message"].clone()).unwrap();
    for (peer, read_end) in [("reader", true), ("abandoned", false)] {
        let (link, controls) =
            super::super::transport::DistributedLink::new(request.clone(), backend.timeouts());
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
        let kinds: Vec<String> = services
            .broker
            .posted(peer)
            .into_iter()
            .map(|message| message.kind)
            .collect();
        let expected = if read_end { vec![] } else { vec!["released"] };
        assert_eq!(kinds, expected);
    }
    backend.close().await.unwrap();
}

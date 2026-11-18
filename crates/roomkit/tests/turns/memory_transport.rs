//! The explicitly supplied single-process transport carries real turns without a Redis feature.
mod owner;
mod presence;
use roomkit::testing::{EchoAgent, EchoAgentOptions};
use roomkit::turns::wire::Frame;
use roomkit::turns::{ChannelControl, InMemoryTurnBackend, TurnBackend, TurnRequest};
use roomkit::{RoomKitError, TurnExecutor, TurnExecutorOptions};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

fn request(id: &str) -> TurnRequest {
    let reference = include_str!("../fixtures/turns/python-case1.jsonl");
    let row: Value = serde_json::from_str(reference.lines().next().unwrap()).unwrap();
    let mut request: TurnRequest = serde_json::from_value(row["message"].clone()).unwrap();
    request.turn_id = id.into();
    request
}

fn echo(name: &'static str) -> Arc<TurnExecutor> {
    Arc::new(
        TurnExecutor::new(
            move |id| {
                std::future::ready(Ok(Some(EchoAgent::new(
                    id,
                    name,
                    EchoAgentOptions::default(),
                ))))
            },
            TurnExecutorOptions {
                hosts: Some(["ai".into()].into()),
                executor_id: Some(name.into()),
                ..Default::default()
            },
        )
        .unwrap(),
    )
}

async fn run(backend: &dyn TurnBackend, request: TurnRequest, expected: &str) {
    let marker = request.clone();
    let link = backend.submit(request).await.unwrap();
    assert!(marker.handed() && link.taken());
    assert_eq!(link.request().turn_id, marker.turn_id);
    assert!(
        matches!(link.receive().await.unwrap(), (1,Frame::Accepted {executor_id}) if executor_id == expected)
    );
    assert!(
        matches!(link.receive().await.unwrap(),(2,Frame::Output {output,..}) if output.responded)
    );
    assert!(matches!(link.receive().await.unwrap(), (3, Frame::End)));
    link.release();
}

#[tokio::test]
async fn moving_a_room_to_another_worker_keeps_it_there_until_detach() {
    let backend: Arc<dyn TurnBackend> = Arc::new(InMemoryTurnBackend::new());
    let first = echo("first");
    let second = echo("second");
    backend
        .serve(Arc::<TurnExecutor>::clone(&first))
        .await
        .unwrap();
    backend
        .serve(Arc::<TurnExecutor>::clone(&second))
        .await
        .unwrap();
    assert_eq!(backend.executors().await.unwrap().len(), 2);
    let mut special = request("needs-the-second");
    special.room_hooks.push("audit".into());
    second
        .room_hook(
            "audit",
            roomkit::HookTrigger::BeforeToolUse,
            |_event: roomkit::ToolCallEvent, _| async {
                Ok(roomkit::model::HookResult::allow(
                    roomkit::model::hook::AllowOptions::default(),
                ))
            },
            roomkit::turns::RoomHookOptions::default(),
        )
        .unwrap();
    run(&*backend, special, "second").await;
    let ordinary = request("ordinary");
    run(&*backend, ordinary.clone(), "second").await;
    backend
        .control(ChannelControl::RoomDetached {
            room_id: ordinary.room_id,
            channel_id: ordinary.channel_id,
            organization_id: None,
        })
        .await
        .unwrap();
    run(&*backend, request("after-detach"), "first").await;
    backend.close().await.unwrap();
    let refused = request("closed");
    assert!(
        matches!(backend.submit(refused.clone()).await, Err(RoomKitError::TurnLink {reason,..}) if reason == "backend_closed")
    );
    assert!(!refused.handed() && !first.closed() && !second.closed());
    first.close().await;
    second.close().await;
}

#[tokio::test]
async fn catalogue_preserves_local_predicates_and_channel_calls_follow_organization() {
    let backend = InMemoryTurnBackend::default();
    let worker = TurnExecutor::new_with_host_predicate(
        |id| {
            std::future::ready(Ok(Some(EchoAgent::new(
                id,
                "tenant",
                EchoAgentOptions::default(),
            ))))
        },
        |id| id.as_str() == "ai",
        TurnExecutorOptions {
            organizations: Some(["acme".into()].into()),
            executor_id: Some("same-name".into()),
            ..Default::default()
        },
    )
    .unwrap();
    backend.serve(Arc::new(worker.clone())).await.unwrap();
    let other = echo("same-name");
    backend
        .serve(Arc::<TurnExecutor>::clone(&other))
        .await
        .unwrap();
    let info = backend.executors().await.unwrap();
    assert_eq!(info, backend.executors_now().await.unwrap());
    let [tenant, global] = <&[_; 2]>::try_from(info.as_slice()).unwrap();
    assert!(tenant.hosts.is_none() && tenant.node.is_none() && tenant.last_beat.is_none());
    assert!(tenant.covers("ai", &[], Some("acme")).unwrap());
    assert!(!tenant.covers("other", &[], Some("acme")).unwrap());
    assert!(!tenant.covers("ai", &[], None).unwrap());
    assert!(global.covers("ai", &[], None).unwrap());
    assert!(
        backend
            .claim_presence("ai", "r1", Some(true))
            .await
            .unwrap()
    );
    assert!(
        !backend
            .claim_presence("ai", "r1", Some(true))
            .await
            .unwrap()
    );
    assert!(!backend.claim_presence("ai", "r1", None).await.unwrap());

    let req = request("tenant");
    let mut call = roomkit::turns::ChannelCall::new(req.channel_id, req.room_id, "unknown");
    call.organization_id = Some("elsewhere".into());
    assert!(
        matches!(backend.control(ChannelControl::ChannelCall(Box::new(call.clone()))).await,Err(RoomKitError::TurnLink{reason,..}) if reason=="no_executor")
    );
    assert!(!call.handed());
    call.organization_id = Some("acme".into());
    let reply = backend
        .control(ChannelControl::ChannelCall(Box::new(call.clone())))
        .await
        .unwrap()
        .unwrap();
    assert!(call.handed() && reply.error.is_some());
    assert_eq!(reply.executor_id.as_deref(), Some("same-name"));
    worker.close().await;
    assert_eq!(
        backend.executors().await.unwrap().as_slice(),
        std::slice::from_ref(global)
    );
    backend.close().await.unwrap();
    other.close().await;
}

#[tokio::test]
async fn closing_the_transport_wakes_waiters_and_keeps_the_executor_independent() {
    let backend = InMemoryTurnBackend::new();
    let gate = Arc::new(tokio::sync::Notify::new());
    let build = Arc::clone(&gate);
    let worker = TurnExecutor::new(
        move |id| {
            let build = Arc::clone(&build);
            async move {
                build.notified().await;
                Ok(Some(EchoAgent::new(
                    id,
                    "late",
                    EchoAgentOptions::default(),
                )))
            }
        },
        TurnExecutorOptions {
            hosts: Some(["ai".into()].into()),
            ..Default::default()
        },
    )
    .unwrap();
    backend.serve(Arc::new(worker.clone())).await.unwrap();
    let link = backend.submit(request("blocked")).await.unwrap();
    assert!(matches!(
        link.receive().await.unwrap().1,
        Frame::Accepted { .. }
    ));
    backend.close().await.unwrap();
    for _ in 0..2 {
        assert!(
            matches!(tokio::time::timeout(Duration::from_secs(2),link.receive()).await.unwrap(),Err(RoomKitError::TurnLink{reason,..}) if reason=="backend_closed")
        );
    }
    link.release();
    assert!(!worker.closed());
    gate.notify_one();
    worker.close().await;
}

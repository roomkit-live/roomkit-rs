//! A Python request run on an independent executor, through the production
//! frame and control endpoints, without an owner or a store shared with it.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::sync::Notify;
use tokio::time::timeout;

use super::support::Waiting;
use crate::turns::{CarriedRequest, WorkerTurn};
use crate::{TurnExecutor, TurnExecutorOptions};

const REQUESTS: &str = include_str!("../../../tests/fixtures/turns/python-case1.jsonl");

fn request(id: &str, organization: Option<&str>) -> CarriedRequest {
    let mut fixture: Value = serde_json::from_str(REQUESTS.lines().next().unwrap()).unwrap();
    let message = &mut fixture["message"];
    message["turn_id"] = json!(id);
    message["context"]["room"]["organization_id"] = json!(organization);
    CarriedRequest::from_wire(&message.to_string()).unwrap()
}

fn options() -> TurnExecutorOptions {
    TurnExecutorOptions {
        executor_id: Some("worker".into()),
        hosts: Some(BTreeSet::from(["ai".to_owned()])),
        ..Default::default()
    }
}

async fn frame(turn: &mut WorkerTurn, id: &str, seq: u64, expected: &str) -> Value {
    let raw = timeout(Duration::from_secs(5), turn.next_frame())
        .await
        .unwrap()
        .unwrap();
    let full: Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(full["turn_id"], id);
    assert_eq!(full["seq"], seq);
    assert_eq!(full["message"]["type"], expected, "{raw}");
    full["message"].clone()
}

#[tokio::test]
async fn a_python_request_uses_one_lazy_build_for_two_distinct_turns() {
    let builds = Arc::new(AtomicUsize::new(0));
    let agent = Waiting::new(Duration::ZERO);
    let executor = TurnExecutor::new(
        {
            let count = Arc::clone(&builds);
            let agent = agent.clone();
            move |_| {
                count.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Ok(Some(agent.clone())))
            }
        },
        options(),
    )
    .unwrap();
    assert_eq!(executor.advertised_organizations(), None);
    let mut first = executor.take(request("one", None)).unwrap();
    let mut second = executor.take(request("two", None)).unwrap();
    for (turn, id) in [(&mut first, "one"), (&mut second, "two")] {
        assert_eq!(
            frame(turn, id, 1, "accepted").await["executor_id"],
            "worker"
        );
        let output = frame(turn, id, 2, "output").await;
        assert_eq!(
            output["output"]["response_events"][0]["content"]["body"],
            "Done."
        );
        frame(turn, id, 3, "end").await;
        assert!(turn.next_frame().await.is_none());
    }
    assert_eq!(builds.load(Ordering::SeqCst), 1);
    assert_eq!(agent.ends(), ["answered", "answered"]);
    executor.close().await;
    assert!(executor.take(request("closed", None)).is_err());
}

#[tokio::test]
async fn the_worker_refuses_another_tenant_before_building() {
    let builds = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&builds);
    let executor = TurnExecutor::new(
        move |_| {
            count.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Ok(None::<Waiting>))
        },
        options(),
    )
    .unwrap();
    let mut turn = executor.take(request("foreign", Some("other"))).unwrap();
    frame(&mut turn, "foreign", 1, "accepted").await;
    let failed = frame(&mut turn, "foreign", 2, "error").await;
    assert_eq!(failed["error"]["attributes"]["reason"], "no_executor");
    assert!(
        failed["error"]["message"]
            .as_str()
            .unwrap()
            .contains("other")
    );
    frame(&mut turn, "foreign", 3, "end").await;
    assert_eq!(builds.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn cancellation_crosses_the_pipe_while_the_factory_is_still_running() {
    let building = Arc::new(Notify::new());
    let finish = Arc::new(Notify::new());
    let executor = TurnExecutor::new(
        {
            let building = Arc::clone(&building);
            let finish = Arc::clone(&finish);
            move |_| {
                building.notify_one();
                let finish = Arc::clone(&finish);
                async move {
                    finish.notified().await;
                    Ok(Some(Waiting::new(Duration::ZERO)))
                }
            }
        },
        options(),
    )
    .unwrap();
    let mut turn = executor.take(request("cancel", None)).unwrap();
    frame(&mut turn, "cancel", 1, "accepted").await;
    timeout(Duration::from_secs(5), building.notified())
        .await
        .unwrap();
    assert!(executor.take(request("cancel", None)).is_err());
    turn.control(
        json!({"turn_id":"cancel", "seq":1, "message":{"type":"cancel", "reason":"cancelled"}})
            .to_string(),
    );
    frame(&mut turn, "cancel", 2, "end").await;
    finish.notify_one();
    executor.close().await;
}

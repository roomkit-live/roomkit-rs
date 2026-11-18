//! A separate Redis peer observes the closing node through real inbox entries.
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::sync::{Notify, oneshot};
use tokio::time::timeout;

use super::stream::Message;
use super::{RedisTurnBackend, RedisTurnBackendOptions};
use crate::testing::{EchoAgent, EchoAgentOptions};
use crate::turns::wire::RoomCallResult;
use crate::turns::{TurnTimeouts, lock};
use crate::{TurnExecutor, TurnExecutorOptions};

const LIMIT: Duration = Duration::from_secs(3);

fn pair() -> Option<(RedisTurnBackend, RedisTurnBackend)> {
    let Ok(url) = std::env::var("REDIS_URL") else {
        eprintln!("Set REDIS_URL to run the Redis shutdown tests");
        return None;
    };
    let options = RedisTurnBackendOptions {
        key_prefix: format!("roomkit:shutdown:{}", uuid::Uuid::new_v4()),
        timeouts: TurnTimeouts {
            cancel: Duration::from_secs(1),
            heartbeat_interval: Duration::from_millis(100),
            heartbeat_timeout: Duration::from_secs(10),
            ..Default::default()
        },
        ..Default::default()
    };
    Some((
        RedisTurnBackend::new(Some(&url), options.clone()).unwrap(),
        RedisTurnBackend::new(Some(&url), options).unwrap(),
    ))
}

fn request(id: &str) -> String {
    let raw = include_str!("../../../tests/fixtures/turns/python-case1.jsonl");
    let mut fixture: Value = serde_json::from_str(raw.lines().next().unwrap()).unwrap();
    fixture["message"]["turn_id"] = json!(id);
    fixture["message"]["context"]["room"]["organization_id"] = Value::Null;
    fixture["message"].to_string()
}

#[tokio::test]
async fn closing_a_worker_writes_its_numbered_end_to_the_owner_before_returning() {
    let Some((host, owner)) = pair() else { return };
    let release = Arc::new(Notify::new());
    let worker = TurnExecutor::new(
        {
            let release = Arc::clone(&release);
            move |id| {
                let release = Arc::clone(&release);
                async move {
                    release.notified().await;
                    Ok(Some(EchoAgent::new(
                        id,
                        "late",
                        EchoAgentOptions::default(),
                    )))
                }
            }
        },
        TurnExecutorOptions {
            timeouts: TurnTimeouts {
                heartbeat_interval: 10 * LIMIT,
                ..Default::default()
            },
            executor_id: Some("worker".into()),
            hosts: Some(BTreeSet::from(["ai".into()])),
            ..Default::default()
        },
    )
    .unwrap();
    host.serve(worker.clone()).await.unwrap();
    let mut link = owner.route_turn("turn", host.node_id(), false).unwrap();
    assert_eq!(
        owner
            .ask(host.node_id(), "request", request("turn"), "worker", LIMIT)
            .await
            .unwrap(),
        "taken"
    );
    let first = timeout(LIMIT, link.inbox.recv()).await.unwrap().unwrap();
    let first: Value = serde_json::from_str(&first.body).unwrap();
    assert_eq!(first["message"]["type"], "accepted");
    let beat = json!({"turn_id":"turn","seq":1,"message":{"type":"heartbeat"}});
    owner
        .post(
            host.node_id(),
            &Message::new("control", owner.node_id(), beat.to_string(), "turn", ""),
        )
        .await
        .unwrap();
    // Keep the final frame in Redis so an immediate XRANGE after close proves
    // it is already written, independently of the owner's task scheduling.
    let reader = owner.0.reader.get().unwrap();
    reader.abort();
    timeout(LIMIT, async {
        while !reader.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    timeout(LIMIT, host.close()).await.unwrap().unwrap();
    let inbox: redis::streams::StreamRangeReply = redis::cmd("XRANGE")
        .arg(owner.key(&format!("inbox:{}", owner.node_id())))
        .arg("-")
        .arg("+")
        .query_async(&mut owner.connection().await.unwrap())
        .await
        .unwrap();
    let frames: Vec<Value> = inbox
        .ids
        .iter()
        .filter_map(Message::read)
        .filter(|msg| msg.kind == "frame" && msg.target == "turn")
        .map(|msg| serde_json::from_str(&msg.body).unwrap())
        .collect();
    let end = frames
        .last()
        .expect("the worker's numbered End was not written");
    assert_eq!(end["message"]["type"], "end");
    assert_eq!(end["turn_id"], "turn");
    assert_eq!(end["seq"], first["seq"].as_u64().unwrap() + 1);
    assert_eq!(owner.executors().await.unwrap(), []);
    assert!(host.route_turn("late", owner.node_id(), true).is_none());
    release.notify_one();
    worker.close().await;
    owner.close().await.unwrap();
}

fn room_call(id: &str) -> String {
    json!({"message":{"type":"room_call", "call_id":id, "channel_id":"ai", "room_id":"r",
        "executor_id":"worker", "turn_id":"old", "verb":"room_vision"}})
    .to_string()
}

#[tokio::test]
async fn a_closing_owner_answers_admitted_calls_and_refuses_new_work_without_applying_it() {
    let Some((host, peer)) = pair() else { return };
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    *lock(&host.0.owner) = Some(Arc::new({
        let entered = Arc::clone(&entered);
        let release = Arc::clone(&release);
        move |msg| {
            let crate::turns::wire::OwnerMessage::RoomCall(call) = msg.message else {
                panic!("expected a call")
            };
            assert_eq!(call.call_id, "first", "new call was applied while closing");
            entered.notify_one();
            let release = Arc::clone(&release);
            Box::pin(async move {
                release.notified().await;
                Some(RoomCallResult {
                    call_id: call.call_id,
                    result: json!("settled"),
                    record_changes: None,
                    error: None,
                })
            })
        }
    }));
    host.attend().await.unwrap();
    let first = peer.ask(host.node_id(), "owner", room_call("first"), "", LIMIT);
    let at_close = async {
        timeout(LIMIT, entered.notified()).await.unwrap();
        let mut draining = host.0.draining.subscribe();
        let during_close = async {
            drop(draining.wait_for(|b| *b).await.unwrap());
            let rejected = peer
                .ask(host.node_id(), "owner", room_call("late"), "", LIMIT)
                .await
                .unwrap();
            let result: Value = serde_json::from_str(&rejected).unwrap();
            assert_eq!(result["error"]["attributes"]["reason"], "backend_closed");
            assert!(host.route_turn("late", peer.node_id(), false).is_none());
            assert_eq!(
                host.ask(peer.node_id(), "owner", room_call("late"), "", LIMIT)
                    .await
                    .unwrap_err(),
                super::routes::closed()
            );
            assert_eq!(host.attend().await.unwrap_err(), super::routes::closed());
            assert_eq!(host.executors().await.unwrap_err(), super::routes::closed());
            assert_eq!(
                host.claim_presence("agent", "room", Some(true))
                    .await
                    .unwrap_err(),
                super::routes::closed()
            );
            release.notify_one();
        };
        let (closed, ()) = tokio::join!(host.close(), during_close);
        closed.unwrap();
    };
    let (first, ()) = tokio::join!(first, at_close);
    let returned: Value = serde_json::from_str(&first.unwrap()).unwrap();
    assert_eq!(returned["call_id"], "first");
    assert_eq!(returned["result"], "settled");
    assert_eq!(host.executors().await.unwrap_err(), super::routes::closed());
    // The refused claim wrote nothing that another owner could mistake for a duplicate.
    assert!(
        peer.claim_presence("agent", "room", Some(true))
            .await
            .unwrap()
    );
    peer.close().await.unwrap();
}

#[tokio::test]
async fn close_cancels_a_handler_still_blocked_at_the_deadline() {
    let backend = RedisTurnBackend::new(
        None,
        RedisTurnBackendOptions {
            timeouts: TurnTimeouts {
                cancel: Duration::from_millis(30),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .unwrap();
    let (cancelled, released) = oneshot::channel::<()>();
    assert!(backend.dispatch(async move {
        let _alive = cancelled;
        std::future::pending::<()>().await;
    }));
    timeout(Duration::from_secs(1), backend.close())
        .await
        .unwrap()
        .unwrap();
    assert!(released.await.is_err());
    assert!(!backend.dispatch(async { panic!("closed backend ran more work") }));
    backend.close().await.unwrap();
}

#[tokio::test]
async fn an_outgoing_question_cannot_register_after_close_while_the_reader_starts() {
    let backend = RedisTurnBackend::new(None, RedisTurnBackendOptions::default()).unwrap();
    let (ready_tx, ready_rx) = oneshot::channel();
    let (go, release) = oneshot::channel();
    let initializing = tokio::spawn({
        let backend = backend.clone();
        async move {
            backend
                .0
                .reader
                .get_or_init(|| async move {
                    ready_tx.send(()).unwrap();
                    release.await.unwrap();
                    tokio::spawn(async {}).abort_handle()
                })
                .await;
        }
    });
    ready_rx.await.unwrap();
    let mut in_flight = Box::pin(backend.ask("peer", "channel", String::new(), "worker", LIMIT));
    let poll = std::future::poll_fn(|cx| std::task::Poll::Ready(in_flight.as_mut().poll(cx))).await;
    assert!(poll.is_pending());
    backend.close().await.unwrap();
    go.send(()).unwrap();
    initializing.await.unwrap();
    assert_eq!(in_flight.await.unwrap_err(), super::routes::closed());
}

#[tokio::test]
async fn an_undeclared_worker_stays_alive_while_a_channel_call_returns_its_result() {
    let Some((host, owner)) = pair() else {
        return;
    };
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let executor = TurnExecutor::new(
        {
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            move |id| {
                let release = Arc::clone(&release);
                entered.notify_one();
                async move {
                    release.notified().await;
                    Ok(Some(EchoAgent::new(
                        id,
                        "answer",
                        EchoAgentOptions::default(),
                    )))
                }
            }
        },
        TurnExecutorOptions {
            executor_id: Some("worker".into()),
            hosts: Some(BTreeSet::from(["ai".into()])),
            ..Default::default()
        },
    )
    .unwrap();
    host.serve(executor.clone()).await.unwrap();
    let raw = json!({"message":{"type":"channel_call","call_id":"one","channel_id":"ai",
        "room_id":"r","verb":"missing","organization_id":null,"previous_turn":null}})
    .to_string();
    let finish = async {
        timeout(LIMIT, entered.notified()).await.unwrap();
        let mut draining = host.0.draining.subscribe();
        let wait_for_checks = async {
            drop(draining.wait_for(|b| *b).await.unwrap());
            assert_eq!(
                owner
                    .ask(host.node_id(), "request", request("late"), "worker", LIMIT)
                    .await
                    .unwrap(),
                "refused"
            );
            tokio::time::sleep(3 * owner.timeouts().interval()).await;
            assert_eq!(owner.executors().await.unwrap(), []);
            release.notify_one();
        };
        let (closed, ()) = tokio::join!(host.close(), wait_for_checks);
        closed.unwrap();
    };
    let (reply, ()) = timeout(LIMIT, async {
        tokio::join!(owner.ask_living(host.node_id(), raw, "worker"), finish)
    })
    .await
    .unwrap();
    let answer: Value = serde_json::from_str(&reply.unwrap()).unwrap();
    assert_eq!(answer["executor_id"], "worker");
    assert_eq!(
        answer["error"]["message"],
        "Channel 'ai' has no per-room verb 'missing'"
    );
    executor.close().await;
    owner.close().await.unwrap();
}

mod refusals;

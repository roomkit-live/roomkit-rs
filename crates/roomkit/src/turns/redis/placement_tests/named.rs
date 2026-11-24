//! A turn submitted to the one executor its caller names (RFC §10.6.7).
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use super::super::placement::Placed;
use super::super::{RedisTurnBackend, RedisTurnBackendOptions};
use crate::testing::{EchoAgent, EchoAgentOptions};
use crate::turns::{ExecutorInfo, ExecutorInfoOptions, ServedExecutor, TurnRequest};
use crate::{
    RoomKit, RoomKitError, RoomKitOptions, TurnExecutor, TurnExecutorOptions, TurnTimeouts,
};

/// One Redis node per name, as many processes: each serves a worker of its
/// name, but `owner`, which serves none.
fn processes<const N: usize>() -> Option<(RedisTurnBackend, [RedisTurnBackend; N])> {
    let url = std::env::var("REDIS_URL").ok()?;
    let options = RedisTurnBackendOptions {
        key_prefix: format!("roomkit:named:{}", uuid::Uuid::new_v4()),
        timeouts: TurnTimeouts {
            heartbeat_interval: Duration::from_millis(50),
            ..Default::default()
        },
        ..Default::default()
    };
    let node = || RedisTurnBackend::new(Some(&url), options.clone()).unwrap();
    Some((node(), std::array::from_fn(|_| node())))
}

fn named(executor_id: &str, node: Option<&RedisTurnBackend>) -> ExecutorInfo {
    ExecutorInfo::new(
        executor_id,
        ExecutorInfoOptions {
            node: node.map(|node| node.node_id().to_owned()),
            ..Default::default()
        },
    )
}

async fn target(
    owner: &RedisTurnBackend,
    hooks: &[String],
    only: &ExecutorInfo,
) -> Result<(String, String), RoomKitError> {
    match owner
        .named(&"agent".parse().unwrap(), hooks, None, only)
        .await?
    {
        Placed::Here(worker) => Ok((owner.node_id().to_owned(), worker.executor_id().to_owned())),
        Placed::Elsewhere(target) => Ok((target.node, target.executor_id)),
    }
}

fn refused(result: Result<(String, String), RoomKitError>) -> String {
    match result {
        Err(RoomKitError::TurnLink { reason, message }) if reason == "no_executor" => {
            message.unwrap_or_default()
        }
        other => panic!("expected no_executor, got {other:?}"),
    }
}

async fn close(nodes: impl IntoIterator<Item = &RedisTurnBackend>) {
    for node in nodes {
        node.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_named_executor_is_chosen_whoever_holds_the_room_and_holds_nothing_yet() {
    let Some((owner, [a, b])) = processes() else {
        return;
    };
    a.serve(super::worker("a")).await.unwrap();
    b.serve(super::worker("b")).await.unwrap();
    let holder = super::choose(&owner, "room").await;
    let (other, node) = if holder == "a" { ("b", &b) } else { ("a", &a) };

    let chosen = target(&owner, &[], &named(other, Some(node)))
        .await
        .unwrap();
    assert_eq!(chosen, (node.node_id().to_owned(), other.to_owned()));
    // Choosing it wrote no holder: only the turn it took makes it one.
    assert_eq!(super::choose(&owner, "room").await, holder);
    close([&owner, &a, &b]).await;
}

#[tokio::test]
async fn an_executor_id_two_processes_serve_is_named_by_its_node() {
    let Some((owner, [old, new])) = processes() else {
        return;
    };
    old.serve(super::worker("same")).await.unwrap();
    new.serve(super::worker("same")).await.unwrap();

    let message = refused(target(&owner, &[], &named("same", None)).await);
    assert_eq!(message, "Executor same does not take channel 'agent'");
    let chosen = target(&owner, &[], &named("same", Some(&new)))
        .await
        .unwrap();
    assert_eq!(chosen.0, new.node_id());
    old.close().await.unwrap();
    // Alone again, its id names it.
    let chosen = target(&owner, &[], &named("same", None)).await.unwrap();
    assert_eq!(chosen.0, new.node_id());
    close([&owner, &new]).await;
}

#[tokio::test]
async fn an_executor_that_would_not_take_the_turn_placed_is_refused_named() {
    let Some((owner, [a])) = processes() else {
        return;
    };
    a.serve(super::worker("a")).await.unwrap();
    owner.serve(super::worker("here")).await.unwrap();

    // No process serves the id; the id is served, in another process; and it
    // defines no room hook of that name.
    refused(target(&owner, &[], &named("ghost", None)).await);
    refused(target(&owner, &[], &named("a", Some(&owner))).await);
    let hooked = ["audit".to_owned()];
    let message = refused(target(&owner, &hooked, &named("a", Some(&a))).await);
    assert_eq!(
        message,
        format!(
            "Executor a at {} does not take channel 'agent' with room hooks ['audit']",
            a.node_id()
        )
    );
    // One of this process is named as any other, with its node or without.
    for only in [named("here", None), named("here", Some(&owner))] {
        let chosen = target(&owner, &[], &only).await.unwrap();
        assert_eq!(chosen, (owner.node_id().to_owned(), "here".to_owned()));
    }
    close([&owner, &a]).await;
}

/// A worker of the id `executor_id` declaring the one channel `channel`.
fn hosting(executor_id: &str, channel: &str) -> TurnExecutor {
    TurnExecutor::new(
        |id| async move { Ok(Some(EchoAgent::new(id, "hi", EchoAgentOptions::default()))) },
        TurnExecutorOptions {
            executor_id: Some(executor_id.into()),
            hosts: Some(BTreeSet::from([channel.to_owned()])),
            ..Default::default()
        },
    )
    .unwrap()
}

#[tokio::test]
async fn an_executor_id_names_the_one_process_that_would_take_the_turn() {
    let Some((owner, [coder, other])) = processes() else {
        return;
    };
    coder.serve(hosting("same", "agent")).await.unwrap();
    other.serve(hosting("same", "another")).await.unwrap();

    // Two processes serve the id; one alone would take a turn of `agent`.
    let chosen = target(&owner, &[], &named("same", None)).await.unwrap();
    assert_eq!(chosen.0, coder.node_id());
    close([&owner, &coder, &other]).await;
}

#[tokio::test]
async fn a_channel_the_owner_holds_live_is_named_on_its_own_executor_alone() {
    let Some((node, [remote])) = processes() else {
        return;
    };
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(EchoAgent::new(
        "agent".parse().unwrap(),
        "live",
        EchoAgentOptions::default(),
    ))
    .unwrap();
    remote.serve(super::worker("remote")).await.unwrap();
    let own: Arc<dyn ServedExecutor> = Arc::new(kit.turn_executor());
    node.serve_erased(Arc::clone(&own)).await.unwrap();
    node.reserve_owner(&own);

    // Another executor declares the channel too: the live channel's turn is
    // submitted to the owner's own, and to no other.
    refused(target(&node, &[], &named("remote", Some(&remote))).await);
    let chosen = target(&node, &[], &named(own.executor_id(), None))
        .await
        .unwrap();
    assert_eq!(
        chosen,
        (node.node_id().to_owned(), own.executor_id().to_owned())
    );
    // Named for a turn it does not take, it refuses by its name.
    let hooked = ["audit".to_owned()];
    let message = refused(target(&node, &hooked, &named(own.executor_id(), None)).await);
    assert_eq!(
        message,
        format!(
            "Executor {} does not take channel 'agent' with room hooks ['audit']",
            own.executor_id()
        )
    );
    kit.close().await.unwrap();
    close([&node, &remote]).await;
}

/// The member holding the room `r1` for the channel `ai`, as Redis keeps it.
async fn holder(node: &RedisTurnBackend) -> Option<String> {
    let mut connection = node.connection().await.unwrap();
    redis::cmd("GET")
        .arg(node.key("holder:ai:r1"))
        .query_async(&mut connection)
        .await
        .unwrap()
}

/// Declares in Redis a worker `ghost` of `node`'s process, alive, declaring
/// what its worker `real` declares: one the registry lists and the process
/// does not serve, as a view older than its close would.
async fn declare_ghost(node: &RedisTurnBackend, real: &str) {
    let mut connection = node.connection().await.unwrap();
    let member = format!("{}:{real}", node.node_id());
    let declared: String = redis::cmd("HGET")
        .arg(node.key("executors"))
        .arg(&member)
        .query_async(&mut connection)
        .await
        .unwrap();
    let beat: f64 = redis::cmd("ZSCORE")
        .arg(node.key("alive"))
        .arg(&member)
        .query_async(&mut connection)
        .await
        .unwrap();
    let mut declared: serde_json::Value = serde_json::from_str(&declared).unwrap();
    declared["executor_id"] = "ghost".into();
    let ghost = format!("{}:ghost", node.node_id());
    redis::cmd("HSET")
        .arg(node.key("executors"))
        .arg(&ghost)
        .arg(declared.to_string())
        .exec_async(&mut connection)
        .await
        .unwrap();
    redis::cmd("ZADD")
        .arg(node.key("alive"))
        .arg(beat)
        .arg(&ghost)
        .exec_async(&mut connection)
        .await
        .unwrap();
}

#[tokio::test]
async fn a_process_that_refuses_the_hand_over_fails_it_unhanded_and_the_holder_stays() {
    let Some((owner, [a, b])) = processes() else {
        return;
    };
    a.serve(hosting("a", "ai")).await.unwrap();
    b.serve(hosting("b", "ai")).await.unwrap();
    let request = |id: &str| -> TurnRequest {
        serde_json::from_str(&super::super::shutdown_tests::request(id)).unwrap()
    };
    // a takes the room's first turn and holds it.
    let first = owner
        .submit_to(request("first"), named("a", Some(&a)))
        .await
        .unwrap();
    let held = holder(&owner).await;
    assert_eq!(held, Some(format!("{}:a", a.node_id())));
    declare_ghost(&b, "b").await;

    // The registry lists ghost in b's process, which refuses its request.
    let second = request("second");
    let witness = second.clone();
    let refusal = owner
        .submit_to(second, named("ghost", Some(&b)))
        .await
        .err();
    assert!(
        matches!(&refusal, Some(RoomKitError::TurnLink { reason, .. }) if reason == "no_executor"),
        "{refusal:?}"
    );
    assert!(!witness.handed());
    assert_eq!(holder(&owner).await, held);
    first.release();
    close([&owner, &a, &b]).await;
}

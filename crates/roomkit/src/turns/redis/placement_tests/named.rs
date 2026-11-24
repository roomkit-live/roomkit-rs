//! A turn submitted to the one executor its caller names (RFC §10.6.7).
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::super::placement::Placed;
use super::super::{RedisTurnBackend, RedisTurnBackendOptions};
use crate::model::{ChannelId, InboundMessage, RoomId, TextContent};
use crate::store::ListEventsOptions;
use crate::turns::{
    ChannelControl, ChannelResult, ExecutorInfo, ExecutorInfoOptions, RoomOwner, ServedExecutor,
    TurnBackend, TurnBackendHandle, TurnFuture, TurnLink, TurnRequest,
};
use crate::{
    ConversationStore, ProcessInboundOptions, RoomKit, RoomKitError, RoomKitOptions, TurnTimeouts,
};

/// One Redis node per name, as many processes: each serves a worker of its
/// name, but `owner`, which serves none.
fn processes<const N: usize>(
    accept: Duration,
) -> Option<(RedisTurnBackend, [RedisTurnBackend; N])> {
    let url = std::env::var("REDIS_URL").ok()?;
    let options = RedisTurnBackendOptions {
        key_prefix: format!("roomkit:named:{}", uuid::Uuid::new_v4()),
        timeouts: TurnTimeouts {
            heartbeat_interval: Duration::from_millis(50),
            heartbeat_timeout: Duration::from_secs(2),
            accept,
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
    let Some((owner, [a, b])) = processes(Duration::from_secs(10)) else {
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
    let Some((owner, [old, new])) = processes(Duration::from_secs(10)) else {
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
    let Some((owner, [a])) = processes(Duration::from_secs(10)) else {
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

/// A host's backend: every turn goes to the executor it reserved, and what
/// each submission came to is kept.
struct Reserving {
    inner: RedisTurnBackend,
    reserved: ExecutorInfo,
    submitted: Mutex<Vec<(Option<String>, bool)>>,
}

impl TurnBackend for Reserving {
    fn submit(
        &self,
        request: TurnRequest,
    ) -> TurnFuture<'_, Result<Arc<dyn TurnLink>, RoomKitError>> {
        Box::pin(async move {
            // A clone shares the request's mark.
            let witness = request.clone();
            let link = self.inner.submit_to(request, self.reserved.clone()).await;
            let reason = match &link {
                Ok(_) => None,
                Err(RoomKitError::TurnLink { reason, .. }) => Some(reason.clone()),
                Err(other) => Some(other.to_string()),
            };
            self.submitted
                .lock()
                .unwrap()
                .push((reason, witness.handed()));
            link
        })
    }
    fn serve(&self, executor: Arc<dyn ServedExecutor>) -> TurnFuture<'_, Result<(), RoomKitError>> {
        TurnBackend::serve(&self.inner, executor)
    }
    fn control(
        &self,
        message: ChannelControl,
    ) -> TurnFuture<'_, Result<Option<ChannelResult>, RoomKitError>> {
        TurnBackend::control(&self.inner, message)
    }
    fn attend(&self, owner: Arc<dyn RoomOwner>) -> TurnFuture<'_, Result<(), RoomKitError>> {
        TurnBackend::attend(&self.inner, owner)
    }
    fn close(&self) -> TurnFuture<'_, Result<(), RoomKitError>> {
        TurnBackend::close(&self.inner)
    }
}

/// Serves a worker `name` on `node` from a runtime of its own, which the
/// returned sender stops as a killed process stops: no close, no last beat.
async fn serve_apart(node: RedisTurnBackend, name: &'static str) -> std::sync::mpsc::Sender<()> {
    let (ready, serving) = tokio::sync::oneshot::channel();
    let (kill, killed) = std::sync::mpsc::channel::<()>();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            node.serve(super::worker(name)).await.unwrap();
            ready.send(()).unwrap();
            tokio::task::spawn_blocking(move || killed.recv())
                .await
                .unwrap()
                .ok();
            // Never closed: its handle goes with its runtime.
            drop(node);
        });
        runtime.shutdown_background();
    });
    serving.await.unwrap();
    kill
}

#[tokio::test]
async fn a_hand_over_never_answered_is_lost_handed_and_given_to_no_other() {
    let Some((owner, [a, b])) = processes(Duration::from_millis(300)) else {
        return;
    };
    a.serve(super::worker("a")).await.unwrap();
    let reserved = named("b", Some(&b));
    let kill = serve_apart(b, "b").await;
    let backend = Arc::new(Reserving {
        inner: owner.clone(),
        reserved,
        submitted: Mutex::default(),
    });
    let kit = RoomKit::new(RoomKitOptions {
        turn_backend: Some(TurnBackendHandle::new(Arc::<Reserving>::clone(&backend))),
        ..Default::default()
    });
    let room: RoomId = "r".parse().unwrap();
    let agent: ChannelId = "agent".parse().unwrap();
    let text: ChannelId = "text".parse().unwrap();
    super::register_channels(&kit, &text, &agent, false);
    super::attach_agents_room(&kit, &room, &text, &agent).await;
    // Its process dies: it reads no request and beats no more, while the
    // registry still counts it alive within the heartbeat timeout.
    kill.send(()).unwrap();
    let result = kit
        .process_inbound(
            InboundMessage::new(text.clone(), "person", TextContent::new("Hi")),
            ProcessInboundOptions {
                room_id: Some(room.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert!(
        result
            .error
            .as_ref()
            .is_some_and(|error| error.message.contains("did not answer the turn")),
        "{:?}",
        result.error
    );
    assert_eq!(
        *backend.submitted.lock().unwrap(),
        [(Some("executor_lost".to_owned()), true)]
    );
    // a declares the channel and was given nothing.
    let rows = kit
        .store()
        .list_events(&room, ListEventsOptions::default())
        .await
        .unwrap();
    assert!(rows.iter().all(|row| row.source.channel_id != agent));
    kit.close().await.unwrap();
    a.close().await.unwrap();
}

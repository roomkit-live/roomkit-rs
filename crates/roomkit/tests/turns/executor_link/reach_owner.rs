//! What a host captured by an application worker can reach after its turn.
use std::future::ready;
use std::sync::Arc;
use std::time::Duration;

use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, InboundMessage,
    RoomContext, RoomEvent,
};
use roomkit::turns::wire::Frame;
use roomkit::{Channel, ChannelError, RoomKitError, TurnExecutor, TurnExecutorOptions, TurnHost};
use tokio::sync::mpsc;

use super::{Link, link};

struct Agent {
    id: ChannelId,
    hosts: mpsc::UnboundedSender<TurnHost>,
}

impl Channel for Agent {
    fn channel_id(&self) -> ChannelId {
        self.id.clone()
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }

    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }

    fn deliver(
        &self,
        _: RoomEvent,
        _: ChannelBinding,
        _: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::empty()))
    }

    fn on_event(
        &self,
        _: RoomEvent,
        _: ChannelBinding,
        _: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let host = roomkit::current_turn_host().unwrap();
        host.emit_turn_event("ai_response", roomkit::model::Metadata::new());
        self.hosts.send(host).unwrap();
        ready(Ok(ChannelOutput::empty()))
    }
}

pub(crate) fn worker() -> (TurnExecutor, mpsc::UnboundedReceiver<TurnHost>) {
    let (hosts, receiver) = mpsc::unbounded_channel();
    let worker = TurnExecutor::new(
        move |id| {
            ready(Ok(Some(Agent {
                id,
                hosts: hosts.clone(),
            })))
        },
        TurnExecutorOptions {
            hosts: Some(["ai".into()].into()),
            executor_id: Some("agent-node".into()),
            ..Default::default()
        },
    )
    .unwrap();
    (worker, receiver)
}

async fn end_turn(
    worker: &TurnExecutor,
    hosts: &mut mpsc::UnboundedReceiver<TurnHost>,
) -> (TurnHost, Vec<Frame>) {
    let (link, mut frames, _controls) = link(false);
    worker.take(Arc::<Link>::clone(&link)).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut received = Vec::new();
        while let Some(frame) = frames.recv().await {
            if matches!(frame, Frame::End) {
                break;
            }
            received.push(frame);
        }
        link.write.notify_one();
        link.released.notified().await;
        (hosts.recv().await.unwrap(), received)
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn a_worker_without_an_owner_port_refuses_work_after_its_turn() {
    let (worker, mut hosts) = worker();
    let (host, frames) = end_turn(&worker, &mut hosts).await;
    assert!(!host.open());
    assert!(
        !host.reachable(),
        "this backend supplied no owner outside the turn"
    );
    assert!(
        matches!(host.room_context().await, Err(RoomKitError::Runtime(message))
        if message == "The turn this host served has ended: room_context is not sent")
    );
    assert!(frames.iter().any(|frame| matches!(frame, Frame::FrameworkEvent { event_type, .. } if event_type == "ai_response")));
    worker.close().await;
}

use roomkit::realtime::{EphemeralEvent, EphemeralEventType};
use roomkit::turns::wire::HostResult;
use roomkit::turns::{RoomCall, RoomOwner, RoomPublication, ServedExecutor, TurnFuture};
use serde_json::json;

struct Owner {
    name: &'static str,
    events: mpsc::UnboundedSender<RoomPublication>,
}

impl RoomOwner for Owner {
    fn room_call(&self, message: RoomCall) -> TurnFuture<'_, Result<HostResult, RoomKitError>> {
        Box::pin(async move {
            let error = match message.verb.as_str() {
                "closed_transport" => return Err(RoomKitError::KitClosed),
                "refuse" => Some(
                    serde_json::from_value(json!({
                        "kind":"builtins:ValueError", "message":"refused by owner"
                    }))
                    .unwrap(),
                ),
                _ => None,
            };
            Ok(HostResult {
                call_id: message.call_id.clone(),
                result: json!({"owner":self.name, "call":message}),
                error,
                record_changes: None,
            })
        })
    }

    fn room_publication(
        &self,
        message: RoomPublication,
    ) -> TurnFuture<'_, Result<(), RoomKitError>> {
        Box::pin(async move {
            // Let the second publication arrive while the first is suspended.
            tokio::task::yield_now().await;
            self.events.send(message).unwrap();
            Ok(())
        })
    }
}

fn owner(name: &'static str) -> (Arc<dyn RoomOwner>, mpsc::UnboundedReceiver<RoomPublication>) {
    let (events, receiver) = mpsc::unbounded_channel();
    (Arc::new(Owner { name, events }), receiver)
}

#[tokio::test]
async fn work_keeps_the_owner_its_turn_received_and_propagates_its_call_results() {
    let (worker, mut hosts) = worker();
    let served: Arc<dyn ServedExecutor> = Arc::new(worker.clone());
    let (first, _events) = owner("first");
    let (second, _other) = owner("second");
    assert!(first.executor().is_none());
    served.reach_owner(first);
    let (host, _) = end_turn(&worker, &mut hosts).await;
    served.reach_owner(second);
    let (next, _) = end_turn(&worker, &mut hosts).await;
    assert!(!host.open() && host.reachable());
    let reply = host
        .call(
            "probe",
            Some([("x".into(), json!(5))].into_iter().collect()),
        )
        .await
        .unwrap();
    assert_eq!(reply["owner"], "first");
    assert_eq!(reply["call"]["channel_id"], "ai");
    assert_eq!(reply["call"]["room_id"], "r1");
    assert_eq!(reply["call"]["executor_id"], "agent-node");
    assert_eq!(reply["call"]["turn_id"], host.turn_id());
    assert_eq!(reply["call"]["verb"], "probe");
    assert_eq!(reply["call"]["arguments"], json!({"x":5}));
    assert_eq!(reply["call"]["chain_depth"], 0);
    assert!(
        reply["call"]["call_id"]
            .as_str()
            .is_some_and(|id| !id.is_empty())
    );
    let answer = next.call("probe", None).await.unwrap();
    assert_eq!(answer["owner"], "second");
    assert_ne!(reply["call"]["call_id"], answer["call"]["call_id"]);
    assert!(
        matches!(host.call("refuse", None).await, Err(RoomKitError::Value(reason)) if reason == "refused by owner")
    );
    assert!(matches!(
        host.call("closed_transport", None).await,
        Err(RoomKitError::KitClosed)
    ));
    assert!(
        matches!(host.room_context().await, Err(RoomKitError::Runtime(reason)) if reason.contains("room_context needs a turn"))
    );
    worker.close().await;
}

#[tokio::test]
async fn ended_turn_publications_reach_their_owner_in_order_and_keep_their_scope() {
    let (worker, mut hosts) = worker();
    let (owner, mut events) = owner("room");
    worker.reach_owner(owner);
    let (host, _) = end_turn(&worker, &mut hosts).await;
    let room = roomkit::model::RoomId::new("r1").unwrap();
    let agent = ChannelId::new("ai").unwrap();
    let event = |kind| {
        let mut event = EphemeralEvent::new(room.clone(), kind, agent.as_str());
        event.channel_id = Some(agent.clone());
        event
    };
    host.emit_turn_event("ai_response", roomkit::model::Metadata::new());
    host.publish_from_channel(event(EphemeralEventType::TextDelta));
    host.publish_from_channel(event(EphemeralEventType::Custom));
    tokio::time::timeout(Duration::from_secs(5), async {
        let framework = events.recv().await.unwrap();
        let ephemeral = events.recv().await.unwrap();
        for publication in [&framework, &ephemeral] {
            assert_eq!(publication.channel_id, agent);
            assert_eq!(publication.room_id, room);
            assert_eq!(publication.executor_id, "agent-node");
            assert_eq!(publication.turn_id, host.turn_id());
        }
        assert!(matches!(Frame::from(*framework.frame), Frame::FrameworkEvent {event_type,..} if event_type == "ai_response"));
        assert!(matches!(Frame::from(*ephemeral.frame), Frame::Ephemeral {channel,event} if channel == "room:r1" && event["type"] == "custom"));
    }).await.unwrap();
    worker.close().await;
    assert!(events.try_recv().is_err());
}

#[tokio::test]
async fn a_worker_served_across_processes_reaches_its_owner_after_an_application_link_ends() {
    use roomkit::turns::{
        DistributedTurnBackend, DistributedTurnBackendOptions, InMemoryTurnBroker,
        InMemoryTurnRegistry, TurnBackend,
    };

    let node = DistributedTurnBackend::new(
        Arc::new(InMemoryTurnBroker::new()),
        Arc::new(InMemoryTurnRegistry::new()),
        DistributedTurnBackendOptions::default(),
    );
    let (attended, mut events) = owner("attended");
    TurnBackend::attend(&node, attended).await.unwrap();
    let (worker, mut hosts) = worker();
    TurnBackend::serve(&node, Arc::new(worker.clone()))
        .await
        .unwrap();
    let (host, _) = end_turn(&worker, &mut hosts).await;

    assert_eq!(host.call("probe", None).await.unwrap()["owner"], "attended");
    host.emit_turn_event("ai_response", roomkit::model::Metadata::new());
    let published = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(published.turn_id, host.turn_id());
    assert!(
        matches!(Frame::from(*published.frame), Frame::FrameworkEvent { event_type, .. } if event_type == "ai_response")
    );
    worker.close().await;
    node.close().await.unwrap();
}

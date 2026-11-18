//! The owner's live executor and work that outlives a turn.
use super::{
    Arc, Duration, EchoAgent, EchoAgentOptions, Frame, InMemoryTurnBackend, RoomKitError,
    TurnBackend, TurnExecutor, echo, request, run,
};
use roomkit::turns::wire::HostResult;
use roomkit::turns::{RoomCall, RoomOwner, RoomPublication, ServedExecutor, TurnFuture};
use roomkit::{RoomKit, RoomKitOptions};
use tokio::sync::mpsc;

struct Owner {
    live: Option<Arc<dyn ServedExecutor>>,
    label: &'static str,
    publications: mpsc::UnboundedSender<RoomPublication>,
}

impl RoomOwner for Owner {
    fn executor(&self) -> Option<Arc<dyn ServedExecutor>> {
        self.live.clone()
    }

    fn room_call(&self, message: RoomCall) -> TurnFuture<'_, Result<HostResult, RoomKitError>> {
        Box::pin(async move {
            assert!(roomkit::current_turn_id().is_none());
            if message.verb == "refuse" {
                return Err(RoomKitError::Value("owner refused".into()));
            }
            Ok(HostResult {
                call_id: message.call_id,
                result: serde_json::json!([self.label, message.turn_id]),
                error: None,
                record_changes: None,
            })
        })
    }

    fn room_publication(
        &self,
        message: RoomPublication,
    ) -> TurnFuture<'_, Result<(), RoomKitError>> {
        Box::pin(async move {
            assert!(roomkit::current_turn_id().is_none());
            self.publications.send(message).unwrap();
            Ok(())
        })
    }
}

#[tokio::test]
async fn the_live_owner_alone_can_take_its_channel_even_after_it_closes() {
    let backend = InMemoryTurnBackend::new();
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(EchoAgent::new(
        "ai".parse().unwrap(),
        "owner",
        EchoAgentOptions::default(),
    ))
    .unwrap();
    let (events, _) = mpsc::unbounded_channel();
    backend
        .attend(Arc::new(Owner {
            live: Some(Arc::new(kit.turn_executor())),
            label: "owner",
            publications: events,
        }))
        .await
        .unwrap();
    let other = echo("other");
    backend
        .serve(Arc::<TurnExecutor>::clone(&other))
        .await
        .unwrap();
    let reserved = request("own-unserved");
    assert!(
        matches!(backend.submit(reserved.clone()).await, Err(RoomKitError::TurnLink{reason,..}) if reason=="no_executor")
    );
    assert!(!reserved.handed());
    backend.serve(Arc::new(kit.turn_executor())).await.unwrap();
    backend.serve(Arc::new(kit.turn_executor())).await.unwrap();
    let infos = backend.executors().await.unwrap();
    assert_eq!(
        infos
            .iter()
            .map(|e| e.executor_id.as_str())
            .collect::<Vec<_>>(),
        ["other"]
    );
    run(
        &backend,
        request("owned"),
        kit.turn_executor().executor_id(),
    )
    .await;
    kit.turn_executor().close().await;
    let reserved = request("own-closed");
    assert!(
        matches!(backend.submit(reserved.clone()).await, Err(RoomKitError::TurnLink{reason,..}) if reason=="no_executor")
    );
    assert!(!reserved.handed() && !other.closed());
    backend.close().await.unwrap();
    other.close().await;
    kit.close().await.unwrap();
}

#[tokio::test]
async fn a_retained_host_can_reach_the_first_owner_even_if_it_attended_after_the_turn() {
    let backend = InMemoryTurnBackend::new();
    let (worker, mut hosts) = crate::executor_link::reach_owner::worker();
    backend.serve(Arc::new(worker.clone())).await.unwrap();
    tokio::time::timeout(Duration::from_secs(4),async {
        let link=backend.submit(request("work")).await.unwrap();
        while !matches!(link.receive().await.unwrap().1,Frame::End) {}
        link.release();
        let host=hosts.recv().await.unwrap();
        assert!(matches!(host.call("probe",None).await,Err(RoomKitError::Runtime(s)) if s=="No room owner is reached outside a turn on this backend"));
        let (tx,mut first)=mpsc::unbounded_channel();
        let (other,mut second)=mpsc::unbounded_channel();
        for (label,publications) in [("first",tx),("second",other)] {
            backend.attend(Arc::new(Owner{label,live:None,publications})).await.unwrap();
        }
        assert_eq!(host.call("probe",None).await.unwrap(),serde_json::json!(["first","work"]));
        assert!(matches!(host.call("refuse",None).await,Err(RoomKitError::Value(s)) if s=="owner refused"));
        host.emit_turn_event("ai_response",roomkit::model::Metadata::new());
        let event=first.recv().await.unwrap();
        assert_eq!(event.turn_id,"work");
        assert!(matches!(Frame::from(*event.frame),Frame::FrameworkEvent {event_type,..} if event_type=="ai_response"));
        assert!(second.try_recv().is_err());
    }).await.unwrap();
    worker.close().await;
    backend.close().await.unwrap();
}

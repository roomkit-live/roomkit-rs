//! The outlets exposed to a caller using the wiring directly.
use super::{Arc, Env, Mutex, link, worker};
use roomkit::model::RoomId;
use roomkit::realtime::{
    EphemeralCallback, EphemeralEvent, EphemeralEventType, RealtimeBackend, RealtimeFuture,
};
use roomkit::{ChannelError, RoomKitError};

#[derive(Default)]
struct Sink(Mutex<Vec<(String, EphemeralEvent)>>);
impl RealtimeBackend for Sink {
    fn publish<'a>(
        &'a self,
        channel: &'a str,
        event: EphemeralEvent,
    ) -> RealtimeFuture<'a, Result<(), ChannelError>> {
        Box::pin(async move {
            if channel == "failed" {
                return Err("sink refused".into());
            }
            self.0.lock().unwrap().push((channel.to_owned(), event));
            Ok(())
        })
    }
    fn publish_to_room<'a>(
        &'a self,
        room_id: &'a RoomId,
        event: EphemeralEvent,
    ) -> RealtimeFuture<'a, Result<(), ChannelError>> {
        Box::pin(async move {
            self.0
                .lock()
                .unwrap()
                .push((format!("custom:{room_id}"), event));
            Ok(())
        })
    }
    fn subscribe<'a>(
        &'a self,
        channel: &'a str,
        _: EphemeralCallback,
    ) -> RealtimeFuture<'a, String> {
        Box::pin(async move { format!("sub:{channel}") })
    }
    fn unsubscribe<'a>(&'a self, id: &'a str) -> RealtimeFuture<'a, bool> {
        Box::pin(async move { id == "sub:custom" })
    }
}

#[tokio::test]
async fn the_text_outlet_retains_destination_room_marks_and_subscription_failures() {
    let (link, _, _) = link(false);
    let room = link.request.room_id.clone();
    let worker = worker();
    let unavailable = worker.wiring().realtime();
    assert!(
        matches!(unavailable.unsubscribe("missing").await, Err(RoomKitError::Runtime(reason)) if reason == "This executor has no realtime backend to subscribe on")
    );
    let sink = Arc::new(Sink::default());
    let mut env = Env::new(link.request.context.clone());
    env.realtime = Some(Arc::<Sink>::clone(&sink));
    worker.use_wiring(Arc::new(env));
    let outlet = worker.wiring().realtime();
    let event = EphemeralEvent::new(room.clone(), EphemeralEventType::ToolCallStart, "agent");
    outlet.publish("custom", event.clone()).await.unwrap();
    outlet.publish_to_room(&room, event.clone()).await.unwrap();
    let failure = outlet.publish("failed", event).await.unwrap_err();
    assert_eq!(failure.to_string(), "sink refused");
    let hidden = EphemeralEvent::new(room.clone(), EphemeralEventType::TextDelta, "agent");
    outlet.publish("custom", hidden.clone()).await.unwrap();
    outlet.publish_to_room(&room, hidden).await.unwrap();
    let sent = sink.0.lock().unwrap().clone();
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[0].0, "custom");
    assert_eq!(sent[0].1.room_created_at, None);
    assert_eq!(sent[1].0, format!("custom:{room}"));
    assert_eq!(
        sent[1].1.room_created_at,
        Some(link.request.context.room.created_at)
    );
    let id = outlet
        .subscribe("custom", Arc::new(|_| Box::pin(async {})))
        .await
        .unwrap();
    assert!(outlet.unsubscribe(&id).await.unwrap());
    worker.close().await;
}

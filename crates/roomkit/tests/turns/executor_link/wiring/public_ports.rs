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
    outlet.close().await;
    let id = outlet
        .subscribe("custom", Arc::new(|_| Box::pin(async {})))
        .await
        .unwrap();
    assert!(outlet.unsubscribe(&id).await.unwrap());
    worker.close().await;
}

#[tokio::test]
async fn both_public_outlets_recheck_the_audience_of_a_direct_prompt() {
    use roomkit::model::Visibility;
    use roomkit::turns::DirectPrompt;
    let (link, _, _) = link(false);
    let req = &link.request;
    let room = &req.context.room;
    let sink = Arc::new(Sink::default());
    let worker = worker();
    let mut env = Env::new(req.context.clone());
    env.context.bindings = vec![req.binding.clone()];
    env.realtime = Some(Arc::<Sink>::clone(&sink));
    worker.use_wiring(Arc::new(env));
    let outlet = worker.wiring().realtime();
    let publish = || async {
        let e = EphemeralEvent::new(room.id.clone(), EphemeralEventType::ToolCallStart, "actor");
        outlet.publish("unrelated", e.clone()).await.unwrap();
        outlet
            .publish(&format!("room:{}", room.id), e.clone())
            .await
            .unwrap();
        outlet.publish_to_room(&room.id, e).await.unwrap();
    };
    let mut trigger = req.event.clone();
    trigger.response_visibility = Some(Visibility::None);
    DirectPrompt::of(room, &trigger, &req.channel_id, &req.binding)
        .run(publish())
        .await;
    assert!(sink.0.lock().unwrap().is_empty());
    trigger.response_visibility = Some(Visibility::All);
    DirectPrompt::of(room, &trigger, &req.channel_id, &req.binding)
        .run(publish())
        .await;
    let sent = sink.0.lock().unwrap().clone();
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[0].0, format!("room:{}", room.id));
    assert_eq!(sent[1].0, format!("custom:{}", room.id));
    assert!(
        sent.iter()
            .all(|(_, event)| event.room_created_at == Some(room.created_at))
    );
    publish().await;
    assert_eq!(sink.0.lock().unwrap().len(), 5);
    worker.close().await;
}

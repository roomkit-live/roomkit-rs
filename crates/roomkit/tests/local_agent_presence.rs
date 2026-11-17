//! Public local presence snapshots, without a remote executor.

use roomkit::testing::{EchoAgent, EchoAgentOptions};
use roomkit::{AgentPresenceOptions, AttachChannelOptions, CreateRoomOptions, RoomKit};

// Python: test_local_presence_is_sorted_and_filters_by_binding_category.
// Shared scenarios cover sorting and category; this guard owns the copy assertion.
#[tokio::test]
async fn changing_a_returned_executor_list_does_not_change_the_next_presence() {
    let kit = RoomKit::new(roomkit::RoomKitOptions::default());
    let room: roomkit::model::RoomId = "r1".parse().unwrap();
    let channel: roomkit::model::ChannelId = "helper".parse().unwrap();
    kit.register_channel(EchoAgent::new(
        channel.clone(),
        "hi",
        EchoAgentOptions::default(),
    ))
    .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.attach_channel(&room, &channel, AttachChannelOptions::default())
        .await
        .unwrap();
    let mut first = kit
        .agent_presence(&room, AgentPresenceOptions::default())
        .await
        .unwrap();
    first[0].executors.push("caller-only".into());
    let fresh = kit
        .agent_presence(&room, AgentPresenceOptions::default())
        .await
        .unwrap();
    assert_eq!(fresh.len(), 1);
    assert_eq!(fresh[0].channel_id, "helper");
    assert!(fresh[0].online);
    assert_eq!(fresh[0].executors, [kit.turn_executor().executor_id()]);
    assert_eq!(first[0].executors.len(), 2);
    kit.close().await.unwrap();
}

// Python: test_a_recreated_room_publishes_presence_when_old_hooks_are_forgotten.
#[tokio::test]
async fn a_recreated_room_publishes_presence_when_old_hooks_are_forgotten() {
    use roomkit::realtime::{
        EphemeralCallback, EphemeralEventType, InMemoryRealtime, RealtimeBackend,
    };
    use roomkit::{ConversationStore, EnableRoomHookOptions, RoomKitOptions};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::Notify;

    let realtime = Arc::new(InMemoryRealtime::new(
        roomkit::realtime::InMemoryRealtimeOptions::default(),
    ));
    let kit = RoomKit::new(RoomKitOptions {
        realtime: Some(Arc::<InMemoryRealtime>::clone(&realtime)),
        ..Default::default()
    });
    let room: roomkit::model::RoomId = "r1".parse().unwrap();
    let channel: roomkit::model::ChannelId = "helper".parse().unwrap();
    kit.register_channel(EchoAgent::new(
        channel.clone(),
        "hi",
        EchoAgentOptions::default(),
    ))
    .unwrap();
    let mut original = kit
        .create_room(CreateRoomOptions {
            room_id: Some(room.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
    let binding = kit
        .attach_channel(&room, &channel, AttachChannelOptions::default())
        .await
        .unwrap();
    kit.enable_room_hook(&room, "old-hook", EnableRoomHookOptions::default())
        .await
        .unwrap();
    kit.store().delete_room(&room).await.unwrap();
    original.created_at =
        roomkit::model::Timestamp::from_unix_millis(original.created_at.as_unix_millis() + 1000)
            .unwrap();
    kit.store().create_room(original).await.unwrap();
    kit.store().add_binding(binding).await.unwrap();
    let heard = Arc::new(Mutex::new(Vec::new()));
    let online = Arc::new(Notify::new());
    let (saved, reached) = (Arc::clone(&heard), Arc::clone(&online));
    let callback: EphemeralCallback = Arc::new(move |event| {
        let (saved, reached) = (Arc::clone(&saved), Arc::clone(&reached));
        Box::pin(async move {
            if event.r#type == EphemeralEventType::PresenceOnline {
                saved.lock().unwrap().push((event.r#type, event.user_id));
                reached.notify_one();
            }
        })
    });
    let _subscription = realtime.subscribe_to_room(&room, callback).await;
    kit.register_channel(roomkit::testing::RecordingTransport::new(
        "ops".parse().unwrap(),
        roomkit::model::ChannelType::WEBHOOK,
        roomkit::testing::RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.attach_channel(
        &room,
        &"ops".parse().unwrap(),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    let published = tokio::time::timeout(Duration::from_secs(2), online.notified()).await;
    let fresh = kit
        .agent_presence(&room, AgentPresenceOptions::default())
        .await
        .unwrap();
    kit.close().await.unwrap();
    assert!(
        published.is_ok(),
        "forgetting the old hook did not publish the new coverage"
    );
    assert_eq!(
        *heard.lock().unwrap(),
        [(EphemeralEventType::PresenceOnline, "helper".into())]
    );
    assert_eq!(fresh[0].executors, [kit.turn_executor().executor_id()]);
}

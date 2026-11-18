//! The owner routes an organization's channel lifecycle to a remote worker.

use super::{Events, Observed};
use crate::turns::tests::{support, worker};
use crate::{
    AttachChannelOptions, ChannelCallOptions, ChannelDeclaration, CreateRoomOptions,
    DetachChannelOptions, GetRoomOptions, RedisTurnBackend, RedisTurnBackendOptions, RoomKit,
    RoomKitError, RoomKitOptions, TurnExecutor, TurnExecutorOptions,
};
use serde_json::json;
use std::sync::{Arc, Mutex};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires REDIS_URL pointing to a test Redis server"]
async fn the_owner_routes_all_five_controls_to_the_organization_worker() {
    let url = std::env::var("REDIS_URL").unwrap();
    let options = RedisTurnBackendOptions {
        key_prefix: format!("roomkit:test:control:{}", uuid::Uuid::new_v4().simple()),
        ..Default::default()
    };
    let hosting = RedisTurnBackend::new(Some(&url), options.clone()).unwrap();
    let events = Events::default();
    let executor = TurnExecutor::new(
        {
            let events = Arc::clone(&events);
            move |_| {
                std::future::ready(Ok(Some(Observed {
                    instance: 1,
                    events: Arc::clone(&events),
                    state: Mutex::default(),
                    wait: None,
                })))
            }
        },
        TurnExecutorOptions {
            hosts: Some(["ai".to_owned()].into()),
            organizations: Some(["org".to_owned()].into()),
            executor_id: Some("worker".to_owned()),
            ..Default::default()
        },
    )
    .unwrap();
    hosting.serve(executor.clone()).await.unwrap();
    let kit = RoomKit::new(RoomKitOptions {
        turn_backend: Some(RedisTurnBackend::new(Some(&url), options).unwrap()),
        ..Default::default()
    });
    let agent = support::channel("ai");
    let room = support::room_id();
    let org = Some("org".to_owned());
    kit.register_channel(ChannelDeclaration::new(agent.clone()))
        .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        organization_id: org.clone(),
        ..Default::default()
    })
    .await
    .unwrap();
    let mut event = worker::request("turn", Some("org")).event;
    event.room_id.clone_from(&room);
    let tell = |hooks| {
        kit.inner.turns.committed(
            &agent,
            &room,
            &event,
            org.clone(),
            hooks,
            kit.inner.channel(&agent),
        )
    };
    assert!(
        matches!(tell(vec!["unavailable".to_owned()]).await, Err(RoomKitError::TurnLink{reason,..}) if reason=="no_executor")
    );
    tell(Vec::new()).await.unwrap();
    kit.attach_channel(&room, &agent, AttachChannelOptions::default())
        .await
        .unwrap();
    let answer = kit
        .channel_call(
            &agent,
            &room,
            "keep",
            ChannelCallOptions {
                arguments: [("a".to_owned(), json!("ok"))].into_iter().collect(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(answer, json!([null, "ok"]));
    let key = crate::turns::state_key(&agent);
    assert_eq!(
        kit.get_room(&room, GetRoomOptions::default())
            .await
            .unwrap()
            .metadata[&key],
        json!({"cursor":17})
    );
    assert!(
        kit.detach_channel(&room, &agent, DetachChannelOptions::default())
            .await
            .unwrap()
    );
    assert!(
        !kit.get_room(&room, GetRoomOptions::default())
            .await
            .unwrap()
            .metadata
            .contains_key(&key)
    );
    kit.release_channel(&agent).await.unwrap();
    assert_eq!(
        *events.lock().unwrap(),
        [
            (1, "event"),
            (1, "attach"),
            (1, "call"),
            (1, "detach"),
            (1, "close")
        ]
    );
    kit.close().await.unwrap();
    hosting.close().await.unwrap();
    executor.close().await;
}

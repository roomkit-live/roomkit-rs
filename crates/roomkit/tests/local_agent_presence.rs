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

/// A backend written outside the crate: each operation of the public
/// contract handed to the in-memory backend. Its heartbeat interval outlasts
/// the test, so the kit learns of an executor from its listener, never from
/// its next look.
struct OnTheContractAlone(roomkit::turns::InMemoryTurnBackend);

impl roomkit::turns::TurnBackend for OnTheContractAlone {
    fn submit(
        &self,
        request: roomkit::turns::TurnRequest,
    ) -> roomkit::turns::TurnFuture<
        '_,
        Result<std::sync::Arc<dyn roomkit::turns::TurnLink>, roomkit::RoomKitError>,
    > {
        roomkit::turns::TurnBackend::submit(&self.0, request)
    }
    fn serve(
        &self,
        executor: std::sync::Arc<dyn roomkit::turns::ServedExecutor>,
    ) -> roomkit::turns::TurnFuture<'_, Result<(), roomkit::RoomKitError>> {
        roomkit::turns::TurnBackend::serve(&self.0, executor)
    }
    fn control(
        &self,
        message: roomkit::turns::ChannelControl,
    ) -> roomkit::turns::TurnFuture<
        '_,
        Result<Option<roomkit::turns::ChannelResult>, roomkit::RoomKitError>,
    > {
        roomkit::turns::TurnBackend::control(&self.0, message)
    }
    fn attend(
        &self,
        owner: std::sync::Arc<dyn roomkit::turns::RoomOwner>,
    ) -> roomkit::turns::TurnFuture<'_, Result<(), roomkit::RoomKitError>> {
        roomkit::turns::TurnBackend::attend(&self.0, owner)
    }
    fn watch_executors(&self, listener: roomkit::turns::ExecutorsListener) {
        roomkit::turns::TurnBackend::watch_executors(&self.0, listener);
    }
    fn executors(
        &self,
    ) -> Option<
        roomkit::turns::TurnFuture<
            '_,
            Result<Vec<roomkit::turns::ExecutorInfo>, roomkit::RoomKitError>,
        >,
    > {
        roomkit::turns::TurnBackend::executors(&self.0)
    }
    fn timeouts(&self) -> Option<roomkit::TurnTimeouts> {
        Some(roomkit::TurnTimeouts {
            heartbeat_interval: std::time::Duration::from_secs(60),
            ..Default::default()
        })
    }
    fn close(&self) -> roomkit::turns::TurnFuture<'_, Result<(), roomkit::RoomKitError>> {
        roomkit::turns::TurnBackend::close(&self.0)
    }
}

// Python: test_an_agent_s_presence_is_published_on_attach_and_as_its_coverage_changes,
// its on-the-contract-alone case.
#[tokio::test]
async fn a_backend_on_the_contract_alone_has_presence_published_as_an_executor_is_served() {
    use roomkit::realtime::{
        EphemeralCallback, EphemeralEventType, InMemoryRealtime, RealtimeBackend,
    };
    use roomkit::turns::{InMemoryTurnBackend, TurnBackendHandle};
    use roomkit::{ChannelDeclaration, RoomKitOptions, TurnExecutor, TurnExecutorOptions};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Notify;

    let realtime = Arc::new(InMemoryRealtime::new(
        roomkit::realtime::InMemoryRealtimeOptions::default(),
    ));
    let backend = TurnBackendHandle::new(Arc::new(OnTheContractAlone(InMemoryTurnBackend::new())));
    let kit = RoomKit::new(RoomKitOptions {
        realtime: Some(Arc::<InMemoryRealtime>::clone(&realtime)),
        turn_backend: Some(backend.clone()),
        ..Default::default()
    });
    let room: roomkit::model::RoomId = "r1".parse().unwrap();
    let agent: roomkit::model::ChannelId = "coder".parse().unwrap();
    kit.register_channel(ChannelDeclaration {
        identity: Some(roomkit::AgentIdentity::default()),
        ..ChannelDeclaration::new(agent.clone())
    })
    .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    let online = Arc::new(Notify::new());
    let reached = Arc::clone(&online);
    let callback: EphemeralCallback = Arc::new(move |event| {
        let reached = Arc::clone(&reached);
        Box::pin(async move {
            if event.r#type == EphemeralEventType::PresenceOnline {
                reached.notify_one();
            }
        })
    });
    let _subscription = realtime.subscribe_to_room(&room, callback).await;
    kit.attach_channel(
        &room,
        &agent,
        AttachChannelOptions {
            category: Some(roomkit::model::ChannelCategory::Intelligence),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let executor = TurnExecutor::new(
        |id| async move {
            Ok(Some(EchoAgent::new(
                id,
                "late",
                EchoAgentOptions::default(),
            )))
        },
        TurnExecutorOptions {
            executor_id: Some("late".into()),
            hosts: Some(std::collections::BTreeSet::from(["coder".into()])),
            ..Default::default()
        },
    )
    .unwrap();

    backend.serve(Arc::new(executor.clone())).await.unwrap();
    let published = tokio::time::timeout(Duration::from_secs(5), online.notified()).await;

    kit.close().await.unwrap();
    executor.close().await;
    assert!(
        published.is_ok(),
        "serving an executor did not publish the agent online before the kit's next look"
    );
}

/// A backend written outside the crate that serves its executors itself,
/// by the public names alone: it lists them, the owner's own aside, and
/// tells its owner once it serves one and when one closes. It carries no
/// turn, and its heartbeat interval outlasts the test.
#[derive(Default)]
struct ServingItsOwnExecutors {
    served: std::sync::Mutex<Vec<std::sync::Arc<dyn roomkit::turns::ServedExecutor>>>,
    owner: std::sync::Mutex<Option<std::sync::Arc<dyn roomkit::turns::RoomOwner>>>,
    watchers: std::sync::Arc<roomkit::turns::ExecutorWatchers>,
}

impl roomkit::turns::TurnBackend for ServingItsOwnExecutors {
    fn submit(
        &self,
        _request: roomkit::turns::TurnRequest,
    ) -> roomkit::turns::TurnFuture<
        '_,
        Result<std::sync::Arc<dyn roomkit::turns::TurnLink>, roomkit::RoomKitError>,
    > {
        Box::pin(async {
            Err(roomkit::RoomKitError::Value(
                "this backend carries no turn".into(),
            ))
        })
    }
    fn serve(
        &self,
        executor: std::sync::Arc<dyn roomkit::turns::ServedExecutor>,
    ) -> roomkit::turns::TurnFuture<'_, Result<(), roomkit::RoomKitError>> {
        Box::pin(async move {
            let watchers = std::sync::Arc::downgrade(&self.watchers);
            executor.watch_closed(std::sync::Arc::new(move || {
                let watchers = watchers.upgrade()?;
                let told: roomkit::turns::TurnFuture<'static, ()> =
                    Box::pin(async move { watchers.changed().await });
                Some(told)
            }));
            held(&self.served).push(executor);
            self.watchers.changed().await;
            Ok(())
        })
    }
    fn control(
        &self,
        _message: roomkit::turns::ChannelControl,
    ) -> roomkit::turns::TurnFuture<
        '_,
        Result<Option<roomkit::turns::ChannelResult>, roomkit::RoomKitError>,
    > {
        Box::pin(async { Ok(None) })
    }
    fn attend(
        &self,
        owner: std::sync::Arc<dyn roomkit::turns::RoomOwner>,
    ) -> roomkit::turns::TurnFuture<'_, Result<(), roomkit::RoomKitError>> {
        *held(&self.owner) = Some(owner);
        Box::pin(async { Ok(()) })
    }
    fn watch_executors(&self, listener: roomkit::turns::ExecutorsListener) {
        self.watchers.watch(listener);
    }
    fn executors(
        &self,
    ) -> Option<
        roomkit::turns::TurnFuture<
            '_,
            Result<Vec<roomkit::turns::ExecutorInfo>, roomkit::RoomKitError>,
        >,
    > {
        let own = held(&self.owner).as_ref().and_then(|owner| {
            owner
                .executor()
                .map(|executor| executor.executor_id().to_owned())
        });
        let listed = held(&self.served)
            .iter()
            .filter(|executor| !executor.closed() && Some(executor.executor_id()) != own.as_deref())
            .map(|executor| {
                roomkit::turns::ExecutorInfo::new(
                    executor.executor_id(),
                    roomkit::turns::ExecutorInfoOptions {
                        hosts: executor.hosted(),
                        organizations: executor.organizations(),
                        room_hooks: executor.room_hooks(),
                        ..Default::default()
                    },
                )
            })
            .collect();
        Some(Box::pin(async move { Ok(listed) }))
    }
    fn timeouts(&self) -> Option<roomkit::TurnTimeouts> {
        Some(roomkit::TurnTimeouts {
            heartbeat_interval: std::time::Duration::from_secs(60),
            ..Default::default()
        })
    }
    fn close(&self) -> roomkit::turns::TurnFuture<'_, Result<(), roomkit::RoomKitError>> {
        Box::pin(async { Ok(()) })
    }
}

/// What `mutex` holds, a panic of another holder aside.
fn held<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

// Python: test_an_agent_s_presence_is_published_on_attach_and_as_its_coverage_changes,
// its serving-its-own-executors case.
#[tokio::test]
async fn a_backend_serving_its_own_executors_has_presence_published_as_one_closes() {
    use roomkit::realtime::{
        EphemeralCallback, EphemeralEventType, InMemoryRealtime, RealtimeBackend,
    };
    use roomkit::turns::TurnBackendHandle;
    use roomkit::{ChannelDeclaration, RoomKitOptions, TurnExecutor, TurnExecutorOptions};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Notify;

    let realtime = Arc::new(InMemoryRealtime::new(
        roomkit::realtime::InMemoryRealtimeOptions::default(),
    ));
    let backend = TurnBackendHandle::new(Arc::new(ServingItsOwnExecutors::default()));
    let kit = RoomKit::new(RoomKitOptions {
        realtime: Some(Arc::<InMemoryRealtime>::clone(&realtime)),
        turn_backend: Some(backend.clone()),
        ..Default::default()
    });
    let room: roomkit::model::RoomId = "r1".parse().unwrap();
    let agent: roomkit::model::ChannelId = "coder".parse().unwrap();
    kit.register_channel(ChannelDeclaration {
        identity: Some(roomkit::AgentIdentity::default()),
        ..ChannelDeclaration::new(agent.clone())
    })
    .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    let (online, offline) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let (went_online, went_offline) = (Arc::clone(&online), Arc::clone(&offline));
    let callback: EphemeralCallback = Arc::new(move |event| {
        let (went_online, went_offline) = (Arc::clone(&went_online), Arc::clone(&went_offline));
        Box::pin(async move {
            if event.r#type == EphemeralEventType::PresenceOnline {
                went_online.notify_one();
            } else if event.r#type == EphemeralEventType::PresenceOffline {
                went_offline.notify_one();
            }
        })
    });
    let _subscription = realtime.subscribe_to_room(&room, callback).await;
    let executor = TurnExecutor::new(
        |id| async move {
            Ok(Some(EchoAgent::new(
                id,
                "host",
                EchoAgentOptions::default(),
            )))
        },
        TurnExecutorOptions {
            executor_id: Some("host".into()),
            hosts: Some(std::collections::BTreeSet::from(["coder".into()])),
            ..Default::default()
        },
    )
    .unwrap();
    backend.serve(Arc::new(executor.clone())).await.unwrap();
    kit.attach_channel(
        &room,
        &agent,
        AttachChannelOptions {
            category: Some(roomkit::model::ChannelCategory::Intelligence),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let served = tokio::time::timeout(Duration::from_secs(5), online.notified()).await;

    executor.close().await;
    let closed = tokio::time::timeout(Duration::from_secs(5), offline.notified()).await;

    kit.close().await.unwrap();
    assert!(
        served.is_ok(),
        "the agent of a served executor was not published online at its attach"
    );
    assert!(
        closed.is_ok(),
        "an executor's close did not publish the agent offline before the kit's next look"
    );
}

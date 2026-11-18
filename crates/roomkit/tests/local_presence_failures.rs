//! Recoverable publication failures through the public local kit (RFC §8.4, §10.6.7).

#![expect(clippy::unwrap_used, reason = "a broken fixture fails its test")]
#[path = "local_presence_failures/backend.rs"]
mod backend;

use backend::{Backend, FailedPublication};
use roomkit::realtime::EphemeralEventType;
use roomkit::testing::{EchoAgent, EchoAgentOptions};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, PublishPresenceOptions, RoomKit, RoomKitError,
    RoomKitOptions,
};
use std::error::Error;
use std::sync::{Arc, atomic::Ordering};
use std::time::Duration;

// Python: test_an_explicit_presence_publication_preserves_the_backend_error_and_cause.
#[tokio::test]
async fn an_explicit_presence_preserves_the_original_backend_error_and_cause() {
    let backend = Arc::new(Backend::new(1, EphemeralEventType::PresenceOnline));
    let kit = RoomKit::new(RoomKitOptions {
        realtime: Some(Arc::<Backend>::clone(&backend)),
        ..Default::default()
    });
    let room = "r1".parse().unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room),
        ..Default::default()
    })
    .await
    .unwrap();
    let error = kit
        .publish_presence(
            &"r1".parse().unwrap(),
            "helper",
            "online",
            PublishPresenceOptions::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "realtime down");
    assert_eq!(error.source().unwrap().to_string(), "realtime down");
    assert_eq!(
        error.source().unwrap().source().unwrap().to_string(),
        "transport unavailable"
    );
    let RoomKitError::Realtime(failure) = error else {
        panic!("not a realtime failure");
    };
    let original = failure.into_inner();
    let classed = original.downcast_ref::<roomkit::ClassedError>().unwrap();
    assert_eq!(classed.class(), "ConnectionError");
    assert!(Arc::ptr_eq(
        &classed
            .error()
            .downcast_ref::<FailedPublication>()
            .unwrap()
            .identity,
        &backend.identity
    ));
    kit.close().await.unwrap();
}

// Python: test_a_live_presence_retries_repeated_publication_errors_once_per_attempt.
#[tokio::test]
async fn a_live_presence_retries_repeated_failures_and_delivers_once() {
    let backend = Arc::new(Backend::new(2, EphemeralEventType::PresenceOnline));
    let kit = RoomKit::new(RoomKitOptions {
        realtime: Some(Arc::<Backend>::clone(&backend)),
        ..Default::default()
    });
    let room = "r1".parse().unwrap();
    let channel = "helper".parse().unwrap();
    kit.register_channel(EchoAgent::new(channel, "hi", EchoAgentOptions::default()))
        .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room),
        ..Default::default()
    })
    .await
    .unwrap();
    let binding = kit
        .attach_channel(
            &"r1".parse().unwrap(),
            &"helper".parse().unwrap(),
            AttachChannelOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(binding.channel_id.as_str(), "helper");
    tokio::time::timeout(Duration::from_secs(10), backend.delivered.notified())
        .await
        .unwrap();
    roomkit::testing::idle(&kit).await;
    kit.close().await.unwrap();
    assert_eq!(backend.attempts.load(Ordering::SeqCst), 3);
    let published = backend.published.lock().unwrap();
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].r#type, EphemeralEventType::PresenceOnline);
    assert_eq!(published[0].user_id, "helper");
    assert_eq!(published[0].room_id.as_str(), "r1");
}

#[path = "local_presence_failures/channel.rs"]
mod channel;
#[path = "local_presence_failures/diagnostics.rs"]
mod diagnostics;

// Python: _acp_events._publish catches failure at DEBUG and later notifications proceed.
#[tokio::test]
async fn a_channel_notification_failure_is_logged_and_the_next_publication_succeeds() {
    use roomkit::realtime::EphemeralEvent;
    let (logs, _capturing) = diagnostics::Diagnostics::capture();
    let backend = Arc::new(Backend::new(1, EphemeralEventType::ToolCallStart));
    let kit = RoomKit::new(RoomKitOptions {
        realtime: Some(Arc::<Backend>::clone(&backend)),
        ..Default::default()
    });
    let room = "r1".parse().unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room),
        ..Default::default()
    })
    .await
    .unwrap();
    let publication = roomkit::channel_publication::ChannelPublication::default();
    kit.register_channel(channel::Publishing(publication.clone()))
        .unwrap();
    for kind in [
        EphemeralEventType::ToolCallStart,
        EphemeralEventType::ToolCallEnd,
    ] {
        publication
            .publish_outside(EphemeralEvent::new(
                "r1".parse().unwrap(),
                kind,
                "publisher",
            ))
            .await;
    }
    kit.close().await.unwrap();
    assert_eq!(backend.attempts.load(Ordering::SeqCst), 1);
    let published = backend.published.lock().unwrap();
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].r#type, EphemeralEventType::ToolCallEnd);
    let failures = logs.containing("publishing a channel notification failed");
    assert_eq!(failures.len(), 1);
    assert!(failures[0].starts_with("DEBUG "));
    assert!(failures[0].contains("realtime down"));
}

#[path = "local_presence_failures/held.rs"]
mod held;

// Python: test_closing_the_kit_cancels_an_inflight_presence_retry.
#[tokio::test]
async fn closing_the_kit_cancels_an_inflight_presence_retry() {
    let backend = Arc::new(held::HeldRetry::default());
    let kit = RoomKit::new(RoomKitOptions {
        realtime: Some(Arc::<held::HeldRetry>::clone(&backend)),
        ..Default::default()
    });
    kit.register_channel(EchoAgent::new(
        "helper".parse().unwrap(),
        "hi",
        EchoAgentOptions::default(),
    ))
    .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some("r1".parse().unwrap()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.attach_channel(
        &"r1".parse().unwrap(),
        &"helper".parse().unwrap(),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(2), backend.started.notified())
        .await
        .unwrap();
    let promptly = tokio::time::timeout(Duration::from_secs(2), kit.close()).await;
    kit.close().await.unwrap();
    assert!(
        promptly.is_ok(),
        "close waited for the stuck presence retry"
    );
    assert!(backend.cancelled.load(Ordering::SeqCst));
    assert_eq!(backend.attempts.load(Ordering::SeqCst), 2);
}

// Python: test_closing_the_kit_ends_an_attach_held_in_presence_publication.
#[tokio::test]
async fn closing_the_kit_ends_an_attach_held_in_presence_publication() {
    let backend = Arc::new(held::HeldRetry::default());
    // Skip the first-attempt refusal: hold the initial publication itself.
    backend.attempts.store(1, Ordering::SeqCst);
    let kit = RoomKit::new(RoomKitOptions {
        realtime: Some(Arc::<held::HeldRetry>::clone(&backend)),
        ..Default::default()
    });
    kit.register_channel(EchoAgent::new(
        "helper".parse().unwrap(),
        "hi",
        EchoAgentOptions::default(),
    ))
    .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some("r1".parse().unwrap()),
        ..Default::default()
    })
    .await
    .unwrap();
    let owner = kit.clone();
    let mut attaching = tokio::spawn(async move {
        owner
            .attach_channel(
                &"r1".parse().unwrap(),
                &"helper".parse().unwrap(),
                AttachChannelOptions::default(),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), backend.started.notified())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), kit.close())
        .await
        .unwrap()
        .unwrap();
    let ended = tokio::time::timeout(Duration::from_secs(2), &mut attaching).await;
    if ended.is_err() {
        attaching.abort();
        drop(attaching.await);
    }
    assert_eq!(
        ended.unwrap().unwrap().unwrap_err(),
        RoomKitError::KitClosed
    );
    assert!(backend.cancelled.load(Ordering::SeqCst));
}

#[path = "local_presence_failures/attach.rs"]
mod attach;
#[path = "local_presence_failures/lifecycle.rs"]
mod lifecycle;

#[cfg(feature = "redis")]
#[tokio::test]
async fn a_redis_owner_retries_a_reserved_failure_and_releases_only_unpublished_claims_on_close() {
    use roomkit::{RedisTurnBackend, RedisTurnBackendOptions, TurnTimeouts};
    let Ok(url) = std::env::var("REDIS_URL") else {
        return;
    };
    for delivered in [true, false] {
        let options = RedisTurnBackendOptions {
            key_prefix: format!("roomkit:test:presence-retry:{}", uuid::Uuid::new_v4()),
            timeouts: TurnTimeouts {
                heartbeat_interval: Duration::from_millis(20),
                ..Default::default()
            },
            ..Default::default()
        };
        let owner = RedisTurnBackend::new(Some(&url), options.clone()).unwrap();
        let peer = RedisTurnBackend::new(Some(&url), options).unwrap();
        let failures = if delivered { 2 } else { usize::MAX };
        let realtime = Arc::new(Backend::new(failures, EphemeralEventType::PresenceOnline));
        let kit = RoomKit::new(RoomKitOptions {
            turn_backend: Some(Arc::new(owner)),
            realtime: Some(Arc::<Backend>::clone(&realtime)),
            ..Default::default()
        });
        let room: roomkit::model::RoomId = "r".parse().unwrap();
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
        assert!(
            !peer
                .claim_presence("helper", "r", Some(true))
                .await
                .unwrap()
        );
        if delivered {
            tokio::time::timeout(Duration::from_secs(2), realtime.delivered.notified())
                .await
                .unwrap();
            assert_eq!(realtime.attempts.load(Ordering::SeqCst), 3);
            assert_eq!(realtime.published.lock().unwrap().len(), 1);
        }
        kit.close().await.unwrap();
        assert_eq!(
            peer.claim_presence("helper", "r", Some(true))
                .await
                .unwrap(),
            !delivered
        );
        peer.close().await.unwrap();
    }
}

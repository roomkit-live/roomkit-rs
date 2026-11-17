//! The call's grace, cancellation and reentrant close also cover its presence.

use super::attach::HeldAttach;
use super::*;
use roomkit::{DetachChannelOptions, DisableRoomHookOptions, EnableRoomHookOptions};

async fn kit(backend: &Arc<HeldAttach>) -> RoomKit {
    let kit = RoomKit::new(RoomKitOptions {
        realtime: Some(Arc::<HeldAttach>::clone(backend)),
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
    kit
}

async fn attach(kit: &RoomKit) -> Result<roomkit_core::model::ChannelBinding, RoomKitError> {
    kit.attach_channel(
        &"r1".parse().unwrap(),
        &"helper".parse().unwrap(),
        AttachChannelOptions::default(),
    )
    .await
}

// Python: test_an_initial_presence_publication_has_the_close_grace.
#[tokio::test]
async fn an_initial_presence_publication_has_the_close_grace() {
    let backend = Arc::new(HeldAttach::default());
    let kit = kit(&backend).await;
    let caller = kit.clone();
    let attaching = tokio::spawn(async move { attach(&caller).await });
    backend.started.notified().await;
    let owner = kit.clone();
    let closing = tokio::spawn(async move { owner.close().await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let held_grace = !closing.is_finished() && !attaching.is_finished();
    backend.release.notify_one();
    let bound = tokio::time::timeout(Duration::from_secs(2), attaching)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), closing)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(held_grace, "close did not grant the publication its grace");
    assert_eq!(bound.channel_id.as_str(), "helper");
    assert!(backend.completed.load(Ordering::SeqCst));
}

// Python: test_a_presence_backend_can_close_its_own_attach.
#[tokio::test]
async fn a_presence_backend_can_close_its_own_attach() {
    let backend = Arc::new(HeldAttach::default());
    let kit = kit(&backend).await;
    *backend.closing.lock().unwrap() = Some(kit.downgrade());
    let bound = tokio::time::timeout(Duration::from_secs(2), attach(&kit))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(bound.channel_id.as_str(), "helper");
    assert!(backend.completed.load(Ordering::SeqCst));
    kit.close().await.unwrap();
}

// Python: test_an_abandoned_presence_releases_the_gate_but_keeps_its_claim.
#[tokio::test]
async fn an_abandoned_presence_releases_the_gate_but_keeps_its_claim() {
    let backend = Arc::new(HeldAttach::default());
    let kit = kit(&backend).await;
    let caller = kit.clone();
    let attaching = tokio::spawn(async move { attach(&caller).await });
    backend.started.notified().await;
    attaching.abort();
    assert!(attaching.await.unwrap_err().is_cancelled());
    // Joining the aborted caller need not poll its already-aborted child.
    roomkit::testing::idle(&kit).await;
    assert!(backend.cancelled.load(Ordering::SeqCst));
    tokio::time::timeout(Duration::from_secs(2), attach(&kit))
        .await
        .unwrap()
        .unwrap();
    roomkit::testing::idle(&kit).await;
    assert_eq!(backend.attempts.load(Ordering::SeqCst), 1);
    assert!(
        kit.detach_channel(
            &"r1".parse().unwrap(),
            &"helper".parse().unwrap(),
            DetachChannelOptions::default()
        )
        .await
        .unwrap()
    );
    attach(&kit).await.unwrap();
    assert_eq!(backend.attempts.load(Ordering::SeqCst), 2);
    kit.close().await.unwrap();
}

// Python: test_closing_the_kit_ends_a_hook_change_held_in_presence_publication.
#[tokio::test]
async fn closing_the_kit_ends_a_hook_change_held_in_presence_publication() {
    for enabling in [true, false] {
        let backend = Arc::new(HeldAttach::default());
        *backend.kind.lock().unwrap() = Some(if enabling {
            EphemeralEventType::PresenceOffline
        } else {
            EphemeralEventType::PresenceOnline
        });
        let kit = kit(&backend).await;
        let room = "r1".parse().unwrap();
        if !enabling {
            kit.enable_room_hook(&room, "unavailable", EnableRoomHookOptions::default())
                .await
                .unwrap();
        }
        attach(&kit).await.unwrap();
        let owner = kit.clone();
        let mut changing = tokio::spawn(async move {
            if enabling {
                owner
                    .enable_room_hook(&room, "unavailable", EnableRoomHookOptions::default())
                    .await
            } else {
                owner
                    .disable_room_hook(&room, "unavailable", DisableRoomHookOptions::default())
                    .await
                    .map(drop)
            }
        });
        tokio::time::timeout(Duration::from_secs(2), backend.started.notified())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), kit.close())
            .await
            .unwrap()
            .unwrap();
        let ended = tokio::time::timeout(Duration::from_secs(2), &mut changing).await;
        if ended.is_err() {
            changing.abort();
            drop(changing.await);
        }
        assert_eq!(
            ended.unwrap().unwrap().unwrap_err(),
            RoomKitError::KitClosed
        );
        assert!(backend.cancelled.load(Ordering::SeqCst));
    }
}

// Python: test_close_ends_a_detach_waiting_on_the_presence_gate.
#[tokio::test]
async fn close_ends_a_detach_waiting_on_the_presence_gate() {
    let backend = Arc::new(HeldAttach::default());
    let kit = kit(&backend).await;
    let removed = Arc::new(tokio::sync::Notify::new());
    let announced = Arc::clone(&removed);
    kit.on("room_channel_detached", move |_| {
        let announced = Arc::clone(&announced);
        async move {
            announced.notify_one();
        }
    })
    .unwrap();
    let caller = kit.clone();
    let attaching = tokio::spawn(async move { attach(&caller).await });
    tokio::time::timeout(Duration::from_secs(2), backend.started.notified())
        .await
        .unwrap();
    let owner = kit.clone();
    let detaching = tokio::spawn(async move {
        owner
            .detach_channel(
                &"r1".parse().unwrap(),
                &"helper".parse().unwrap(),
                DetachChannelOptions::default(),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), removed.notified())
        .await
        .unwrap();
    assert!(!detaching.is_finished());
    tokio::time::timeout(Duration::from_secs(10), kit.close())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        attaching.await.unwrap().unwrap_err(),
        RoomKitError::KitClosed
    );
    assert_eq!(
        detaching.await.unwrap().unwrap_err(),
        RoomKitError::KitClosed
    );
    assert!(backend.cancelled.load(Ordering::SeqCst));
}

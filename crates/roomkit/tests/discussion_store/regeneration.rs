//! Python: `test_discussion_regeneration_lifecycle.py`. Gates distinguish the
//! initial read, locked selection and the fresh read after selection.

use super::*;
use roomkit::model::hook::AllowOptions;
use roomkit::{RegenerateResponseOptions, RegenerateTargetOptions, RoomLockManager};

#[derive(Default)]
pub(super) struct ReadGate {
    left: std::sync::Mutex<u32>,
    reached: Notify,
    opened: Notify,
    cancelled: AtomicBool,
}

impl ReadGate {
    fn arm(&self, nth: u32) {
        self.cancelled.store(false, Ordering::SeqCst);
        *self.left.lock().unwrap() = nth;
    }

    pub(super) async fn pass(&self) {
        struct Cancelled<'a>(&'a AtomicBool, bool);
        impl Drop for Cancelled<'_> {
            fn drop(&mut self) {
                self.0.store(self.1, Ordering::SeqCst);
            }
        }
        let hold = {
            let mut left = self.left.lock().unwrap();
            let hold = *left == 1;
            *left = left.saturating_sub(1);
            hold
        };
        if !hold {
            return;
        }
        let mut guard = Cancelled(&self.cancelled, true);
        self.reached.notify_one();
        self.opened.notified().await;
        guard.1 = false;
    }
}

/// A stored discussion followed here, with no local owner to race the
/// store gates. Its trigger predates the discussion, as after a restart.
async fn followed() -> (
    RoomKit<FlakyStore>,
    Arc<FlakyStore>,
    Arc<dyn RoomLockManager>,
) {
    followed_with(Arc::new(roomkit::InMemoryLockManager::default())).await
}

async fn followed_with(
    locks: Arc<dyn RoomLockManager>,
) -> (
    RoomKit<FlakyStore>,
    Arc<FlakyStore>,
    Arc<dyn RoomLockManager>,
) {
    let store = Arc::new(FlakyStore::default());
    let kit = RoomKit::new(RoomKitOptions {
        store: Arc::clone(&store),
        lock_manager: Some(Arc::clone(&locks)),
        ..RoomKitOptions::with_store(FlakyStore::default())
    });
    let room = RoomId::new("r1").unwrap();
    kit.register_channel(RecordingTransport::new(
        id("ops"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.attach_channel(&room, &id("ops"), AttachChannelOptions::default())
        .await
        .unwrap();
    let mut message = hello();
    message.addressed_to = Some(vec![id("a")]);
    kit.process_inbound(
        message,
        ProcessInboundOptions {
            room_id: Some(room.clone()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    idle(&kit).await;
    store
        .patch_room_metadata(
            &room,
            Metadata::from_iter([(
                "_discussion".to_owned(),
                serde_json::json!({"agents": ["a"], "max_depth": 5}),
            )]),
            store::PatchRoomMetadataOptions::default(),
        )
        .await
        .unwrap();
    (kit, store, locks)
}

fn regenerating(
    kit: &RoomKit<FlakyStore>,
) -> tokio::task::JoinHandle<Result<Option<roomkit::InboundResult>, roomkit::RoomKitError>> {
    let kit = kit.clone();
    tokio::spawn(async move {
        kit.regenerate_response(
            &RoomId::new("r1").unwrap(),
            RegenerateResponseOptions::default(),
        )
        .await
    })
}

// Initial read is pre-selection; close cancels it after its grace.
#[tokio::test(start_paused = true)]
async fn close_cuts_the_initial_discussion_read() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let (kit, store, _) = followed().await;
        store.room_read.arm(1);
        let run = regenerating(&kit);
        store.room_read.reached.notified().await;
        kit.close().await.unwrap();
        let result = run.await.unwrap().unwrap().unwrap();
        assert!(store.room_read.cancelled.load(Ordering::SeqCst));
        assert!(result.blocked);
        assert_eq!(result.reason.as_deref(), Some("kit_closed"));
        assert_eq!(result.cancellation_reason.as_deref(), Some("kit_closed"));
    })
    .await
    .unwrap();
}

// Selection is over, even though the fresh stored-over read is unfinished.
#[tokio::test(start_paused = true)]
async fn close_does_not_cut_regeneration_after_selection() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let (kit, store, _) = followed().await;
        store.room_read.arm(4);
        let run = regenerating(&kit);
        store.room_read.reached.notified().await;
        kit.close().await.unwrap();
        assert!(!run.is_finished());
        assert!(!store.room_read.cancelled.load(Ordering::SeqCst));
        store.room_read.opened.notify_one();
        let result = run.await.unwrap().unwrap().unwrap();
        assert!(result.event.is_some());
        assert!(!result.blocked);
        assert!(result.error.is_none());
        assert!(result.cancellation_reason.is_none());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn uninstall_after_selection_keeps_the_original_discussion() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let (kit, store, _) = followed().await;
        store.room_read.arm(4);
        let run = regenerating(&kit);
        store.room_read.reached.notified().await;
        kit.uninstall_strategy(
            &RoomId::new("r1").unwrap(),
            UninstallStrategyOptions::default(),
        )
        .await
        .unwrap();
        store.room_read.opened.notify_one();
        let result = run.await.unwrap().unwrap().unwrap();
        assert!(result.event.is_some());
        assert!(!result.blocked);
        assert!(result.error.is_none());
        kit.close().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn another_kit_uninstalling_before_queueing_announces_no_queued_turn() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let (kit, store, locks) = followed().await;
        let other = RoomKit::new(RoomKitOptions {
            store: Arc::clone(&store),
            lock_manager: Some(locks),
            ..RoomKitOptions::with_store(FlakyStore::default())
        });
        let queued = Arc::new(AtomicU32::new(0));
        let seen = Arc::clone(&queued);
        kit.hook(
            HookTrigger::OnSpeakQueue,
            move |event: SpeakQueueEvent, _: RoomContext| {
                if event.change == SpeakQueueChange::Queued {
                    seen.fetch_add(1, Ordering::SeqCst);
                }
                async {}
            },
            HookOptions::default(),
        )
        .unwrap();
        store.room_read.arm(4);
        let run = regenerating(&kit);
        store.room_read.reached.notified().await;
        other
            .uninstall_strategy(
                &RoomId::new("r1").unwrap(),
                UninstallStrategyOptions::default(),
            )
            .await
            .unwrap();
        store.room_read.opened.notify_one();
        let result = run.await.unwrap().unwrap().unwrap();
        idle(&kit).await;
        assert!(result.event.is_some());
        assert!(!result.blocked);
        assert!(result.error.is_none());
        assert_eq!(queued.load(Ordering::SeqCst), 0);
        kit.close().await.unwrap();
        other.close().await.unwrap();
    })
    .await
    .unwrap();
}

#[path = "release.rs"]
mod release;

#[tokio::test]
async fn regeneration_from_a_locked_hook_uses_the_existing_lease() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let (kit, _, _) = followed().await;
        let expected = kit
            .regenerate_target(
                &RoomId::new("r1").unwrap(),
                RegenerateTargetOptions::default(),
            )
            .await
            .unwrap()
            .unwrap()
            .id;
        let weak = kit.downgrade();
        let selected = Arc::new(AtomicU32::new(0));
        let seen = Arc::clone(&selected);
        kit.hook(
            HookTrigger::BeforeBroadcast,
            move |_event: RoomEvent, _context: RoomContext| {
                let (kit, seen, expected) =
                    (weak.upgrade().unwrap(), Arc::clone(&seen), expected.clone());
                async move {
                    let result = kit
                        .regenerate_response(
                            &RoomId::new("r1").unwrap(),
                            RegenerateResponseOptions::default(),
                        )
                        .await
                        .unwrap()
                        .unwrap();
                    assert_eq!(result.event.unwrap().id, expected);
                    assert_eq!(result.response_events, Vec::<RoomEvent>::new());
                    seen.fetch_add(1, Ordering::SeqCst);
                    HookResult::allow(AllowOptions::default())
                }
            },
            HookOptions::default(),
        )
        .unwrap();
        let mut message = say("a later message");
        message.addressed_to = Some(vec![]);
        kit.process_inbound(
            message,
            ProcessInboundOptions {
                room_id: Some(RoomId::new("r1").unwrap()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        idle(&kit).await;
        assert_eq!(selected.load(Ordering::SeqCst), 1);
        kit.close().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn dropping_the_caller_cancels_the_read_after_selection() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let (kit, store, _) = followed().await;
        store.room_read.arm(4);
        let run = regenerating(&kit);
        store.room_read.reached.notified().await;
        run.abort();
        assert!(run.await.unwrap_err().is_cancelled());
        assert!(store.room_read.cancelled.load(Ordering::SeqCst));
        let room = store
            .get_room(&RoomId::new("r1").unwrap())
            .await
            .unwrap()
            .unwrap();
        assert!(room.metadata.get("_speak_queue").is_none());
        kit.close().await.unwrap();
    })
    .await
    .unwrap();
}

//! A channel's final framework callback and its explicitly retained cleanup.
//! ProtocolTrace/SIP adapters are outside this core ownership test.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit_core::model::{ChannelId, ChannelType, EventSource, RoomEvent, RoomId, TextContent};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use super::shutdown_contracts::Witness;
use crate::tasks::Kind;
use crate::{
    ConversationStore, CreateRoomOptions, FrameworkEvent, RoomKit, RoomKitError, RoomKitOptions,
};

#[derive(Clone, Default)]
struct Trace {
    steps: Arc<Mutex<Vec<(&'static str, bool)>>>,
    pending: Arc<Notify>,
    cleanup: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

struct End {
    kit: RoomKit,
    trace: Trace,
    how: &'static str,
}

impl End {
    fn note(&self, what: &'static str) {
        self.trace
            .steps
            .lock()
            .unwrap()
            .push((what, self.kit.inner.store_released.load(Ordering::Acquire)));
    }

    fn release_refused_run(&self) {
        let end = Terminal {
            kit: self.kit.clone(),
            trace: self.trace.clone(),
        };
        // Already cut: the retained obligation ends even though
        // the new async body is never polled.
        assert!(
            self.kit
                .inner
                .tasks
                .spawn(Kind::Turn, async move {
                    let _end = end;
                    std::future::pending::<()>().await;
                })
                .is_none()
        );
    }

    fn start_commit_cleanup(&self) {
        let (kit, trace) = (self.kit.clone(), self.trace.clone());
        // A framework finalizer retains its obligation before
        // transferring the async cleanup. The real shutdown's
        // last unwind must wait for that obligation to end.
        let held = kit.inner.tasks.track_unabortable(Kind::Write);
        let cleanup = tokio::spawn(async move {
            let _held = held;
            tokio::task::yield_now().await;
            let room = RoomId::new("r").unwrap();
            let event = RoomEvent::new(
                room.clone(),
                EventSource::new(ChannelId::new("witness").unwrap(), ChannelType::WEBSOCKET),
                TextContent::new("record"),
            );
            assert_eq!(
                kit.commit_event(&room, event, crate::CommitEventOptions::default())
                    .await
                    .unwrap_err(),
                RoomKitError::KitClosed
            );
            trace.steps.lock().unwrap().push((
                "commit refused",
                kit.inner.store_released.load(Ordering::Acquire),
            ));
        });
        self.trace.cleanup.lock().unwrap().push(cleanup);
    }
}

impl Drop for End {
    fn drop(&mut self) {
        self.note("hook ended");
        match self.how {
            "holds-run" => self.release_refused_run(),
            "commits-unawaited" => self.start_commit_cleanup(),
            "ends" => (),
            _ => unreachable!(),
        }
    }
}

struct Terminal {
    kit: RoomKit,
    trace: Trace,
}
impl Drop for Terminal {
    fn drop(&mut self) {
        self.trace.steps.lock().unwrap().push((
            "run ended",
            self.kit.inner.store_released.load(Ordering::Acquire),
        ));
    }
}

fn observer(kit: &RoomKit, trace: &Trace, how: &'static str) {
    let (weak, trace) = (kit.downgrade(), trace.clone());
    kit.on("channel_teardown", move |_| {
        let (kit, trace) = (weak.upgrade().unwrap(), trace.clone());
        async move {
            let end = End {
                kit,
                trace: trace.clone(),
                how,
            };
            end.note("hook started");
            trace.pending.notify_one();
            std::future::pending::<()>().await;
        }
    })
    .unwrap();
}

fn final_callback_channel(kit: &RoomKit, trace: &Trace) -> Witness {
    let (weak, pending) = (kit.downgrade(), Arc::clone(&trace.pending));
    Witness {
        action: Some(Arc::new(move || {
            let (kit, pending) = (weak.upgrade().unwrap(), Arc::clone(&pending));
            Box::pin(async move {
                let handled = kit
                    .inner
                    .events
                    .emit(&kit.inner.tasks, FrameworkEvent::new("channel_teardown"));
                assert!(
                    handled.is_some(),
                    "the channel's final observer must be admitted"
                );
                tokio::time::timeout(Duration::from_secs(1), pending.notified())
                    .await
                    .unwrap();
                assert_eq!(
                    kit.create_room(CreateRoomOptions::default())
                        .await
                        .unwrap_err(),
                    RoomKitError::KitClosed
                );
                assert!(kit.inner.tasks.spawn(Kind::Write, async {}).is_none());
            })
        })),
        ..Witness::default()
    }
}

async fn closing_callback(how: &'static str) {
    let kit = RoomKit::new(RoomKitOptions::default());
    let room = RoomId::new("r").unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..CreateRoomOptions::default()
    })
    .await
    .unwrap();
    let trace = Trace::default();
    observer(&kit, &trace, how);
    let witness = final_callback_channel(&kit, &trace);
    kit.register_channel(witness.clone()).unwrap();
    tokio::time::timeout(Duration::from_secs(8), kit.close())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(witness.closed.load(Ordering::SeqCst), 1);
    let mut expected = vec![("hook started", false), ("hook ended", false)];
    if how == "holds-run" {
        expected.push(("run ended", false));
    }
    if how == "commits-unawaited" {
        expected.push(("commit refused", false));
    }
    assert_eq!(*trace.steps.lock().unwrap(), expected);
    let cleanup = std::mem::take(&mut *trace.cleanup.lock().unwrap());
    for handle in cleanup {
        assert!(handle.is_finished());
        handle.await.unwrap();
    }
    assert_eq!(kit.inner.tasks.left(), Vec::<&str>::new());
    assert!(kit.inner.store_released.load(Ordering::Acquire));
    assert_eq!(
        kit.store()
            .list_events(&room, crate::store::ListEventsOptions::default())
            .await
            .unwrap(),
        Vec::<RoomEvent>::new()
    );
}

// Python: tests/test_kit_close_doors.py::test_a_hook_task_started_as_a_channel_closes_ends_with_the_close
// The generic callback/ownership contract. Async cleanup is transferred
// explicitly; this is neither an automatic async finally nor a SIP test.
#[tokio::test(start_paused = true)]
async fn a_channels_final_observer_and_its_cleanup_end_before_store_release() {
    for how in ["ends", "holds-run", "commits-unawaited"] {
        closing_callback(how).await;
    }
}

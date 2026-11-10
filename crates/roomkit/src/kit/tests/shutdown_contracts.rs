//! Native ownership seams of the real shutdown, beyond the scenario DSL.

use std::future::pending;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit_core::model::{
    ChannelBinding, ChannelId, ChannelOutput, ChannelType, InboundMessage, RoomContext, RoomEvent,
    RoomId, TextContent,
};
use roomkit_core::rules::hook_registry::HookTrigger;
use tokio::sync::{Notify, oneshot};

use crate::channel::BoxFuture;
use crate::tasks::{Kind, Phase};
use crate::{
    Channel, ChannelError, CreateRoomOptions, HookExecution, HookOptions, ProcessInboundOptions,
    RoomKit, RoomKitOptions,
};

const BOUND: Duration = Duration::from_secs(8);
type Closing = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;

#[derive(Clone, Default)]
pub(super) struct Witness {
    pub(super) closed: Arc<AtomicUsize>,
    pub(super) action: Option<Closing>,
}

impl Channel for Witness {
    fn channel_id(&self) -> ChannelId {
        ChannelId::new("witness").unwrap()
    }
    fn channel_type(&self) -> ChannelType {
        ChannelType::WEBSOCKET
    }
    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        std::future::ready(Ok(crate::default_inbound_event(self, &message, &context)))
    }
    fn deliver(
        &self,
        _: RoomEvent,
        _: ChannelBinding,
        _: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        std::future::ready(Ok(ChannelOutput::empty()))
    }
    async fn close(&self) {
        if let Some(action) = &self.action {
            action().await;
        }
        self.closed.fetch_add(1, Ordering::SeqCst);
    }
}

struct Released(Arc<AtomicUsize>);
impl Drop for Released {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

// Python: tests/test_kit_close_doors.py::test_a_close_from_the_kit_s_own_work_returns_and_closes_the_kit
// Python: tests/test_kit_close_doors.py::test_a_close_from_the_kit_s_own_work_joining_another_spares_that_work
// The private held-write door: an actual Write task beneath a held parent.
#[tokio::test(start_paused = true)]
async fn a_held_write_owns_or_joins_shutdown_without_waiting_for_its_parent() {
    for joining in [false, true] {
        let kit = RoomKit::new(RoomKitOptions::default());
        let witness = Witness::default();
        kit.register_channel(witness.clone()).unwrap();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let parent = kit
            .inner
            .tasks
            .spawn(Kind::Observer, {
                let (kit, witness, entered, release) = (
                    kit.clone(),
                    witness.clone(),
                    Arc::clone(&entered),
                    Arc::clone(&release),
                );
                async move {
                    let writer = kit
                        .inner
                        .tasks
                        .spawn(Kind::Write, {
                            let kit = kit.clone();
                            async move {
                                entered.notify_one();
                                if joining {
                                    release.notified().await;
                                }
                                kit.close().await.unwrap();
                                assert_eq!(witness.closed.load(Ordering::SeqCst), 1);
                            }
                        })
                        .unwrap();
                    writer.await.unwrap();
                }
            })
            .unwrap();
        tokio::time::timeout(BOUND, entered.notified())
            .await
            .unwrap();
        if joining {
            let mut closer = tokio::spawn({
                let kit = kit.clone();
                async move { kit.close().await }
            });
            assert!(
                tokio::time::timeout(Duration::from_millis(50), &mut closer)
                    .await
                    .is_err()
            );
            release.notify_one();
            tokio::time::timeout(BOUND, closer)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
        tokio::time::timeout(BOUND, parent).await.unwrap().unwrap();
        assert!(kit.inner.tasks.is_sealed());
        assert_eq!(kit.inner.tasks.left(), Vec::<&str>::new());
    }
}

// Python: tests/test_kit_close_doors.py::test_a_background_run_started_in_the_close_s_grace_ends_with_its_close
#[tokio::test(start_paused = true)]
async fn an_admitted_observer_can_start_held_work_during_grace_and_close_cuts_it() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let witness = Witness::default();
    kit.register_channel(witness.clone()).unwrap();
    let room = RoomId::new("r").unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..CreateRoomOptions::default()
    })
    .await
    .unwrap();
    kit.attach_channel(
        &room,
        &witness.channel_id(),
        crate::AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    let entered = Arc::new(Notify::new());
    let ended = Arc::new(AtomicUsize::new(0));
    let (sent, heard) = oneshot::channel();
    let sent = Arc::new(Mutex::new(Some(sent)));
    let weak = kit.downgrade();
    let (entering, ending) = (Arc::clone(&entered), Arc::clone(&ended));
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |_: RoomEvent, _| {
            let (weak, entered, ended, sent) = (
                weak.clone(),
                Arc::clone(&entering),
                Arc::clone(&ending),
                Arc::clone(&sent),
            );
            async move {
                let kit = weak.upgrade().unwrap();
                let mut phases = kit.inner.tasks.phases();
                entered.notify_one();
                phases
                    .wait_for(|phase| *phase == Phase::Closing)
                    .await
                    .unwrap();
                let (started, running) = oneshot::channel();
                let released = Released(ended);
                let child = kit
                    .inner
                    .tasks
                    .spawn(Kind::Turn, async move {
                        let _released = released;
                        started.send(()).unwrap();
                        pending::<()>().await;
                    })
                    .unwrap();
                running.await.unwrap();
                sent.lock().unwrap().take().unwrap().send(child).unwrap();
            }
        },
        HookOptions {
            execution: HookExecution::Async,
            ..HookOptions::default()
        },
    )
    .unwrap();
    kit.process_inbound(
        InboundMessage::new(witness.channel_id(), "u", TextContent::new("Hi.")),
        ProcessInboundOptions {
            room_id: Some(room),
            ..ProcessInboundOptions::default()
        },
    )
    .await
    .unwrap();
    tokio::time::timeout(BOUND, entered.notified())
        .await
        .unwrap();
    tokio::time::timeout(BOUND, kit.close())
        .await
        .unwrap()
        .unwrap();
    let child = tokio::time::timeout(BOUND, heard).await.unwrap().unwrap();
    assert!(
        tokio::time::timeout(BOUND, child)
            .await
            .unwrap()
            .unwrap_err()
            .is_cancelled()
    );
    assert_eq!(ended.load(Ordering::SeqCst), 1);
    assert_eq!(kit.inner.tasks.left(), Vec::<&str>::new());
}

/// The obligation is acquired before admission, so refusal releases it
/// even when Rust never polls the async body (no eager/finally claim).
fn held(kit: &RoomKit, starts: Arc<AtomicBool>, ends: Arc<AtomicUsize>) -> bool {
    let release = Released(ends);
    kit.inner
        .tasks
        .spawn(Kind::Turn, async move {
            let _release = release;
            starts.store(true, Ordering::SeqCst);
            pending::<()>().await;
        })
        .is_some()
}

// Python: tests/test_kit_close_doors.py::test_a_run_held_around_the_close_is_cut_with_its_end_run
#[tokio::test(start_paused = true)]
async fn late_held_work_releases_its_obligation_even_when_admission_refuses_it() {
    for when in ["before-first-poll", "channel-close", "sealed"] {
        let kit = RoomKit::new(RoomKitOptions::default());
        let starts = Arc::new(AtomicBool::new(false));
        let ends = Arc::new(AtomicUsize::new(0));
        if when == "channel-close" {
            let (weak, starts, ends) = (kit.downgrade(), Arc::clone(&starts), Arc::clone(&ends));
            kit.register_channel(Witness {
                action: Some(Arc::new(move || {
                    let (kit, starts, ends) = (
                        weak.upgrade().unwrap(),
                        Arc::clone(&starts),
                        Arc::clone(&ends),
                    );
                    Box::pin(async move {
                        assert!(!held(&kit, starts, ends));
                    })
                })),
                ..Witness::default()
            })
            .unwrap();
        } else if when == "before-first-poll" {
            assert!(held(&kit, Arc::clone(&starts), Arc::clone(&ends)));
            assert!(!starts.load(Ordering::SeqCst));
        }
        tokio::time::timeout(BOUND, kit.close())
            .await
            .unwrap()
            .unwrap();
        if when == "sealed" {
            assert!(!held(&kit, Arc::clone(&starts), Arc::clone(&ends)));
        }
        assert_eq!(ends.load(Ordering::SeqCst), 1, "{when}");
        if when != "before-first-poll" {
            assert!(!starts.load(Ordering::SeqCst));
        }
        assert!(kit.inner.tasks.is_sealed());
        assert_eq!(kit.inner.tasks.left(), Vec::<&str>::new());
    }
}

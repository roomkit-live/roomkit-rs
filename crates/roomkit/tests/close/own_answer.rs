//! A closing observer's own answer, placed but not yet given a delivery claim.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{RoomEvent, TextContent};
use roomkit::{HookExecution, HookOptions, HookTrigger, SendEventOptions};
use roomkit_core::rules::transcoding::text_of;
use tokio::sync::Notify;

use crate::held::{Holding, into, kit_of, room, said, stored};
use crate::store::StuckStore;
use crate::support::channel;
use crate::warnings::Warnings;

// Python: tests/test_kit_close_doors.py::test_a_close_from_the_lane_s_own_work_drops_what_it_kept_and_the_lane_ends
// Hold the answer before its delivery claim, so "the lane still holds it"
// is a witnessed premise even when another worker runs its driver.
async fn retained_answer() {
    let target = Holding::new("ws", []);
    let kit = kit_of(StuckStore::default(), &[&target], &["r"]).await;
    let returned = Arc::new(Notify::new());
    let (weak, completed) = (kit.downgrade(), Arc::clone(&returned));
    kit.hook(
        HookTrigger::AfterBroadcast,
        move |event: RoomEvent, _| {
            let (weak, completed) = (weak.clone(), Arc::clone(&completed));
            async move {
                if text_of(&event.content) != "Hi." {
                    return;
                }
                let kit = weak.upgrade().unwrap();
                *kit.store().held_read.lock().unwrap() = Some("cursor");
                let answer = kit
                    .send_event(
                        &room("r"),
                        &channel("sms"),
                        TextContent::new("Hello."),
                        SendEventOptions::default(),
                    )
                    .await
                    .unwrap();
                assert_eq!(answer.index, event.index + 1);
                tokio::time::timeout(Duration::from_secs(1), kit.store().reading.notified())
                    .await
                    .unwrap();
                kit.close().await.unwrap();
                completed.notify_one();
            }
        },
        HookOptions {
            execution: HookExecution::Async,
            ..HookOptions::default()
        },
    )
    .unwrap();
    kit.process_inbound(said("Hi."), into("r")).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), returned.notified())
        .await
        .unwrap();
    // A close that merely spares a run forever is not a dropped answer.
    tokio::time::timeout(Duration::from_secs(1), roomkit::testing::idle(&kit))
        .await
        .unwrap();
    let reads = kit.store().reads_after_close.load(Ordering::SeqCst);
    *kit.store().held_read.lock().unwrap() = None;
    kit.store().release_read.notify_one();
    tokio::time::timeout(Duration::from_secs(1), roomkit::testing::idle(&kit))
        .await
        .unwrap();
    assert_eq!(kit.store().reads_after_close.load(Ordering::SeqCst), reads);
    assert_eq!(target.sent(), ["Hi."]);
    assert_eq!(stored(&kit, "r").await, ["Hi.", "Hello."]);
}

#[tokio::test(start_paused = true)]
async fn close_drops_its_observers_own_answer_held_before_the_cursor_claim() {
    let (logs, _capture) = Warnings::capture();
    retained_answer().await;
    assert_eq!(logs.containing("failed a round"), Vec::<String>::new());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn close_drops_its_observers_own_held_answer_on_four_workers() {
    retained_answer().await;
}

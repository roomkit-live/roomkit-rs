//! RFC §12.10.4: an observer closes a lane whose first cursor read suspends.

use std::sync::Arc;
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::RoomEvent;
use roomkit::{HookExecution, HookOptions, HookTrigger, ProcessInboundOptions};
use roomkit_core::rules::transcoding::text_of;
use tokio::sync::Notify;

use crate::held::{Hold, Holding, into, kit_of, said};
use crate::store::StuckStore;

// Python: tests/test_kit_close_doors.py::test_an_observer_closing_the_kit_finds_a_set_placed_in_an_idle_lane_running
// The slow-cursor store, both idle and busy; SQL stores have separate gates.
#[tokio::test(start_paused = true)]
async fn an_observer_closing_over_a_slow_cursor_preserves_only_the_first_place() {
    for busy in [false, true] {
        let target = Holding::new("ws", [("hold", Hold::ForEver)]);
        let kit = kit_of(StuckStore::default(), &[&target], &["r"]).await;
        *kit.store().delayed_read.lock().unwrap() = Some(("cursor", Duration::from_millis(100)));
        let held = if busy {
            let result = kit
                .process_inbound(
                    said("hold"),
                    ProcessInboundOptions {
                        defer_delivery: true,
                        ..into("r")
                    },
                )
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(1), target.entered("hold"))
                .await
                .unwrap();
            result.delivery
        } else {
            None
        };
        let returned = Arc::new(Notify::new());
        let (weak, closed) = (kit.downgrade(), Arc::clone(&returned));
        kit.hook(
            HookTrigger::BeforeBroadcast,
            move |event: RoomEvent, _| {
                let (weak, closed) = (weak.clone(), Arc::clone(&closed));
                async move {
                    if text_of(&event.content) == "Bye." {
                        weak.upgrade().unwrap().close().await.unwrap();
                        closed.notify_one();
                    }
                }
            },
            HookOptions {
                execution: HookExecution::Async,
                ..HookOptions::default()
            },
        )
        .unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(7),
            kit.process_inbound(said("Bye."), into("r")),
        )
        .await
        .unwrap()
        .unwrap();
        tokio::time::timeout(Duration::from_secs(7), returned.notified())
            .await
            .unwrap();
        if let Some(held) = held {
            tokio::time::timeout(Duration::from_secs(1), held.wait())
                .await
                .unwrap()
                .unwrap();
        }
        assert_eq!(result.cancellation_reason.as_deref(), Some("kit_closed"));
        assert_eq!(result.error, None);
        assert_eq!(result.delivery_results.len(), usize::from(!busy));
        if !busy {
            assert_eq!(
                result
                    .delivery_results
                    .get(&crate::support::channel("ws"))
                    .unwrap()
                    .status,
                roomkit::model::DeliveryResultStatus::Sent
            );
        }
        assert_eq!(
            target.sent(),
            if busy {
                vec![]
            } else {
                vec!["Bye.".to_owned()]
            }
        );
        assert_eq!(*kit.store().delayed_read.lock().unwrap(), None);
        tokio::time::timeout(Duration::from_secs(1), roomkit::testing::idle(&kit))
            .await
            .unwrap();
    }
}

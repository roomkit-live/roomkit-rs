//! RFC §10.6.8: a busy turn is never lost. The executor beats from a task
//! of its own, so a tool that blocks its thread (a synchronous call, a
//! CPU-bound job) past `heartbeat_timeout` leaves the turn to end as it
//! would have, on a runtime of several threads, as `#[tokio::main]` runs.
//!
//! The block is a real one, so the bounds are a few milliseconds: a beat
//! every 5 ms, 25 ms of silence lost, a tool blocking 150 ms.

use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::EventType;
use roomkit::{ConversationStore, TurnTimeouts};
use roomkit_ai::ToolOutput;

use crate::support::{asks, channel, kit, received, room};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tool_blocking_its_thread_past_the_heartbeat_timeout_ends_normally() {
    let kit = kit(|_call, _context| async {
        std::thread::sleep(Duration::from_millis(150));
        Ok(ToolOutput::Text("42".to_owned()))
    })
    .await;
    roomkit::testing::set_turn_timeouts(
        &kit,
        TurnTimeouts {
            heartbeat_interval: Duration::from_millis(5),
            heartbeat_timeout: Duration::from_millis(25),
            ..Default::default()
        },
    );
    let result = asks(&kit, "What is it?").await;
    let rows: Vec<EventType> = kit
        .store()
        .list_events(&room(), received())
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.source.channel_id == channel("ai"))
        .map(|event| event.r#type)
        .collect();
    kit.close().await.unwrap();
    assert_eq!(
        (result.error, rows),
        (
            None,
            vec![
                EventType::Message,
                EventType::ToolCallStart,
                EventType::ToolCallEnd,
                EventType::Message
            ]
        )
    );
}

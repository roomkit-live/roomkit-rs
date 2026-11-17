use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit::Channel;
use roomkit_ai::{ToolHandler, ToolResult, current_tool_room_id, turn_report_claim};
use tokio::sync::{Barrier, Notify};

use super::support::{Claim, call, channel, done, respond};

// Python: tests/tools/test_turn_report_claim.py::test_two_turns_on_one_channel_keep_separate_claims
#[tokio::test]
async fn two_turns_on_one_channel_keep_separate_claims() {
    concurrent(false).await;
}

// Python: tests/tools/test_turn_report_claim.py::test_concurrent_turns_can_invoke_each_others_captured_claims
#[tokio::test]
async fn concurrent_turns_can_invoke_each_others_captured_claims() {
    concurrent(true).await;
}

async fn concurrent(cross: bool) {
    for streaming in [false, true] {
        let callbacks: Arc<Mutex<HashMap<String, Claim>>> = Arc::default();
        let ready = Arc::new(Barrier::new(2));
        let finished = Arc::new(Notify::new());
        let seen = Arc::new(Mutex::new(HashMap::new()));
        let observed = Arc::clone(&seen);
        let handler = ToolHandler::new(move |_, _| {
            let (callbacks, ready, finished, observed) = (
                Arc::clone(&callbacks),
                Arc::clone(&ready),
                Arc::clone(&finished),
                Arc::clone(&observed),
            );
            async move {
                let room = current_tool_room_id().unwrap().to_string();
                let claim = turn_report_claim("c1", "ai").unwrap();
                callbacks
                    .lock()
                    .unwrap()
                    .insert(room.clone(), Arc::clone(&claim));
                ready.wait().await;
                let values = if cross {
                    if room == "r2" {
                        finished.notified().await;
                    }
                    let callbacks = callbacks.lock().unwrap();
                    [
                        callbacks.get("r2").unwrap()(),
                        callbacks.get("r1").unwrap()(),
                    ]
                } else {
                    [claim(), claim()]
                };
                observed.lock().unwrap().insert(room.clone(), values);
                if room == "r1" {
                    finished.notify_one();
                }
                Ok(ToolResult::Text("found".into()))
            }
        });
        let channel = channel(
            streaming,
            handler,
            vec![call("first"), call("second"), done(), done()],
        );
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(
                Box::pin(respond(&channel, "r1")),
                Box::pin(respond(&channel, "r2"))
            );
        })
        .await
        .unwrap();
        channel.close().await;
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen.get("r1"), Some(&[true, cross]));
        assert_eq!(seen.get("r2"), Some(&[!cross, false]));
    }
}

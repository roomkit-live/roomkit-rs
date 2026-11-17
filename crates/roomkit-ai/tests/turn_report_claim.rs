//! Python's public report callback, through actual local AI turns.

#![expect(clippy::unwrap_used, reason = "test fixtures must be valid")]

#[path = "turn_report_claim/concurrency.rs"]
mod concurrency;
#[path = "turn_report_claim/support.rs"]
mod support;

use std::sync::{Arc, Mutex};

use roomkit::Channel;
use roomkit_ai::{
    ToolCallContext, ToolError, ToolHandler, ToolResult, ToolTurnContextOptions, tool_turn_context,
    turn_report_claim,
};

use support::{Claim, call, channel, done, respond};

// Python: tests/tools/test_turn_report_claim.py::test_no_claim_exists_without_an_announced_turn_call
#[tokio::test]
async fn no_claim_exists_without_an_announced_turn_call() {
    assert!(turn_report_claim("c1", "ai").is_none());
    tool_turn_context(
        ToolTurnContextOptions {
            room_id: Some("r1".parse().unwrap()),
            call: Some(ToolCallContext {
                room_id: Some("r1".parse().unwrap()),
                channel_id: Some("ai".parse().unwrap()),
                tool_call_id: "c1".into(),
                ..ToolCallContext::default()
            }),
            ..ToolTurnContextOptions::default()
        },
        async {
            assert!(turn_report_claim("c1", "ai").is_none());
        },
    )
    .await
    .unwrap();
}

// Python: tests/tools/test_turn_report_claim.py::test_a_claim_matches_its_channel_and_announced_id
#[tokio::test]
async fn a_claim_matches_its_channel_and_announced_id() {
    for streaming in [false, true] {
        let handler = ToolHandler::new(|_, _| async {
            assert!(turn_report_claim("missing", "ai").is_none());
            assert!(turn_report_claim("c1", "other").is_none());
            let first = turn_report_claim("c1", "ai").unwrap();
            let second = turn_report_claim("c1", "ai").unwrap();
            assert_eq!([first(), second(), first()], [true, false, false]);
            Ok(ToolResult::Text("found".into()))
        });
        let channel = channel(streaming, handler, vec![call("once"), done()]);
        respond(&channel, "r1").await;
        channel.close().await;
        assert!(turn_report_claim("c1", "ai").is_none());
    }
}

// Python: tests/tools/test_turn_report_claim.py::test_a_retained_callback_resolves_a_reused_id_in_its_turn
#[tokio::test]
async fn a_retained_callback_resolves_a_reused_id_in_its_turn() {
    for streaming in [false, true] {
        let first: Arc<Mutex<Option<Claim>>> = Arc::default();
        let retained = Arc::clone(&first);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let claimed = Arc::clone(&seen);
        let handler = ToolHandler::new(move |_, _| {
            let (retained, claimed) = (Arc::clone(&retained), Arc::clone(&claimed));
            async move {
                let claim = turn_report_claim("c1", "ai").unwrap();
                let first = Arc::clone(
                    retained
                        .lock()
                        .unwrap()
                        .get_or_insert_with(|| Arc::clone(&claim)),
                );
                claimed.lock().unwrap().extend([first(), claim()]);
                Ok(ToolResult::Text("found".into()))
            }
        });
        let channel = channel(
            streaming,
            handler,
            vec![call("first"), call("second"), done()],
        );
        respond(&channel, "r1").await;
        channel.close().await;
        assert_eq!(*seen.lock().unwrap(), [true, false, true, false]);
        assert!(!first.lock().unwrap().as_ref().unwrap()());
    }
}

// Python: tests/tools/test_turn_report_claim.py::test_a_completed_calls_callback_keeps_its_claim_after_the_turn
#[tokio::test]
async fn a_completed_calls_callback_keeps_its_claim_after_the_turn() {
    after_turn(false).await;
}

// Python: tests/tools/test_turn_report_claim.py::test_an_unreported_failure_remains_claimable_after_its_turn
#[tokio::test]
async fn an_unreported_failure_remains_claimable_after_its_turn() {
    after_turn(true).await;
}

async fn after_turn(fails: bool) {
    for streaming in [false, true] {
        let callbacks: Arc<Mutex<Vec<Claim>>> = Arc::default();
        let retained = Arc::clone(&callbacks);
        let handler = ToolHandler::new(move |_, _| {
            retained
                .lock()
                .unwrap()
                .push(turn_report_claim("c1", "ai").unwrap());
            async move {
                if fails {
                    Err(ToolError::raised_as(
                        "RuntimeError",
                        "failed before reporting",
                    ))
                } else {
                    Ok(ToolResult::Text("found".into()))
                }
            }
        });
        let channel = channel(streaming, handler, vec![call("once"), done()]);
        respond(&channel, "r1").await;
        channel.close().await;
        assert!(turn_report_claim("c1", "ai").is_none());
        let callbacks = callbacks.lock().unwrap();
        assert_eq!(callbacks.len(), 1);
        let claim = callbacks.first().unwrap();
        assert_eq!([claim(), claim()], [fails, false]);
    }
}

// Python: tests/tools/test_turn_report_claim.py::test_a_callback_keeps_its_turn_inside_an_explicit_context
#[tokio::test]
async fn a_callback_keeps_its_turn_inside_an_explicit_context() {
    for streaming in [false, true] {
        let handler = ToolHandler::new(|_, _| async {
            let claim = turn_report_claim("c1", "ai").unwrap();
            tool_turn_context(
                ToolTurnContextOptions {
                    room_id: Some("another".parse().unwrap()),
                    ..ToolTurnContextOptions::default()
                },
                async {
                    assert!(turn_report_claim("c1", "ai").is_none());
                    assert!(claim());
                },
            )
            .await
            .unwrap();
            assert!(!turn_report_claim("c1", "ai").unwrap()());
            Ok(ToolResult::Text("found".into()))
        });
        let channel = channel(streaming, handler, vec![call("once"), done()]);
        respond(&channel, "r1").await;
        channel.close().await;
    }
}

// Python: tests/tools/test_turn_report_claim.py::test_an_earlier_turns_callback_cannot_claim_a_later_turn
#[tokio::test]
async fn an_earlier_turns_callback_cannot_claim_a_later_turn() {
    across_turns(false).await;
}

// Python: tests/tools/test_turn_report_claim.py::test_an_unreported_earlier_call_keeps_its_claim_during_a_later_turn
#[tokio::test]
async fn an_unreported_earlier_call_keeps_its_claim_during_a_later_turn() {
    across_turns(true).await;
}

async fn across_turns(fails: bool) {
    for streaming in [false, true] {
        let first: Arc<Mutex<Option<Claim>>> = Arc::default();
        let retained = Arc::clone(&first);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let claimed = Arc::clone(&seen);
        let handler = ToolHandler::new(move |_, _| {
            let (retained, claimed) = (Arc::clone(&retained), Arc::clone(&claimed));
            async move {
                let claim = turn_report_claim("c1", "ai").unwrap();
                let mut retained = retained.lock().unwrap();
                if let Some(first) = retained.as_ref() {
                    let mut values = claimed.lock().unwrap();
                    values.extend([first(), claim()]);
                    if fails {
                        values.extend([first(), claim()]);
                    }
                } else {
                    *retained = Some(Arc::clone(&claim));
                    if fails {
                        return Err(ToolError::raised_as(
                            "RuntimeError",
                            "failed before reporting",
                        ));
                    }
                    claimed.lock().unwrap().push(claim());
                }
                Ok(ToolResult::Text("found".into()))
            }
        });
        let channel = channel(
            streaming,
            handler,
            vec![call("first"), done(), call("second"), done()],
        );
        respond(&channel, "r1").await;
        respond(&channel, "r1").await;
        channel.close().await;
        let expected = if fails {
            vec![true, true, false, false]
        } else {
            vec![true, false, true]
        };
        assert_eq!(*seen.lock().unwrap(), expected);
        assert!(!first.lock().unwrap().as_ref().unwrap()());
    }
}

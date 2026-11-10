//! The handler's own rules, outside any turn (RFC §9.3).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit_core::model::{ChannelType, Metadata};
use serde_json::json;

use super::{
    CloseOptions, CreateOptions, HumanInputHandler, HumanInputHandlerOptions, PendingInputStatus,
    RejectOptions, WaitOptions,
};
use crate::error::RoomKitError;

fn of_channel(channel_id: &str) -> CreateOptions {
    CreateOptions {
        room_id: "r1".to_owned(),
        tool_call_id: "tc1".to_owned(),
        channel_id: channel_id.to_owned(),
        ..CreateOptions::default()
    }
}

fn within(timeout_ms: u64) -> WaitOptions {
    WaitOptions {
        timeout: Duration::from_millis(timeout_ms),
    }
}

fn rejected(reason: &str) -> RejectOptions {
    RejectOptions {
        reason: reason.to_owned(),
    }
}

async fn asked(handler: &HumanInputHandler, channel_id: &str) -> String {
    handler
        .create("AskUser", Metadata::new(), of_channel(channel_id))
        .await
        .unwrap()
        .pending_id
}

// Python: tests/test_human_input.py::test_create_returns_pending
#[tokio::test]
async fn create_returns_pending() {
    let handler = HumanInputHandler::default();
    let pending = handler
        .create(
            "AskUser",
            json!({"amount": 100}).as_object().unwrap().clone(),
            of_channel("ai"),
        )
        .await
        .unwrap();
    assert_eq!(
        (
            pending.status(),
            pending.tool_name.as_str(),
            pending.room_id.as_str()
        ),
        (PendingInputStatus::Pending, "AskUser", "r1")
    );
    assert_eq!(
        pending.arguments,
        json!({"amount": 100}).as_object().unwrap().clone()
    );
    assert!(!pending.detached);
    assert!(handler.pending().contains_key(&pending.pending_id));
}

// Python: tests/test_human_input.py::test_resolve_unblocks_wait
#[tokio::test]
async fn resolve_unblocks_wait() {
    let handler = HumanInputHandler::default();
    let id = asked(&handler, "ai").await;
    let live = handler.pending()[&id].clone();
    let waiting = tokio::spawn({
        let (handler, id) = (handler.clone(), id.clone());
        async move { handler.wait(&id, within(2000)).await }
    });
    tokio::task::yield_now().await;
    assert!(handler.resolve(&id, "the answer"));
    assert_eq!(waiting.await.unwrap().unwrap(), "the answer");
    assert!(handler.pending().is_empty());
    assert_eq!(live.status(), PendingInputStatus::Resolved);
    assert_eq!(live.result().as_deref(), Some("the answer"));
}

// Python: tests/test_human_input.py::test_reject_raises_runtime_error
#[tokio::test]
async fn a_rejection_is_human_input_rejected() {
    let handler = HumanInputHandler::default();
    let id = asked(&handler, "ai").await;
    let live = handler.pending()[&id].clone();
    assert!(handler.reject(&id, rejected("User denied")));
    assert_eq!(live.status(), PendingInputStatus::Rejected);
    assert_eq!(live.reject_reason().as_deref(), Some("User denied"));
    assert_eq!(
        handler.wait(&id, within(1000)).await,
        Err(RoomKitError::HumanInputRejected("User denied".to_owned()))
    );
}

// Python: tests/test_human_input.py::test_wait_timeout, test_wait_replays_a_timeout
#[tokio::test]
async fn a_wait_past_its_bound_times_out_and_is_replayed() {
    let handler = HumanInputHandler::default();
    let live = handler
        .create("AskUser", Metadata::new(), of_channel("ai"))
        .await
        .unwrap();
    let id = live.pending_id.clone();
    assert_eq!(
        handler.wait(&id, within(20)).await,
        Err(RoomKitError::Timeout)
    );
    assert!(handler.pending().is_empty());
    assert_eq!(live.status(), PendingInputStatus::TimedOut);
    assert_eq!(
        handler.wait(&id, within(20)).await,
        Err(RoomKitError::Timeout)
    );
}

// Python: tests/test_human_input.py::test_wait_returns_an_answer_accepted_at_the_deadline
#[tokio::test(start_paused = true)]
async fn an_answer_accepted_as_the_bound_passes_is_returned() {
    let handler = HumanInputHandler::default();
    let id = asked(&handler, "ai").await;
    let live = handler.pending()[&id].clone();
    // The answer lands once the bound passed, before the wait reads the
    // request: another thread answering at the deadline.
    handler.at_bound({
        let (handler, id) = (handler.clone(), id.clone());
        move || assert!(handler.resolve(&id, "yes"))
    });
    assert_eq!(handler.wait(&id, within(20)).await, Ok("yes".to_owned()));
    assert_eq!(live.status(), PendingInputStatus::Resolved);
    assert_eq!(live.result().as_deref(), Some("yes"));
    // Recorded as answered, not timed out.
    assert_eq!(handler.wait(&id, within(20)).await, Ok("yes".to_owned()));
}

// Python: tests/test_human_input.py::test_wait_nonexistent_raises_value_error,
// test_resolve_nonexistent_returns_false, test_reject_nonexistent_returns_false,
// test_resolve_already_resolved_returns_false
#[tokio::test]
async fn an_unknown_or_settled_request_is_not_settled_again() {
    let handler = HumanInputHandler::default();
    assert!(matches!(
        handler.wait("nope", within(10)).await,
        Err(RoomKitError::Value(_))
    ));
    assert!(!handler.resolve("nope", "x"));
    assert!(!handler.reject("nope", rejected("")));
    let id = asked(&handler, "ai").await;
    assert!(handler.resolve(&id, "first"));
    assert!(!handler.resolve(&id, "second"));
    assert!(!handler.reject(&id, rejected("late")));
}

// Python: tests/test_human_input.py::test_concurrent_pending_requests
#[tokio::test]
async fn concurrent_requests_settle_apart() {
    let handler = HumanInputHandler::default();
    let (first, second) = (asked(&handler, "ai").await, asked(&handler, "ai").await);
    assert_eq!(handler.pending().len(), 2);
    assert!(handler.resolve(&second, "two"));
    assert!(handler.resolve(&first, "one"));
    assert_eq!(handler.wait(&first, within(100)).await.unwrap(), "one");
    assert_eq!(handler.wait(&second, within(100)).await.unwrap(), "two");
    assert!(handler.pending().is_empty());
}

// Python: tests/test_human_input.py::test_close_cancels_only_one_channels_notifications_and_requests,
// test_close_prevents_new_requests_in_the_closed_scope
#[tokio::test]
async fn a_channel_s_close_settles_its_requests_alone_and_takes_no_more() {
    let handler = HumanInputHandler::default();
    let (mine, theirs) = (asked(&handler, "a").await, asked(&handler, "b").await);
    let options = CloseOptions {
        channel_id: Some("a".to_owned()),
        registration: None,
    };
    handler.close(options).await;

    let given_up = handler.wait(&mine, within(100)).await.unwrap_err();
    assert_eq!(given_up.to_string(), "Human input handler closed");
    assert!(handler.pending().contains_key(&theirs));
    let refused = handler
        .create("AskUser", Metadata::new(), of_channel("a"))
        .await
        .unwrap_err();
    assert_eq!(
        refused.to_string(),
        "Human input handler is closed for channel a"
    );
    assert!(
        handler
            .create("AskUser", Metadata::new(), of_channel("b"))
            .await
            .is_ok()
    );
}

// Python: tests/test_human_input.py::test_close_cancels_only_one_channels_notifications_and_requests
#[tokio::test]
async fn close_finishes_only_its_channels_notifications() {
    struct Cancelled(Arc<Mutex<Vec<String>>>, String);
    impl Drop for Cancelled {
        fn drop(&mut self) {
            self.0.lock().unwrap().push(self.1.clone());
        }
    }
    let handler = HumanInputHandler::default();
    let cancelled = Arc::new(Mutex::new(Vec::new()));
    let (started, mut starts) = tokio::sync::mpsc::unbounded_channel();
    for channel in ["a", "b"] {
        handler.register(channel).unwrap();
        let (cancelled, started) = (Arc::clone(&cancelled), started.clone());
        handler.set_on_input_required(
            channel,
            Arc::new(move |event| {
                let guard = Cancelled(Arc::clone(&cancelled), event.channel_id.clone());
                let started = started.clone();
                Box::pin(async move {
                    let _guard = guard;
                    started.send(event.channel_id).unwrap();
                    std::future::pending().await
                })
            }),
        );
    }
    let (mine, theirs) = (asked(&handler, "a").await, asked(&handler, "b").await);
    starts.recv().await.unwrap();
    starts.recv().await.unwrap();
    let options = CloseOptions {
        channel_id: Some("a".to_owned()),
        registration: None,
    };
    let mut first_close = Box::pin(handler.close(options.clone()));
    let mut second_close = Box::pin(handler.close(options));
    std::future::poll_fn(|cx| {
        use std::future::Future;
        // Neither close returns until the callback has dropped. Abandoning
        // the first close must not lose the task the second is joining.
        assert!(first_close.as_mut().poll(cx).is_pending());
        assert!(second_close.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    drop(first_close);
    second_close.await;
    assert_eq!(cancelled.lock().unwrap().clone(), ["a"]);
    assert_eq!(
        handler
            .wait(&mine, within(100))
            .await
            .unwrap_err()
            .to_string(),
        "Human input handler closed"
    );
    assert!(handler.pending().contains_key(&theirs));
    assert!(handler.resolve(&theirs, "ok"));
    assert_eq!(handler.wait(&theirs, within(100)).await.unwrap(), "ok");
    handler.close(CloseOptions::default()).await;
    assert_eq!(cancelled.lock().unwrap().clone(), ["a", "b"]);
}

// Python: tests/test_human_input.py::test_registering_reopens_a_closed_channel_scope
#[tokio::test]
async fn a_registration_reopens_a_closed_channel_scope() {
    let handler = HumanInputHandler::default();
    let options = CloseOptions {
        channel_id: Some("ch1".to_owned()),
        registration: None,
    };
    handler.close(options).await;
    assert!(
        handler
            .create("ask", Metadata::new(), of_channel("ch1"))
            .await
            .is_err()
    );
    // A channel object registering under that id is a new owner.
    handler.register("ch1").unwrap();
    let id = asked(&handler, "ch1").await;
    assert!(handler.pending().contains_key(&id));
}

// Python: tests/test_human_input.py::test_displaced_owner_close_spares_the_live_channel
#[tokio::test]
async fn a_displaced_owner_s_close_spares_the_live_channel() {
    let handler = HumanInputHandler::default();
    let displaced = handler.register("ch1").unwrap();
    let live = handler.register("ch1").unwrap();
    assert_ne!(displaced, live);
    let close = |registration| CloseOptions {
        channel_id: Some("ch1".to_owned()),
        registration: Some(registration),
    };
    let id = asked(&handler, "ch1").await;
    handler.close(close(displaced)).await;
    assert_eq!(
        handler.pending().get(&id).map(super::PendingInput::status),
        Some(PendingInputStatus::Pending)
    );
    asked(&handler, "ch1").await;
    // The owner that does hold the id still closes it.
    handler.close(close(live)).await;
    assert!(
        handler
            .create("ask", Metadata::new(), of_channel("ch1"))
            .await
            .is_err()
    );
}

// Python: tests/test_human_input.py::test_pending_property_returns_snapshot
#[tokio::test]
async fn pending_is_a_snapshot() {
    let handler = HumanInputHandler::default();
    let id = asked(&handler, "ai").await;
    let mut snapshot = handler.pending();
    snapshot.clear();
    assert!(handler.pending().contains_key(&id));
}

// Python: tests/test_human_input.py::test_wait_replays_a_consumed_answer,
// test_wait_replays_a_rejection
#[tokio::test]
async fn a_settled_outcome_is_replayed() {
    let handler = HumanInputHandler::default();
    let answered = asked(&handler, "ai").await;
    assert!(handler.resolve(&answered, "yes"));
    assert_eq!(handler.wait(&answered, within(100)).await.unwrap(), "yes");
    assert_eq!(handler.wait(&answered, within(100)).await.unwrap(), "yes");
    let refused = asked(&handler, "ai").await;
    assert!(handler.reject(&refused, rejected("no")));
    for _ in 0..2 {
        assert_eq!(
            handler.wait(&refused, within(100)).await,
            Err(RoomKitError::HumanInputRejected("no".to_owned()))
        );
    }
}

// Python: tests/test_human_input.py::test_retention_is_bounded, test_retention_can_be_switched_off
#[tokio::test]
async fn the_retention_is_bounded_and_may_be_none() {
    let handler = HumanInputHandler::new(HumanInputHandlerOptions { retention: 1 });
    let first = asked(&handler, "ai").await;
    let second = asked(&handler, "ai").await;
    assert!(handler.resolve(&first, "one"));
    assert!(handler.resolve(&second, "two"));
    // The first outcome is already out of retention, but remains active:
    // the caller's first wait must still read the answer it was given.
    assert_eq!(handler.wait(&first, within(100)).await.unwrap(), "one");
    assert_eq!(handler.wait(&second, within(100)).await.unwrap(), "two");
    assert_eq!(handler.wait(&second, within(100)).await.unwrap(), "two");
    assert!(matches!(
        handler.wait(&first, within(10)).await,
        Err(RoomKitError::Value(_))
    ));

    let forgetful = HumanInputHandler::new(HumanInputHandlerOptions { retention: 0 });
    let id = asked(&forgetful, "ai").await;
    assert!(forgetful.resolve(&id, "once"));
    assert_eq!(forgetful.wait(&id, within(100)).await.unwrap(), "once");
    assert!(matches!(
        forgetful.wait(&id, within(10)).await,
        Err(RoomKitError::Value(_))
    ));
}

// Python: tests/test_human_input.py::test_create_detached_marks_the_request,
// test_release_drops_the_request_but_keeps_its_answer
#[tokio::test]
async fn a_detached_request_is_released_by_its_creator_its_answer_kept() {
    let handler = HumanInputHandler::default();
    let pending = handler
        .create_detached("AskUser", Metadata::new(), of_channel("ai").into())
        .await
        .unwrap();
    assert!(pending.detached);
    assert!(handler.resolve(&pending.pending_id, "yes"));
    assert!(handler.release(&pending.pending_id));
    assert!(!handler.release(&pending.pending_id));
    assert!(handler.pending().is_empty());
    assert_eq!(
        handler.wait(&pending.pending_id, within(10)).await.unwrap(),
        "yes"
    );
}

// Python: tests/test_human_input.py::test_release_rejects_a_request_still_unanswered,
// test_release_unknown_returns_false, test_release_is_idempotent
#[tokio::test]
async fn a_release_gives_an_unanswered_request_up_once() {
    let handler = HumanInputHandler::default();
    let id = asked(&handler, "ai").await;
    let waiting = tokio::spawn({
        let (handler, id) = (handler.clone(), id.clone());
        async move { handler.wait(&id, within(2000)).await }
    });
    tokio::task::yield_now().await;
    assert!(handler.release(&id));
    let given_up = waiting.await.unwrap().unwrap_err();
    assert_eq!(given_up.to_string(), "Released before an answer arrived");
    assert_eq!(
        given_up,
        RoomKitError::Runtime("Released before an answer arrived".to_owned())
    );
    assert_eq!(given_up.class_name(), "RuntimeError");
    assert!(!handler.release(&id));
    assert!(!handler.release("nope"));
}

// Python: tests/test_human_input.py::test_create_carries_the_actor_to_the_request_and_the_event,
// test_a_request_with_no_actor_says_so
#[tokio::test]
async fn a_request_names_its_actor_when_one_is_given() {
    let handler = HumanInputHandler::default();
    handler.register("ai").unwrap();
    let (announced, mut events) = tokio::sync::mpsc::unbounded_channel();
    handler.set_on_input_required(
        "ai",
        Arc::new(move |event| {
            announced.send(event).unwrap();
            Box::pin(async { true })
        }),
    );
    let named = CreateOptions {
        actor_id: Some("alice".to_owned()),
        ..of_channel("ai")
    };
    let pending = handler
        .create("AskUser", Metadata::new(), named)
        .await
        .unwrap();
    assert_eq!(pending.actor_id.as_deref(), Some("alice"));
    let event = events.recv().await.unwrap();
    assert_eq!(event.actor_id.as_deref(), Some("alice"));
    assert_eq!(event.pending_id, pending.pending_id);
    assert_eq!(event.tool_name, "AskUser");
    assert_eq!(event.room_id, "r1");
    assert_eq!(event.tool_call_id, "tc1");
    assert_eq!(event.channel_id, "ai");
    assert_eq!(event.channel_type, ChannelType::AI);
    let nobody = handler
        .create_detached("AskUser", Metadata::new(), of_channel("ai").into())
        .await
        .unwrap();
    assert_eq!(nobody.actor_id, None);
}

// Python: tests/test_human_input.py (handler closed whole)
#[tokio::test]
async fn a_handler_closed_whole_takes_nothing_more() {
    let handler = HumanInputHandler::default();
    handler.close(CloseOptions::default()).await;
    let refused = handler
        .create("AskUser", Metadata::new(), of_channel("ai"))
        .await
        .unwrap_err();
    assert_eq!(refused.to_string(), "Human input handler is closed");
    assert!(handler.register("ai").is_err());
}

// Python: tests/test_human_input.py::test_release_rejects_a_request_still_unanswered,
// test_close_prevents_new_requests_in_the_closed_scope (RMK-1637: retained request handles)
#[tokio::test]
async fn returned_requests_keep_their_outcome_after_release_and_close() {
    let handler = HumanInputHandler::default();
    let released = handler
        .create("AskUser", Metadata::new(), of_channel("ai"))
        .await
        .unwrap();
    let closed = handler
        .create("AskUser", Metadata::new(), of_channel("ai"))
        .await
        .unwrap();
    let old_snapshot = handler.pending();
    assert!(handler.release(&released.pending_id));
    handler.close(CloseOptions::default()).await;
    for (request, reason) in [
        (&released, "Released before an answer arrived"),
        (&closed, "Human input handler closed"),
    ] {
        assert_eq!(request.status(), PendingInputStatus::Rejected);
        assert_eq!(request.reject_reason().as_deref(), Some(reason));
        assert_eq!(
            old_snapshot[&request.pending_id].status(),
            PendingInputStatus::Rejected
        );
        assert_eq!(request.result(), None);
    }
}

#[tokio::test]
async fn a_live_outcome_keeps_its_flat_serialized_fields() {
    let handler = HumanInputHandler::default();
    let live = handler
        .create("AskUser", Metadata::new(), of_channel("ai"))
        .await
        .unwrap();
    let earlier = serde_json::to_value(&live).unwrap();
    assert_eq!(earlier["status"], "pending");
    assert!(handler.resolve(&live.pending_id, "yes"));
    let written = serde_json::to_value(&live).unwrap();
    assert_eq!(written["status"], "resolved");
    assert_eq!(written["result"], "yes");
    assert_eq!(written["reject_reason"], serde_json::Value::Null);
    assert!(written.get("outcome").is_none());
    let read: super::PendingInput = serde_json::from_value(written).unwrap();
    assert_eq!(read, live);
    assert_eq!(live.clone(), live);
    // Serialized data is a value, independent of the live request.
    let read_before: super::PendingInput = serde_json::from_value(earlier).unwrap();
    assert_eq!(read_before.status(), PendingInputStatus::Pending);
    assert_eq!(live.status(), PendingInputStatus::Resolved);
}

#[test]
fn pending_input_constructor_keeps_python_defaults_and_given_outcomes() {
    let record = super::PendingInput::new(
        "p1",
        "AskUser",
        Metadata::new(),
        "r1",
        "c1",
        "ai",
        super::PendingInputOptions::default(),
    );
    assert_eq!(record.status(), PendingInputStatus::Pending);
    assert_eq!(record.result(), None);
    assert_eq!(record.reject_reason(), None);
    assert!(!record.detached);
    assert_eq!(record.actor_id, None);
    let given = super::PendingInput::new(
        "p1",
        "AskUser",
        Metadata::new(),
        "r1",
        "c1",
        "ai",
        super::PendingInputOptions {
            status: PendingInputStatus::Rejected,
            result: Some("earlier".to_owned()),
            reject_reason: Some("no".to_owned()),
            detached: true,
            created_at: Some(record.created_at),
            actor_id: Some("alice".to_owned()),
        },
    );
    assert_eq!(given.status(), PendingInputStatus::Rejected);
    assert_eq!(given.result().as_deref(), Some("earlier"));
    assert_eq!(given.reject_reason().as_deref(), Some("no"));
    assert!(given.detached);
    assert_eq!(given.created_at, record.created_at);
    assert_eq!(given.actor_id.as_deref(), Some("alice"));
}

// RMK-1637: two handles to the same live request stay equal as it settles.
#[tokio::test]
async fn a_clone_stays_equal_while_the_request_is_resolved() {
    let handler = HumanInputHandler::default();
    let mut requests = Vec::new();
    for _ in 0..2048 {
        requests.push(
            handler
                .create("AskUser", Metadata::new(), of_channel("ai"))
                .await
                .unwrap(),
        );
    }
    let copies = requests.clone();
    let start = std::sync::Barrier::new(2);
    std::thread::scope(|scope| {
        let writer = scope.spawn(|| {
            start.wait();
            for request in &requests {
                assert!(handler.resolve(&request.pending_id, "yes"));
            }
        });
        start.wait();
        for _ in 0..8 {
            for (request, copy) in requests.iter().zip(&copies) {
                assert_eq!(request, copy);
            }
        }
        writer.join().unwrap();
    });
}

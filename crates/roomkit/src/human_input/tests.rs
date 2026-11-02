//! The handler's own rules, outside any turn (RFC §9.3).

use std::time::Duration;

use roomkit_core::model::Metadata;

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
        .create("AskUser", Metadata::new(), of_channel("ai"))
        .await
        .unwrap();
    assert_eq!(
        (
            pending.status,
            pending.tool_name.as_str(),
            pending.room_id.as_str()
        ),
        (PendingInputStatus::Pending, "AskUser", "r1")
    );
    assert!(handler.pending().contains_key(&pending.pending_id));
}

// Python: tests/test_human_input.py::test_resolve_unblocks_wait
#[tokio::test]
async fn resolve_unblocks_wait() {
    let handler = HumanInputHandler::default();
    let id = asked(&handler, "ai").await;
    let waiting = tokio::spawn({
        let (handler, id) = (handler.clone(), id.clone());
        async move { handler.wait(&id, within(2000)).await }
    });
    tokio::task::yield_now().await;
    assert!(handler.resolve(&id, "the answer"));
    assert_eq!(waiting.await.unwrap().unwrap(), "the answer");
    assert!(handler.pending().is_empty());
}

// Python: tests/test_human_input.py::test_reject_raises_runtime_error
#[tokio::test]
async fn a_rejection_is_human_input_rejected() {
    let handler = HumanInputHandler::default();
    let id = asked(&handler, "ai").await;
    assert!(handler.reject(&id, rejected("User denied")));
    assert_eq!(
        handler.wait(&id, within(1000)).await,
        Err(RoomKitError::HumanInputRejected("User denied".to_owned()))
    );
}

// Python: tests/test_human_input.py::test_wait_timeout, test_wait_replays_a_timeout
#[tokio::test]
async fn a_wait_past_its_bound_times_out_and_is_replayed() {
    let handler = HumanInputHandler::default();
    let id = asked(&handler, "ai").await;
    assert_eq!(
        handler.wait(&id, within(20)).await,
        Err(RoomKitError::Timeout)
    );
    assert!(handler.pending().is_empty());
    assert_eq!(
        handler.wait(&id, within(20)).await,
        Err(RoomKitError::Timeout)
    );
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
    assert!(handler.resolve(&second, "two"));
    assert!(handler.resolve(&first, "one"));
    assert_eq!(handler.wait(&first, within(100)).await.unwrap(), "one");
    assert_eq!(handler.wait(&second, within(100)).await.unwrap(), "two");
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
        handler.pending().get(&id).map(|pending| pending.status),
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
// test_wait_reads_an_answer_the_host_dropped, test_wait_replays_a_rejection
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
    let handler = HumanInputHandler::new(HumanInputHandlerOptions { retention: 2 });
    let mut ids = Vec::new();
    for answer in ["a", "b", "c"] {
        let id = asked(&handler, "ai").await;
        assert!(handler.resolve(&id, answer));
        assert_eq!(handler.wait(&id, within(100)).await.unwrap(), answer);
        ids.push(id);
    }
    assert!(matches!(
        handler.wait(&ids[0], within(10)).await,
        Err(RoomKitError::Value(_))
    ));
    assert_eq!(handler.wait(&ids[2], within(10)).await.unwrap(), "c");

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
    assert_eq!(given_up.class_name(), "RuntimeError");
    assert!(!handler.release(&id));
    assert!(!handler.release("nope"));
}

// Python: tests/test_human_input.py::test_create_carries_the_actor_to_the_request_and_the_event,
// test_a_request_with_no_actor_says_so
#[tokio::test]
async fn a_request_names_its_actor_when_one_is_given() {
    let handler = HumanInputHandler::default();
    let named = CreateOptions {
        actor_id: Some("alice".to_owned()),
        ..of_channel("ai")
    };
    let pending = handler
        .create("AskUser", Metadata::new(), named)
        .await
        .unwrap();
    assert_eq!(pending.actor_id.as_deref(), Some("alice"));
    let nobody = handler
        .create("AskUser", Metadata::new(), of_channel("ai"))
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

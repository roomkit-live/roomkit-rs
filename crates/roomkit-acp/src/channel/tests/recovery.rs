//! Recovery boundaries that require native transport gates and stream dropping.

mod context;
pub(in crate::channel) mod fixture;

use std::future::poll_fn;
use std::pin::Pin;
use std::sync::atomic::Ordering;

use futures_core::Stream;
use roomkit::Channel;
use roomkit::model::{RoomId, ToolCallOutcome, ToolCallStatus};
use serde_json::json;

use super::{PATIENCE, TurnStream, drained, poll_once, turn};
use fixture::{Stage, channel};

// Python: tests/test_channels/test_acp_recovery.py::test_activity_makes_even_an_authorized_signal_terminal[permission]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_activity_makes_even_an_authorized_signal_terminal_permission() {
    let (channel, state) = channel(Stage::Permission);
    let (mut stream, record) = turn(&channel, "r1");
    assert_refused_permission(&mut stream).await;
    let item = poll_fn(|cx| Pin::new(&mut stream).poll_next(cx)).await;
    let error = item.unwrap().unwrap_err();
    assert!(error.to_string().contains("activity; recovery refused"));
    let cause = error
        .source()
        .unwrap()
        .downcast_ref::<crate::AcpSessionInvalidatedError>()
        .unwrap();
    assert_eq!(cause.message, "unusable");
    assert!(cause.recovery_authorized);
    assert_eq!(state.opened.load(Ordering::SeqCst), 1);
    assert_eq!(state.prompts.lock().unwrap().len(), 1);
    assert!(state.closed.lock().unwrap().is_empty());
    assert_eq!(record.to_metadata()["acp"]["interrupted"], true);
    assert_eq!(channel.active_turns(), 0);
    channel.close().await;
}

async fn assert_refused_permission(stream: &mut TurnStream<fixture::Direct>) {
    // A permission names a tool even without an agent announcement. Its
    // refused start/end precede the terminal session error (RFC §9.3).
    let start = poll_fn(|cx| Pin::new(&mut *stream).poll_next(cx))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(start, roomkit::ResponseItem::ToolCallStart(row) if row.tool_id == "tool-1"));
    let end = poll_fn(|cx| Pin::new(&mut *stream).poll_next(cx))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(end, roomkit::ResponseItem::ToolCallEnd(row)
        if row.tool_id == "tool-1"
            && row.status == ToolCallStatus::Failed
            && row.outcome == Some(ToolCallOutcome::Refused)
            && !row.refused_but_ran));
}

// Python: tests/test_channels/test_acp_recovery.py::test_cancel_or_abandon_recovery_cleans_state_and_releases_the_lock
#[tokio::test]
async fn test_cancel_or_abandon_recovery_cleans_state_and_releases_the_lock() {
    for stage in [Stage::Close, Stage::Open, Stage::Configured, Stage::Prompt] {
        let (channel, state) = channel(stage);
        let (mut stream, record) = turn(&channel, "r1");
        let consuming = tokio::spawn(async move { drained(&mut stream).await });
        tokio::time::timeout(PATIENCE, state.reached.notified())
            .await
            .unwrap();
        consuming.abort();
        assert!(consuming.await.unwrap_err().is_cancelled());
        let room = RoomId::new("r1").unwrap();
        assert!(channel.session_id(&room).is_none());
        assert_eq!(
            channel.config_options(&room),
            Vec::<serde_json::Value>::new()
        );
        assert_eq!(record.to_metadata()["acp"]["interrupted"], true);
        let (mut next, _) = turn(&channel, "r1");
        let (_, failed) = tokio::time::timeout(PATIENCE, drained(&mut next))
            .await
            .unwrap();
        assert!(!failed);
        assert_eq!(channel.active_turns(), 0);
        assert!(
            !state
                .cancelled
                .lock()
                .unwrap()
                .contains(&"session-1".to_owned())
        );
        channel.close().await;
    }
}

// Python: tests/test_channels/test_acp_recovery.py::test_cancel_or_abandon_recovery_cleans_state_and_releases_the_lock[abandon]
#[tokio::test]
async fn test_abandon_recovery_after_partial_output_discards_the_session() {
    let (channel, state) = channel(Stage::Prompt);
    let (mut stream, record) = turn(&channel, "r1");
    let item = poll_fn(|cx| Pin::new(&mut stream).poll_next(cx))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(item, roomkit::ResponseItem::Text(text) if text == "partial"));
    drop(stream);
    assert!(channel.session_id(&RoomId::new("r1").unwrap()).is_none());
    assert_eq!(record.to_metadata()["acp"]["interrupted"], true);
    let (mut next, _) = turn(&channel, "r1");
    assert!(
        !tokio::time::timeout(PATIENCE, drained(&mut next))
            .await
            .unwrap()
            .1
    );
    assert_eq!(*state.cancelled.lock().unwrap(), ["session-2"]);
    channel.close().await;
}

// Python: tests/test_channels/test_acp_recovery.py::test_close_stops_reconstruction_without_cancelling_the_invalid_remote_session
#[tokio::test]
async fn test_close_stops_reconstruction_without_cancelling_the_invalid_remote_session() {
    for stage in [Stage::Close, Stage::Open, Stage::Configured] {
        let (channel, state) = channel(stage);
        let (mut stream, record) = turn(&channel, "r1");
        let consuming = tokio::spawn(async move { drained(&mut stream).await });
        tokio::time::timeout(PATIENCE, state.reached.notified())
            .await
            .unwrap();
        tokio::time::timeout(PATIENCE, channel.close())
            .await
            .unwrap();
        assert_eq!(
            tokio::time::timeout(PATIENCE, consuming)
                .await
                .unwrap()
                .unwrap(),
            (vec![], false)
        );
        assert_eq!(state.prompts.lock().unwrap().len(), 1);
        assert!(state.cancelled.lock().unwrap().is_empty());
        assert!(channel.session_id(&RoomId::new("r1").unwrap()).is_none());
        assert_eq!(channel.active_turns(), 0);
        assert_eq!(record.to_metadata()["acp"]["interrupted"], true);
    }
}

// Python: tests/test_channels/test_acp_recovery.py::test_recovery_is_bounded_and_requires_authorization
// Also proves the room lock spans the replacement, by polling a competing turn at that lock.
#[tokio::test]
async fn test_recovery_keeps_the_room_lock_between_attempts() {
    let (channel, state) = channel(Stage::Open);
    let (mut first, _) = turn(&channel, "r1");
    let consuming = tokio::spawn(async move { drained(&mut first).await });
    tokio::time::timeout(PATIENCE, state.reached.notified())
        .await
        .unwrap();
    let (mut second, _) = turn(&channel, "r1");
    poll_once(&mut second).await;
    assert_eq!(state.prompts.lock().unwrap().len(), 1);
    assert_eq!(channel.active_turns(), 1);
    state.resume.notify_one();
    assert!(
        !tokio::time::timeout(PATIENCE, consuming)
            .await
            .unwrap()
            .unwrap()
            .1
    );
    assert!(
        !tokio::time::timeout(PATIENCE, drained(&mut second))
            .await
            .unwrap()
            .1
    );
    let attempts: Vec<_> = state
        .prompts
        .lock()
        .unwrap()
        .iter()
        .map(|r| r.session_id.0.to_string())
        .collect();
    assert_eq!(attempts, ["session-1", "session-2", "session-2"]);
    assert_eq!(state.opened.load(Ordering::SeqCst), 2);
    assert_eq!(channel.info()["active_turns"], json!(0));
    channel.close().await;
}

// Python: tests/test_channels/test_acp_recovery.py::test_activity_makes_even_an_authorized_signal_terminal
// The recovered turn still owns its room until the caller consumes its queued text and terminal error.
#[tokio::test]
async fn failed_replacement_holds_room_until_terminal_is_read() {
    let (channel, state) = channel(Stage::FailedPrompt);
    let room = RoomId::new("r1").unwrap();
    let (mut first, _) = turn(&channel, "r1");
    let item = poll_fn(|cx| Pin::new(&mut first).poll_next(cx))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(item, roomkit::ResponseItem::Text(text) if text == "partial"));
    assert_eq!(channel.session_id(&room).as_deref(), Some("session-2"));
    assert_eq!(*state.closed.lock().unwrap(), ["session-1"]);
    let (mut second, _) = turn(&channel, "r1");
    poll_once(&mut second).await;
    assert_eq!(channel.active_turns(), 1);
    assert!(
        tokio::time::timeout(PATIENCE, drained(&mut first))
            .await
            .unwrap()
            .1
    );
    assert!(
        !tokio::time::timeout(PATIENCE, drained(&mut second))
            .await
            .unwrap()
            .1
    );
    assert_eq!(state.opened.load(Ordering::SeqCst), 3);
    channel.close().await;
}

// Python: tests/test_channels/test_acp_recovery.py::test_cancel_or_abandon_recovery_cleans_state_and_releases_the_lock[abandon]
// Unlike the suspended-prompt case, the remote prompt has returned before the stream is dropped.
#[tokio::test]
async fn abandoning_a_finished_replacement_discards_it_without_remote_cancel() {
    let (channel, state) = channel(Stage::AnsweredPrompt);
    let (mut stream, record) = turn(&channel, "r1");
    let item = poll_fn(|cx| Pin::new(&mut stream).poll_next(cx))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(item, roomkit::ResponseItem::Text(text) if text == "partial"));
    tokio::time::timeout(PATIENCE, async {
        while record.to_metadata()["acp"].get("prompt_returned") != Some(&json!(true)) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(stream);
    assert!(channel.session_id(&RoomId::new("r1").unwrap()).is_none());
    assert_eq!(record.to_metadata()["acp"]["interrupted"], true);
    let (mut next, _) = turn(&channel, "r1");
    assert!(
        !tokio::time::timeout(PATIENCE, drained(&mut next))
            .await
            .unwrap()
            .1
    );
    assert!(state.cancelled.lock().unwrap().is_empty());
    assert_eq!(state.opened.load(Ordering::SeqCst), 3);
    channel.close().await;
}

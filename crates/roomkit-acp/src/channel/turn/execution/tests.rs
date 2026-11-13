//! The boundary after a replacement is ready but before its prompt enters.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use roomkit::Channel;
use roomkit::model::{ResponseMetadata, RoomId};
use serde_json::json;
use tokio::sync::mpsc;

use super::{Hold, one_prompt};
use crate::channel::tests::recovery::fixture::{Stage, channel};
use crate::channel::updates::{Sink, ThinkingWindow};

// Python: tests/test_channels/test_acp_recovery.py::test_close_after_reconstruction_finishes_prevents_replacement_prompt
#[tokio::test]
async fn test_close_after_reconstruction_finishes_prevents_replacement_prompt() {
    let (channel, state) = channel(Stage::Open);
    let room = RoomId::new("r1").unwrap();
    let connection = channel.shared.connector.connection().await.unwrap();
    channel
        .shared
        .connector
        .session(&room, &connection, &channel.shared.cwd)
        .await
        .unwrap();
    let record = ResponseMetadata::new();
    record.insert("acp", json!({"protocol_version":1}));
    let mut hold = Hold::acquire(
        Arc::clone(&channel.shared),
        room.clone(),
        record.clone(),
        None,
    )
    .await;
    hold.recovering = true;
    state.resume.notify_one();
    let session = channel
        .shared
        .connector
        .rebuild(&room, &connection, &channel.shared.cwd)
        .await
        .unwrap();
    assert_eq!(session, "session-2");
    channel.close().await;
    let (text, _receiver) = mpsc::unbounded_channel();
    let text = Sink::new(
        text,
        Arc::new(ThinkingWindow::new(
            None,
            room.clone(),
            channel.shared.channel_id.clone(),
            false,
        )),
        Arc::new(crate::channel::tools::Tools::new(
            None,
            room.clone(),
            channel.shared.channel_id.clone(),
            true,
        )),
    );
    let activity = Arc::new(AtomicBool::new(false));
    assert!(
        one_prompt(
            &mut hold,
            &connection,
            &session,
            "event-1",
            "hello",
            &text,
            &activity
        )
        .await
        .unwrap()
        .is_none()
    );
    assert!(state.prompts.lock().unwrap().is_empty());
    assert!(state.cancelled.lock().unwrap().is_empty());
    assert_eq!(record.to_metadata()["acp"]["interrupted"], true);
    drop(hold);
    assert_eq!(channel.active_turns(), 0);
    assert!(channel.session_id(&room).is_none());
}

// Python: tests/test_channels/test_acp_recovery.py::test_public_cancel_stops_reconstruction_before_prompt
#[tokio::test]
async fn test_cancel_after_reconstruction_finishes_prevents_replacement_prompt() {
    let (channel, state) = channel(Stage::Open);
    let room = RoomId::new("r1").unwrap();
    let connection = channel.shared.connector.connection().await.unwrap();
    channel
        .shared
        .connector
        .session(&room, &connection, &channel.shared.cwd)
        .await
        .unwrap();
    let record = ResponseMetadata::new();
    record.insert("acp", json!({"protocol_version":1}));
    let mut hold = Hold::acquire(
        Arc::clone(&channel.shared),
        room.clone(),
        record.clone(),
        None,
    )
    .await;
    hold.recovering = true;
    state.resume.notify_one();
    let session = channel
        .shared
        .connector
        .rebuild(&room, &connection, &channel.shared.cwd)
        .await
        .unwrap();
    assert_eq!(session, "session-2");
    assert!(channel.cancel(&room).await.unwrap());
    let (text, _receiver) = mpsc::unbounded_channel();
    let text = Sink::new(
        text,
        Arc::new(ThinkingWindow::new(
            None,
            room.clone(),
            channel.shared.channel_id.clone(),
            false,
        )),
        Arc::new(crate::channel::tools::Tools::new(
            None,
            room.clone(),
            channel.shared.channel_id.clone(),
            true,
        )),
    );
    let activity = Arc::new(AtomicBool::new(false));
    assert!(
        one_prompt(
            &mut hold,
            &connection,
            &session,
            "event-1",
            "hello",
            &text,
            &activity
        )
        .await
        .unwrap()
        .is_none()
    );
    assert!(state.prompts.lock().unwrap().is_empty());
    assert!(state.cancelled.lock().unwrap().is_empty());
    assert_eq!(record.to_metadata()["acp"]["interrupted"], true);
    hold.cleanup().await;
    drop(hold);
    assert_eq!(channel.active_turns(), 0);
    assert!(channel.session_id(&room).is_none());
}

// Python: tests/test_channels/test_acp_recovery.py::test_invalidation_retains_provider_attributes
#[test]
fn native_invalidation_keeps_provider_attributes() {
    let refusal = crate::AcpSessionInvalidatedError::new(
        "unusable",
        crate::AcpSessionInvalidatedErrorOptions::default(),
    );
    let error = super::super::raised(refusal.into());
    let classed = error.downcast_ref::<roomkit::ClassedError>().unwrap();
    assert_eq!(classed.class(), "ACPSessionInvalidatedError");
    assert_eq!(
        serde_json::to_value(classed.attributes()).unwrap(),
        json!({
            "recovery_authorized": false, "provider": "acp", "retryable": false,
            "status_code": null, "context_overflow": null, "before_request": false,
        })
    );
}

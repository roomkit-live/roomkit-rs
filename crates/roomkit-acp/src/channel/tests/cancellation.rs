//! Public cancellation reaches preparation before there is a remote prompt.

use std::sync::atomic::Ordering;

use roomkit::Channel;
use roomkit::model::RoomId;
use serde_json::json;

use super::recovery::fixture::{Stage, channel};
use super::{PATIENCE, drained, turn};

// Python: tests/test_channels/test_acp_session_setup.py::TestACancelReachesATurnWhileItsSessionOpens
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_cancel_reaches_connection_opening_and_initial_configuration() {
    for stage in [Stage::Start, Stage::InitialOpen, Stage::InitialConfigured] {
        cancelled_preparation(stage, 0).await;
    }
}

// Python: tests/test_channels/test_acp_recovery.py::test_public_cancel_stops_reconstruction_before_prompt
// The configured boundary here holds the setup RPC. The publication callback
// is covered separately when that callback is ported.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_cancel_stops_reconstruction_before_prompt() {
    for stage in [Stage::Close, Stage::Open, Stage::Configured] {
        cancelled_preparation(stage, 1).await;
    }
}

async fn cancelled_preparation(boundary: Stage, prompted: usize) {
    let (channel, state) = channel(boundary);
    let room = RoomId::new("r1").unwrap();
    let (mut stream, record) = turn(&channel, "r1");
    let reading = tokio::spawn(async move { drained(&mut stream).await });
    tokio::time::timeout(PATIENCE, state.reached.notified())
        .await
        .unwrap();
    assert_eq!(channel.active_turns(), 1);
    assert!(channel.cancel(&room).await.unwrap());
    assert_eq!(
        tokio::time::timeout(PATIENCE, reading)
            .await
            .unwrap()
            .unwrap(),
        (Vec::new(), false)
    );
    assert_eq!(state.prompts.lock().unwrap().len(), prompted);
    assert!(state.cancelled.lock().unwrap().is_empty());
    assert!(channel.session_id(&room).is_none());
    assert_eq!(
        record.to_metadata()["acp"],
        json!({"protocol_version":1,"interrupted":true})
    );
    assert_eq!(channel.active_turns(), 0);
    assert!(channel.shared.preparations.lock().unwrap().is_empty());
    let opened = state.opened.load(Ordering::SeqCst);
    let (mut next, _) = turn(&channel, "r1");
    assert!(
        !tokio::time::timeout(PATIENCE, drained(&mut next))
            .await
            .unwrap()
            .1
    );
    assert!(state.opened.load(Ordering::SeqCst) > opened);
    channel.close().await;
}

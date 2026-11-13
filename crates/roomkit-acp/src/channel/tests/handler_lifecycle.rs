//! Python: `tests/test_channels/test_acp_handler_lifecycle.py`.

mod fixture;
mod hooks;

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use roomkit::model::{HookResult, RoomContext, RoomId};
use roomkit::{
    Channel, CreateRoomOptions, HookExecution, HookRegistration, HookTrigger, RoomKit,
    RoomKitOptions, ToolCallEvent,
};
use roomkit_ai::ExternalToolHandler;
use serde_json::json;

use super::PATIENCE;
use crate::AcpChannel;
use fixture::{Direct, channel};

async fn open(agent: &AcpChannel<Direct>, room: &str) -> Result<(), crate::AcpError> {
    agent
        .set_config_option(&RoomId::new(room).unwrap(), "model", json!("opus"))
        .await
        .map(|_| ())
}

#[tokio::test]
async fn registration_and_unused_close_do_not_start_a_handler() {
    let (agent, _, service) = channel(0, false);
    assert_eq!(service.channel_id(), "");
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(agent).unwrap();
    assert_eq!(service.channel_id(), "acp-agent");
    kit.close().await.unwrap();
    assert_eq!(service.starts.load(Ordering::SeqCst), 0);
    assert_eq!(service.stops.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn reconnect_keeps_the_started_service_and_close_stops_it_once() {
    let (agent, state, service) = channel(0, false);
    open(&agent, "r1").await.unwrap();
    let first = agent.session_id(&RoomId::new("r1").unwrap());
    open(&agent, "r2").await.unwrap();
    state.alive.store(false, Ordering::SeqCst);
    open(&agent, "r1").await.unwrap();
    assert_ne!(agent.session_id(&RoomId::new("r1").unwrap()), first);
    assert_eq!(service.starts.load(Ordering::SeqCst), 1);
    agent.close().await;
    agent.close().await;
    assert_eq!(service.stops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn failed_handler_start_closes_transport_and_can_be_retried() {
    let (agent, state, service) = channel(1, false);
    let error = open(&agent, "r1").await.unwrap_err();
    assert_eq!(error.to_string(), "approval service down");
    assert!(!state.alive.load(Ordering::SeqCst));
    assert_eq!(service.stops.load(Ordering::SeqCst), 0);
    open(&agent, "r1").await.unwrap();
    assert_eq!(service.starts.load(Ordering::SeqCst), 2);
    agent.close().await;
    assert_eq!(service.stops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn failed_handler_start_is_not_stopped() {
    let (agent, _, service) = channel(1, false);
    assert!(open(&agent, "r1").await.is_err());
    agent.close().await;
    assert_eq!(service.starts.load(Ordering::SeqCst), 1);
    assert_eq!(service.stops.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn handler_stop_failure_does_not_fail_or_repeat_close() {
    let (agent, _, service) = channel(0, true);
    open(&agent, "r1").await.unwrap();
    agent.close().await;
    agent.close().await;
    assert_eq!(service.starts.load(Ordering::SeqCst), 1);
    assert_eq!(service.stops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn failed_authentication_never_starts_the_handler() {
    let (agent, state, service) = channel(0, false);
    state.authentication_fails.store(true, Ordering::SeqCst);
    assert!(open(&agent, "r1").await.is_err());
    agent.close().await;
    assert!(!state.alive.load(Ordering::SeqCst));
    assert_eq!(service.starts.load(Ordering::SeqCst), 0);
    assert_eq!(service.stops.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_permission_during_graceful_close_keeps_its_room() {
    let (agent, state, _) = channel(0, false);
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(agent.clone()).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(RoomId::new("r1").unwrap()),
        ..Default::default()
    })
    .await
    .unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&seen);
    kit.turn_executor()
        .hooks()
        .register(HookRegistration::new(
            HookTrigger::BeforeToolUse,
            HookExecution::Sync,
            move |_: ToolCallEvent, context: RoomContext| {
                observed.lock().unwrap().push(context.room.id);
                async {
                    HookResult::block("no reads", roomkit::model::hook::BlockOptions::default())
                }
            },
        ))
        .unwrap();
    open(&agent, "r1").await.unwrap();
    state.hold_close.store(true, Ordering::SeqCst);
    let closing_agent = agent.clone();
    let closing = tokio::spawn(async move {
        closing_agent.close().await;
    });
    tokio::time::timeout(PATIENCE, state.closing.notified())
        .await
        .unwrap();
    let answer = state.permission("session-1").await;
    state.resume.notify_one();
    tokio::time::timeout(PATIENCE, closing)
        .await
        .unwrap()
        .unwrap();
    kit.close().await.unwrap();
    assert_eq!(
        serde_json::to_value(answer).unwrap()["outcome"]["optionId"],
        "r"
    );
    assert_eq!(*seen.lock().unwrap(), [RoomId::new("r1").unwrap()]);
}

//! Real registered-channel publications at boundaries the shared DSL cannot hold.

mod foreign_scope;
mod idle;
mod tool_text;

use std::sync::{Arc, Mutex, atomic::Ordering};

use roomkit::model::{ChannelBinding, ChannelOutput, ChannelType, RoomId};
use roomkit::realtime::{
    EphemeralCallback, EphemeralEvent, EphemeralEventType, RealtimeBackend, RealtimeFuture,
};
use roomkit::{Channel, RoomKit, RoomKitOptions};
use serde_json::{Value, json};
use tokio::sync::Notify;

use super::recovery::fixture::{self, Stage};
use super::{PATIENCE, test_input};
use crate::AcpChannel;

/// This backend awaits its one room subscriber, so holding that subscriber
/// holds the publication itself (`InMemoryRealtime` schedules callbacks instead).
#[derive(Default)]
struct AwaitedRealtime {
    callback: Mutex<Option<EphemeralCallback>>,
    topics: Mutex<Vec<String>>,
}

impl RealtimeBackend for AwaitedRealtime {
    fn publish<'a>(
        &'a self,
        topic: &'a str,
        event: EphemeralEvent,
    ) -> RealtimeFuture<'a, Result<(), roomkit::ChannelError>> {
        self.topics.lock().unwrap().push(topic.to_owned());
        let callback = self.callback.lock().unwrap().clone();
        Box::pin(async move {
            if let Some(callback) = callback {
                callback(event).await;
            }
            Ok(())
        })
    }
    fn subscribe<'a>(
        &'a self,
        _: &'a str,
        callback: EphemeralCallback,
    ) -> RealtimeFuture<'a, String> {
        *self.callback.lock().unwrap() = Some(callback);
        Box::pin(async { "subscriber".to_owned() })
    }
    fn unsubscribe<'a>(&'a self, _: &'a str) -> RealtimeFuture<'a, bool> {
        let existed = self.callback.lock().unwrap().take().is_some();
        Box::pin(async move { existed })
    }
}

async fn direct_turn(channel: &AcpChannel<fixture::Direct>) -> ChannelOutput {
    direct_in_room(channel, test_input("r1").context.room).await
}

async fn live_room(kit: &RoomKit, channel: &AcpChannel<fixture::Direct>) -> roomkit::model::Room {
    let room = kit
        .create_room(roomkit::CreateRoomOptions {
            room_id: Some(RoomId::new("r1").unwrap()),
            ..Default::default()
        })
        .await
        .unwrap();
    kit.attach_channel(
        &room.id,
        &channel.channel_id(),
        roomkit::AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    room
}

async fn direct_in_room(
    channel: &AcpChannel<fixture::Direct>,
    room: roomkit::model::Room,
) -> ChannelOutput {
    let mut input = test_input("r1");
    input.context.room = room;
    let binding = ChannelBinding::new(
        channel.channel_id(),
        input.context.room.id.clone(),
        ChannelType::AI,
    );
    channel
        .on_event(input.trigger, binding, input.context)
        .await
        .unwrap()
}

async fn notify(state: &fixture::State, session: &str, update: Value) {
    let client = state.client.lock().unwrap().clone().unwrap();
    client
        .session_update(
            serde_json::from_value(json!({"sessionId":session,"update":update})).unwrap(),
        )
        .await;
}

// Python: test_acp_recovery.py::test_public_cancel_stops_reconstruction_before_prompt[configured]
// and test_close_stops_reconstruction_without_cancelling_the_invalid_remote_session[configured].
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cancel_and_close_while_the_configured_publication_is_awaited() {
    for close in [false, true] {
        let (channel, state) = fixture::channel(Stage::AnsweredPrompt);
        let realtime = Arc::new(AwaitedRealtime::default());
        let kit = RoomKit::new(RoomKitOptions {
            realtime: Some(Arc::<AwaitedRealtime>::clone(&realtime)),
            ..Default::default()
        });
        kit.register_channel(channel.clone()).unwrap();
        let reached = Arc::new(Notify::new());
        let seen = Arc::clone(&reached);
        let room = RoomId::new("r1").unwrap();
        realtime
            .subscribe_to_room(
                &room,
                Arc::new(move |event| {
                    let seen = Arc::clone(&seen);
                    Box::pin(async move {
                        if event.data.get("session_id") == Some(&json!("session-2")) {
                            assert_eq!(event.data["type"], "acp_config_options");
                            seen.notify_one();
                            std::future::pending::<()>().await;
                        }
                    })
                }),
            )
            .await;
        let snapshot = live_room(&kit, &channel).await;
        let running = channel.clone();
        let pending = tokio::spawn(async move { direct_in_room(&running, snapshot).await });
        tokio::time::timeout(PATIENCE, reached.notified())
            .await
            .unwrap();
        assert_eq!(state.prompts.lock().unwrap().len(), 1);
        if close {
            channel.close().await;
        } else {
            assert!(channel.cancel(&room).await.unwrap());
        }
        let output = tokio::time::timeout(PATIENCE, pending)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            output.response_metadata.get("acp").unwrap()["interrupted"],
            true
        );
        assert_eq!(state.prompts.lock().unwrap().len(), 1);
        assert!(state.cancelled.lock().unwrap().is_empty());
        assert!(channel.session_id(&room).is_none());
        assert_eq!(channel.active_turns(), 0);
        if !close {
            assert_reusable_after_cancel(&channel, &state, &room).await;
        }
        kit.close().await.unwrap();
    }
}

// Python: test_acp_publication_scope.py::test_a_subscriber_can_change_configuration_during_the_opening_announcement.
#[tokio::test]
async fn test_a_subscriber_can_change_configuration_during_the_opening_announcement() {
    let (channel, _) = fixture::channel(Stage::AnsweredPrompt);
    let realtime = Arc::new(AwaitedRealtime::default());
    let kit = RoomKit::new(RoomKitOptions {
        realtime: Some(Arc::<AwaitedRealtime>::clone(&realtime)),
        ..Default::default()
    });
    kit.register_channel(channel.clone()).unwrap();
    let armed = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let setter = channel.clone();
    let room = RoomId::new("r1").unwrap();
    realtime
        .subscribe_to_room(
            &room,
            Arc::new(move |event| {
                let setter = setter.clone();
                let armed = Arc::clone(&armed);
                Box::pin(async move {
                    if armed.swap(false, Ordering::SeqCst) {
                        assert_eq!(
                            setter
                                .set_config_option(&event.room_id, "model", json!("opus"))
                                .await
                                .unwrap()["model"],
                            "opus"
                        );
                    }
                })
            }),
        )
        .await;
    let values = tokio::time::timeout(
        PATIENCE,
        channel.set_config_option(&room, "model", json!("sonnet")),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(values["model"], "opus");
    assert_eq!(channel.session_config(&room)["model"], "opus");
    assert_eq!(
        channel.room_state(&room).unwrap()["config"]["model"],
        "opus"
    );
    kit.close().await.unwrap();
}

async fn assert_reusable_after_cancel(
    channel: &AcpChannel<fixture::Direct>,
    state: &fixture::State,
    room: &RoomId,
) {
    let output = tokio::time::timeout(PATIENCE, direct_turn(channel))
        .await
        .unwrap();
    assert_eq!(
        output.response_metadata.get("acp").unwrap()["prompt_returned"],
        true
    );
    assert_eq!(channel.session_id(room).as_deref(), Some("session-3"));
    let sessions: Vec<_> = state
        .prompts
        .lock()
        .unwrap()
        .iter()
        .map(|prompt| prompt.session_id.0.to_string())
        .collect();
    assert_eq!(sessions, ["session-1", "session-3"]);
}

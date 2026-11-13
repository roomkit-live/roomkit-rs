//! Catch-up is rebuilt under the same room lock as the rejected attempt.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use roomkit::Channel;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelType, ResponseMetadata, RoomEvent,
    TextContent, Visibility,
};
use serde_json::json;

use super::fixture::{Direct, Stage, with_options};
use crate::AcpChannelOptions;
use crate::channel::context::Input;
use crate::channel::tests::{PATIENCE, drained, poll_once, test_input};
use crate::channel::turn::{Ask, TurnStream};

fn event(index: u64, body: &str) -> RoomEvent {
    let mut event = test_input("r1").trigger;
    event.index = index;
    event.content = TextContent::new(body).into();
    event
}

fn turn(
    channel: &crate::AcpChannel<Direct>,
    trigger: RoomEvent,
    history: Vec<RoomEvent>,
) -> TurnStream<Direct> {
    let mut input: Input = test_input("r1");
    input.text = crate::text::acp_event_text(&trigger);
    input.context.recent_events = history;
    let mut binding = ChannelBinding::new(
        ChannelId::new("claude").unwrap(),
        trigger.room_id.clone(),
        ChannelType::AI,
    );
    binding.category = ChannelCategory::Intelligence;
    input.context.bindings.push(binding);
    input.trigger = trigger;
    input.blocks = vec!["Host policy".to_owned()];
    let record = ResponseMetadata::new();
    record.insert("acp", json!({"protocol_version": 1}));
    TurnStream::new(
        Arc::clone(&channel.shared),
        Ask {
            room_id: input.trigger.room_id.clone(),
            event_id: input.trigger.id.to_string(),
            input,
            record,
            host: None,
            answers_room: true,
        },
    )
}

// Python: tests/test_channels/test_acp.py::TestACPSessionRecovery::test_authorized_recovery_rebuilds_catch_up_without_interleaving
#[tokio::test]
async fn test_authorized_recovery_rebuilds_catch_up_without_interleaving() {
    let (channel, state) = with_options(
        Stage::Close,
        AcpChannelOptions {
            room_history: 1,
            ..Default::default()
        },
    );
    state.refuse_at.store(2, Ordering::SeqCst);
    let earlier = event(4, "earlier request");
    assert!(
        !drained(&mut turn(&channel, earlier.clone(), vec![earlier.clone()]))
            .await
            .1
    );
    let mut hidden = event(5, "secret");
    hidden.visibility = Visibility::Transport;
    let trigger = event(6, "retry me");
    let mut first = turn(
        &channel,
        trigger.clone(),
        vec![event(0, "outside window"), earlier, hidden, trigger.clone()],
    );
    let consuming = tokio::spawn(async move { drained(&mut first).await });
    tokio::time::timeout(PATIENCE, state.reached.notified())
        .await
        .unwrap();
    let following = event(7, "following");
    let mut second = turn(&channel, following.clone(), vec![trigger, following]);
    poll_once(&mut second).await;
    assert_eq!(state.prompts.lock().unwrap().len(), 2);
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
    let prompts = state.prompts.lock().unwrap().clone();
    let text = |index: usize| {
        serde_json::to_value(&prompts[index]).unwrap()["prompt"][0]["text"]
            .as_str()
            .unwrap()
            .to_owned()
    };
    assert_eq!(text(1), "Host policy\n\nretry me");
    assert_eq!(
        text(2),
        "Host policy\n\n[Room context — the 1 most recent of 2 messages you did not receive; the earlier ones are not shown. Context only; the request follows.]\n[1] @ws: “earlier request”\n[End of room context]\n\nretry me"
    );
    assert_eq!(text(3), "Host policy\n\nfollowing");
    assert_eq!(
        prompts
            .iter()
            .map(|request| request.session_id.0.as_ref())
            .collect::<Vec<_>>(),
        ["session-1", "session-1", "session-2", "session-2"]
    );
    assert_eq!(prompts[1].meta, prompts[2].meta);
    assert_eq!(channel.active_turns(), 0);
    channel.close().await;
}

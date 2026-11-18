//! Idle tool updates never acquire a room publication scope.

use super::*;

#[tokio::test]
async fn tool_notifications_outside_a_turn_publish_nothing() {
    for publish_tool_call_text in [false, true] {
        check_idle(publish_tool_call_text).await;
    }
}

async fn check_idle(publish_tool_call_text: bool) {
    let (channel, state) = fixture::with_options(
        Stage::AnsweredPrompt,
        crate::AcpChannelOptions {
            publish_tool_call_text,
            ..Default::default()
        },
    );
    state.refuse_at.store(usize::MAX, Ordering::SeqCst);
    let realtime = Arc::new(AwaitedRealtime::default());
    let kit = RoomKit::new(RoomKitOptions {
        realtime: Some(Arc::<AwaitedRealtime>::clone(&realtime)),
        ..Default::default()
    });
    kit.register_channel(channel.clone()).unwrap();
    let events = Arc::new(Mutex::new(Vec::<EphemeralEvent>::new()));
    let seen = Arc::clone(&events);
    realtime
        .subscribe_to_room(
            &RoomId::new("r1").unwrap(),
            Arc::new(move |event| {
                let seen = Arc::clone(&seen);
                Box::pin(async move {
                    seen.lock().unwrap().push(event);
                })
            }),
        )
        .await;
    direct_turn(&channel).await;
    events.lock().unwrap().clear();
    for session in ["session-1", "unknown"] {
        for update in [
            json!({"sessionUpdate":"tool_call","toolCallId":"read","title":"Read",
                "rawInput":{"secret":"private-input"},"status":"in_progress"}),
            json!({"sessionUpdate":"tool_call_update","toolCallId":"read","status":"in_progress",
                "rawOutput":"private-progress"}),
            json!({"sessionUpdate":"tool_call_update","toolCallId":"read","status":"completed",
                "rawOutput":"private-result"}),
        ] {
            notify(&state, session, update).await;
        }
    }
    assert!(events.lock().unwrap().is_empty());
    assert_eq!(channel.active_turns(), 0);
    kit.close().await.unwrap();
}

#[tokio::test]
async fn a_directly_wired_acp_prompt_publishes_its_thinking_and_tools_in_order() {
    for publish_text in [false, true] {
        tokio::time::timeout(PATIENCE, check_direct(publish_text))
            .await
            .unwrap();
    }
}

async fn check_direct(publish_text: bool) {
    let (channel, state) = fixture::with_options(
        Stage::Prompt,
        crate::AcpChannelOptions {
            publish_thinking_text: publish_text,
            publish_tool_call_text: publish_text,
            ..Default::default()
        },
    );
    let room = RoomId::new("r1").unwrap();
    let realtime = Arc::new(AwaitedRealtime::default());
    let kit = RoomKit::new(RoomKitOptions {
        realtime: Some(Arc::<AwaitedRealtime>::clone(&realtime)),
        ..Default::default()
    });
    let created = kit
        .create_room(roomkit::CreateRoomOptions {
            room_id: Some(room.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
    kit.register_channel(roomkit::DeclaredChannel::new(channel.declaration()).unwrap())
        .unwrap();
    let binding = kit
        .attach_channel(
            &room,
            &channel.channel_id(),
            roomkit::AttachChannelOptions::default(),
        )
        .await
        .unwrap();
    kit.turn_executor().wire(&channel).unwrap();
    let (seen, mut events) = tokio::sync::mpsc::unbounded_channel();
    realtime
        .subscribe_to_room(
            &room,
            Arc::new(move |event| {
                seen.send(event).unwrap();
                Box::pin(std::future::ready(()))
            }),
        )
        .await;
    let active = channel.clone();
    let mut input = test_input("r1");
    input.context.room = created.clone();
    let pending = tokio::spawn(async move {
        active
            .on_event(input.trigger, binding, input.context)
            .await
            .unwrap()
    });
    state.reached.notified().await;
    notify(
        &state,
        "session-2",
        json!({"sessionUpdate":"agent_thought_chunk","content":{"type":"text","text":"thinking"}}),
    )
    .await;
    notify(&state, "session-2", json!({"sessionUpdate":"tool_call","toolCallId":"t1","title":"lookup","rawInput":{"q":"value"},"status":"in_progress"})).await;
    notify(&state, "session-2", json!({"sessionUpdate":"tool_call_update","toolCallId":"t1","status":"completed","rawOutput":"done"})).await;
    state.resume.notify_one();
    let output = pending.await.unwrap();
    assert!(output.responded);
    let wanted = [
        EphemeralEventType::ThinkingStart,
        EphemeralEventType::ThinkingDelta,
        EphemeralEventType::ToolCallStart,
        EphemeralEventType::ToolCallEnd,
        EphemeralEventType::ThinkingEnd,
    ];
    let mut observed = Vec::new();
    while observed.len() < wanted.len() {
        let event = events.recv().await.unwrap();
        if !wanted.contains(&event.r#type) {
            continue;
        }
        assert_eq!(event.room_created_at, Some(created.created_at));
        assert_eq!(event.channel_id.as_ref(), Some(&channel.channel_id()));
        if event.r#type == EphemeralEventType::ThinkingDelta {
            assert_eq!(event.data.contains_key("thinking"), publish_text);
        } else if event.r#type == EphemeralEventType::ToolCallStart {
            assert_eq!(
                event.data["tool_calls"][0].get("arguments").is_some(),
                publish_text
            );
        } else if event.r#type == EphemeralEventType::ToolCallEnd {
            assert_eq!(
                event.data["tool_calls"][0].get("result").is_some(),
                publish_text
            );
        }
        observed.push(event.r#type);
    }
    assert_eq!(observed, wanted);
    channel.close().await;
    kit.close().await.unwrap();
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Audience {
    PrivateTrigger,
    Revoked,
    Missing,
    OldRoom,
}

#[tokio::test]
async fn the_direct_acp_outlet_checks_the_current_binding_for_each_publication() {
    for audience in [
        Audience::PrivateTrigger,
        Audience::Revoked,
        Audience::Missing,
        Audience::OldRoom,
    ] {
        tokio::time::timeout(PATIENCE, check_audience(audience))
            .await
            .unwrap();
    }
}

async fn check_audience(audience: Audience) {
    let (channel, state) = fixture::channel(Stage::Prompt);
    let room = RoomId::new("r1").unwrap();
    let realtime = Arc::new(AwaitedRealtime::default());
    let kit = RoomKit::new(RoomKitOptions {
        realtime: Some(Arc::<AwaitedRealtime>::clone(&realtime)),
        ..Default::default()
    });
    let created = kit
        .create_room(roomkit::CreateRoomOptions {
            room_id: Some(room.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
    kit.register_channel(roomkit::DeclaredChannel::new(channel.declaration()).unwrap())
        .unwrap();
    let binding = kit
        .attach_channel(
            &room,
            &channel.channel_id(),
            roomkit::AttachChannelOptions::default(),
        )
        .await
        .unwrap();
    if audience == Audience::Missing {
        kit.detach_channel(
            &room,
            &channel.channel_id(),
            roomkit::DetachChannelOptions::default(),
        )
        .await
        .unwrap();
    }
    kit.turn_executor().wire(&channel).unwrap();
    let (seen, mut events) = tokio::sync::mpsc::unbounded_channel();
    realtime
        .subscribe_to_room(
            &room,
            Arc::new(move |event| {
                seen.send(event).unwrap();
                Box::pin(std::future::ready(()))
            }),
        )
        .await;
    let active = channel.clone();
    let mut input = test_input("r1");
    if audience != Audience::OldRoom {
        input.context.room = created;
    }
    if audience == Audience::PrivateTrigger {
        input.trigger.response_visibility = Some(Visibility::Intelligence);
    }
    let pending = tokio::spawn(async move {
        active
            .on_event(input.trigger, binding, input.context)
            .await
            .unwrap()
    });
    state.reached.notified().await;
    notify(
        &state,
        "session-2",
        json!({"sessionUpdate":"agent_thought_chunk","content":{"type":"text","text":"thinking"}}),
    )
    .await;
    if audience == Audience::Revoked {
        while events.recv().await.unwrap().r#type != EphemeralEventType::ThinkingDelta {}
        kit.set_visibility(
            &room,
            &channel.channel_id(),
            Visibility::Intelligence,
            roomkit::SetVisibilityOptions::default(),
        )
        .await
        .unwrap();
    }
    notify(&state, "session-2", json!({"sessionUpdate":"tool_call","toolCallId":"t1","title":"lookup","status":"in_progress"})).await;
    notify(&state, "session-2", json!({"sessionUpdate":"tool_call_update","toolCallId":"t1","status":"completed","rawOutput":"done"})).await;
    state.resume.notify_one();
    assert!(pending.await.unwrap().responded);
    channel.close().await;
    kit.close().await.unwrap();
    assert!(
        events.try_recv().is_err(),
        "a private or no-longer-public prompt published room activity"
    );
}

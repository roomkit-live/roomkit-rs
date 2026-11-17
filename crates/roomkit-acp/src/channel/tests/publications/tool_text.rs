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

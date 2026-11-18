//! Known idle sessions publish without borrowing a turn.

use super::*;

// Python: test_acp.py::test_session_config_follows_the_agent and
// test_acp_backend.py::test_subscriber_reads_agent_reasoning_only_when_opted_in.
#[tokio::test]
async fn test_idle_session_publications_follow_configuration_and_thinking_opt_in() {
    for publishes_text in [false, true] {
        let (channel, state) = fixture::with_options(
            Stage::AnsweredPrompt,
            crate::AcpChannelOptions {
                publish_thinking_text: publishes_text,
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
        let snapshot = live_room(&kit, &channel).await;
        let events = Arc::new(Mutex::new(Vec::<EphemeralEvent>::new()));
        let recorded = Arc::clone(&events);
        let room = RoomId::new("r1").unwrap();
        realtime
            .subscribe_to_room(
                &room,
                Arc::new(move |event| {
                    let recorded = Arc::clone(&recorded);
                    Box::pin(async move {
                        recorded.lock().unwrap().push(event);
                    })
                }),
            )
            .await;
        direct_in_room(&channel, snapshot.clone()).await;
        let opening = events.lock().unwrap().clone();
        assert_eq!(opening.len(), 1);
        assert_eq!(opening[0].data["values"], json!({"model":"opus"}));
        assert_eq!(opening[0].data["labels"], json!({"model":"Opus"}));
        let options = json!([{"id":"model","name":"Model","type":"select","currentValue":"sonnet","options":[{"value":"sonnet","name":"Sonnet"}]}]);
        notify(
            &state,
            "session-1",
            json!({"sessionUpdate":"config_option_update","configOptions":options}),
        )
        .await;
        assert_eq!(
            channel.session_config(&room),
            serde_json::from_value(json!({"model":"sonnet"})).unwrap()
        );
        let text = "é".repeat(1100);
        notify(
            &state,
            "session-1",
            json!({"sessionUpdate":"agent_thought_chunk","content":{"type":"text","text":text}}),
        )
        .await;
        notify(
            &state,
            "session-1",
            json!({"sessionUpdate":"usage_update","used":0,"size":100}),
        )
        .await;
        let read = events.lock().unwrap().clone();
        assert_eq!(read.len(), 4);
        assert_eq!(read[1].data["labels"], json!({"model":"Sonnet"}));
        assert_eq!(read[1].data["config_options"], options);
        assert_eq!(read[2].r#type, EphemeralEventType::ThinkingDelta);
        assert_eq!(read[2].data["thinking_length"], 1100);
        assert_eq!(
            read[2].data.get("thinking"),
            publishes_text.then(|| json!("é".repeat(1000))).as_ref()
        );
        assert_eq!(
            read[3].data["usage_metadata"]["usage_report"]["model_at_observation"],
            "sonnet"
        );
        assert!(
            read.iter()
                .all(|e| e.room_id == room && e.channel_id.as_ref() == Some(&channel.channel_id()))
        );
        // Neither an unknown session nor a forgotten one may reach the raw fallback.
        let plan = json!({"sessionUpdate":"plan","entries":[]});
        notify(&state, "unknown", plan.clone()).await;
        assert_eq!(events.lock().unwrap().len(), 4);
        assert!(channel.close_session(&room).await);
        notify(&state, "session-1", plan.clone()).await;
        assert_eq!(events.lock().unwrap().len(), 4);
        direct_in_room(&channel, snapshot).await;
        kit.close().await.unwrap();
        let before = events.lock().unwrap().len();
        notify(&state, "session-2", plan).await;
        assert_eq!(events.lock().unwrap().len(), before);
        let weak = kit.downgrade();
        drop(kit);
        assert!(weak.upgrade().is_none());
    }
}

// Python: test_acp_publication_scope.py::test_an_idle_callback_does_not_borrow_its_transports_record.
#[tokio::test]
async fn test_an_idle_callback_does_not_borrow_its_transports_record() {
    let (channel, state) = fixture::channel(Stage::AnsweredPrompt);
    let realtime = Arc::new(AwaitedRealtime::default());
    let kit = RoomKit::new(RoomKitOptions {
        realtime: Some(Arc::<AwaitedRealtime>::clone(&realtime)),
        ..Default::default()
    });
    kit.register_channel(channel.clone()).unwrap();
    let records = Arc::new(Mutex::new(Vec::<bool>::new()));
    let recorded = Arc::clone(&records);
    let room = RoomId::new("r1").unwrap();
    realtime
        .subscribe_to_room(
            &room,
            Arc::new(move |_| {
                let recorded = Arc::clone(&recorded);
                Box::pin(async move {
                    recorded
                        .lock()
                        .unwrap()
                        .push(roomkit_ai::current_response_metadata().is_some());
                })
            }),
        )
        .await;
    channel
        .set_config_option(&room, "model", json!("sonnet"))
        .await
        .unwrap();
    records.lock().unwrap().clear();
    roomkit::TurnRecord::default()
        .scope(async {
            assert!(roomkit_ai::current_response_metadata().is_some());
            notify(
                &state,
                "session-1",
                json!({"sessionUpdate":"usage_update","used":0,"size":100}),
            )
            .await;
            assert!(roomkit_ai::current_response_metadata().is_some());
        })
        .await;
    assert_eq!(*records.lock().unwrap(), [false]);
    kit.close().await.unwrap();
}

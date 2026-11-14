//! Python: `test_acp_usage.py::test_interrupted_usage_remains_observable_without_response`.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_interrupted_usage_remains_observable_without_response() {
    for abandon in [false, true] {
        tokio::time::timeout(PATIENCE, check(abandon))
            .await
            .unwrap();
    }
}

async fn check(abandon: bool) {
    let (channel, mut agents, _) = channel();
    let kit = if abandon {
        room_with_transport(channel, abandoned::Abandon::new(TurnRecord::default())).await
    } else {
        room(channel).await
    };
    let mut observed = observe(&kit);
    let (sent, mut notices) = mpsc::unbounded_channel();
    kit.subscribe_room(
        &RoomId::new("r1").unwrap(),
        move |event| {
            let sent = sent.clone();
            async move {
                if event.data.get("type") == Some(&json!("acp_usage")) {
                    sent.send(event).unwrap();
                }
            }
        },
        roomkit::SubscribeRoomOptions::default(),
    )
    .await
    .unwrap();
    let parent = TurnRecord::default();
    parent.current().insert("parent", json!(true));
    let running = tokio::spawn(parent.scope(send(Arc::clone(&kit))));
    let mut agent = Agent::new(agents.recv().await.unwrap());
    let prompt = agent.prompted(true).await;
    let usage = json!({"jsonrpc":"2.0","method":"session/update","params":{
        "sessionId":"session-r1", "update":{"sessionUpdate":"usage_update","used":0,"size":100}
    }});
    agent
        .writer
        .write_all(format!("{usage}\n").as_bytes())
        .await
        .unwrap();
    // The subscriber receives the update while the prompt is still unresolved.
    let notice = notices.recv().await.unwrap();
    if abandon {
        let text = json!({"jsonrpc":"2.0","method":"session/update","params":{
            "sessionId":"session-r1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"partial"}}
        }});
        agent
            .writer
            .write_all(format!("{text}\n").as_bytes())
            .await
            .unwrap();
    } else {
        let failure = json!({"jsonrpc":"2.0","id":prompt["id"],"error":{"code":-32000,"message":"lost agent"}});
        agent
            .writer
            .write_all(format!("{failure}\n").as_bytes())
            .await
            .unwrap();
    }
    let result = running.await.unwrap();
    if !abandon {
        assert!(result.error.is_some());
    }
    assert_eq!(notice.data["usage_metadata"]["session_id"], "session-r1");
    assert!(
        notice.data["usage_metadata"]["usage_report"]["update"]
            .get("cost")
            .is_none()
    );
    close(&kit, &mut agent).await;
    assert!(observed.try_recv().is_err());
    assert!(notices.try_recv().is_err());
}

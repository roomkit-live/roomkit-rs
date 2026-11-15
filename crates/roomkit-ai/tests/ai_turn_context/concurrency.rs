use super::lifetime::pending;
use super::support::{binding, snapshot};
use roomkit_ai::{AiChannel, AiChannelOptions, ToolHandler, ToolResult};
use std::sync::Arc;

// Python: TestCurrentToolRoomId::test_interleaved_streams_keep_tool_and_hook_room
// Python: TestLoopContextRestoration::test_a_stream_drained_in_another_task_restores_that_task_context
// Outside a kit Rust collects the stream into ChannelOutput; poll the real
// collecting future, then move it to a consumer with its own tool context.
#[tokio::test]
async fn interleaved_turns_restore_each_consumer() {
    use roomkit::Channel;
    use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
    let gates: Arc<std::collections::HashMap<String, Arc<tokio::sync::Notify>>> = Arc::new(
        ["r1", "r2"]
            .into_iter()
            .map(|room| (room.into(), Arc::default()))
            .collect(),
    );
    let handlers = Arc::clone(&gates);
    let agent = Arc::new(
        AiChannel::new(
            "ai1".parse().unwrap(),
            ScriptedProvider::new([
                ScriptedRound::calling("", [("a", "check", "{}")]),
                ScriptedRound::calling("", [("b", "check", "{}")]),
                ScriptedRound::answering("done"),
                ScriptedRound::answering("done"),
            ]),
            AiChannelOptions {
                tool_handler: Some(ToolHandler::new(move |_, _| {
                    let gates = Arc::clone(&handlers);
                    async move {
                        let before = snapshot("before suspension");
                        let room = before.room_id.clone().unwrap();
                        gates.get(&room).unwrap().notified().await;
                        let after = snapshot("after suspension");
                        after.check(&room, Some(&room), 3);
                        assert!(Arc::ptr_eq(
                            before.room.as_ref().unwrap(),
                            after.room.as_ref().unwrap()
                        ));
                        Ok(ToolResult::Text("ok".into()))
                    }
                })),
                ..AiChannelOptions::default()
            },
        )
        .unwrap(),
    );
    let start = |room: &'static str| {
        let agent = Arc::clone(&agent);
        Box::pin(async move {
            let (event, context) = super::support::event(room, Some(room));
            agent.on_event(event, binding(room), context).await.unwrap()
        })
    };
    let (mut first, mut second) = (start("r1"), start("r2"));
    pending(first.as_mut()).await;
    pending(second.as_mut()).await;
    assert!(roomkit_ai::current_tool_room_id().is_none());
    gates.get("r1").unwrap().notify_one();
    gates.get("r2").unwrap().notify_one();
    let first = tokio::spawn(async move {
        roomkit_ai::tool_turn_context(super::parent(), async {
            first.await;
            snapshot("consumer").check("outer", Some("parent"), 1);
        })
        .await
        .unwrap();
    });
    second.await;
    first.await.unwrap();
    assert!(roomkit_ai::current_tool_room_id().is_none());
}

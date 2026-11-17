//! A declared verb sees the id of the turn that called it, including on its owner.

use super::*;

#[tokio::test]
async fn a_call_and_its_owner_read_the_same_id_without_lending_the_host() {
    assert!(roomkit::current_turn_id().is_none());
    for streaming in [false, true] {
        let seen = Arc::new(Mutex::new(None));
        let captured = Arc::clone(&seen);
        let handler = ToolHandler::new(move |_, _| {
            let captured = Arc::clone(&captured);
            async move {
                let host = current_turn_host().unwrap();
                let executor = roomkit::current_turn_id().unwrap();
                assert_eq!(executor, host.turn_id());
                let owner = host.call("app.id", None).await.unwrap();
                *captured.lock().unwrap() = Some((executor, owner));
                Ok(ToolResult::Text("handled".into()))
            }
        });
        let kit = kit(streaming, handler).await;
        kit.declare_host_verb(
            "app.id",
            DeclaredVerb::new(|_arguments| async {
                assert!(current_turn_host().is_none());
                Ok(json!(roomkit::turns::current_turn_id()))
            }),
            DeclareHostVerbOptions::default(),
        )
        .unwrap();
        kit.process_inbound(
            InboundMessage::new("web".parse().unwrap(), "alice", TextContent::new("go")),
            ProcessInboundOptions {
                room_id: Some("r".parse().unwrap()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let (executor, owner) = seen.lock().unwrap().take().unwrap();
        assert_eq!(owner.as_str(), Some(executor.as_str()));
        assert!(roomkit::current_turn_id().is_none());
        kit.close().await.unwrap();
    }
}

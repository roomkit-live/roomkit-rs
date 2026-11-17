use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit::model::Metadata;
use roomkit_ai::{
    AiChannelOptions, AiResponse, AiTool, AiToolCall, HumanInputToolHandler,
    HumanInputToolHandlerOptions, MockAiProvider, MockAiProviderOptions, ToolHandler, ToolResult,
};

use super::support::Door;

// Python: tests/test_channels/test_ai_extra_tools_human.py::test_renaming_a_host_definition_follows_human_interception
#[tokio::test]
async fn renaming_a_host_definition_follows_human_interception() {
    for agent in [false, true] {
        for streaming in [false, true] {
            for to_human in [false, true] {
                check_interception(agent, streaming, to_human).await;
            }
        }
    }
}

async fn check_interception(agent: bool, streaming: bool, to_human: bool) {
    let called = Arc::new(Mutex::new(Vec::new()));
    let calls = Arc::clone(&called);
    let handler = ToolHandler::new(move |name, _| {
        calls.lock().unwrap().push(name);
        async { Ok(ToolResult::Text("host answer".into())) }
    });
    let human =
        HumanInputToolHandler::new(["ask"], HumanInputToolHandlerOptions::default()).unwrap();
    let (before, after) = if to_human {
        ("read", "ask")
    } else {
        ("ask", "read")
    };
    let model = MockAiProvider::new(MockAiProviderOptions {
        streaming,
        ai_responses: Some(vec![
            AiResponse {
                tool_calls: vec![AiToolCall::new("c1", after)],
                ..AiResponse::new("")
            },
            AiResponse::new("done"),
        ]),
        ..Default::default()
    });
    let door = Door::new(
        agent,
        model.clone(),
        AiChannelOptions {
            tools: vec![AiTool::new(before, "host definition").unwrap().into()],
            tool_handler: Some(handler),
            human_input_handler: Some(human.clone()),
            tool_search: Some(false),
            ..Default::default()
        },
    );
    let handle = door.tools().remove(0);
    let mut definition = handle.get();
    definition.name = after.into();
    handle.replace(definition);
    let answer = async {
        if !to_human {
            return;
        }
        loop {
            if let Some(pending) = human.handler().pending().values().next() {
                assert_eq!(pending.tool_call_id, "c1");
                assert!(human.handler().resolve(&pending.pending_id, "human answer"));
                return;
            }
            tokio::task::yield_now().await;
        }
    };
    let completed = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(door.turn(Metadata::new()), answer)
    })
    .await;
    door.close().await;
    completed.unwrap();
    let requests = model.calls();
    let results: Vec<_> = requests
        .get(1)
        .unwrap()
        .messages
        .iter()
        .flat_map(|m| m.tool_results())
        .collect();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results.first().unwrap().as_text(),
        if to_human {
            "human answer"
        } else {
            "host answer"
        }
    );
    assert_eq!(
        *called.lock().unwrap(),
        if to_human {
            vec![]
        } else {
            vec!["read".to_owned()]
        }
    );
}
